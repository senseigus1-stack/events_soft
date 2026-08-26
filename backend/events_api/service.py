from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, selectinload

from .models import Attendance, City, Event, Friendship, Interaction, User, UserInterest
from .recommender import Candidate, HistoryItem, rank_candidates


def ensure_user(session: Session, user_id: str) -> User:
    user = session.get(User, user_id)
    if user is None:
        user = User(id=user_id)
        session.add(user)
        session.flush()
    return user


def list_events(
    session: Session,
    *,
    city: str | None,
    category: str | None,
    query: str | None,
    free_only: bool,
    starts_after: datetime,
    starts_before: datetime | None,
    limit: int,
    offset: int,
) -> tuple[list[Event], int]:
    filters = [Event.status == "published", Event.starts_at >= starts_after]
    if city:
        filters.append(Event.city_slug == city)
    if category:
        filters.append(Event.category == category)
    if query:
        pattern = f"%{query.strip()}%"
        filters.append(or_(Event.title.ilike(pattern), Event.description.ilike(pattern)))
    if free_only:
        filters.append(Event.is_free.is_(True))
    if starts_before:
        filters.append(Event.starts_at <= starts_before)

    total = session.scalar(select(func.count()).select_from(Event).where(*filters)) or 0
    items = list(
        session.scalars(
            select(Event)
            .where(*filters)
            .order_by(Event.starts_at, Event.popularity.desc())
            .limit(limit)
            .offset(offset)
        )
    )
    return items, total


def recommendations(
    session: Session,
    user_id: str,
    *,
    city: str | None,
    limit: int,
    now: datetime | None = None,
) -> list[tuple[Event, float, list[str]]]:
    now = now or datetime.now(timezone.utc)
    user = session.scalar(
        select(User).options(selectinload(User.interests)).where(User.id == user_id)
    )
    explicit = [interest.tag for interest in user.interests] if user else []
    selected_city = city or (user.city_slug if user else None)

    filters = [Event.status == "published", Event.starts_at >= now]
    if selected_city:
        filters.append(Event.city_slug == selected_city)
    events = list(
        session.scalars(
            select(Event).where(*filters).order_by(Event.starts_at).limit(500)
        )
    )
    interactions = list(
        session.scalars(
            select(Interaction)
            .options(selectinload(Interaction.event))
            .where(Interaction.user_id == user_id)
            .order_by(Interaction.created_at.desc())
            .limit(200)
        )
    )
    excluded_event_ids = {
        item.event_id for item in interactions if item.action in {"dismiss", "attend"}
    }
    friend_rows = list(
        session.scalars(
            select(Friendship).where(
                Friendship.status == "accepted",
                or_(
                    Friendship.requester_id == user_id,
                    Friendship.addressee_id == user_id,
                ),
            )
        )
    )
    friend_ids = [
        item.addressee_id if item.requester_id == user_id else item.requester_id
        for item in friend_rows
    ]
    friend_counts: dict[int, int] = {}
    if friend_ids:
        friend_counts = {
            event_id: count
            for event_id, count in session.execute(
                select(Attendance.event_id, func.count(Attendance.id))
                .where(
                    Attendance.user_id.in_(friend_ids),
                    Attendance.status == "going",
                )
                .group_by(Attendance.event_id)
            )
        }
    history = [
        HistoryItem(
            action=item.action,
            occurred_at=item.created_at,
            category=item.event.category,
            tags=tuple(item.event.tags),
        )
        for item in interactions
    ]
    candidates = [
        Candidate(
            id=event.id,
            category=event.category,
            tags=tuple(event.tags),
            starts_at=event.starts_at,
            popularity=event.popularity,
            is_free=event.is_free,
            venue_name=event.venue_name,
            friend_going_count=friend_counts.get(event.id, 0),
            source=event.source,
            created_at=event.created_at,
        )
        for event in events
        if event.id not in excluded_event_ids
    ]
    ranked = rank_candidates(candidates, explicit, history, now=now, limit=limit)
    by_id = {event.id: event for event in events}
    return [(by_id[item.candidate.id], item.score, item.reasons) for item in ranked]


def replace_interests(session: Session, user_id: str, tags: list[str]) -> User:
    user = ensure_user(session, user_id)
    normalized = list(dict.fromkeys(tag.strip() for tag in tags if tag.strip()))
    session.query(UserInterest).filter(UserInterest.user_id == user_id).delete()
    session.add_all(UserInterest(user_id=user_id, tag=tag) for tag in normalized)
    session.commit()
    session.refresh(user)
    return user


def record_interaction(
    session: Session, user_id: str, event_id: int, action: str
) -> Interaction:
    ensure_user(session, user_id)
    if session.get(Event, event_id) is None:
        raise LookupError("event_not_found")
    interaction = Interaction(user_id=user_id, event_id=event_id, action=action)
    session.add(interaction)
    session.commit()
    session.refresh(interaction)
    return interaction


def upsert_city(session: Session, values: dict) -> City:
    city = session.get(City, values["slug"])
    if city is None:
        city = City(**values)
        session.add(city)
    else:
        for key, value in values.items():
            setattr(city, key, value)
    return city


def upsert_event(session: Session, values: dict) -> Event:
    event = session.scalar(
        select(Event).where(
            Event.source == values["source"], Event.source_id == values["source_id"]
        )
    )
    if event is None:
        event = Event(**values)
        session.add(event)
    else:
        for key, value in values.items():
            setattr(event, key, value)
    return event
