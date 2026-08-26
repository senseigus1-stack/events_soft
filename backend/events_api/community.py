from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session, selectinload

from .models import (
    Attendance,
    City,
    DiscussionPost,
    DiscussionSpace,
    Event,
    Friendship,
    Notification,
    User,
)
from .schemas import EventSubmissionWrite, ProfileWrite
from .service import ensure_user, record_interaction


def update_profile(session: Session, user_id: str, payload: ProfileWrite) -> User:
    user = ensure_user(session, user_id)
    if payload.city_slug and session.get(City, payload.city_slug) is None:
        raise LookupError("city_not_found")
    user.display_name = payload.display_name.strip()
    user.city_slug = payload.city_slug
    user.avatar_url = payload.avatar_url.strip()
    session.commit()
    session.refresh(user)
    return user


def set_attendance(
    session: Session,
    user_id: str,
    event_id: int,
    *,
    attendance_status: str,
    reminder_minutes_before: int,
) -> Attendance:
    ensure_user(session, user_id)
    event = session.get(Event, event_id)
    if event is None or event.status != "published":
        raise LookupError("event_not_found")
    attendance = session.scalar(
        select(Attendance).where(
            Attendance.user_id == user_id, Attendance.event_id == event_id
        )
    )
    if attendance is None:
        attendance = Attendance(user_id=user_id, event_id=event_id)
        session.add(attendance)
    attendance.status = attendance_status
    attendance.reminder_at = (
        event.starts_at - timedelta(minutes=reminder_minutes_before)
        if attendance_status == "going" and reminder_minutes_before > 0
        else None
    )
    session.commit()
    session.refresh(attendance)
    if attendance_status == "going":
        record_interaction(session, user_id, event_id, "attend")
    return attendance


def list_attendance(session: Session, user_id: str) -> list[Attendance]:
    return list(
        session.scalars(
            select(Attendance)
            .options(selectinload(Attendance.event))
            .join(Attendance.event)
            .where(
                Attendance.user_id == user_id,
                Attendance.status == "going",
                Event.status == "published",
                Event.starts_at >= datetime.now(timezone.utc),
            )
            .order_by(Event.starts_at)
        )
    )


def accepted_friend_ids(session: Session, user_id: str) -> list[str]:
    friendships = list(
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
    return [
        item.addressee_id if item.requester_id == user_id else item.requester_id
        for item in friendships
    ]


def request_friendship(session: Session, user_id: str, friend_user_id: str) -> Friendship:
    if user_id == friend_user_id:
        raise ValueError("cannot_friend_self")
    ensure_user(session, user_id)
    if session.get(User, friend_user_id) is None:
        raise LookupError("user_not_found")
    existing = session.scalar(
        select(Friendship).where(
            or_(
                and_(
                    Friendship.requester_id == user_id,
                    Friendship.addressee_id == friend_user_id,
                ),
                and_(
                    Friendship.requester_id == friend_user_id,
                    Friendship.addressee_id == user_id,
                ),
            )
        )
    )
    if existing:
        return existing
    friendship = Friendship(requester_id=user_id, addressee_id=friend_user_id)
    session.add(friendship)
    session.commit()
    session.refresh(friendship)
    return friendship


def accept_friendship(session: Session, user_id: str, friendship_id: int) -> Friendship:
    friendship = session.get(Friendship, friendship_id)
    if friendship is None or friendship.addressee_id != user_id:
        raise LookupError("friendship_not_found")
    friendship.status = "accepted"
    friendship.accepted_at = datetime.now(timezone.utc)
    session.commit()
    session.refresh(friendship)
    return friendship


def list_friendships(session: Session, user_id: str) -> list[tuple[Friendship, User, list[Event]]]:
    friendships = list(
        session.scalars(
            select(Friendship)
            .where(
                Friendship.status.in_(["pending", "accepted"]),
                or_(
                    Friendship.requester_id == user_id,
                    Friendship.addressee_id == user_id,
                ),
            )
            .order_by(Friendship.created_at.desc())
        )
    )
    result: list[tuple[Friendship, User, list[Event]]] = []
    now = datetime.now(timezone.utc)
    for friendship in friendships:
        friend_id = (
            friendship.addressee_id
            if friendship.requester_id == user_id
            else friendship.requester_id
        )
        friend = session.get(User, friend_id)
        if friend is None:
            continue
        shared_events = list(
            session.scalars(
                select(Event)
                .join(Attendance, Attendance.event_id == Event.id)
                .where(
                    Attendance.user_id == friend_id,
                    Attendance.status == "going",
                    Event.status == "published",
                    Event.starts_at >= now,
                )
                .order_by(Event.starts_at)
                .limit(3)
            )
        ) if friendship.status == "accepted" else []
        result.append((friendship, friend, shared_events))
    return result


def submit_event(
    session: Session, user_id: str, payload: EventSubmissionWrite
) -> Event:
    ensure_user(session, user_id)
    if session.get(City, payload.city_slug) is None:
        raise LookupError("city_not_found")
    starts_at = payload.starts_at
    if starts_at.tzinfo is None:
        starts_at = starts_at.replace(tzinfo=timezone.utc)
    if starts_at <= datetime.now(timezone.utc):
        raise ValueError("event_must_be_in_future")
    event = Event(
        source="community",
        source_id=f"submission-{uuid4()}",
        submitted_by_user_id=user_id,
        status="pending",
        city_slug=payload.city_slug,
        title=payload.title.strip(),
        description=payload.description.strip(),
        category=payload.category.strip(),
        tags=list(dict.fromkeys(tag.strip() for tag in payload.tags if tag.strip())),
        starts_at=starts_at,
        ends_at=payload.ends_at,
        venue_name=payload.venue_name.strip(),
        address=payload.address.strip(),
        image_url=payload.image_url.strip(),
        event_url=payload.event_url.strip(),
        price_text=payload.price_text.strip(),
        is_free=payload.is_free,
        age_min=payload.age_min,
        popularity=0,
    )
    session.add(event)
    session.commit()
    session.refresh(event)
    return event


def moderate_event(
    session: Session, event_id: int, *, decision: str, note: str
) -> Event:
    event = session.get(Event, event_id)
    if event is None or event.source != "community":
        raise LookupError("submission_not_found")
    event.status = "published" if decision == "approved" else "rejected"
    event.moderation_note = note.strip()
    event.reviewed_at = datetime.now(timezone.utc)
    session.add(
        Notification(
            user_id=event.submitted_by_user_id,
            event_id=event.id,
            kind="submission_review",
            title="Событие одобрено" if decision == "approved" else "Нужны изменения",
            body=note.strip() or (
                "Событие появилось в общей ленте."
                if decision == "approved"
                else "Проверьте требования к публикации."
            ),
        )
    ) if event.submitted_by_user_id else None
    session.commit()
    session.refresh(event)
    return event


def list_submissions(session: Session, status: str = "pending") -> list[Event]:
    return list(
        session.scalars(
            select(Event)
            .where(Event.source == "community", Event.status == status)
            .order_by(Event.created_at)
        )
    )


def generate_due_notifications(
    session: Session, now: datetime | None = None
) -> int:
    now = now or datetime.now(timezone.utc)
    due = list(
        session.scalars(
            select(Attendance)
            .options(selectinload(Attendance.event))
            .join(Attendance.event)
            .where(
                Attendance.status == "going",
                Attendance.reminder_at.is_not(None),
                Attendance.reminder_at <= now,
                Event.starts_at > now,
                ~select(Notification.id)
                .where(
                    Notification.user_id == Attendance.user_id,
                    Notification.event_id == Attendance.event_id,
                    Notification.kind == "event_reminder",
                )
                .exists(),
            )
        )
    )
    for attendance in due:
        session.add(
            Notification(
                user_id=attendance.user_id,
                event_id=attendance.event_id,
                kind="event_reminder",
                title=f"Скоро: {attendance.event.title}",
                body=f"{attendance.event.venue_name} · не забудьте проверить время и билеты.",
            )
        )
    session.commit()
    return len(due)


def list_notifications(session: Session, user_id: str) -> list[Notification]:
    return list(
        session.scalars(
            select(Notification)
            .where(Notification.user_id == user_id)
            .order_by(Notification.created_at.desc())
            .limit(50)
        )
    )


def mark_notification_read(session: Session, user_id: str, notification_id: int) -> None:
    notification = session.get(Notification, notification_id)
    if notification is None or notification.user_id != user_id:
        raise LookupError("notification_not_found")
    notification.read_at = datetime.now(timezone.utc)
    session.commit()


def create_discussion_space(
    session: Session,
    user_id: str,
    *,
    kind: str,
    title: str,
    city_slug: str | None,
    event_id: int | None,
    venue_name: str,
) -> DiscussionSpace:
    ensure_user(session, user_id)
    if city_slug and session.get(City, city_slug) is None:
        raise LookupError("city_not_found")
    if event_id and session.get(Event, event_id) is None:
        raise LookupError("event_not_found")
    space = DiscussionSpace(
        kind=kind,
        title=title.strip(),
        city_slug=city_slug,
        event_id=event_id,
        venue_name=venue_name.strip(),
        created_by_user_id=user_id,
    )
    session.add(space)
    session.commit()
    session.refresh(space)
    return space


def list_discussion_spaces(
    session: Session, city_slug: str | None = None
) -> list[tuple[DiscussionSpace, int]]:
    query = (
        select(DiscussionSpace, func.count(DiscussionPost.id))
        .outerjoin(
            DiscussionPost,
            and_(
                DiscussionPost.space_id == DiscussionSpace.id,
                DiscussionPost.status == "published",
            ),
        )
        .where(DiscussionSpace.status == "active")
        .group_by(DiscussionSpace.id)
        .order_by(DiscussionSpace.created_at.desc())
    )
    if city_slug:
        query = query.where(DiscussionSpace.city_slug == city_slug)
    return [(space, count) for space, count in session.execute(query)]


def add_discussion_post(
    session: Session, user_id: str, space_id: int, body: str
) -> DiscussionPost:
    ensure_user(session, user_id)
    space = session.get(DiscussionSpace, space_id)
    if space is None or space.status != "active":
        raise LookupError("discussion_not_found")
    post = DiscussionPost(space_id=space_id, user_id=user_id, body=body.strip())
    session.add(post)
    session.commit()
    session.refresh(post)
    return post


def list_discussion_posts(
    session: Session, space_id: int, limit: int = 100
) -> list[tuple[DiscussionPost, str]]:
    rows = session.execute(
        select(DiscussionPost, User.display_name)
        .join(User, User.id == DiscussionPost.user_id)
        .where(
            DiscussionPost.space_id == space_id,
            DiscussionPost.status == "published",
        )
        .order_by(DiscussionPost.created_at)
        .limit(limit)
    )
    return [(post, display_name or "Участник") for post, display_name in rows]
