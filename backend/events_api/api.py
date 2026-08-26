from datetime import datetime, timedelta, timezone
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from fastapi.responses import RedirectResponse
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from .config import Settings, get_settings
from .database import get_session
from .community import (
    accept_friendship,
    add_discussion_post,
    create_discussion_space,
    list_attendance,
    list_discussion_posts,
    list_discussion_spaces,
    list_friendships,
    list_notifications,
    list_submissions,
    mark_notification_read,
    moderate_event,
    request_friendship,
    set_attendance,
    submit_event,
    update_profile,
)
from .security import issue_user_token, require_user, signing_settings
from .models import City, Event, User
from .oauth import ProviderId, configured_providers, finish_oauth, start_oauth
from .schemas import (
    AuthSessionRead,
    AttendanceRead,
    AttendanceWrite,
    CityRead,
    DiscussionPostRead,
    DiscussionPostWrite,
    DiscussionSpaceRead,
    DiscussionSpaceWrite,
    EventList,
    EventRead,
    EventSubmissionWrite,
    FriendRequestWrite,
    FriendshipRead,
    InteractionWrite,
    InterestWrite,
    ModerationWrite,
    NotificationRead,
    OAuthProviderRead,
    OAuthStartRead,
    OAuthStartWrite,
    ProfileRead,
    ProfileWrite,
    RecommendationList,
    RecommendationRead,
)
from .service import ensure_user, list_events, recommendations, record_interaction, replace_interests

router = APIRouter()


@router.get("/healthz", tags=["system"])
def health(session: Session = Depends(get_session)) -> dict[str, str]:
    session.execute(text("SELECT 1"))
    return {"status": "ok"}


@router.post("/api/v1/auth/session", response_model=AuthSessionRead, tags=["people"])
def create_session(
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> AuthSessionRead:
    if not settings.app_secret and settings.environment == "production":
        raise HTTPException(status_code=503, detail="auth_not_configured")
    user_id = str(uuid4())
    ensure_user(session, user_id)
    session.commit()
    return AuthSessionRead(
        user_id=user_id,
        token=issue_user_token(user_id, signing_settings(settings)),
    )


@router.get(
    "/api/v1/auth/providers",
    response_model=list[OAuthProviderRead],
    tags=["people"],
)
def auth_providers(settings: Settings = Depends(get_settings)) -> list[dict[str, str | bool]]:
    return configured_providers(settings)


@router.post(
    "/api/v1/auth/oauth/{provider_id}/start",
    response_model=OAuthStartRead,
    tags=["people"],
)
def begin_oauth(
    provider_id: ProviderId,
    payload: OAuthStartWrite,
    authorization: str = Header(default=""),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> OAuthStartRead:
    return OAuthStartRead(
        authorization_url=start_oauth(
            session,
            provider_id,
            settings,
            user_id=payload.user_id,
            authorization=authorization,
        )
    )


@router.get("/api/v1/auth/oauth/{provider_id}/callback", tags=["people"])
async def oauth_callback(
    provider_id: ProviderId,
    code: str = Query(min_length=1, max_length=2048),
    state: str = Query(min_length=20, max_length=256),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> RedirectResponse:
    location = await finish_oauth(
        session, provider_id, settings, code=code, state=state
    )
    return RedirectResponse(location, status_code=status.HTTP_303_SEE_OTHER)


@router.get("/api/v1/cities", response_model=list[CityRead], tags=["events"])
def cities(session: Session = Depends(get_session)) -> list[City]:
    return list(session.scalars(select(City).where(City.active.is_(True)).order_by(City.name)))


@router.get("/api/v1/events", response_model=EventList, tags=["events"])
def events(
    city: str | None = None,
    category: str | None = None,
    q: str | None = Query(default=None, max_length=120),
    free_only: bool = False,
    starts_after: datetime | None = None,
    starts_before: datetime | None = None,
    limit: int = Query(default=24, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
) -> EventList:
    items, total = list_events(
        session,
        city=city,
        category=category,
        query=q,
        free_only=free_only,
        starts_after=starts_after or datetime.now(timezone.utc),
        starts_before=starts_before,
        limit=limit,
        offset=offset,
    )
    return EventList(items=items, total=total, limit=limit, offset=offset)


@router.get("/api/v1/events/{event_id}", response_model=EventRead, tags=["events"])
def event(event_id: int, session: Session = Depends(get_session)) -> Event:
    item = session.get(Event, event_id)
    if item is None or item.status != "published":
        raise HTTPException(status_code=404, detail="event_not_found")
    return item


@router.get("/api/v1/users/{user_id}/profile", response_model=ProfileRead, tags=["people"], dependencies=[Depends(require_user)])
def profile(user_id: str, session: Session = Depends(get_session)) -> User:
    user = ensure_user(session, user_id)
    session.commit()
    session.refresh(user)
    return user


@router.put("/api/v1/users/{user_id}/profile", response_model=ProfileRead, tags=["people"], dependencies=[Depends(require_user)])
def save_profile(
    user_id: str, payload: ProfileWrite, session: Session = Depends(get_session)
) -> User:
    try:
        return update_profile(session, user_id, payload)
    except LookupError:
        raise HTTPException(status_code=404, detail="city_not_found") from None


@router.put("/api/v1/users/{user_id}/interests", status_code=204, tags=["personalization"], dependencies=[Depends(require_user)])
def interests(
    user_id: str,
    payload: InterestWrite,
    session: Session = Depends(get_session),
) -> None:
    replace_interests(session, user_id, payload.tags)


@router.post(
    "/api/v1/users/{user_id}/interactions",
    status_code=status.HTTP_201_CREATED,
    tags=["personalization"],
    dependencies=[Depends(require_user)],
)
def interaction(
    user_id: str,
    payload: InteractionWrite,
    session: Session = Depends(get_session),
) -> dict[str, str]:
    try:
        record_interaction(session, user_id, payload.event_id, payload.action)
    except LookupError:
        raise HTTPException(status_code=404, detail="event_not_found") from None
    return {"status": "recorded"}


@router.get(
    "/api/v1/users/{user_id}/recommendations",
    response_model=RecommendationList,
    tags=["personalization"],
    dependencies=[Depends(require_user)],
)
def recommended(
    user_id: str,
    city: str | None = None,
    limit: int = Query(default=12, ge=1, le=50),
    session: Session = Depends(get_session),
) -> RecommendationList:
    items = recommendations(session, user_id, city=city, limit=limit)
    return RecommendationList(
        items=[
            RecommendationRead(
                **EventRead.model_validate(event).model_dump(),
                score=score,
                reasons=reasons,
            )
            for event, score, reasons in items
        ]
    )


@router.put(
    "/api/v1/users/{user_id}/plans/{event_id}",
    response_model=AttendanceRead,
    tags=["plans"],
    dependencies=[Depends(require_user)],
)
def plan_event(
    user_id: str,
    event_id: int,
    payload: AttendanceWrite,
    session: Session = Depends(get_session),
) -> AttendanceRead:
    try:
        item = set_attendance(
            session,
            user_id,
            event_id,
            attendance_status=payload.status,
            reminder_minutes_before=payload.reminder_minutes_before,
        )
    except LookupError:
        raise HTTPException(status_code=404, detail="event_not_found") from None
    return AttendanceRead(
        id=item.id,
        status=item.status,
        reminder_at=item.reminder_at,
        event=EventRead.model_validate(item.event),
    )


@router.get(
    "/api/v1/users/{user_id}/plans",
    response_model=list[AttendanceRead],
    tags=["plans"],
    dependencies=[Depends(require_user)],
)
def plans(user_id: str, session: Session = Depends(get_session)) -> list[AttendanceRead]:
    return [
        AttendanceRead(
            id=item.id,
            status=item.status,
            reminder_at=item.reminder_at,
            event=EventRead.model_validate(item.event),
        )
        for item in list_attendance(session, user_id)
    ]


@router.post(
    "/api/v1/users/{user_id}/friends",
    response_model=dict[str, int | str],
    status_code=status.HTTP_201_CREATED,
    tags=["people"],
    dependencies=[Depends(require_user)],
)
def add_friend(
    user_id: str,
    payload: FriendRequestWrite,
    session: Session = Depends(get_session),
) -> dict[str, int | str]:
    try:
        item = request_friendship(session, user_id, payload.friend_user_id)
    except LookupError:
        raise HTTPException(status_code=404, detail="user_not_found") from None
    except ValueError:
        raise HTTPException(status_code=422, detail="cannot_friend_self") from None
    return {"id": item.id, "status": item.status}


@router.post(
    "/api/v1/users/{user_id}/friends/{friendship_id}/accept",
    response_model=dict[str, str],
    tags=["people"],
    dependencies=[Depends(require_user)],
)
def accept_friend(
    user_id: str, friendship_id: int, session: Session = Depends(get_session)
) -> dict[str, str]:
    try:
        item = accept_friendship(session, user_id, friendship_id)
    except LookupError:
        raise HTTPException(status_code=404, detail="friendship_not_found") from None
    return {"status": item.status}


@router.get(
    "/api/v1/users/{user_id}/friends",
    response_model=list[FriendshipRead],
    tags=["people"],
    dependencies=[Depends(require_user)],
)
def friends(user_id: str, session: Session = Depends(get_session)) -> list[FriendshipRead]:
    result: list[FriendshipRead] = []
    for item, friend, shared_events in list_friendships(session, user_id):
        direction = "accepted"
        if item.status == "pending":
            direction = "outgoing" if item.requester_id == user_id else "incoming"
        result.append(
            FriendshipRead(
                id=item.id,
                status=item.status,
                direction=direction,
                friend=ProfileRead.model_validate(friend),
                shared_events=[EventRead.model_validate(event) for event in shared_events],
            )
        )
    return result


@router.get(
    "/api/v1/users/{user_id}/notifications",
    response_model=list[NotificationRead],
    tags=["plans"],
    dependencies=[Depends(require_user)],
)
def notifications(
    user_id: str, session: Session = Depends(get_session)
) -> list[NotificationRead]:
    return [NotificationRead.model_validate(item) for item in list_notifications(session, user_id)]


@router.post(
    "/api/v1/users/{user_id}/notifications/{notification_id}/read",
    status_code=204,
    tags=["plans"],
    dependencies=[Depends(require_user)],
)
def read_notification(
    user_id: str, notification_id: int, session: Session = Depends(get_session)
) -> None:
    try:
        mark_notification_read(session, user_id, notification_id)
    except LookupError:
        raise HTTPException(status_code=404, detail="notification_not_found") from None


@router.post(
    "/api/v1/users/{user_id}/event-submissions",
    response_model=EventRead,
    status_code=status.HTTP_201_CREATED,
    tags=["community events"],
    dependencies=[Depends(require_user)],
)
def create_event_submission(
    user_id: str,
    payload: EventSubmissionWrite,
    session: Session = Depends(get_session),
) -> Event:
    try:
        return submit_event(session, user_id, payload)
    except LookupError:
        raise HTTPException(status_code=404, detail="city_not_found") from None
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from None


@router.get(
    "/api/v1/users/{user_id}/event-submissions",
    response_model=list[EventRead],
    tags=["community events"],
    dependencies=[Depends(require_user)],
)
def own_event_submissions(
    user_id: str, session: Session = Depends(get_session)
) -> list[Event]:
    return list(
        session.scalars(
            select(Event)
            .where(Event.source == "community", Event.submitted_by_user_id == user_id)
            .order_by(Event.created_at.desc())
        )
    )


@router.get(
    "/api/v1/discussions",
    response_model=list[DiscussionSpaceRead],
    tags=["discussions"],
)
def discussion_spaces(
    city: str | None = None, session: Session = Depends(get_session)
) -> list[DiscussionSpaceRead]:
    return [
        DiscussionSpaceRead(
            id=item.id,
            kind=item.kind,
            title=item.title,
            city_slug=item.city_slug,
            event_id=item.event_id,
            venue_name=item.venue_name,
            posts_count=count,
        )
        for item, count in list_discussion_spaces(session, city)
    ]


@router.post(
    "/api/v1/users/{user_id}/discussions",
    response_model=DiscussionSpaceRead,
    status_code=status.HTTP_201_CREATED,
    tags=["discussions"],
    dependencies=[Depends(require_user)],
)
def new_discussion_space(
    user_id: str,
    payload: DiscussionSpaceWrite,
    session: Session = Depends(get_session),
) -> DiscussionSpaceRead:
    try:
        item = create_discussion_space(session, user_id, **payload.model_dump())
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from None
    return DiscussionSpaceRead(
        id=item.id,
        kind=item.kind,
        title=item.title,
        city_slug=item.city_slug,
        event_id=item.event_id,
        venue_name=item.venue_name,
        posts_count=0,
    )


@router.get(
    "/api/v1/discussions/{space_id}/posts",
    response_model=list[DiscussionPostRead],
    tags=["discussions"],
)
def discussion_posts(
    space_id: int, session: Session = Depends(get_session)
) -> list[DiscussionPostRead]:
    return [
        DiscussionPostRead(
            id=post.id,
            user_id=post.user_id,
            author_name=author_name,
            body=post.body,
            created_at=post.created_at,
        )
        for post, author_name in list_discussion_posts(session, space_id)
    ]


@router.post(
    "/api/v1/users/{user_id}/discussions/{space_id}/posts",
    response_model=DiscussionPostRead,
    status_code=status.HTTP_201_CREATED,
    tags=["discussions"],
    dependencies=[Depends(require_user)],
)
def new_discussion_post(
    user_id: str,
    space_id: int,
    payload: DiscussionPostWrite,
    session: Session = Depends(get_session),
) -> DiscussionPostRead:
    try:
        post = add_discussion_post(session, user_id, space_id, payload.body)
    except LookupError:
        raise HTTPException(status_code=404, detail="discussion_not_found") from None
    author = session.get(User, user_id)
    return DiscussionPostRead(
        id=post.id,
        user_id=user_id,
        author_name=(author.display_name if author else "") or "Участник",
        body=post.body,
        created_at=post.created_at,
    )


def require_sync_key(
    x_sync_key: str = Header(default=""),
    settings: Settings = Depends(get_settings),
) -> None:
    if not settings.sync_api_key or x_sync_key != settings.sync_api_key:
        raise HTTPException(status_code=401, detail="invalid_sync_key")


def require_admin_key(
    x_admin_key: str = Header(default=""),
    settings: Settings = Depends(get_settings),
) -> None:
    if not settings.admin_api_key or x_admin_key != settings.admin_api_key:
        raise HTTPException(status_code=401, detail="invalid_admin_key")


@router.get(
    "/api/v1/admin/event-submissions",
    response_model=list[EventRead],
    dependencies=[Depends(require_admin_key)],
    tags=["admin"],
)
def pending_submissions(
    moderation_status: str = Query(default="pending", pattern="^(pending|published|rejected)$"),
    session: Session = Depends(get_session),
) -> list[Event]:
    return list_submissions(session, moderation_status)


@router.post(
    "/api/v1/admin/event-submissions/{event_id}/decision",
    response_model=EventRead,
    dependencies=[Depends(require_admin_key)],
    tags=["admin"],
)
def review_submission(
    event_id: int,
    payload: ModerationWrite,
    session: Session = Depends(get_session),
) -> Event:
    try:
        return moderate_event(
            session, event_id, decision=payload.decision, note=payload.note
        )
    except LookupError:
        raise HTTPException(status_code=404, detail="submission_not_found") from None


@router.get("/internal/ready", include_in_schema=False, dependencies=[Depends(require_sync_key)])
def ready() -> dict[str, str]:
    return {"status": "ready"}
