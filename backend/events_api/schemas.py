from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class CityRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    slug: str
    name: str
    timezone: str
    latitude: float | None = None
    longitude: float | None = None


class AuthSessionRead(BaseModel):
    user_id: str
    token: str


class OAuthProviderRead(BaseModel):
    id: Literal["google", "yandex", "github"]
    name: str
    available: bool


class OAuthStartWrite(BaseModel):
    user_id: str | None = Field(default=None, max_length=128)


class OAuthStartRead(BaseModel):
    authorization_url: str


class EventRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    source: str
    city_slug: str
    title: str
    description: str
    category: str
    tags: list[str]
    starts_at: datetime
    ends_at: datetime | None
    venue_name: str
    address: str
    latitude: float | None
    longitude: float | None
    image_url: str
    event_url: str
    price_text: str
    is_free: bool
    age_min: int | None
    popularity: int
    status: str


class EventList(BaseModel):
    items: list[EventRead]
    total: int
    limit: int
    offset: int


class InterestWrite(BaseModel):
    tags: list[str] = Field(min_length=1, max_length=20)


class InteractionWrite(BaseModel):
    event_id: int
    action: Literal["open", "like", "save", "attend", "dismiss"]


class RecommendationRead(EventRead):
    score: float
    reasons: list[str]


class RecommendationList(BaseModel):
    items: list[RecommendationRead]
    strategy: str = "kytchi-v1"


class ProfileWrite(BaseModel):
    display_name: str = Field(min_length=1, max_length=80)
    city_slug: str | None = Field(default=None, max_length=64)
    avatar_url: str = Field(default="", max_length=1000)


class ProfileRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    display_name: str
    city_slug: str | None
    avatar_url: str


class AttendanceWrite(BaseModel):
    status: Literal["going", "cancelled"] = "going"
    reminder_minutes_before: int = Field(default=1440, ge=0, le=10080)


class AttendanceRead(BaseModel):
    id: int
    status: str
    reminder_at: datetime | None
    event: EventRead


class FriendRequestWrite(BaseModel):
    friend_user_id: str = Field(min_length=1, max_length=128)


class FriendshipRead(BaseModel):
    id: int
    status: str
    direction: Literal["incoming", "outgoing", "accepted"]
    friend: ProfileRead
    shared_events: list[EventRead] = Field(default_factory=list)


class NotificationRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    event_id: int | None
    kind: str
    title: str
    body: str
    created_at: datetime
    read_at: datetime | None


class EventSubmissionWrite(BaseModel):
    city_slug: str = Field(min_length=1, max_length=64)
    title: str = Field(min_length=4, max_length=300)
    description: str = Field(min_length=20, max_length=10000)
    category: str = Field(min_length=2, max_length=80)
    tags: list[str] = Field(default_factory=list, max_length=20)
    starts_at: datetime
    ends_at: datetime | None = None
    venue_name: str = Field(min_length=2, max_length=250)
    address: str = Field(min_length=3, max_length=1000)
    image_url: str = Field(default="", max_length=2000)
    event_url: str = Field(default="", max_length=2000)
    price_text: str = Field(default="", max_length=250)
    is_free: bool = False
    age_min: int | None = Field(default=None, ge=0, le=21)


class ModerationWrite(BaseModel):
    decision: Literal["approved", "rejected"]
    note: str = Field(default="", max_length=1000)


class DiscussionSpaceWrite(BaseModel):
    kind: Literal["city", "event", "venue"]
    title: str = Field(min_length=3, max_length=200)
    city_slug: str | None = Field(default=None, max_length=64)
    event_id: int | None = None
    venue_name: str = Field(default="", max_length=250)


class DiscussionPostWrite(BaseModel):
    body: str = Field(min_length=1, max_length=4000)


class DiscussionPostRead(BaseModel):
    id: int
    user_id: str
    author_name: str
    body: str
    created_at: datetime


class DiscussionSpaceRead(BaseModel):
    id: int
    kind: str
    title: str
    city_slug: str | None
    event_id: int | None
    venue_name: str
    posts_count: int = 0
