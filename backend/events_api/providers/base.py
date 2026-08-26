from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True)
class ProviderCity:
    slug: str
    name: str
    timezone: str = "Europe/Moscow"
    latitude: float | None = None
    longitude: float | None = None


@dataclass(frozen=True)
class ProviderEvent:
    source: str
    source_id: str
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
    raw: dict = field(default_factory=dict)


class EventProvider(Protocol):
    async def list_cities(self) -> list[ProviderCity]: ...

    async def list_events(
        self, city_slug: str, starts_after: datetime, starts_before: datetime
    ) -> list[ProviderEvent]: ...
