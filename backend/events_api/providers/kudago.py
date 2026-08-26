from __future__ import annotations

import html
import re
from datetime import datetime, timezone

import httpx

from .base import ProviderCity, ProviderEvent


CATEGORY_MAP = {
    "concert": "Музыка",
    "theater": "Театр",
    "exhibition": "Выставки",
    "festival": "Фестивали",
    "lecture": "Лекции",
    "education": "Образование",
    "party": "Вечеринки",
    "kids": "С детьми",
    "sport": "Спорт",
    "cinema": "Кино",
}


def _plain_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", value)).split())


def _age_min(value: str | None) -> int | None:
    match = re.search(r"\d+", value or "")
    return int(match.group()) if match else None


def _timestamp(value: int | float | None) -> datetime | None:
    return datetime.fromtimestamp(value, tz=timezone.utc) if value else None


class KudaGoProvider:
    source = "kudago"

    def __init__(self, base_url: str, client: httpx.AsyncClient | None = None):
        self.base_url = base_url.rstrip("/")
        self.client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(20.0),
            headers={"User-Agent": "Vayobyzh/1.0.1 (+https://github.com/senseigus1-stack/events_soft)"},
        )
        self._owns_client = client is None

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def list_cities(self) -> list[ProviderCity]:
        response = await self.client.get(
            f"{self.base_url}/locations/",
            params={"lang": "ru", "fields": "slug,name,timezone,coords"},
        )
        response.raise_for_status()
        cities = []
        for item in response.json():
            coords = item.get("coords") or {}
            cities.append(
                ProviderCity(
                    slug=item["slug"],
                    name=item["name"],
                    timezone=item.get("timezone") or "Europe/Moscow",
                    latitude=coords.get("lat"),
                    longitude=coords.get("lon"),
                )
            )
        return cities

    async def list_events(
        self, city_slug: str, starts_after: datetime, starts_before: datetime
    ) -> list[ProviderEvent]:
        fields = ",".join(
            [
                "id", "title", "description", "categories", "tags", "dates", "place",
                "images", "site_url", "price", "is_free", "age_restriction",
                "favorites_count", "location",
            ]
        )
        page = 1
        events: list[ProviderEvent] = []
        while True:
            response = await self.client.get(
                f"{self.base_url}/events/",
                params={
                    "lang": "ru",
                    "location": city_slug,
                    "actual_since": int(starts_after.timestamp()),
                    "actual_until": int(starts_before.timestamp()),
                    "fields": fields,
                    "expand": "place,location,dates",
                    "order_by": "-publication_date",
                    "page_size": 100,
                    "page": page,
                },
            )
            response.raise_for_status()
            payload = response.json()
            for item in payload.get("results", []):
                parsed = self._parse_event(item, city_slug, starts_after)
                if parsed:
                    events.append(parsed)
            if not payload.get("next"):
                break
            page += 1
        return events

    def _parse_event(
        self, item: dict, city_slug: str, starts_after: datetime
    ) -> ProviderEvent | None:
        dates = [date for date in item.get("dates", []) if date.get("start")]
        future = [
            date for date in dates
            if _timestamp(date.get("start")) and _timestamp(date["start"]) >= starts_after
        ]
        if not future:
            return None
        occurrence = min(future, key=lambda date: date["start"])
        place = item.get("place") or {}
        coords = place.get("coords") or {}
        images = item.get("images") or []
        image_url = images[0].get("image", "") if images else ""
        categories = item.get("categories") or ["other"]
        category_slug = categories[0]
        raw_tags = [tag.get("name", "") if isinstance(tag, dict) else str(tag) for tag in item.get("tags", [])]
        tags = list(dict.fromkeys([CATEGORY_MAP.get(value, value) for value in categories] + raw_tags))
        return ProviderEvent(
            source=self.source,
            source_id=str(item["id"]),
            city_slug=city_slug,
            title=_plain_text(item.get("title")),
            description=_plain_text(item.get("description")),
            category=CATEGORY_MAP.get(category_slug, category_slug.capitalize()),
            tags=[tag for tag in tags if tag][:12],
            starts_at=_timestamp(occurrence["start"]),  # type: ignore[arg-type]
            ends_at=_timestamp(occurrence.get("end")),
            venue_name=_plain_text(place.get("title")),
            address=_plain_text(place.get("address")),
            latitude=coords.get("lat"),
            longitude=coords.get("lon"),
            image_url=image_url,
            event_url=item.get("site_url") or "",
            price_text=_plain_text(item.get("price")),
            is_free=bool(item.get("is_free")),
            age_min=_age_min(item.get("age_restriction")),
            popularity=int(item.get("favorites_count") or 0),
            raw={"source_payload_id": item["id"]},
        )
