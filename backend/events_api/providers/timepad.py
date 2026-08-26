from __future__ import annotations

from datetime import datetime

import httpx

from .base import ProviderCity, ProviderEvent


class TimepadProvider:
    """Optional Timepad feed for explicitly configured organizer IDs."""

    source = "timepad"

    def __init__(
        self,
        base_url: str,
        organization_ids: list[int],
        client: httpx.AsyncClient | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.organization_ids = organization_ids
        self.client = client or httpx.AsyncClient(timeout=httpx.Timeout(20.0))
        self._owns_client = client is None

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def list_cities(self) -> list[ProviderCity]:
        return []

    async def list_events(
        self, city_slug: str, starts_after: datetime, starts_before: datetime
    ) -> list[ProviderEvent]:
        if not self.organization_ids:
            return []
        response = await self.client.get(
            f"{self.base_url}/events.json",
            params={
                "organization_ids": ",".join(map(str, self.organization_ids)),
                "starts_at_min": starts_after.isoformat(),
                "starts_at_max": starts_before.isoformat(),
                "fields": "location,ticket_types,description_short",
                "limit": 100,
            },
        )
        response.raise_for_status()
        result: list[ProviderEvent] = []
        for item in response.json().get("values", []):
            location = item.get("location") or {}
            if location.get("city") and city_slug.casefold() not in {
                str(location.get("city", "")).casefold(),
                str(location.get("city_subdomain", "")).casefold(),
            }:
                continue
            starts_at = datetime.fromisoformat(item["starts_at"].replace("Z", "+00:00"))
            ends_at = (
                datetime.fromisoformat(item["ends_at"].replace("Z", "+00:00"))
                if item.get("ends_at")
                else None
            )
            ticket_types = item.get("ticket_types") or []
            prices = [ticket.get("price") for ticket in ticket_types if ticket.get("price")]
            result.append(
                ProviderEvent(
                    source=self.source,
                    source_id=str(item["id"]),
                    city_slug=city_slug,
                    title=item.get("name", ""),
                    description=item.get("description_short") or "",
                    category=(item.get("categories") or ["События"])[0],
                    tags=item.get("categories") or [],
                    starts_at=starts_at,
                    ends_at=ends_at,
                    venue_name=location.get("name") or "",
                    address=location.get("address") or "",
                    latitude=location.get("coordinates", [None, None])[0],
                    longitude=location.get("coordinates", [None, None])[1],
                    image_url=(item.get("poster_image") or {}).get("default_url", ""),
                    event_url=item.get("url", ""),
                    price_text=f"от {min(prices):g} ₽" if prices else "",
                    is_free=not prices,
                    age_min=None,
                    popularity=0,
                    raw={"source_payload_id": item["id"]},
                )
            )
        return result
