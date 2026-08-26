import asyncio
from datetime import datetime, timedelta, timezone

import httpx

from events_api.providers.timepad import TimepadProvider


def test_provider_is_disabled_without_organizations():
    provider = TimepadProvider("https://example.test", [])
    result = asyncio.run(provider.list_events("msk", datetime.now(timezone.utc), datetime.now(timezone.utc) + timedelta(days=1)))
    asyncio.run(provider.close())
    assert result == []


def test_provider_parses_public_organization_feed():
    now = datetime(2026, 8, 25, tzinfo=timezone.utc)

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"values": [{
            "id": 88, "name": "Лекция", "description_short": "О городе",
            "starts_at": (now + timedelta(days=2)).isoformat(),
            "ends_at": (now + timedelta(days=2, hours=2)).isoformat(),
            "location": {"city_subdomain": "msk", "name": "Лекторий", "address": "Улица, 2", "coordinates": [55.7, 37.6]},
            "ticket_types": [{"price": 500}], "categories": ["Образование"],
            "poster_image": {"default_url": "https://example.test/poster.jpg"},
            "url": "https://example.test/event/88",
        }]})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = TimepadProvider("https://example.test", [1040], client)
    events = asyncio.run(provider.list_events("msk", now, now + timedelta(days=30)))
    asyncio.run(client.aclose())

    assert events[0].source_id == "88"
    assert events[0].price_text == "от 500 ₽"
    assert events[0].category == "Образование"
