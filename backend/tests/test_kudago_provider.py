import asyncio
from datetime import datetime, timedelta, timezone

import httpx

from events_api.providers.kudago import KudaGoProvider, _age_min, _plain_text


def test_plain_text_removes_html_and_decodes_entities():
    assert _plain_text("<p>Музыка&nbsp;и <b>город</b></p>") == "Музыка и город"


def test_age_min_parses_common_format():
    assert _age_min("16+") == 16
    assert _age_min("") is None


def test_parse_event_uses_nearest_future_occurrence():
    now = datetime(2026, 8, 25, tzinfo=timezone.utc)
    item = {
        "id": 42,
        "title": "<b>Концерт</b>",
        "description": "Описание",
        "categories": ["concert"],
        "tags": [{"name": "Джаз"}],
        "dates": [
            {"start": int((now - timedelta(days=1)).timestamp())},
            {"start": int((now + timedelta(days=3)).timestamp()), "end": int((now + timedelta(days=3, hours=2)).timestamp())},
        ],
        "place": {"title": "Клуб", "address": "Улица, 1", "coords": {"lat": 55.7, "lon": 37.6}},
        "images": [{"image": "https://example.test/image.jpg"}],
        "site_url": "https://example.test/event",
        "price": "от 500 рублей",
        "is_free": False,
        "age_restriction": "12+",
        "favorites_count": 77,
    }

    parsed = KudaGoProvider("https://example.test")._parse_event(item, "msk", now)

    assert parsed is not None
    assert parsed.title == "Концерт"
    assert parsed.category == "Музыка"
    assert parsed.starts_at == now + timedelta(days=3)
    assert parsed.tags == ["Музыка", "Джаз"]


def test_provider_lists_cities_and_events():
    now = datetime(2026, 8, 25, tzinfo=timezone.utc)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/locations/"):
            return httpx.Response(200, json=[{
                "slug": "msk", "name": "Москва", "timezone": "Europe/Moscow",
                "coords": {"lat": 55.7, "lon": 37.6},
            }])
        return httpx.Response(200, json={
            "next": None,
            "results": [{
                "id": 7, "title": "Событие", "description": "Описание",
                "categories": ["theater"], "tags": [],
                "dates": [{"start": int((now + timedelta(days=1)).timestamp())}],
                "place": {}, "images": [], "site_url": "https://example.test/7",
            }],
        })

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = KudaGoProvider("https://example.test/public-api/v1.4", client)

    cities = asyncio.run(provider.list_cities())
    events = asyncio.run(provider.list_events("msk", now, now + timedelta(days=30)))
    asyncio.run(client.aclose())

    assert cities[0].name == "Москва"
    assert events[0].category == "Театр"


def test_owned_client_can_be_closed():
    asyncio.run(KudaGoProvider("https://example.test").close())
