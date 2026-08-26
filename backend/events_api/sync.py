from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from .config import get_settings
from .database import SessionLocal, create_schema
from .providers import KudaGoProvider, TimepadProvider
from .service import upsert_city, upsert_event


logger = logging.getLogger(__name__)


async def sync_all(city_slugs: list[str] | None = None) -> dict[str, int]:
    settings = get_settings()
    kudago = KudaGoProvider(settings.kudago_base_url)
    timepad = TimepadProvider(
        settings.timepad_base_url, settings.timepad_organization_ids
    )
    now = datetime.now(timezone.utc)
    until = now + timedelta(days=settings.sync_horizon_days)
    counts: dict[str, int] = {}
    try:
        provider_cities = await kudago.list_cities()
        if city_slugs and "all" not in city_slugs:
            provider_cities = [city for city in provider_cities if city.slug in city_slugs]
        with SessionLocal() as session:
            for city in provider_cities:
                upsert_city(session, asdict(city))
            session.commit()

        for city in provider_cities:
            imported = 0
            for provider in (kudago, timepad):
                try:
                    events = await provider.list_events(city.slug, now, until)
                except Exception:
                    logger.exception("Provider %s failed for %s", provider.source, city.slug)
                    continue
                with SessionLocal() as session:
                    for event in events:
                        upsert_event(session, asdict(event))
                    session.commit()
                imported += len(events)
            counts[city.slug] = imported
            logger.info("Imported %s events for %s", imported, city.slug)
    finally:
        await kudago.close()
        await timepad.close()
    return counts


def main() -> None:
    logging.basicConfig(level=get_settings().log_level)
    create_schema()
    counts = asyncio.run(sync_all(get_settings().sync_city_slugs))
    logger.info("Sync completed: %s", counts)


if __name__ == "__main__":
    main()
