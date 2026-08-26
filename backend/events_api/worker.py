import asyncio
import logging

from .config import get_settings
from .database import create_schema
from .sync import sync_all


logger = logging.getLogger(__name__)


async def run_forever() -> None:
    settings = get_settings()
    while True:
        try:
            counts = await sync_all(settings.sync_city_slugs)
            logger.info("Scheduled sync completed: %s", counts)
        except Exception:
            logger.exception("Scheduled sync failed")
        await asyncio.sleep(settings.sync_interval_minutes * 60)


def main() -> None:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)
    create_schema()
    asyncio.run(run_forever())


if __name__ == "__main__":
    main()

