"""Create durable in-app reminders for events users plan to attend."""

from __future__ import annotations

import logging
import time

from .community import generate_due_notifications
from .config import get_settings
from .database import SessionLocal, create_schema


def run_once() -> int:
    with SessionLocal() as session:
        return generate_due_notifications(session)


def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger = logging.getLogger("vayobyzh.notifications")
    create_schema()
    while True:
        try:
            created = run_once()
            if created:
                logger.info("created_reminders count=%s", created)
        except Exception:
            logger.exception("reminder_cycle_failed")
        time.sleep(settings.notification_interval_seconds)


if __name__ == "__main__":
    main()
