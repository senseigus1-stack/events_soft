from __future__ import annotations

import logging
import os
import time
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from .config import get_settings
from .database import build_engine

logger = logging.getLogger(__name__)
LOCK_ID = 702_219_103


def _alembic_config(database_url: str) -> Config:
    backend_dir = Path(__file__).resolve().parent.parent
    config = Config(str(backend_dir / "alembic.ini"))
    config.set_main_option("script_location", str(backend_dir / "migrations"))
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    return config


def migrate_once(database_url: str) -> None:
    engine = build_engine(database_url)
    try:
        with engine.begin() as connection:
            is_postgres = connection.dialect.name == "postgresql"
            if is_postgres:
                connection.execute(text("SELECT pg_advisory_lock(:lock_id)"), {"lock_id": LOCK_ID})
            try:
                config = _alembic_config(database_url)
                config.attributes["connection"] = connection
                command.upgrade(config, "head")
            finally:
                if is_postgres:
                    connection.execute(
                        text("SELECT pg_advisory_unlock(:lock_id)"), {"lock_id": LOCK_ID}
                    )
    finally:
        engine.dispose()


def run_migrations(database_url: str | None = None) -> None:
    url = database_url or get_settings().database_url
    attempts = int(os.getenv("MIGRATION_MAX_ATTEMPTS", "60"))
    delay = float(os.getenv("MIGRATION_RETRY_SECONDS", "2"))
    for attempt in range(1, attempts + 1):
        try:
            migrate_once(url)
            logger.info("Database is at the latest migration")
            return
        except OperationalError:
            if attempt == attempts:
                raise
            logger.warning("Database is unavailable; retrying migration (%s/%s)", attempt, attempts)
            time.sleep(delay)


def main() -> None:
    logging.basicConfig(
        level=get_settings().log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    run_migrations()


if __name__ == "__main__":
    main()
