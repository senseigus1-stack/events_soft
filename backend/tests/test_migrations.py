from sqlalchemy import create_engine, inspect, text

from events_api.migrate import run_migrations


def test_migrations_create_schema_and_are_idempotent(tmp_path):
    database_url = f"sqlite:///{tmp_path / 'migrations.db'}"

    run_migrations(database_url)
    run_migrations(database_url)

    engine = create_engine(database_url)
    tables = set(inspect(engine).get_table_names())
    assert {"alembic_version", "users", "events", "interactions"} <= tables
    with engine.connect() as connection:
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == "20260830_0001"
