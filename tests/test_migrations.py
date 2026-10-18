from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import inspect


def test_migration_roundtrip_and_metadata_match(database):
    config = Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))
    config.attributes["database_url"] = database.engine.url.render_as_string(hide_password=False)
    command.check(config)
    command.downgrade(config, "base")
    assert "posts" not in inspect(database.engine).get_table_names()
    command.upgrade(config, "head")
    assert database.ready()


def test_upgrade_populated_0002_preserves_current_posts(database):
    from datetime import UTC, datetime

    from sqlalchemy.orm import Session

    from bluesky_ingest.events import parse_event
    from bluesky_ingest.models import Post
    from bluesky_ingest.synthetic import make_event

    now = datetime(2026, 10, 18, 12, 0, tzinfo=UTC)
    event = parse_event(make_event("#preserved", now, "keep"))
    database.apply(event, now.isoformat())
    config = Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))
    config.attributes["database_url"] = database.engine.url.render_as_string(hide_password=False)
    command.downgrade(config, "0002")
    assert not database.ready()
    command.upgrade(config, "head")
    assert database.ready()
    with Session(database.engine) as session:
        assert session.get(Post, event.uri).text == "#preserved"
