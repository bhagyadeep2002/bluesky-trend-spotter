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
