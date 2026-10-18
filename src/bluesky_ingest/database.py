from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import create_engine, delete, event, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from .events import PostEvent
from .models import Assignment, Base, Post, PostEventRecord


def utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class Database:
    def __init__(self, url: str):
        options = {"pool_pre_ping": True}
        if url.startswith("sqlite"):
            if url.startswith("sqlite:///") and ":memory:" not in url:
                Path(url.removeprefix("sqlite:///")).parent.mkdir(parents=True, exist_ok=True)
            options["connect_args"] = {"check_same_thread": False, "timeout": 30}
        else:
            options["connect_args"] = {"connect_timeout": 3}
        self.engine = create_engine(url, **options)
        if self.engine.dialect.name == "sqlite":
            @event.listens_for(self.engine, "connect")
            def configure(connection, record):
                connection.execute("PRAGMA foreign_keys=ON")
                connection.execute("PRAGMA journal_mode=WAL")

    def ready(self) -> bool:
        with self.engine.connect() as connection:
            connection.execute(text("SELECT 1 FROM posts LIMIT 1"))
            version = connection.execute(text("SELECT version_num FROM alembic_version")).scalar()
            return version == "0001"

    def migrate(self) -> None:
        from alembic import command
        from alembic.config import Config
        root = Path(__file__).resolve().parents[2]
        config = Config(str(root / "alembic.ini"))
        config.attributes["database_url"] = self.engine.url.render_as_string(hide_password=False)
        command.upgrade(config, "head")

    def lock(self, session: Session) -> None:
        # One ingestion/detection writer at a time; readers remain nonblocking.
        if self.engine.dialect.name == "postgresql":
            session.execute(text("SELECT pg_advisory_xact_lock(847019)"))

    def insert(self, model):
        return (pg_insert if self.engine.dialect.name == "postgresql" else sqlite_insert)(model)

    def apply(self, post: PostEvent, ingested_at: str, raw_ref: str | None = None) -> bool:
        received = datetime.fromisoformat(ingested_at)
        source = datetime.fromisoformat(post.source_time) if post.source_time else received
        with Session(self.engine) as session, session.begin():
            self.lock(session)
            inserted = session.execute(self.insert(PostEventRecord).values(
                event_key=post.event_key, uri=post.uri, author_did=post.author_did,
                operation=post.operation, source_time=source, ingested_at=received,
                raw_event_ref=raw_ref).on_conflict_do_nothing(index_elements=["event_key"]).returning(PostEventRecord.id)).scalar()
            if inserted is None:
                return False
            current = session.get(Post, post.uri)
            if current:
                old_order = (utc(current.indexed_at), current.source_cursor or 0,
                             current.deleted_at is not None, current.event_key)
                new_order = (utc(source), post.cursor or 0, post.operation == "delete", post.event_key)
                if new_order <= old_order:
                    return False
            else:
                current = Post(uri=post.uri, activity_at=source)
                session.add(current)
            current.author_did = post.author_did
            current.indexed_at, current.ingested_at = source, received
            current.source_cursor, current.raw_event_ref = post.cursor, raw_ref
            current.event_key = post.event_key
            current.deleted_at = source if post.operation == "delete" else None
            current.text, current.normalized_text = post.text, post.normalized_text
            current.created_at = datetime.fromisoformat(post.created_at) if post.created_at else None
            current.language = post.languages[0] if post.languages and current.deleted_at is None else None
            session.flush()
            session.execute(delete(Assignment).where(Assignment.post_uri == post.uri))
        return True

    def dispose(self) -> None:
        self.engine.dispose()
