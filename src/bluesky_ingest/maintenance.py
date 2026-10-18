"""Rolling retention, including bounded deduplication and ordering metadata."""

import time
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

from .config import Settings
from .database import Database, utc
from .events import PostEvent
from .models import (
    Assignment,
    DetectionRun,
    Post,
    PostEventRecord,
    RetentionState,
    Topic,
    WindowMetric,
)


def maintain(database: Database, settings: Settings, now: datetime | None = None) -> int:
    now = utc(now or datetime.now(UTC))
    cutoff = now - timedelta(hours=settings.post_retention_hours)
    with Session(database.engine) as session, session.begin():
        database.lock(session)
        state = session.get(RetentionState, 1)
        if state:
            cutoff = max(cutoff, utc(state.cutoff))
            state.cutoff = cutoff
        else:
            session.add(RetentionState(id=1, cutoff=cutoff))
        # Updates preserve activity_at, so editing a post does not extend its life.
        expired = select(Post.uri).where(Post.activity_at < cutoff)
        session.execute(delete(Assignment).where(Assignment.post_uri.in_(expired)))
        # A recent update/delete keeps only its ordering metadata until it too expires.
        session.execute(
            update(Post)
            .where(Post.activity_at < cutoff)
            .values(text=None, normalized_text=None, language=None, deleted_at=Post.indexed_at)
        )
        result = session.execute(delete(Post).where(Post.indexed_at < cutoff))
        session.execute(delete(PostEventRecord).where(PostEventRecord.source_time < cutoff))
        session.execute(delete(WindowMetric).where(WindowMetric.window_start < cutoff))
        session.execute(delete(DetectionRun).where(DetectionRun.started_at < cutoff))
        session.execute(delete(Topic).where(Topic.last_seen_at < cutoff))
        return result.rowcount


class RetainedSink:
    """Run retention on receipt and idle ticks in the standalone ingestion CLI."""

    def __init__(self, database: Database, settings: Settings):
        self.database, self.settings = database, settings
        self.last_maintenance = float("-inf")

    def maintain(self) -> None:
        if time.monotonic() - self.last_maintenance >= 60:
            maintain(self.database, self.settings)
            self.last_maintenance = time.monotonic()

    def __call__(self, post: PostEvent, ingested_at: str, raw_ref: str | None = None) -> bool:
        self.maintain()
        return self.database.apply(post, ingested_at, raw_ref)
