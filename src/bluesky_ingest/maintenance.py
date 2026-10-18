"""Bound text retention while preserving minimal tombstones and dedup keys."""

from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

from .config import Settings
from .database import Database
from .models import Assignment, DetectionRun, Post, Topic, WindowMetric


def maintain(database: Database, settings: Settings, now: datetime | None = None) -> int:
    now = now or datetime.now(UTC)
    cutoff = now - timedelta(hours=settings.post_retention_hours)
    with Session(database.engine) as session, session.begin():
        database.lock(session)
        expired = select(Post.uri).where(Post.ingested_at < cutoff, Post.deleted_at.is_(None))
        session.execute(delete(Assignment).where(Assignment.post_uri.in_(expired)))
        result = session.execute(
            update(Post)
            .where(Post.ingested_at < cutoff, Post.deleted_at.is_(None))
            .values(text=None, normalized_text=None, language=None, deleted_at=now)
        )
        history = now - timedelta(days=7)
        session.execute(delete(WindowMetric).where(WindowMetric.window_start < history))
        session.execute(delete(DetectionRun).where(DetectionRun.started_at < history))
        session.execute(delete(Topic).where(Topic.active.is_(False), Topic.last_seen_at < history))
        return result.rowcount
