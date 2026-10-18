"""Small portable schema; PostgreSQL is the normal deployment target."""

from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Post(Base):
    __tablename__ = "posts"
    uri: Mapped[str] = mapped_column(Text, primary_key=True)
    author_did: Mapped[str] = mapped_column(Text)
    text: Mapped[str | None] = mapped_column(Text)
    normalized_text: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    indexed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    activity_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    language: Mapped[str | None] = mapped_column(String(64))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    source_cursor: Mapped[int | None] = mapped_column(BigInteger)
    raw_event_ref: Mapped[str | None] = mapped_column(Text)
    event_key: Mapped[str] = mapped_column(String(64))


class PostEventRecord(Base):
    __tablename__ = "post_events"
    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    event_key: Mapped[str] = mapped_column(String(64), unique=True)
    uri: Mapped[str] = mapped_column(Text, index=True)
    author_did: Mapped[str] = mapped_column(Text)
    operation: Mapped[str] = mapped_column(String(16))
    source_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    raw_event_ref: Mapped[str | None] = mapped_column(Text)


class Topic(Base):
    __tablename__ = "topic_candidates"
    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    label: Mapped[str] = mapped_column(Text)
    normalized_key: Mapped[str] = mapped_column(Text, unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)


class TopicTerm(Base):
    __tablename__ = "topic_terms"
    topic_id: Mapped[int] = mapped_column(
        ForeignKey("topic_candidates.id", ondelete="CASCADE"), primary_key=True
    )
    term: Mapped[str] = mapped_column(Text, primary_key=True)
    weight: Mapped[float] = mapped_column(Float, default=1)


class Assignment(Base):
    __tablename__ = "topic_post_assignments"
    topic_id: Mapped[int] = mapped_column(
        ForeignKey("topic_candidates.id", ondelete="CASCADE"), primary_key=True
    )
    post_uri: Mapped[str] = mapped_column(
        ForeignKey("posts.uri", ondelete="CASCADE"), primary_key=True
    )
    assigned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    relevance_score: Mapped[float] = mapped_column(Float, default=1)
    __table_args__ = (Index("ix_assignment_post", "post_uri"),)


class WindowMetric(Base):
    __tablename__ = "topic_window_metrics"
    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    topic_id: Mapped[int] = mapped_column(ForeignKey("topic_candidates.id", ondelete="CASCADE"))
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    window_minutes: Mapped[int] = mapped_column(Integer)
    post_count: Mapped[int] = mapped_column(Integer)
    baseline_expected_count: Mapped[float] = mapped_column(Float)
    absolute_increase: Mapped[float] = mapped_column(Float)
    rate_ratio: Mapped[float] = mapped_column(Float)
    spike_score: Mapped[float] = mapped_column(Float)
    score_components: Mapped[dict] = mapped_column(JSON)
    explanation: Mapped[str] = mapped_column(Text)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        UniqueConstraint("topic_id", "window_start", "window_minutes"),
        Index("ix_topic_window_lookup", "window_start", "window_minutes", "spike_score"),
    )


class DetectionRun(Base):
    __tablename__ = "detection_runs"
    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(16))
    events_processed: Mapped[int] = mapped_column(Integer, default=0)
    topics_evaluated: Mapped[int] = mapped_column(Integer, default=0)
    error_summary: Mapped[str | None] = mapped_column(Text)
