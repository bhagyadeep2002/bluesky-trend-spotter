from __future__ import annotations

import hashlib
import json
import math
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import BigInteger, case, cast, delete, func, select, update
from sqlalchemy.orm import Session

from .candidates import STOP
from .config import Settings
from .database import Database, utc
from .models import Assignment, DetectionRun, Post, PostEventRecord, Topic, WindowMetric


def floor_window(value: datetime, minutes: int) -> datetime:
    seconds = minutes * 60
    return datetime.fromtimestamp(int(utc(value).timestamp()) // seconds * seconds, UTC)


def history_ready(session: Session, start: datetime, settings: Settings) -> bool:
    # This measures observed source history, not complete network coverage.
    first = session.scalar(select(func.min(PostEventRecord.source_time)))
    return first is not None and floor_window(first, settings.window_minutes) <= (
        start - timedelta(minutes=settings.window_minutes * settings.baseline_windows)
    )


def score_window(
    count: int, baseline_counts: list[int], settings: Settings, *, baseline_ready: bool = True
) -> dict:
    baseline = sum(baseline_counts) / len(baseline_counts) if baseline_counts else 0.0
    increase = count - baseline
    ratio = (count + settings.alpha) / (max(baseline, settings.baseline_floor) + settings.alpha)
    eligible = (
        baseline_ready
        and count >= settings.min_current_count
        and increase >= settings.min_absolute_increase
        and ratio >= settings.min_rate_ratio
    )
    score = max(0, increase) * math.log2(max(ratio, 1)) if eligible else 0.0
    return {
        "baseline_expected_count": baseline,
        "absolute_increase": increase,
        "rate_ratio": ratio,
        "spike_score": score,
        "score_components": {
            "alpha": settings.alpha,
            "baseline_floor": settings.baseline_floor,
            "min_current_count": settings.min_current_count,
            "min_absolute_increase": settings.min_absolute_increase,
            "min_rate_ratio": settings.min_rate_ratio,
            "baseline_counts": baseline_counts,
            "eligible": eligible,
            "baseline_ready": baseline_ready,
            "formula": "max(0, absolute_increase) * log2(rate_ratio)",
        },
        "explanation": (
            f"Activity rose from an estimated {baseline:g} posts per {settings.window_minutes} minutes to {count} in the latest completed window."
            if baseline_ready
            else "Building observed baseline history; spike ranking is withheld until enough history is available."
        ),
    }


# Bound each publication transaction rather than locking ingestion for a full run.
PUBLISH_TOPICS = 32
INSERT_ROWS = 200
SIGNATURE_VERSION = 1


def chunks(values, size):
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def signature(value) -> str:
    return hashlib.sha256(json.dumps(value, separators=(",", ":")).encode()).hexdigest()


@dataclass
class Observation:
    counts: dict[int, dict[datetime, int]]
    first_window: datetime | None
    active: set[int]
    fillers: set[int]
    signatures: dict[tuple[int, datetime], tuple[str | None, str | None]]
    events_processed: int


@contextmanager
def read_snapshot(database: Database):
    # SELECTs never acquire the ingestion advisory lock. PostgreSQL MVCC gives
    # all count/signature queries one coherent view while event writes continue.
    with database.engine.connect() as connection:
        if database.engine.dialect.name == "postgresql":
            connection = connection.execution_options(isolation_level="REPEATABLE READ")
        with connection.begin():
            if database.engine.dialect.name == "sqlite":
                # sqlite3's legacy transaction mode doesn't BEGIN for SELECT.
                connection.exec_driver_sql("BEGIN")
            with Session(bind=connection) as session:
                yield session


def observe(database: Database, settings: Settings, boundary: datetime) -> Observation:
    width = timedelta(minutes=settings.window_minutes)
    start = boundary - width
    oldest = start - width * (2 * settings.baseline_windows)
    with read_snapshot(database) as session:
        post_count = session.scalar(
            select(func.count())
            .select_from(Post)
            .where(
                Post.deleted_at.is_(None), Post.activity_at >= oldest, Post.activity_at < boundary
            )
        )
        if post_count > settings.analysis_max_posts:
            raise RuntimeError("analysis post budget exceeded")
        seconds = settings.window_minutes * 60
        if database.engine.dialect.name == "postgresql":
            bucket = cast(func.floor(func.extract("epoch", Post.activity_at) / seconds), BigInteger)
        else:
            epoch = cast(func.strftime("%s", Post.activity_at), BigInteger)
            bucket = case(
                (epoch >= 0, epoch.op("/")(seconds)), else_=(epoch - (seconds - 1)).op("/")(seconds)
            )
        rows = session.execute(
            select(Assignment.topic_id, bucket, func.count())
            .join(Post, Post.uri == Assignment.post_uri)
            .where(
                Post.deleted_at.is_(None), Post.activity_at >= oldest, Post.activity_at < boundary
            )
            .group_by(Assignment.topic_id, bucket)
        )
        counts: dict[int, dict[datetime, int]] = {}
        for topic_id, window, count in rows:
            counts.setdefault(topic_id, {})[datetime.fromtimestamp(int(window) * seconds, UTC)] = (
                count
            )
        first = session.scalar(select(func.min(PostEventRecord.source_time)))
        active = set(session.scalars(select(Topic.id).where(Topic.active.is_(True))))
        fillers = set(
            session.scalars(
                select(Topic.id).where(Topic.normalized_key.in_(["term:" + term for term in STOP]))
            )
        )
        # Only scalar metadata is fetched, never 200k full ORM metric objects.
        rows = session.execute(
            select(
                WindowMetric.topic_id,
                WindowMetric.window_start,
                WindowMetric.score_components["observation_signature"].as_string(),
                WindowMetric.score_components["metric_signature"].as_string(),
            ).where(
                WindowMetric.window_minutes == settings.window_minutes,
                WindowMetric.window_start >= start - width * settings.baseline_windows,
                WindowMetric.window_start <= start,
            )
        )
        signatures = {
            (topic_id, utc(at)): (observed, metric) for topic_id, at, observed, metric in rows
        }
        return Observation(
            counts,
            floor_window(first, settings.window_minutes) if first else None,
            active,
            fillers,
            signatures,
            session.scalar(select(func.count()).select_from(PostEventRecord)),
        )


def publish_topics(database: Database, rows_by_topic: dict[int, list[dict]], inactive: set[int]):
    # Metrics are owned only by this detector. PostgreSQL KEY SHARE protects
    # referenced topics from retention deletion without blocking ingestion's
    # non-key topic updates. Never acquire ingestion's advisory lock here.
    topic_ids = sorted(set(rows_by_topic) | inactive)
    for batch in chunks(topic_ids, PUBLISH_TOPICS):
        with Session(database.engine) as session, session.begin():
            topics = select(Topic.id).where(Topic.id.in_(batch)).order_by(Topic.id)
            if database.engine.dialect.name == "postgresql":
                topics = topics.with_for_update(read=True, key_share=True)
            alive = set(session.scalars(topics))
            enabled = alive & rows_by_topic.keys()
            disabled = alive & inactive
            if disabled:
                session.execute(
                    update(Topic)
                    .where(Topic.id.in_(disabled), Topic.active.is_(True))
                    .values(active=False)
                )
                session.execute(delete(WindowMetric).where(WindowMetric.topic_id.in_(disabled)))
            if enabled:
                session.execute(
                    update(Topic)
                    .where(Topic.id.in_(enabled), Topic.active.is_(False))
                    .values(active=True)
                )
                rows = [row for topic_id in enabled for row in rows_by_topic[topic_id]]
                for part in chunks(rows, INSERT_ROWS):
                    statement = database.insert(WindowMetric)
                    statement = statement.on_conflict_do_update(
                        index_elements=["topic_id", "window_start", "window_minutes"],
                        set_={
                            column: getattr(statement.excluded, column)
                            for column in part[0]
                            if column not in {"topic_id", "window_start", "window_minutes"}
                        },
                    )
                    session.connection().execute(statement, part)


def prune_metrics(database: Database, settings: Settings, earliest: datetime):
    # Keep the prior 13-window timeline without one large deletion transaction.
    while True:
        with Session(database.engine) as session, session.begin():
            ids = list(
                session.scalars(
                    select(WindowMetric.id)
                    .where(
                        WindowMetric.window_minutes == settings.window_minutes,
                        WindowMetric.window_start < earliest,
                    )
                    .limit(1000)
                )
            )
            if ids:
                session.execute(delete(WindowMetric).where(WindowMetric.id.in_(ids)))
        if len(ids) < 1000:
            return


def detect(database: Database, settings: Settings, now: datetime | None = None) -> int:
    """Snapshot counts without blocking ingestion, then publish changed windows.

    Concurrent updates/deletes after the snapshot are observed on the next run;
    fingerprints are derived from data, so no concurrently changed dirty flag can
    be lost. Evidence/search always filter deleted posts immediately.
    """
    now = utc(now or datetime.now(UTC))
    boundary = floor_window(now, settings.window_minutes)
    width = timedelta(minutes=settings.window_minutes)
    current_start = boundary - width
    earliest = current_start - width * settings.baseline_windows
    with Session(database.engine) as session, session.begin():
        run = DetectionRun(started_at=now, status="running", events_processed=0, topics_evaluated=0)
        session.add(run)
        session.flush()
        run_id = run.id
    try:
        observation = observe(database, settings, boundary)
        for batch in chunks(sorted(observation.fillers), PUBLISH_TOPICS):
            with Session(database.engine) as session, session.begin():
                database.lock(session)
                session.execute(delete(Topic).where(Topic.id.in_(batch)))
        config_signature = signature(
            [
                SIGNATURE_VERSION,
                settings.window_minutes,
                settings.baseline_windows,
                settings.alpha,
                settings.baseline_floor,
                settings.min_current_count,
                settings.min_absolute_increase,
                settings.min_rate_ratio,
                settings.candidate_min_frequency,
            ]
        )
        eligible = {
            topic_id: windows
            for topic_id, windows in observation.counts.items()
            if topic_id not in observation.fillers
            and sum(windows.values()) >= settings.candidate_min_frequency
        }
        rows_by_topic = {}
        for topic_id, windows in eligible.items():
            observed_signature = signature(
                [
                    config_signature,
                    observation.first_window.isoformat() if observation.first_window else None,
                    [
                        windows.get(current_start - width * n, 0)
                        for n in range(2 * settings.baseline_windows + 1)
                    ],
                ]
            )
            previous, _ = observation.signatures.get((topic_id, current_start), (None, None))
            if previous == observed_signature:
                continue
            persistence = 0
            changed = []
            for offset in range(settings.baseline_windows, -1, -1):
                start = current_start - width * offset
                history = [
                    windows.get(start - width * n, 0)
                    for n in range(1, settings.baseline_windows + 1)
                ]
                count = windows.get(start, 0)
                ready = observation.first_window is not None and (
                    observation.first_window <= start - width * settings.baseline_windows
                )
                result = score_window(count, history, settings, baseline_ready=ready)
                persistence = persistence + 1 if result["spike_score"] > 0 else 0
                metric_signature = signature([config_signature, count, history, ready, persistence])
                _, prior_metric = observation.signatures.get((topic_id, start), (None, None))
                if metric_signature == prior_metric and start != current_start:
                    continue
                components = result["score_components"]
                components["consecutive_spike_windows"] = persistence
                components["metric_signature"] = metric_signature
                components["observation_signature"] = (
                    observed_signature if start == current_start else None
                )
                changed.append(
                    dict(
                        topic_id=topic_id,
                        window_start=start,
                        window_minutes=settings.window_minutes,
                        post_count=count,
                        detected_at=now,
                        **result,
                    )
                )
            rows_by_topic[topic_id] = changed
        # A disappeared or below-frequency candidate must not leave a stale score.
        previous_topics = {topic_id for topic_id, _ in observation.signatures}
        inactive = (observation.active | previous_topics) - eligible.keys() - observation.fillers
        publish_topics(database, rows_by_topic, inactive)
        prune_metrics(database, settings, earliest)
        with Session(database.engine) as session, session.begin():
            run = session.get(DetectionRun, run_id)
            run.finished_at = datetime.now(UTC)
            run.status = "success"
            run.events_processed = observation.events_processed
            run.topics_evaluated = len(eligible)
        return len(eligible)
    except Exception as exc:
        with Session(database.engine) as session, session.begin():
            run = session.get(DetectionRun, run_id)
            run.finished_at = datetime.now(UTC)
            run.status, run.error_summary = "failed", type(exc).__name__
        raise


def detect_in_worker(url: str, settings: Settings) -> int:
    """Process-pool entry point: never share an engine across process boundaries."""
    database = Database(url)
    try:
        return detect(database, settings)
    finally:
        database.dispose()
