from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, func, select
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


def detect(database: Database, settings: Settings, now: datetime | None = None) -> int:
    """Recompute the bounded observation horizon; zero windows are explicit.

    Score only completed windows, so an incomplete window never competes with
    full historical windows. Changes/deletes rebuild all affected horizon counts.
    """
    now = utc(now or datetime.now(UTC))
    boundary = floor_window(now, settings.window_minutes)
    width = timedelta(minutes=settings.window_minutes)
    current_start = boundary - width
    # Two baseline lengths make prior scores/persistence inspectable as well.
    oldest = current_start - width * (settings.baseline_windows * 2)
    with Session(database.engine) as session, session.begin():
        database.lock(session)
        run = DetectionRun(started_at=now, status="running", events_processed=0, topics_evaluated=0)
        session.add(run)
        session.flush()
        run_id = run.id
    try:
        with Session(database.engine) as session, session.begin():
            database.lock(session)
            # Remove previously extracted fillers too when upgrading existing data.
            session.execute(
                delete(Topic).where(Topic.normalized_key.in_(["term:" + term for term in STOP]))
            )
            first_source = session.scalar(select(func.min(PostEventRecord.source_time)))
            first_window = (
                floor_window(first_source, settings.window_minutes) if first_source else None
            )
            post_count = session.scalar(
                select(func.count())
                .select_from(Post)
                .where(
                    Post.deleted_at.is_(None),
                    Post.activity_at >= oldest,
                    Post.activity_at < boundary,
                )
            )
            if post_count > settings.analysis_max_posts:
                raise RuntimeError("analysis post budget exceeded")
            counts: dict[int, dict[datetime, int]] = {}
            query = (
                select(Assignment.topic_id, Post.activity_at)
                .join(Post, Post.uri == Assignment.post_uri)
                .where(
                    Post.deleted_at.is_(None),
                    Post.activity_at >= oldest,
                    Post.activity_at < boundary,
                )
            )
            for topic_id, observed_at in session.execute(query):
                start = floor_window(observed_at, settings.window_minutes)
                windows = counts.setdefault(topic_id, {})
                windows[start] = windows.get(start, 0) + 1
            # Old scores must not survive a deletion, emptied candidate or new configuration.
            session.execute(delete(WindowMetric).where(WindowMetric.window_start >= oldest))
            topics = list(session.scalars(select(Topic)))
            for topic in topics:
                windows = counts.get(topic.id, {})
                frequency = sum(windows.values())
                topic.active = frequency >= settings.candidate_min_frequency
                if not topic.active:
                    continue
                persistence = 0
                for offset in range(settings.baseline_windows, -1, -1):
                    start = current_start - width * offset
                    history = [
                        windows.get(start - width * n, 0)
                        for n in range(1, settings.baseline_windows + 1)
                    ]
                    count = windows.get(start, 0)
                    result = score_window(
                        count,
                        history,
                        settings,
                        baseline_ready=(
                            first_window is not None
                            and first_window <= start - width * settings.baseline_windows
                        ),
                    )
                    persistence = persistence + 1 if result["spike_score"] > 0 else 0
                    result["score_components"]["consecutive_spike_windows"] = persistence
                    session.add(
                        WindowMetric(
                            topic_id=topic.id,
                            window_start=start,
                            window_minutes=settings.window_minutes,
                            post_count=count,
                            detected_at=now,
                            **result,
                        )
                    )
            run = session.get(DetectionRun, run_id)
            run.finished_at = datetime.now(UTC)
            run.status = "success"
            run.events_processed = session.scalar(select(func.count()).select_from(PostEventRecord))
            run.topics_evaluated = sum(
                1
                for windows in counts.values()
                if sum(windows.values()) >= settings.candidate_min_frequency
            )
            return run.topics_evaluated
    except Exception as exc:
        with Session(database.engine) as session, session.begin():
            run = session.get(DetectionRun, run_id)
            run.finished_at = datetime.now(UTC)
            run.status, run.error_summary = "failed", type(exc).__name__
        raise
