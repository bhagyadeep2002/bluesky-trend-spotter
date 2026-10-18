from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from bluesky_ingest.candidates import extract_candidates
from bluesky_ingest.config import Settings
from bluesky_ingest.detection import detect, floor_window, score_window
from bluesky_ingest.events import parse_event
from bluesky_ingest.models import Assignment, DetectionRun, Post, Topic, WindowMetric
from bluesky_ingest.synthetic import make_event, scenario


def test_candidate_filters_and_determinism():
    text = "The people are really going with #Aurora #aurora https://www.science.example/news"
    candidates = extract_candidates(text)
    assert [c.key for c in candidates] == ["hashtag:aurora", "domain:science.example"]
    assert candidates == extract_candidates(text)
    assert len(extract_candidates(" ".join(f"#tag{i}" for i in range(100)))) == 12


def test_score_smoothing_zero_windows_and_noise():
    settings = Settings()
    assert score_window(2, [0] * 12, settings)["spike_score"] == 0
    assert score_window(100, [100] * 12, settings)["spike_score"] == 0
    spike = score_window(24, [4] * 12, settings)
    assert spike["rate_ratio"] == 26 / 6
    assert spike["absolute_increase"] == 20
    assert spike["spike_score"] > 0


def test_synthetic_spike_ranks_above_stable_volume(database):
    now = datetime(2026, 10, 18, 12, 5, tzinfo=UTC)
    for raw in scenario(now):
        post = parse_event(raw)
        database.apply(post, now.isoformat())
        assert not database.apply(post, now.isoformat())
    detect(database, Settings(), now)
    with Session(database.engine) as session:
        rows = list(
            session.execute(
                select(Topic.label, WindowMetric)
                .join(WindowMetric)
                .where(WindowMetric.window_start == floor_window(now, 5) - timedelta(minutes=5))
                .order_by(WindowMetric.spike_score.desc())
            )
        )
        assert rows[0][0] == "#aurora"
        metrics = {label: metric for label, metric in rows}
        assert metrics["#aurora"].post_count == 24
        assert metrics["#aurora"].baseline_expected_count == 4
        assert metrics["#steady"].spike_score == 0
        assert "#tiny" not in metrics  # Frequency threshold excludes tiny evidence.
        assert session.scalar(select(func.count()).select_from(DetectionRun)) == 1
    # Reruns must replace metrics, not duplicate them.
    detect(database, Settings(), now)
    with Session(database.engine) as session:
        total = session.scalar(select(func.count()).select_from(WindowMetric))
    detect(database, Settings(), now)
    with Session(database.engine) as session:
        assert session.scalar(select(func.count()).select_from(WindowMetric)) == total


def test_updates_and_deletes_remove_assignments_and_scores(database):
    now = datetime(2026, 10, 18, 12, 5, tzinfo=UTC)
    start = now - timedelta(minutes=4)
    settings = Settings(
        min_current_count=2, min_absolute_increase=1, candidate_min_frequency=1, min_rate_ratio=1.1
    )
    for i in range(4):
        database.apply(parse_event(make_event("#aurora", start, str(i))), now.isoformat())
    detect(database, settings, now)
    for i in range(4):
        database.apply(parse_event(make_event("", now, str(i), "delete")), now.isoformat())
    detect(database, settings, now)
    with Session(database.engine) as session:
        assert session.scalar(select(func.count()).select_from(Assignment)) == 0
        assert session.scalar(select(func.count()).select_from(WindowMetric)) == 0
        assert all(post.text is None for post in session.scalars(select(Post)))


def test_analysis_budget_failure_is_recorded(database):
    now = datetime(2026, 10, 18, 12, 5, tzinfo=UTC)
    for i in range(2):
        database.apply(
            parse_event(make_event("#aurora", now - timedelta(minutes=2), str(i))), now.isoformat()
        )
    with pytest.raises(RuntimeError, match="budget exceeded"):
        detect(database, Settings(analysis_max_posts=1), now)
    with Session(database.engine) as session:
        run = session.scalar(select(DetectionRun))
        assert run.status == "failed" and run.error_summary == "RuntimeError"
