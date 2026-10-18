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


def test_filler_words_filtered_but_meaningful_candidates_remain():
    text = "three million words hundred eighty thousand digit love back Trump Ukraine earthquake #Love https://science.example/news"
    keys = {candidate.key for candidate in extract_candidates(text)}
    assert keys == {
        "term:trump",
        "term:ukraine",
        "term:earthquake",
        "hashtag:love",
        "domain:science.example",
    }


def test_cold_start_withholds_spikes_until_observed_history_exists(database):
    now = datetime(2026, 10, 18, 12, 5, tzinfo=UTC)
    for i in range(12):
        database.apply(
            parse_event(make_event("#aurora Trump", now - timedelta(minutes=2), str(i))),
            now.isoformat(),
        )
    # Simulate a filler candidate extracted by an earlier application version.
    with Session(database.engine) as session, session.begin():
        session.add(
            Topic(
                label="million",
                normalized_key="term:million",
                created_at=now,
                last_seen_at=now,
                active=True,
            )
        )
    detect(database, Settings(), now)
    with Session(database.engine) as session:
        assert session.scalar(select(Topic).where(Topic.normalized_key == "term:million")) is None
        metrics = list(session.scalars(select(WindowMetric)))
        assert metrics and all(metric.spike_score == 0 for metric in metrics)
        assert all(not metric.score_components["baseline_ready"] for metric in metrics)
    # A genuinely new topic may have zero baseline once global history exists.
    database.apply(
        parse_event(make_event("#history", now - timedelta(minutes=65), "history")), now.isoformat()
    )
    detect(database, Settings(), now)
    with Session(database.engine) as session:
        metric = session.scalar(select(WindowMetric).where(WindowMetric.spike_score > 0))
        assert metric is not None and metric.baseline_expected_count == 0
        assert metric.score_components["baseline_ready"]


def test_unchanged_and_open_window_posts_do_not_rewrite_metrics(database):
    from sqlalchemy import event

    now = datetime(2026, 10, 18, 12, 5, tzinfo=UTC)
    for raw in scenario(now):
        database.apply(parse_event(raw), now.isoformat())
    detect(database, Settings(), now)
    with Session(database.engine) as session:
        before = {row.id: row.detected_at for row in session.scalars(select(WindowMetric))}
    database.apply(
        parse_event(make_event("#aurora", now + timedelta(seconds=10), "open")), now.isoformat()
    )
    writes = []

    def capture(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith(
            (
                "INSERT INTO topic_window_metrics",
                "UPDATE topic_window_metrics",
                "DELETE FROM topic_window_metrics",
            )
        ):
            writes.append(statement)

    event.listen(database.engine, "before_cursor_execute", capture)
    try:
        detect(database, Settings(), now + timedelta(seconds=20))
    finally:
        event.remove(database.engine, "before_cursor_execute", capture)
    assert writes == []
    with Session(database.engine) as session:
        assert before == {row.id: row.detected_at for row in session.scalars(select(WindowMetric))}
    # Advancing the boundary publishes the newly completed window, preserving old rows.
    detect(database, Settings(), now + timedelta(minutes=5))
    with Session(database.engine) as session:
        aurora = session.scalar(select(Topic.id).where(Topic.label == "#aurora"))
        metric = session.scalar(
            select(WindowMetric).where(
                WindowMetric.topic_id == aurora, WindowMetric.window_start == now
            )
        )
        assert metric.post_count == 1
        prior = session.scalar(
            select(WindowMetric).where(
                WindowMetric.topic_id == aurora,
                WindowMetric.window_start == now - timedelta(minutes=5),
            )
        )
        assert prior.post_count == 24
        assert prior.detected_at == before[prior.id]


def test_configuration_and_late_events_invalidate_cached_scores(database):
    from dataclasses import replace

    now = datetime(2026, 10, 18, 12, 5, tzinfo=UTC)
    for raw in scenario(now):
        database.apply(parse_event(raw), now.isoformat())
    settings = Settings()
    detect(database, settings, now)
    database.apply(
        parse_event(make_event("#aurora", now - timedelta(minutes=3), "late")), now.isoformat()
    )
    detect(database, settings, now)
    with Session(database.engine) as session:
        metric = session.scalar(
            select(WindowMetric)
            .join(Topic)
            .where(
                Topic.label == "#aurora", WindowMetric.window_start == now - timedelta(minutes=5)
            )
        )
        assert metric.post_count == 25 and metric.spike_score > 0
    detect(database, replace(settings, min_current_count=100), now)
    with Session(database.engine) as session:
        assert all(metric.spike_score == 0 for metric in session.scalars(select(WindowMetric)))


def test_snapshot_does_not_block_ingestion(database):
    from concurrent.futures import ThreadPoolExecutor

    from bluesky_ingest.detection import read_snapshot

    now = datetime(2026, 10, 18, 12, 5, tzinfo=UTC)
    with read_snapshot(database) as session:
        assert session.scalar(select(func.count()).select_from(Post)) == 0
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                database.apply, parse_event(make_event("#concurrent", now, "new")), now.isoformat()
            )
            assert future.result(timeout=3)
        # Repeatable read retains its original coherent view as new writes commit.
        assert session.scalar(select(func.count()).select_from(Post)) == 0
    with Session(database.engine) as session:
        assert session.scalar(select(func.count()).select_from(Post)) == 1


@pytest.mark.parametrize("operation", ["delete", "update"])
def test_change_during_detection_is_seen_next_run(database, monkeypatch, operation):
    import bluesky_ingest.detection as detection

    now = datetime(2026, 10, 18, 12, 5, tzinfo=UTC)
    settings = Settings(candidate_min_frequency=1)
    database.apply(
        parse_event(make_event("#aurora", now - timedelta(minutes=2), "same")), now.isoformat()
    )
    original = detection.observe

    def observe_then_change(*args):
        snapshot = original(*args)
        database.apply(
            parse_event(
                make_event("#replacement" if operation == "update" else "", now, "same", operation)
            ),
            now.isoformat(),
        )
        return snapshot

    monkeypatch.setattr(detection, "observe", observe_then_change)
    detect(database, settings, now)
    with Session(database.engine) as session:
        assert session.scalar(select(Post)).text == (
            "#replacement" if operation == "update" else None
        )
        assert session.scalar(select(func.count()).select_from(Assignment)) == (
            1 if operation == "update" else 0
        )
    monkeypatch.setattr(detection, "observe", original)
    detect(database, settings, now)
    with Session(database.engine) as session:
        assert (
            session.scalar(
                select(func.count())
                .select_from(WindowMetric)
                .join(Topic)
                .where(Topic.label == "#aurora")
            )
            == 0
        )
        if operation == "update":
            assert (
                session.scalar(
                    select(WindowMetric)
                    .join(Topic)
                    .where(
                        Topic.label == "#replacement",
                        WindowMetric.window_start == now - timedelta(minutes=5),
                    )
                ).post_count
                == 1
            )


def test_partial_publication_failure_can_be_retried(database, monkeypatch):
    import bluesky_ingest.detection as detection

    now = datetime(2026, 10, 18, 12, 5, tzinfo=UTC)
    for raw in scenario(now):
        database.apply(parse_event(raw), now.isoformat())
    original = detection.publish_topics

    def publish_then_fail(*args):
        original(*args)
        raise RuntimeError("synthetic publication failure")

    monkeypatch.setattr(detection, "publish_topics", publish_then_fail)
    with pytest.raises(RuntimeError, match="synthetic"):
        detect(database, Settings(), now)
    monkeypatch.setattr(detection, "publish_topics", original)
    detect(database, Settings(), now)
    with Session(database.engine) as session:
        latest_run = session.scalar(select(DetectionRun).order_by(DetectionRun.id.desc()))
        assert (
            session.scalar(select(func.count()).select_from(WindowMetric))
            == 13 * latest_run.topics_evaluated
        )
        assert [
            run.status for run in session.scalars(select(DetectionRun).order_by(DetectionRun.id))
        ] == ["failed", "success"]


def test_metric_publication_does_not_wait_for_ingestion_lock(database):
    from concurrent.futures import ThreadPoolExecutor

    from bluesky_ingest.detection import prune_metrics, publish_topics

    now = datetime(2026, 10, 18, 12, 5, tzinfo=UTC)
    for raw in scenario(now):
        database.apply(parse_event(raw), now.isoformat())
    detect(database, Settings(), now)
    with Session(database.engine) as session:
        metric = session.scalar(select(WindowMetric))
        row = {
            column.name: getattr(metric, column.name)
            for column in WindowMetric.__table__.columns
            if column.name != "id"
        }
    # Simulate an ingestion transaction occupying the coarse correctness lock.
    with Session(database.engine) as session, session.begin():
        database.lock(session)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(publish_topics, database, {row["topic_id"]: [row]}, set())
            future.result(timeout=3)
            executor.submit(prune_metrics, database, Settings(), now + timedelta(minutes=5)).result(
                timeout=3
            )
    with Session(database.engine) as session:
        assert session.scalar(select(func.count()).select_from(WindowMetric)) == 0
