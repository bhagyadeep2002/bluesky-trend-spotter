import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from bluesky_ingest.config import Settings
from bluesky_ingest.database import Database
from bluesky_ingest.events import parse_event
from bluesky_ingest.maintenance import maintain
from bluesky_ingest.models import Assignment, Post, PostEventRecord, RetentionState
from bluesky_ingest.storage import RawWriter
from bluesky_ingest.synthetic import make_event


def test_raw_retention_and_budget_keep_newest_file(tmp_path):
    old = tmp_path / "events-old.jsonl"
    old.write_bytes(b"x" * 50)
    os.utime(old, (time.time() - 100, time.time() - 100))
    newer = tmp_path / "events-newer.jsonl"
    newer.write_bytes(b"x" * 50)
    writer = RawWriter(
        tmp_path, max_bytes=60, rotate_seconds=300, retention_seconds=60, budget_bytes=100
    )
    writer.append({"test": "new"})
    writer.close()
    assert not old.exists()
    assert not newer.exists()
    assert len(list(tmp_path.glob("events-*.jsonl"))) == 1


def test_six_hour_retention_prunes_rows_and_rejects_expired_replay(database):
    now = datetime(2026, 10, 18, 12, 0, tzinfo=UTC)
    old = now - timedelta(hours=6, seconds=1)
    event = parse_event(make_event("#private-old-text", old, "old"))
    keep = parse_event(make_event("#recent", now - timedelta(hours=6), "keep"))
    database.apply(event, now.isoformat())  # Backfill receipt does not extend retention.
    database.apply(keep, now.isoformat())
    assert maintain(database, Settings(), now) == 1
    with Session(database.engine) as session:
        assert session.get(Post, event.uri) is None
        assert session.get(Post, keep.uri).text == "#recent"
        assert session.scalar(select(func.count()).select_from(PostEventRecord)) == 1
        assert session.scalar(select(func.count()).select_from(Assignment)) == 1
    # The persisted watermark survives a new Database instance and clock rollback.
    restarted = Database(database.engine.url.render_as_string(hide_password=False))
    try:
        assert not restarted.apply(event, now.isoformat())
        maintain(restarted, Settings(), now - timedelta(hours=1))
        assert not restarted.apply(event, now.isoformat())
    finally:
        restarted.dispose()
    with Session(database.engine) as session:
        assert session.get(RetentionState, 1) is not None


def test_recent_delete_ordering_survives_post_expiry(database):
    now = datetime(2026, 10, 18, 12, 0, tzinfo=UTC)
    original = now - timedelta(hours=7)
    create = parse_event(make_event("#old", original, "old"))
    deletion = parse_event(make_event("", now - timedelta(minutes=1), "old", "delete"))
    database.apply(create, original.isoformat())
    database.apply(deletion, now.isoformat())
    maintain(database, Settings(), now)
    stale = parse_event(make_event("#stale", now - timedelta(minutes=2), "old"))
    assert not database.apply(stale, now.isoformat())
    with Session(database.engine) as session:
        assert session.get(Post, create.uri).text is None
    maintain(database, Settings(), now + timedelta(hours=6))
    with Session(database.engine) as session:
        assert session.get(Post, create.uri) is None


def test_idle_raw_maintenance_expires_closed_file(tmp_path):
    ticks = [0.0]
    writer = RawWriter(tmp_path, 1000, 5, clock=lambda: ticks[0], retention_seconds=60)
    writer.append({"test": "idle"})
    path = Path(writer.file.name)
    os.utime(path, (time.time() - 61, time.time() - 61))
    ticks[0] = 6
    writer.maintain()
    assert writer.file is None
    assert not path.exists()


def test_retained_sink_runs_cleanup_every_minute(database, monkeypatch):
    from bluesky_ingest.maintenance import RetainedSink

    now = datetime.now(UTC)
    ticks = [0.0]
    monkeypatch.setattr("bluesky_ingest.maintenance.time.monotonic", lambda: ticks[0])
    calls = []
    monkeypatch.setattr("bluesky_ingest.maintenance.maintain", lambda *args: calls.append(1))
    sink = RetainedSink(database, Settings())
    post = parse_event(make_event("#current", now, "keep"))
    sink(post, now.isoformat())
    ticks[0] = 59
    sink.maintain()
    assert len(calls) == 1
    ticks[0] = 60
    sink.maintain()
    assert len(calls) == 2


def test_update_does_not_extend_text_lifetime_or_revive_expired_creation(database):
    import json

    now = datetime(2026, 10, 18, 12, 0, tzinfo=UTC)
    original = now - timedelta(hours=7)
    database.apply(parse_event(make_event("#old", original, "old")), original.isoformat())
    update = parse_event(make_event("#updated", now - timedelta(minutes=1), "old", "update"))
    database.apply(update, now.isoformat())
    maintain(database, Settings(), now)
    with Session(database.engine) as session:
        post = session.get(Post, update.uri)
        assert post.text is None and post.normalized_text is None
        assert session.scalar(select(func.count()).select_from(Assignment)) == 0
    expired_creation = json.loads(make_event("#ancient", now, "new-key", "update"))
    expired_creation["commit"]["record"]["createdAt"] = original.isoformat()
    assert not database.apply(parse_event(json.dumps(expired_creation)), now.isoformat())
