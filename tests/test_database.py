import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from bluesky_ingest.config import Settings
from bluesky_ingest.events import parse_event
from bluesky_ingest.models import Assignment, Post, PostEventRecord, Topic
from bluesky_ingest.service import Consumer
from bluesky_ingest.synthetic import make_event

FIXTURE = Path("tests/fixtures/events.jsonl")


def test_migrations_and_idempotent_updates_deletes(database):
    assert database.ready()
    events = [parse_event(line) for line in FIXTURE.read_text().splitlines()[:3]]
    now = datetime.now(UTC).isoformat()
    assert database.apply(events[0], now)
    assert not database.apply(events[0], now)
    assert database.apply(events[1], now)
    with Session(database.engine) as session:
        assert session.get(Post, events[0].uri).text == events[1].text
    assert database.apply(events[2], now)
    assert not database.apply(events[0], now)
    with Session(database.engine) as session:
        post = session.get(Post, events[0].uri)
        assert post.text is None and post.deleted_at is not None
        assert session.scalar(select(func.count()).select_from(PostEventRecord)) == 3


def test_delete_before_older_create_keeps_tombstone(database):
    events = [parse_event(line) for line in FIXTURE.read_text().splitlines()[:3]]
    now = datetime.now(UTC).isoformat()
    database.apply(events[2], now)
    assert not database.apply(events[0], now)
    assert not database.apply(events[1], now)
    with Session(database.engine) as session:
        assert session.get(Post, events[0].uri).text is None


async def test_database_retry_and_replay_do_not_inflate_counts(database, tmp_path):
    calls = 0

    def flaky_sink(*args):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OperationalError("synthetic unavailable", {}, Exception())
        return database.apply(*args)

    settings = Settings(raw_dir=tmp_path / "raw", database_retry_seconds=0.001)
    first = Consumer(settings, sink=flaky_sink)
    await first.run(asyncio.Event(), replay=FIXTURE)
    second = Consumer(settings, sink=database.apply)
    await second.run(asyncio.Event(), replay=FIXTURE)
    assert first.metrics.database_retries == 1
    assert second.metrics.events_applied == 0
    assert second.metrics.duplicate_or_stale_events == 4
    with Session(database.engine) as session:
        assert session.scalar(select(func.count()).select_from(PostEventRecord)) == 4


async def test_exhausted_database_retries_leave_raw_for_recovery(tmp_path):
    def unavailable(*args):
        raise OperationalError("synthetic unavailable", {}, Exception())

    consumer = Consumer(
        Settings(raw_dir=tmp_path, database_retries=2, database_retry_seconds=0.001),
        sink=unavailable,
    )
    with pytest.raises(OperationalError):
        await consumer.run(asyncio.Event(), replay=FIXTURE)
    assert consumer.cursor is None
    assert not (tmp_path / "checkpoint.json").exists()
    assert len(list(tmp_path.glob("events-*.jsonl"))) == 1
    assert consumer.metrics.connection_state == "failed"


def test_concurrent_duplicate_delivery_is_idempotent(database):
    raw = FIXTURE.read_text().splitlines()[0]
    event = parse_event(raw)
    now = datetime.now(UTC).isoformat()
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: database.apply(event, now), range(2)))
    assert sorted(results) == [False, True]
    with Session(database.engine) as session:
        assert session.scalar(select(func.count()).select_from(PostEventRecord)) == 1


def test_update_reassigns_terms_without_creating_extra_activity(database):
    now = datetime.now(UTC)
    before = parse_event(make_event("#original", now, "same"))
    from datetime import timedelta

    after = parse_event(make_event("#replacement", now + timedelta(seconds=1), "same", "update"))
    database.apply(before, now.isoformat())
    database.apply(after, now.isoformat())
    with Session(database.engine) as session:
        keys = list(session.scalars(select(Topic.normalized_key).join(Assignment)))
        assert keys == ["hashtag:replacement"]
        assert session.scalar(select(func.count()).select_from(Post)) == 1


def test_batched_candidates_keep_keys_weights_and_rollback_atomically(database):
    from sqlalchemy import event

    from bluesky_ingest.candidates import extract_candidates
    from bluesky_ingest.models import TopicTerm

    now = datetime.now(UTC)
    text = "#zeta #alpha #alpha https://science.example/ earthquake"
    post = parse_event(make_event(text, now, "batch"))

    def fail_assignment(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO topic_post_assignments"):
            raise OperationalError("synthetic assignment failure", {}, Exception())

    event.listen(database.engine, "before_cursor_execute", fail_assignment)
    try:
        with pytest.raises(OperationalError):
            database.apply(post, now.isoformat())
    finally:
        event.remove(database.engine, "before_cursor_execute", fail_assignment)
    with Session(database.engine) as session:
        for model in [Post, PostEventRecord, Topic, TopicTerm, Assignment]:
            assert session.scalar(select(func.count()).select_from(model)) == 0
    assert database.apply(post, now.isoformat())
    assert not database.apply(post, now.isoformat())
    with Session(database.engine) as session:
        actual = dict(
            session.execute(
                select(Topic.normalized_key, Assignment.relevance_score).join(Assignment)
            ).all()
        )
        assert actual == {candidate.key: candidate.weight for candidate in extract_candidates(text)}
