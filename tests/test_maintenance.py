import os
import time
from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session

from bluesky_ingest.config import Settings
from bluesky_ingest.database import utc
from bluesky_ingest.events import parse_event
from bluesky_ingest.maintenance import maintain
from bluesky_ingest.models import Post
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


def test_text_retention_preserves_ordering_tombstone(database):
    now = datetime.now(UTC)
    old = now - timedelta(hours=49)
    event = parse_event(make_event("#private-old-text", old, "old"))
    database.apply(event, old.isoformat())
    assert maintain(database, Settings(), now) == 1
    with Session(database.engine) as session:
        post = session.get(Post, event.uri)
        assert post.text is None and post.normalized_text is None
        assert utc(post.deleted_at) == now
    assert not database.apply(event, now.isoformat())
