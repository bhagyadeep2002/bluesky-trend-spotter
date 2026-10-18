import json
import asyncio
from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest

from bluesky_ingest.checkpoint import Checkpoint
from bluesky_ingest.config import Settings
from bluesky_ingest.service import Consumer


def test_checkpoint_restart_and_source_identity(tmp_path):
    checkpoint = Checkpoint(tmp_path, "ws://source")
    assert checkpoint.load() is None
    checkpoint.save(100)
    assert Checkpoint(tmp_path, "ws://source").load() == 100
    with pytest.raises(ValueError, match="source differs"):
        Checkpoint(tmp_path, "ws://another").load()


def test_failed_checkpoint_replace_keeps_previous_cursor(tmp_path):
    checkpoint = Checkpoint(tmp_path, "ws://source")
    checkpoint.save(100)
    with patch("bluesky_ingest.checkpoint.os.replace", side_effect=OSError("disk")):
        with pytest.raises(OSError):
            checkpoint.save(200)
    assert checkpoint.load() == 100


def test_corrupt_checkpoint_fails_closed(tmp_path):
    checkpoint = Checkpoint(tmp_path, "ws://source")
    checkpoint.path.write_text(json.dumps({"source": checkpoint.source, "cursor": True}))
    with pytest.raises(ValueError):
        checkpoint.load()


async def test_consumer_restarts_from_persisted_cursor(tmp_path):
    settings = Settings(raw_dir=tmp_path)
    raw = open("tests/fixtures/events.jsonl").readline()
    urls = []
    class Socket:
        async def recv(self):
            return raw
    @asynccontextmanager
    async def connector(url):
        urls.append(url)
        yield Socket()
    await Consumer(settings, connector=connector).run(asyncio.Event(), limit=1)
    await Consumer(settings, connector=connector).run(asyncio.Event(), limit=1)
    assert "cursor=1725911162329308" in urls[1]
    assert len(list(tmp_path.glob("events-*.jsonl"))) == 2
