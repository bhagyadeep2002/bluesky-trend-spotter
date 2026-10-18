import asyncio
import json
import tempfile
import threading
import unittest
from contextlib import asynccontextmanager
from pathlib import Path

from bluesky_ingest.config import Settings
from bluesky_ingest.service import Consumer, backoff_delay, subscription_url
from bluesky_ingest.storage import RawWriter


def rows(directory):
    return [
        json.loads(line)
        for p in sorted(directory.glob("*.jsonl"))
        for line in p.read_text().splitlines()
    ]


class FakeSocket:
    def __init__(self, messages, stop=None):
        self.messages = iter(messages)
        self.stop = stop

    async def recv(self):
        await asyncio.sleep(0)
        message = next(self.messages, None)
        if message is None:
            if self.stop:
                self.stop.set()
                await asyncio.Future()
            raise OSError("synthetic disconnect")
        return message


class SyncTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.tmp_path = Path(self.temp.name)
        self.messages = (Path(__file__).parent / "fixtures/events.jsonl").read_text().splitlines()

    def test_backoff_growth_and_cap(self):
        assert [backoff_delay(i, 1, 8, 1) for i in range(7)] == [1, 2, 4, 8, 8, 8, 8]
        assert backoff_delay(100000, 1, 8, 0) == 4

    def test_subscription_filters_and_cursor(self):
        legacy = subscription_url("ws://localhost/subscribe?cursor=42", 43)
        assert "wantedCollections=app.bsky.feed.post" in legacy and "cursor=43" in legacy
        v2 = subscription_url("ws://localhost/xrpc/network.bsky.jetstream.subscribeEvents", 1)
        assert "collections=app.bsky.feed.post" in v2 and "kinds=commit" in v2


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.tmp_path = Path(self.temp.name)
        self.messages = (Path(__file__).parent / "fixtures/events.jsonl").read_text().splitlines()

    async def test_offline_fixture(self):
        tmp_path = self.tmp_path
        consumer = Consumer(Settings(raw_dir=tmp_path, queue_size=1))
        await asyncio.wait_for(
            consumer.run(asyncio.Event(), replay=Path("tests/fixtures/events.jsonl")), 5
        )
        m = consumer.metrics
        assert (m.events_received, m.events_persisted, m.valid_post_events) == (6, 6, 4)
        assert (m.malformed_events, m.ignored_events) == (1, 1)
        assert m.queue_depth == 0
        assert m.connection_state == "stopped"
        assert all((r["ingested_at"] for r in rows(tmp_path)))

    async def test_reconnect_duplicate_and_graceful_stop(self):
        tmp_path = self.tmp_path
        messages = self.messages
        stop = asyncio.Event()
        urls, closed = ([], [])

        @asynccontextmanager
        async def connector(url):
            urls.append(url)
            try:
                if len(urls) == 1:
                    yield FakeSocket([messages[0]])
                else:
                    yield FakeSocket([messages[0], messages[1], messages[2]], stop)
            finally:
                closed.append(True)

        consumer = Consumer(
            Settings(raw_dir=tmp_path, backoff_initial=0.01, backoff_max=0.02),
            connector=connector,
            random_value=lambda: 0,
        )
        await asyncio.wait_for(consumer.run(stop), 5)
        saved = rows(tmp_path)
        assert len(saved) == 4
        assert saved[0]["post"]["event_key"] == saved[1]["post"]["event_key"]
        assert saved[3]["post"]["operation"] == "delete"
        assert "cursor=1725911162329308" in urls[1]
        assert len(closed) == 2
        assert consumer.metrics.retries == 1

    async def test_retry_connection_failure_then_success(self):
        tmp_path = self.tmp_path
        messages = self.messages
        calls = 0

        @asynccontextmanager
        async def connector(url):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OSError("no server")
            yield FakeSocket([messages[0]])

        consumer = Consumer(Settings(raw_dir=tmp_path, backoff_initial=0.001), connector=connector)
        await asyncio.wait_for(consumer.run(asyncio.Event(), limit=1), 5)
        assert calls == 2
        assert consumer.metrics.events_persisted == 1

    async def test_stop_interrupts_backoff(self):
        tmp_path = self.tmp_path
        stop = asyncio.Event()

        @asynccontextmanager
        async def connector(url):
            raise OSError("offline")
            yield

        consumer = Consumer(Settings(raw_dir=tmp_path, backoff_initial=30), connector=connector)
        task = asyncio.create_task(consumer.run(stop))

        async def wait_for_backoff():
            while consumer.metrics.connection_state != "backoff":
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_backoff(), 1)
        stop.set()
        await asyncio.wait_for(task, 1)
        assert consumer.metrics.connection_state == "stopped"

    async def test_stop_while_idle_closes_socket(self):
        tmp_path = self.tmp_path
        stop, connected = (asyncio.Event(), asyncio.Event())
        closed = []

        class IdleSocket:
            async def recv(self):
                connected.set()
                await asyncio.Future()

        @asynccontextmanager
        async def connector(url):
            try:
                yield IdleSocket()
            finally:
                closed.append(True)

        consumer = Consumer(Settings(raw_dir=tmp_path), connector=connector)
        task = asyncio.create_task(consumer.run(stop))
        await asyncio.wait_for(connected.wait(), 1)
        stop.set()
        await asyncio.wait_for(task, 1)
        assert closed == [True]

    async def test_bounded_queue_and_drain_with_slow_disk(self):
        tmp_path = self.tmp_path
        blocked, release = (threading.Event(), threading.Event())

        class SlowWriter(RawWriter):
            def append(self, envelope):
                blocked.set()
                if not release.wait(3):
                    raise TimeoutError("test did not release disk")
                super().append(envelope)

        consumer = Consumer(
            Settings(raw_dir=tmp_path, queue_size=2), writer=SlowWriter(tmp_path, 100000, 300)
        )
        task = asyncio.create_task(
            consumer.run(asyncio.Event(), replay=Path("tests/fixtures/events.jsonl"))
        )
        try:
            assert await asyncio.to_thread(blocked.wait, 1)
            for _ in range(20):
                await asyncio.sleep(0)
            assert consumer.queue.qsize() == 2
            assert consumer.metrics.events_received <= 4
            assert consumer.metrics.events_persisted == 0
        finally:
            release.set()
        await asyncio.wait_for(task, 5)
        assert consumer.metrics.events_persisted == 6

    async def test_disk_failure_fails_without_hang(self):
        for queue_size in [1, 20]:
            tmp_path = self.tmp_path
            with self.subTest(queue_size=queue_size):

                class BrokenWriter(RawWriter):
                    def append(self, envelope):
                        raise OSError("synthetic disk full")

                consumer = Consumer(
                    Settings(raw_dir=tmp_path, queue_size=queue_size),
                    writer=BrokenWriter(tmp_path, 10000, 300),
                )
                with self.assertRaises(OSError):
                    await asyncio.wait_for(
                        consumer.run(asyncio.Event(), replay=Path("tests/fixtures/events.jsonl")), 2
                    )
                assert consumer.metrics.events_persisted == 0
                assert consumer.cursor is None
                assert consumer.metrics.connection_state == "failed"

    async def test_raw_replay_roundtrip(self):
        tmp_path = self.tmp_path
        first, second = (tmp_path / "first", tmp_path / "second")
        c1 = Consumer(Settings(raw_dir=first))
        await c1.run(asyncio.Event(), replay=Path("tests/fixtures/events.jsonl"))
        c2 = Consumer(Settings(raw_dir=second))
        await c2.run(asyncio.Event(), replay=next(first.glob("*.jsonl")))
        assert [r["raw"] for r in rows(first)] == [r["raw"] for r in rows(second)]
        assert [r["post"] for r in rows(first)] == [r["post"] for r in rows(second)]


class IdleMaintenanceTests(unittest.IsolatedAsyncioTestCase):
    async def test_idle_worker_runs_raw_and_database_cleanup(self):
        from unittest.mock import Mock, patch

        writer, maintenance = Mock(), Mock()
        consumer = Consumer(Settings(), writer=writer, maintenance=maintenance)
        await consumer.queue.put(None)
        first = True

        async def timeout_then_get(awaitable, timeout):
            nonlocal first
            if first:
                first = False
                awaitable.close()
                raise TimeoutError
            return await awaitable

        with patch("bluesky_ingest.service.asyncio.wait_for", timeout_then_get):
            await consumer._persist()
        writer.maintain.assert_called_once_with()
        maintenance.assert_called_once_with()
        assert consumer.queue._unfinished_tasks == 0

    async def test_idle_cleanup_failure_is_not_swallowed(self):
        from unittest.mock import Mock, patch

        writer = Mock()
        writer.maintain.side_effect = OSError("synthetic disk failure")
        consumer = Consumer(Settings(), writer=writer)

        async def timeout(awaitable, timeout):
            awaitable.close()
            raise TimeoutError

        with patch("bluesky_ingest.service.asyncio.wait_for", timeout):
            with self.assertRaises(OSError):
                await consumer._persist()
