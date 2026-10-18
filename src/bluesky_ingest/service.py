from __future__ import annotations

import asyncio
import json
import logging
import random
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import AsyncContextManager, Callable, Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from .config import Settings
from .checkpoint import Checkpoint
from .events import POST_COLLECTION, MalformedEvent, parse_event
from sqlalchemy.exc import OperationalError
from .metrics import Metrics
from .storage import RawWriter, raw_fields, replay_messages

logger = logging.getLogger(__name__)


def log_event(event: str, **fields) -> None:
    logger.info(json.dumps({"time": datetime.now(UTC).isoformat(), "event": event, **fields}))


class Socket(Protocol):
    async def recv(self) -> str | bytes: ...


Connector = Callable[[str], AsyncContextManager[Socket]]


def subscription_url(base: str, cursor: int | None) -> str:
    parts = urlsplit(base)
    query = dict(parse_qsl(parts.query))
    if "subscribeEvents" in parts.path:
        query.update(collections=POST_COLLECTION, kinds="commit")
    else:
        query.update(wantedCollections=POST_COLLECTION)
    if cursor is not None:
        query["cursor"] = str(cursor)
    return urlunsplit(parts._replace(query=urlencode(query)))


def backoff_delay(attempt: int, initial: float, maximum: float, jitter: float) -> float:
    ceiling = min(maximum, initial * 2 ** min(attempt, 30))
    # Equal jitter keeps the delay positive and bounded, even on rapid clean closes.
    return ceiling * (0.5 + 0.5 * jitter)


class Consumer:
    def __init__(self, settings: Settings, *, metrics: Metrics | None = None,
                 connector: Connector | None = None, writer: RawWriter | None = None,
                 random_value: Callable[[], float] = random.random, sink=None):
        self.settings = settings
        self.metrics = metrics if metrics is not None else Metrics()
        self.metrics.queue_capacity = settings.queue_size
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=settings.queue_size)
        self.writer = writer if writer is not None else RawWriter(
            settings.raw_dir, settings.raw_max_bytes, settings.raw_rotate_seconds)
        self.connector = connector or self._connect
        self.random_value = random_value
        self.cursor: int | None = None
        self.checkpoint = Checkpoint(settings.raw_dir, settings.jetstream_url)
        self.replaying = False
        self.sink = sink

    def _connect(self, url: str):
        return connect(url, max_size=self.settings.max_message_bytes, max_queue=16,
                       open_timeout=10, close_timeout=5, ping_interval=20, ping_timeout=20,
                       subprotocols=["xrpc.v1.json"] if "subscribeEvents" in urlsplit(url).path else None)

    async def _enqueue(self, raw: str | bytes) -> None:
        size = len(raw.encode("utf-8", errors="surrogatepass")) if isinstance(raw, str) else len(raw)
        if size > self.settings.max_message_bytes:
            raise ValueError("source message exceeds configured size limit")
        ingested_at = datetime.now(UTC).isoformat()
        self.metrics.events_received += 1
        self.metrics.last_event_at = ingested_at
        await self.queue.put((raw, ingested_at))
        self.metrics.queue_depth = self.queue.qsize()

    async def _produce(self, stop: asyncio.Event, replay: Path | None, limit: int) -> None:
        if replay is not None:
            self.metrics.connection_state = "replaying"
            for raw in replay_messages(replay, self.settings.max_message_bytes):
                if stop.is_set():
                    break
                await self._enqueue(raw)
                if limit and self.metrics.events_received >= limit:
                    break
            return
        attempt = 0
        while not stop.is_set():
            received_before = self.metrics.events_received
            self.metrics.connection_state = "connecting"
            try:
                async with self.connector(subscription_url(self.settings.jetstream_url, self.cursor)) as socket:
                    self.metrics.connection_state = "connected"
                    log_event("connected")
                    stop_task = asyncio.create_task(stop.wait())
                    try:
                        while not stop.is_set():
                            receive = asyncio.create_task(socket.recv())
                            try:
                                done, _ = await asyncio.wait({receive, stop_task}, return_when=asyncio.FIRST_COMPLETED)
                                # If receive and stop finish together, persist the received message.
                                if receive in done:
                                    await self._enqueue(receive.result())
                                    if limit and self.metrics.events_received >= limit:
                                        return
                                else:
                                    break
                            finally:
                                receive.cancel()
                                await asyncio.gather(receive, return_exceptions=True)
                    finally:
                        stop_task.cancel()
                        await asyncio.gather(stop_task, return_exceptions=True)
            except (OSError, TimeoutError, ConnectionClosed, InvalidHandshake) as exc:
                if stop.is_set():
                    break
                self.metrics.errors += 1
                self.metrics.last_error = type(exc).__name__
                log_event("stream_interrupted", error_type=type(exc).__name__)
            if stop.is_set():
                break
            if self.metrics.events_received > received_before:
                attempt = 0
            delay = backoff_delay(attempt, self.settings.backoff_initial,
                                  self.settings.backoff_max, self.random_value())
            attempt += 1
            self.metrics.retries += 1
            self.metrics.connection_state = "backoff"
            log_event("reconnect_scheduled", delay_seconds=delay)
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), delay)

    async def _persist(self) -> None:
        while True:
            item = await self.queue.get()
            self.metrics.queue_depth = self.queue.qsize()
            try:
                if item is None:
                    return
                raw, ingested_at = item
                post = None
                try:
                    post = parse_event(raw)
                    disposition = "post" if post else "ignored"
                except MalformedEvent:
                    disposition = "malformed"
                    log_event("malformed_event")
                envelope = {"schema_version": 1, "ingested_at": ingested_at,
                            "disposition": disposition, **raw_fields(raw),
                            "post": post.to_dict() if post else None}
                # One worker owns the writer; disk operations don't block HTTP or receipt.
                reference = await asyncio.to_thread(self.writer.append, envelope)
                if post and self.sink:
                    for attempt in range(self.settings.database_retries):
                        try:
                            changed = await asyncio.to_thread(self.sink, post, ingested_at, reference)
                            self.metrics.events_applied += int(changed)
                            self.metrics.duplicate_or_stale_events += int(not changed)
                            break
                        except OperationalError as exc:
                            self.metrics.errors += 1
                            self.metrics.last_error = type(exc).__name__
                            self.metrics.database_retries += 1
                            log_event("database_retry", attempt=attempt + 1)
                            if attempt + 1 == self.settings.database_retries:
                                raise
                            await asyncio.sleep(self.settings.database_retry_seconds)
                now = datetime.now(UTC)
                self.metrics.events_persisted += 1
                self.metrics.last_persisted_at = now.isoformat()
                self.metrics.processing_lag_seconds = max(0, (now - datetime.fromisoformat(ingested_at)).total_seconds())
                if post:
                    self.metrics.valid_post_events += 1
                    if post.source_time:
                        self.metrics.source_lag_seconds = max(0, (now - datetime.fromisoformat(post.source_time)).total_seconds())
                    if post.cursor is not None:
                        if not self.replaying:
                            await asyncio.to_thread(self.checkpoint.save, max(self.cursor or 0, post.cursor))
                        self.cursor = max(self.cursor or 0, post.cursor)
                elif disposition == "malformed":
                    self.metrics.malformed_events += 1
                else:
                    self.metrics.ignored_events += 1
            finally:
                self.queue.task_done()

    async def run(self, stop: asyncio.Event, *, replay: Path | None = None, limit: int = 0) -> None:
        if limit < 0:
            raise ValueError("limit must be nonnegative")
        self.replaying = replay is not None
        if not self.replaying:
            self.cursor = self.checkpoint.load()
        worker = asyncio.create_task(self._persist())
        producer = asyncio.create_task(self._produce(stop, replay, limit))
        try:
            done, _ = await asyncio.wait({worker, producer}, return_when=asyncio.FIRST_COMPLETED)
            if worker in done:
                await worker  # Fail immediately on disk errors, even when the queue is full.
                raise RuntimeError("persistence worker stopped unexpectedly")
            await producer
            self.metrics.connection_state = "draining"
            await self._drain(worker)
            self.metrics.connection_state = "stopped"
            log_event("ingestion_stopped", events_persisted=self.metrics.events_persisted)
        except BaseException as exc:
            self.metrics.connection_state = "failed"
            self.metrics.errors += 1
            self.metrics.last_error = type(exc).__name__
            log_event("ingestion_failed", error_type=type(exc).__name__)
            raise
        finally:
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)
            # Allow an outstanding disk write to finish before closing its file.
            if not worker.done():
                with suppress(Exception):
                    await self._drain(worker)
            await asyncio.gather(worker, return_exceptions=True)
            await asyncio.to_thread(self.writer.close)

    async def _drain(self, worker: asyncio.Task) -> None:
        finish = asyncio.create_task(self.queue.put(None))
        try:
            await asyncio.wait({finish, worker}, return_when=asyncio.FIRST_COMPLETED)
            if worker.done():
                await worker
                return
            await finish
            await worker
        finally:
            finish.cancel()
            await asyncio.gather(finish, return_exceptions=True)
