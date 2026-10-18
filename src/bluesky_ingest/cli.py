from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from contextlib import nullcontext
from pathlib import Path

import uvicorn

from .config import Settings
from .metrics import Metrics, create_app
from .service import Consumer, log_event


class MetricsServer(uvicorn.Server):
    # The CLI owns SIGINT/SIGTERM so ingestion can drain its bounded queue.
    def capture_signals(self):
        return nullcontext()

    async def serve(self, sockets=None):
        try:
            await super().serve(sockets=sockets)
        except SystemExit as exc:
            # Uvicorn exits on bind errors. Convert this to a task error so the
            # CLI can stop receipt and drain accepted events before exiting.
            raise RuntimeError("metrics server startup failed") from exc


async def launch(settings: Settings, replay: Path | None, limit: int, serve_metrics: bool) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    metrics = Metrics()
    consumer = Consumer(settings, metrics=metrics)
    server = MetricsServer(uvicorn.Config(create_app(metrics), host=settings.metrics_host,
                                         port=settings.metrics_port, log_config=None, access_log=False))
    ingest_task = asyncio.create_task(consumer.run(stop, replay=replay, limit=limit))
    server_task = asyncio.create_task(server.serve()) if serve_metrics else None
    failure: str | None = None
    try:
        if server_task:
            await asyncio.wait({ingest_task, server_task}, return_when=asyncio.FIRST_COMPLETED)
            if server_task.done() and not ingest_task.done():
                stop.set()
                await server_task
                raise RuntimeError("metrics server stopped before ingestion")
        await ingest_task
    except Exception as exc:
        failure = type(exc).__name__
        raise
    finally:
        stop.set()
        server.should_exit = True
        await asyncio.gather(ingest_task, *([server_task] if server_task else []), return_exceptions=True)
        if failure and metrics.connection_state != "failed":
            metrics.connection_state = "failed"
            metrics.errors += 1
            metrics.last_error = failure
            log_event("service_failed", error_type=failure)
        log_event("final_metrics", **metrics.snapshot())
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


def main() -> None:
    parser = argparse.ArgumentParser(description="Persist and validate public Jetstream post events")
    parser.add_argument("--replay", type=Path, help="replay source JSONL or a saved raw event file offline")
    parser.add_argument("--limit", type=int, default=0, help="stop after N messages; 0 means unlimited")
    parser.add_argument("--no-metrics", action="store_true", help="disable the local HTTP metrics server")
    args = parser.parse_args()
    if args.limit < 0:
        parser.error("--limit must be nonnegative")
    try:
        settings = Settings.from_env()
    except ValueError as exc:
        parser.error(str(exc))
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    # Our structured messages contain error classes only. Suppress third-party
    # connection logs which may include URL query values or exception payloads.
    logging.getLogger("websockets").setLevel(logging.CRITICAL)
    try:
        asyncio.run(launch(settings, args.replay, args.limit, not args.no_metrics))
    except (OSError, ValueError, RuntimeError):
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
