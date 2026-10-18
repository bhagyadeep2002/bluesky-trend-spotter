from __future__ import annotations

import argparse
import asyncio
import logging
import tempfile
from contextlib import asynccontextmanager, suppress
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import uvicorn
from sqlalchemy import delete
from sqlalchemy.orm import Session

from .api import create_app
from .config import Settings
from .database import Database
from .detection import detect, floor_window
from .metrics import Metrics
from .models import Assignment, DetectionRun, Post, PostEventRecord, Topic, TopicTerm, WindowMetric
from .service import Consumer, log_event
from .synthetic import scenario


async def seed_demo(database: Database, settings: Settings, metrics: Metrics) -> None:
    # This is called only for the CLI's dedicated SQLite demo database.
    def clear_demo():
        with Session(database.engine) as session, session.begin():
            for model in (
                Assignment,
                WindowMetric,
                TopicTerm,
                PostEventRecord,
                Post,
                Topic,
                DetectionRun,
            ):
                session.execute(delete(model))

    await asyncio.to_thread(clear_demo)
    with tempfile.TemporaryDirectory(prefix="bluesky-demo-") as temporary:
        fixture = Path(temporary) / "scenario.jsonl"
        fixture.write_text(
            "\n".join(
                scenario(datetime.now(UTC), settings.baseline_windows, settings.window_minutes)
            )
            + "\n"
        )
        consumer = Consumer(settings, metrics=metrics, sink=database.apply)
        await consumer.run(asyncio.Event(), replay=fixture)
    metrics.connection_state = "demo"


def application(settings: Settings, *, demo: bool = False, stream: bool = True):
    if not settings.database_url:
        raise ValueError("Set DATABASE_URL for live mode or use --demo")
    database = Database(settings.database_url)
    metrics = Metrics(queue_capacity=settings.queue_size)

    @asynccontextmanager
    async def lifespan(app):
        stop = asyncio.Event()
        try:
            if not await asyncio.to_thread(database.ready):
                raise RuntimeError("Apply database migrations before launching")
        except Exception:
            database.dispose()
            raise RuntimeError(
                "Database unavailable or migrations missing; run alembic upgrade head"
            ) from None
        if demo:
            await seed_demo(database, settings, metrics)
        elif not stream:
            metrics.connection_state = "idle"

        async def detector():
            demo_boundary = floor_window(datetime.now(UTC), settings.window_minutes)
            while not stop.is_set():
                try:
                    boundary = floor_window(datetime.now(UTC), settings.window_minutes)
                    if demo and boundary != demo_boundary:
                        await seed_demo(database, settings, metrics)
                        demo_boundary = boundary
                    metrics.topics_evaluated = await asyncio.to_thread(detect, database, settings)
                    metrics.last_detection_at = datetime.now(UTC).isoformat()
                    metrics.detection_error = None
                except Exception as exc:
                    metrics.errors += 1
                    metrics.detection_error = type(exc).__name__
                    log_event("detection_failed", error_type=type(exc).__name__)
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), settings.detect_interval_seconds)

        detection_task = asyncio.create_task(detector())
        ingest_task = None
        if stream and not demo:
            ingest_task = asyncio.create_task(
                Consumer(settings, metrics=metrics, sink=database.apply).run(stop)
            )

            def ingestion_done(task):
                if not task.cancelled() and task.exception():
                    metrics.connection_state = "failed"
                    metrics.last_error = type(task.exception()).__name__
                    log_event("ingestion_requires_restart", error_type=metrics.last_error)

            ingest_task.add_done_callback(ingestion_done)
        try:
            yield
        finally:
            stop.set()
            await asyncio.gather(
                detection_task, *([ingest_task] if ingest_task else []), return_exceptions=True
            )
            database.dispose()

    return create_app(database, settings, metrics, demo=demo, lifespan=lifespan)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Signal Watch's local dashboard and pipeline")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="use a separate seeded SQLite demo, no external connection",
    )
    parser.add_argument(
        "--api-only", action="store_true", help="serve existing data without opening a stream"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("websockets").setLevel(logging.CRITICAL)
    try:
        settings = Settings.from_env()
        if args.demo:
            settings = replace(
                settings, database_url="sqlite:///data/demo.db", raw_dir=Path("data/demo-raw")
            )
            database = Database(settings.database_url)
            database.migrate()
            database.dispose()
        app = application(settings, demo=args.demo, stream=not args.api_only)
        uvicorn.run(
            app,
            host=settings.metrics_host,
            port=settings.metrics_port,
            log_config=None,
            access_log=False,
        )
    except Exception as exc:
        log_event("startup_failed", error_type=type(exc).__name__)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
