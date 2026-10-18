"""Reproducible synthetic ingestion, real local WebSocket recovery and API benchmark.

Run: uv run python tools/benchmark.py --output docs/benchmark-sqlite.json
Add --postgres-url URL for an isolated temporary PostgreSQL schema.
"""

import argparse
import asyncio
import importlib.util
import json
import platform
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import httpx
import psutil
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session
from websockets.asyncio.server import serve

from bluesky_ingest.api import create_app
from bluesky_ingest.config import Settings
from bluesky_ingest.database import Database
from bluesky_ingest.detection import detect, floor_window
from bluesky_ingest.models import PostEventRecord
from bluesky_ingest.service import Consumer
from bluesky_ingest.synthetic import scenario


async def benchmark(database: Database, directory: Path) -> dict:
    database.migrate()

    def relation_bytes():
        if database.engine.dialect.name == "postgresql":
            with database.engine.connect() as connection:
                return connection.scalar(
                    text(
                        "SELECT coalesce(sum(pg_total_relation_size((quote_ident(schemaname) || '.' || quote_ident(tablename))::regclass)),0) FROM pg_tables WHERE schemaname=current_schema()"
                    )
                )
        path = Path(database.engine.url.database)
        return sum(
            item.stat().st_size for item in path.parent.glob(path.name + "*") if item.is_file()
        )

    before_relation_bytes = int(relation_bytes())
    now = datetime.now(UTC)
    unique = sorted(scenario(now), key=lambda raw: json.loads(raw)["time_us"])
    messages = [
        item for i, raw in enumerate(unique) for item in ([raw, raw] if i % 10 == 0 else [raw])
    ]
    halfway = len(messages) // 2
    metrics_holder = {}
    timing = {}
    connections = 0
    current_start = floor_window(now, 5) - timedelta(minutes=5)

    async def source(socket):
        nonlocal connections
        connections += 1
        if connections == 2:
            timing["recovery_seconds"] = time.perf_counter() - timing["disconnect"]
        chunk = messages[:halfway] if connections == 1 else messages[halfway:]
        for raw in chunk:
            event = json.loads(raw)
            if "#aurora" in event["commit"]["record"]["text"] and event["time_us"] >= int(
                current_start.timestamp() * 1_000_000
            ):
                timing.setdefault("spike_emitted", time.perf_counter())
                timing.setdefault("spike_source_time", event["time_us"] / 1_000_000)
            await socket.send(raw)
        if connections == 1:
            while metrics_holder["consumer"].metrics.events_persisted < halfway:
                await asyncio.sleep(0.001)
            timing["disconnect"] = time.perf_counter()
            await socket.close(1011, "synthetic interruption")
        else:
            await socket.wait_closed()

    process = psutil.Process()
    peak = {"rss": process.memory_info().rss, "queue": 0}
    cpu_before = process.cpu_times()
    start = time.perf_counter()
    done = asyncio.Event()

    async def sample():
        while not done.is_set():
            peak["rss"] = max(peak["rss"], process.memory_info().rss)
            if "consumer" in metrics_holder:
                peak["queue"] = max(peak["queue"], metrics_holder["consumer"].queue.qsize())
            await asyncio.sleep(0.001)

    sampler = asyncio.create_task(sample())
    try:
        async with serve(source, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            settings = Settings(
                jetstream_url=f"ws://127.0.0.1:{port}/subscribe",
                raw_dir=directory,
                queue_size=64,
                backoff_initial=0.01,
                backoff_max=0.02,
            )
            consumer = Consumer(settings, sink=database.apply, random_value=lambda: 0)
            metrics_holder["consumer"] = consumer
            await asyncio.wait_for(consumer.run(asyncio.Event(), limit=len(messages)), 60)
        ingestion_seconds = time.perf_counter() - start
        detect_start = time.perf_counter()
        detect(database, settings, now)
        detector_seconds = time.perf_counter() - detect_start
        app = create_app(database, settings, demo=True, clock=lambda: now)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://local"
        ) as client:
            result = (await client.get("/api/v1/topics/trending")).json()
        assert result["items"][0]["topic"]["label"] == "#aurora"
        observed = time.perf_counter()
        with Session(database.engine) as session:
            event_count = session.scalar(select(func.count()).select_from(PostEventRecord))
        assert event_count == len(unique)
        assert consumer.metrics.retries == 1
        assert (
            consumer.metrics.events_received == consumer.metrics.events_persisted == len(messages)
        )
        cpu_after = process.cpu_times()
        cpu_seconds = cpu_after.user + cpu_after.system - cpu_before.user - cpu_before.system
        raw_bytes = sum(path.stat().st_size for path in directory.glob("events-*.jsonl"))
        after_relation_bytes = int(relation_bytes())
        footprint_growth = raw_bytes + max(0, after_relation_bytes - before_relation_bytes)
        snapshots = consumer.metrics.snapshot()
        return {
            "measured_at": datetime.now(UTC).isoformat(),
            "python": platform.python_version(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "cpu_count": psutil.cpu_count(),
            "database": database.engine.dialect.name,
            "source": "local real WebSocket; deterministic synthetic scenario",
            "source_unique_events": len(unique),
            "received_deliveries": len(messages),
            "persisted_deliveries": consumer.metrics.events_persisted,
            "stored_unique_events": event_count,
            "ingestion_seconds": ingestion_seconds,
            "events_received_per_second": len(messages) / ingestion_seconds,
            "events_persisted_per_second": consumer.metrics.events_persisted / ingestion_seconds,
            "duplicate_rate": (len(messages) - event_count) / len(messages),
            "latency_p50_seconds": snapshots["latency_p50_seconds"],
            "latency_p95_seconds": snapshots["latency_p95_seconds"],
            "peak_queue_depth": peak["queue"],
            "queue_capacity": 64,
            "latest_source_lag_seconds": consumer.metrics.source_lag_seconds,
            "reconnect_count": consumer.metrics.retries,
            "recovery_seconds": timing["recovery_seconds"],
            "detector_seconds": detector_seconds,
            "synthetic_spike_emission_to_api_seconds": observed - timing["spike_emitted"],
            "synthetic_source_spike_to_api_seconds": datetime.now(UTC).timestamp()
            - timing["spike_source_time"],
            "peak_process_rss_bytes": peak["rss"],
            "process_cpu_seconds": cpu_seconds,
            "average_cpu_percent_of_one_core": cpu_seconds / (observed - start) * 100,
            "raw_bytes": raw_bytes,
            "normalized_relation_bytes_before": before_relation_bytes,
            "normalized_relation_bytes_after": after_relation_bytes,
            "raw_plus_relation_growth_bytes": footprint_growth,
            "raw_plus_relation_growth_bytes_per_hour_at_fixture_rate": footprint_growth
            / ingestion_seconds
            * 3600,
            "raw_bytes_per_hour_at_measured_fixture_rate": raw_bytes / ingestion_seconds * 3600,
            "limitations": "Short local run, not production capacity. Latency includes queue/raw/DB time; checkpoint fsync follows it. Spike events are backdated into a completed window; live boundary/scheduler waits are excluded from emission-to-API time. Disk growth is a linear projection before retention. Relation footprint includes PostgreSQL tables/indexes/toast but excludes cluster WAL; SQLite includes database/WAL/SHM files. RSS includes Python/API dependencies. Recovery is a controlled local 1011 close, not relay availability.",
        }
    finally:
        done.set()
        await sampler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--postgres-url", help="test database URL; a new owned schema is created then removed"
    )
    parser.add_argument("--output", type=Path, default=Path("docs/benchmark-sqlite.json"))
    parser.add_argument(
        "--database-revision", help="compare the database writer from a local Git revision"
    )
    args = parser.parse_args()
    admin = None
    schema = "benchmark_" + uuid4().hex
    with tempfile.TemporaryDirectory(prefix="bluesky-benchmark-") as temporary:
        database_type = Database
        if args.database_revision:
            source = Path(temporary) / "database_baseline.py"
            source.write_text(
                subprocess.run(
                    ["git", "show", f"{args.database_revision}:src/bluesky_ingest/database.py"],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout
            )
            spec = importlib.util.spec_from_file_location(
                "bluesky_ingest._database_baseline", source
            )
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)
            database_type = module.Database
            # The temporary source path is not the repository migration root.
            database_type.migrate = Database.migrate
        if args.postgres_url:
            admin = create_engine(args.postgres_url)
            with admin.begin() as connection:
                connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            url = make_url(args.postgres_url).update_query_dict(
                {"options": f"-csearch_path={schema}"}
            )
            database = database_type(url.render_as_string(hide_password=False))
        else:
            database = database_type(f"sqlite:///{temporary}/benchmark.db")
        try:
            result = asyncio.run(benchmark(database, Path(temporary) / "raw"))
            result["database_writer_revision"] = args.database_revision or "working-tree"
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(result, indent=2) + "\n")
            print(json.dumps(result, indent=2))
        finally:
            database.dispose()
            if admin:
                with admin.begin() as connection:
                    connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
                admin.dispose()


if __name__ == "__main__":
    main()
