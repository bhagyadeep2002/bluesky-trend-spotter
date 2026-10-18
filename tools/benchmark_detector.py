"""Compare detector write contention using generated data in an owned PG schema.

uv run python tools/benchmark_detector.py --postgres-url postgresql+psycopg://bluesky@127.0.0.1:55432/bluesky_test
No live content is read or written. Each stage uses a separate detector process.
"""

import argparse
import hashlib
import importlib.util
import json
import multiprocessing
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, insert, text
from sqlalchemy.engine import make_url

from bluesky_ingest.config import Settings
from bluesky_ingest.database import Database
from bluesky_ingest.detection import detect, floor_window
from bluesky_ingest.events import parse_event
from bluesky_ingest.models import Assignment, Post, PostEventRecord, Topic
from bluesky_ingest.synthetic import make_event


def seed(database, now, posts, topics):
    start = floor_window(now, 5) - timedelta(minutes=125)
    with database.engine.begin() as connection:
        for offset in range(0, topics, 5000):
            connection.execute(
                insert(Topic),
                [
                    dict(
                        id=i + 1,
                        label=f"#synthetic{i}",
                        normalized_key=f"hashtag:synthetic{i}",
                        created_at=start,
                        last_seen_at=now,
                        active=True,
                    )
                    for i in range(offset, min(offset + 5000, topics))
                ],
            )
        for offset in range(0, posts, 2000):
            batch_posts, events, assignments = [], [], []
            for i in range(offset, min(offset + 2000, posts)):
                at = start + timedelta(
                    minutes=((i // topics * 7 + i % 17) % 25) * 5, seconds=i % 200
                )
                uri = f"at://did:plc:synthetic/app.bsky.feed.post/load-{i}"
                key = hashlib.sha256(uri.encode()).hexdigest()
                batch_posts.append(
                    dict(
                        uri=uri,
                        author_did="did:plc:synthetic",
                        text="Synthetic load fixture",
                        normalized_text="Synthetic load fixture",
                        created_at=at,
                        indexed_at=at,
                        activity_at=at,
                        ingested_at=now,
                        event_key=key,
                    )
                )
                events.append(
                    dict(
                        event_key=key,
                        uri=uri,
                        author_did="did:plc:synthetic",
                        operation="create",
                        source_time=at,
                        ingested_at=now,
                    )
                )
                for term in range(4):
                    assignments.append(
                        dict(
                            topic_id=(i + term * 7919) % topics + 1,
                            post_uri=uri,
                            assigned_at=now,
                            relevance_score=1,
                        )
                    )
            connection.execute(insert(Post), batch_posts)
            connection.execute(insert(PostEventRecord), events)
            connection.execute(insert(Assignment), assignments)
        # Explicit seed IDs must leave the PostgreSQL sequence above the fixture.
        connection.execute(
            text(
                "SELECT setval(pg_get_serial_sequence('topic_candidates', 'id'), (SELECT max(id) FROM topic_candidates))"
            )
        )


def run_detector(url, settings, now, baseline_path):
    database = Database(url)
    started = time.perf_counter()
    try:
        function = detect
        if baseline_path:
            spec = importlib.util.spec_from_file_location("bluesky_ingest._baseline", baseline_path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)
            function = module.detect
        evaluated = function(database, settings, now)
        return dict(seconds=time.perf_counter() - started, topics_evaluated=evaluated)
    finally:
        database.dispose()


def stage(pool, database, url, settings, now, name, baseline=None):
    stop = threading.Event()
    latencies, failures = [], []

    def ingest():
        try:
            i = 0
            while not stop.is_set():
                before = time.perf_counter()
                event = parse_event(
                    make_event("#syntheticprobe", now - timedelta(minutes=2), f"{name}-{i}")
                )
                database.apply(event, now.isoformat())
                latencies.append(time.perf_counter() - before)
                i += 1
                stop.wait(max(0, 0.025 - latencies[-1]))
        except Exception as exc:
            failures.append(type(exc).__name__)

    writer = threading.Thread(target=ingest)
    writer.start()
    try:
        result = pool.submit(run_detector, url, settings, now, baseline).result(timeout=180)
    finally:
        stop.set()
        writer.join(timeout=30)
    assert not writer.is_alive() and not failures
    ordered = sorted(latencies)
    result.update(
        probe_writes=len(ordered),
        probe_target_per_second=40,
        probe_latency_p95_seconds=ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))],
        probe_latency_max_seconds=max(ordered),
    )
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--postgres-url", required=True)
    parser.add_argument("--posts", type=int, default=200_000)
    parser.add_argument("--topics", type=int, default=25_000)
    parser.add_argument("--baseline-revision", default="980783c")
    parser.add_argument("--output", type=Path, default=Path("docs/detector-benchmark.json"))
    args = parser.parse_args()
    schema = "detector_bench_" + uuid4().hex
    admin = create_engine(args.postgres_url)
    with admin.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    url = (
        make_url(args.postgres_url)
        .update_query_dict({"options": f"-csearch_path={schema}"})
        .render_as_string(hide_password=False)
    )
    database = Database(url)
    try:
        database.migrate()
        now = datetime.now(UTC)
        seed(database, now, args.posts, args.topics)
        settings = Settings(analysis_max_posts=1_000_000)
        with tempfile.TemporaryDirectory(prefix="detector-baseline-") as temporary:
            baseline = Path(temporary) / "baseline.py"
            baseline.write_text(
                subprocess.run(
                    ["git", "show", f"{args.baseline_revision}:src/bluesky_ingest/detection.py"],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout
            )
            with ProcessPoolExecutor(
                max_workers=1, mp_context=multiprocessing.get_context("spawn")
            ) as pool:
                result = dict(
                    measured_at=now.isoformat(),
                    posts=args.posts,
                    candidate_topics=args.topics,
                    assignments=args.posts * 4,
                    baseline_revision=args.baseline_revision,
                    stages={},
                )
                for name, at, source in [
                    ("baseline", now, str(baseline)),
                    ("optimized_bootstrap", now, None),
                    ("optimized_steady", now, None),
                    ("optimized_next_window", now + timedelta(minutes=5), None),
                ]:
                    result["stages"][name] = stage(pool, database, url, settings, at, name, source)
                    print(name, json.dumps(result["stages"][name]), flush=True)
        result["limitations"] = (
            "Generated PostgreSQL fixture, four assignments/post, one process per detector and a 40/sec writer probe; not full live ingestion, raw/checkpoint fsync or six-hour soak. The concurrent probe changes one candidate, so steady-state is nearly unchanged. Snapshot scores converge on a later run for concurrent events. Baseline and optimized stages share data; probe posts accumulate. Wall times include detector work, not worker startup."
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    finally:
        database.dispose()
        with admin.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


if __name__ == "__main__":
    main()
