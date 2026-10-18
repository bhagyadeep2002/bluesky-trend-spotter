# Reproducible measurements

Measured on 2026-10-18, Python 3.14.4, macOS 27.0.1, arm64, 8 logical CPUs.
The development machine was not isolated from other workloads. These are short
fixture measurements, not sustained capacity or relay availability guarantees.
Raw result files: [PostgreSQL](benchmark-postgresql.json), [SQLite](benchmark-sqlite.json).

Each run emits 594 unique synthetic post events plus 60 duplicate deliveries
through a **real loopback WebSocket**. The server closes with 1011 midway;
the consumer reconnects once and resumes. Raw fsync, database transactions,
checkpoint fsync and a bounded queue are active. Exactly 654 deliveries are
persisted and 594 unique event rows stored (9.17% duplicates). The controlled
spike rises from 4 to 24 posts/window and is visible as #aurora through the real
FastAPI endpoint via an in-process ASGI client. Stable volume (40/window) and
two-post noise are suppressed. No external Bluesky connection is needed.

| Measurement | PostgreSQL 18 | SQLite demo adapter |
| --- | ---: | ---: |
| Ingested/persisted deliveries per second | 194.2 | 301.1 |
| Queue + raw + DB latency p50 (ms) | 332.7 | 213.9 |
| Queue + raw + DB latency p95 (ms) | 348.5 | 221.8 |
| Peak queue depth / capacity | 64 / 64 | 64 / 64 |
| Controlled reconnect recovery (ms) | 6.76 | 6.55 |
| Detector execution (ms) | 20.69 | 14.72 |
| Synthetic emission to API (seconds) | 1.738 | 1.113 |
| CPU percent of one core | 75.7% | 99.1% |
| Peak process RSS (MiB) | 104.2 | 98.5 |
| Raw bytes written | 626,988 | 626,988 |
| Raw + relation footprint growth (MiB) | 2.36 | 5.87 |
| Short-run raw growth projection (MiB/hour) | 639.3 | 990.9 |
| Short-run raw + relation growth projection (MiB/hour) | 2527.2 | 9730.6 |

Latency is receipt through successful raw/database persistence, including queue
wait; the checkpoint fsync follows that measurement. Percentiles use the bounded
rolling sample (all 654 deliveries fit within the 1,024-sample limit).

Emission-to-API latency uses real elapsed time from emitting the first spike
message until a successful API response after detector execution. The events
were backdated into an already completed window, and the API query uses a fixed
scenario clock. This excludes the live completed-window boundary wait (up to
five minutes), the detector scheduling interval (default 15 seconds), and any
live backlog. The JSON also records source-timestamp-to-API latency; its larger
value reflects those deliberately backdated source timestamps.

Disk numbers are observed **short-run footprint changes**, extrapolated linearly
before retention. PostgreSQL relation size includes tables, indexes and TOAST,
but excludes cluster WAL. SQLite includes DB, WAL and SHM files, whose initial
allocation makes its short-run projection especially unsuitable as a sustained
storage forecast. Disk use also depends on indexes, vacuum, compression,
retention and traffic mix. Re-run longer scenarios before choosing capacity.

```sh
uv sync --locked
uv run python tools/benchmark.py --output docs/benchmark-sqlite.json
uv run python tools/benchmark.py \
  --postgres-url postgresql+psycopg://bluesky:local-development-only@127.0.0.1:5432/bluesky \
  --output docs/benchmark-postgresql.json
```

The PostgreSQL benchmark creates and drops only its own randomly named schema;
use a development database with CREATE SCHEMA permission. SQLite uses a temporary
file. All temporary benchmark data is removed; no credentials are written to
result JSON. The CPU/memory sampler and test transport are included in process
measurements.

## Live Compose failure check

[Recorded result](compose-smoke.json). The isolated Docker stack successfully
built and migrated a populated database to revision 0002, served health/readiness,
connected to public Jetstream, and exposed pipeline and Prometheus metrics.
Stopping PostgreSQL made readiness return 503 and triggered bounded database
retry/backpressure; the queue reached its configured maximum of 256. Restarting
the database restored persistence/readiness. All accepted events drained before
shutdown. Restarting the app exercised inclusive checkpoint replay and observed
one duplicate delivery without inflating normalized counts.

Uvicorn intentionally re-raises SIGTERM after lifespan cleanup: Docker exit 143
is expected. The smoke checks the structured application_stopped record for
received = persisted and queue depth zero rather than interpreting that signal
exit as a persistence error. This is a short live check, not a continuous
availability or complete-capture guarantee.

```sh
APP_PORT=18080 POSTGRES_PORT=15432 docker compose -p bluesky-validation up --build -d
APP_PORT=18080 POSTGRES_PORT=15432 uv run python tools/compose-smoke.py
APP_PORT=18080 POSTGRES_PORT=15432 docker compose -p bluesky-validation down --volumes
```

This optional smoke intentionally stops/restarts the selected project's database
and app, then leaves its app stopped. Use a dedicated validation project, never
an unrelated deployment. It needs Docker and upstream network access; the pytest
suite does not depend on it. The CI workflow is supplied; it has not been run on
a remote GitHub runner in this session.
