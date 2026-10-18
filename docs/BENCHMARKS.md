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


## Detector contention fix (2026-10-18)

A live inspection found ingestion waiting on advisory lock 847019 while detection
rebuilt approximately 200,000 metrics in a 32-second transaction. This motivated a
separate detector process, consistent SQL aggregation, input fingerprints and
small publication transactions. The final refinement also uses PostgreSQL FOR KEY
SHARE and removes ingestion's advisory lock from metric publication/pruning.
Read-only snapshots allow ingestion to commit;
changes arriving after a snapshot are reflected on the next run.

Reproduce the generated-data comparison in an isolated, automatically removed
PostgreSQL schema (no live content is copied):

```sh
uv run python tools/benchmark_detector.py --postgres-url "$TEST_DATABASE_URL"
```

Defaults: 200,000 posts, 25,000 candidates, four assignments per post (800,000),
13 metric windows and a concurrent writer targeting 40 events/second. The old
implementation is loaded from commit `980783c`. Each detector runs in its own
process; generated writer probes accumulate between stages. The writer target is
not a measurement of full-pipeline sustained throughput. This excludes raw and
checkpoint fsync and is not a six-hour soak.

The table below measures the intermediate implementation with bounded advisory-lock
publication. The final refinement removes that lock entirely; its large rerun was
interrupted during severe host contention. It is covered by targeted concurrency
tests proving publication/pruning can finish while ingestion holds the advisory
lock. These timings are not measurements of that final refinement.

| Stage | Detector seconds | Writer p95 seconds | Writer max seconds |
| --- | ---: | ---: | ---: |
| Old detector | 43.695 | 43.599 | 43.599 |
| Optimized initial cache rebuild | 25.960 | 0.0241 | 0.3756 |
| Optimized repeat, nearly unchanged | 6.867 | 0.0111 | 0.0226 |
| Optimized next completed window | 13.905 | 0.0281 | 0.1982 |

The initial rebuild still writes historical rows, but no longer holds the ingestion
lock for that entire work. The repeat stage changes only the probe candidate;
real workloads with many late updates can require more work. Each window boundary
also costs more than a repeat. These observations demonstrate shorter write waits
for this fixture, rather than establishing full-network capacity at the new
1,000,000-post analysis ceiling. Machine/DB contention affects wall times.
Complete aggregate measurements are in [detector-benchmark.json](detector-benchmark.json).
The pre/post rollout point samples are in [lag-validation.json](lag-validation.json);
counters restart at deployment, and source recovery must be read independently
from local processing latency. Live observations are recorded in PROJECT_CONTEXT.MD.


### Batched ingestion writer

After the detector rollout, live source backlog kept the queue full. The writer
also performed separate topic and term statements for every extracted candidate.
It now resolves all topic IDs with one bulk RETURNING statement, inserts terms and
assignments in batches, and keeps the entire post update atomic. A forced failure
in the assignment batch was tested to roll back the post, event key and candidates;
retry succeeds and duplicate replay remains idempotent.

The same local real-WebSocket scenario (594 unique events, 654 deliveries, one
forced disconnect, raw and checkpoint fsync enabled) was run sequentially against
native PostgreSQL with no concurrent test suite. The optional revision flag loads
only the older DB writer, using current fixtures, detector and migration paths:

```sh
uv run python tools/benchmark.py --postgres-url "$TEST_DATABASE_URL" --database-revision 285b56f --output docs/ingestion-before-batching.json
uv run python tools/benchmark.py --postgres-url "$TEST_DATABASE_URL" --output docs/ingestion-after-batching.json
```

| Writer | Deliveries/sec | p95 receipt-to-persistence | Process CPU seconds |
| --- | ---: | ---: | ---: |
| Before batching | 118.0 | 670 ms | 4.622 |
| Batched candidates | 191.5 | 384 ms | 2.825 |

Both stored exactly 594 unique events, retained the expected spike ranking and
reconnected once. These are short, synthetic fixture measurements; source events
are backdated and network coverage, live catch-up time and sustained capacity are
not established by these figures. Complete results are in the two linked JSONs.
The six-hour retention and existing live DB/checkpoint volumes are preserved.


Final live limitation: the host reported load average >140 and ~4.9GiB swap use
on an 8GiB machine during later checks. The large rerun was interrupted, its owned
schema removed and the native test cluster stopped. Docker data was preserved.
Live readiness stayed 200, but source backlog did not clear during the sampled
rollout. See aggregate samples in lag-validation.json. Do not infer sustained
capacity or resolved upstream lag from the fixture improvements.
