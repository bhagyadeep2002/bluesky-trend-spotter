# Signal Watch

A local Bluesky activity early-warning system: reliable Jetstream ingestion,
replayable raw events, PostgreSQL current state, transparent spike detection,
and a dashboard that links every signal to its evidence.

![Synthetic demo overview](docs/screenshots/overview.png)

## Try it without a live connection

Requirements: Python 3.14 and [uv](https://docs.astral.sh/uv/).

```sh
uv sync --locked
uv run bluesky-serve --demo
```

Open **http://127.0.0.1:8000**. The demo uses a dedicated SQLite database at
`data/demo.db`, resets and seeds it at startup and window boundaries, and labels
all screens and synthetic evidence. It ignores `DATABASE_URL` and never opens
Bluesky. It demonstrates a topic rising from 4 to 24 posts/window, a stable topic
at 40, and low-volume noise. Counts and charts come from the database and detector.
Stop with Ctrl+C.

Installation needs package access or a populated uv cache. Runtime dashboard
assets are local HTML/CSS/JavaScript; no frontend build or npm installation is
required. FastAPI's optional interactive `/docs` page loads Swagger assets from
its CDN; `/openapi.json` is available locally.

## Live mode with Docker Compose

```sh
docker compose up --build
```

This starts PostgreSQL 18, applies Alembic migrations, and launches ingestion,
detection and the dashboard on http://127.0.0.1:8000. Health checks use database
readiness and `/readyz`. Stop the demo first if it occupies port 8000, or override
ports with `APP_PORT=8001 POSTGRES_PORT=5433 docker compose up --build`.

Default credentials are explicitly local development values. `POSTGRES_PASSWORD`
can override them; use URL-safe characters or URL-encode the password when
configuring a separate connection URL. Ports bind to loopback. Data persists in
Compose volumes. Stop with `docker compose down`; adding `--volumes` also deletes
that project's stored data. No cloud deployment or external publishing is involved.

## Live mode with local Python

Start a local PostgreSQL database, then:

```sh
export DATABASE_URL='postgresql+psycopg://bluesky:local-development-only@127.0.0.1:5432/bluesky'
uv run alembic upgrade head
uv run bluesky-serve
```

`--api-only` serves existing data and runs detection without opening the stream.
`DATABASE_URL` is required for normal server mode. A fresh database initially has
no topics; activity must meet configured thresholds. Missing history is treated
as zero, so interpret signals cautiously during baseline warmup.

The original `bluesky_firehose.py` printing demo is preserved. For capture/replay
without the dashboard, use the separate ingestion command:

```sh
# Raw capture only: leave DATABASE_URL unset.
env -u DATABASE_URL uv run bluesky-ingest --limit 100 --no-metrics

# Fully offline synthetic fixture replay.
env -u DATABASE_URL RAW_DIR=data/fixture uv run bluesky-ingest --replay tests/fixtures/events.jsonl --no-metrics

# Replay one saved raw file into a different output directory.
env -u DATABASE_URL RAW_DIR=data/replayed uv run bluesky-ingest --replay data/raw/events-REPLACE.jsonl --no-metrics
```

With `DATABASE_URL` set, the ingestion command also writes normalized records.
Its default limit is 0 (continuous); limits count all received messages, including
ignored/malformed messages. Replay accepts source JSONL or saved envelopes, one
file per invocation. Replay rotated files in filename order. It preserves source
event keys/times, assigns new local receipt timestamps, and never changes the live
checkpoint. Source ordering and stable event keys make repeated replay idempotent
in normalized storage. The standalone command does not run the full runtime's
hourly database maintenance.

## Pipeline and scoring

```text
Jetstream v2 (or legacy v1)
    → bounded async queue
    → rotating raw JSONL + fsync
    → validated current-post transaction + candidate assignments
    → atomic durable checkpoint
    → completed-window aggregates + spike scores
    → FastAPI + local dashboard
```

Creates and updates replace current state; deletes clear text and remove topic
assignments. Source timestamps/cursors and tombstones prevent older events from
resurrecting deleted posts. The event table stores deduplication keys and minimal
operational metadata, with no duplicate post text. Concurrent duplicate writes
are tested. Raw duplicate deliveries remain intentionally inspectable.

Candidates are deterministic hashtags, URL domains and conservative salient
terms, capped at 12 per post. Stop words and generic terms are excluded. Topics
are shared-term groups, not semantic event clusters. Each topic's fixed windows
include zeros and use its own recent baseline.

Default scoring uses the latest **completed five-minute window** and 12 prior
windows. Let `current` be its count and `baseline` the mean of those prior counts:

```text
ratio = (current + alpha) / (max(baseline, baseline_floor) + alpha)
increase = current - baseline
score = increase × log2(ratio)
```

The score is zero unless current ≥ 10, increase ≥ 5 and ratio ≥ 2. Defaults:
alpha = 2 and baseline floor = 1. All thresholds, baseline counts, formula,
explanation, window times, detection runs and consecutive qualifying windows are
stored. Two or more consecutive qualifying windows are labeled sustained.
The bounded horizon is recomputed so updates/deletes change current scores.
Rankings never treat old windows as current.

These are observed **activity spikes**. They do not establish truth, importance,
virality, certainty, misinformation status or sensitive user attributes.

## API and monitoring

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | Process/pipeline fatal-error health |
| `GET /readyz` | Migrated database and pipeline/detector readiness |
| `GET /api/v1/pipeline/status` | Connection, rates, lag, queue, errors, last event/detection |
| `GET /api/v1/topics/trending?window_minutes=5&limit=20` | Current ranked spikes |
| `GET /api/v1/topics/{id}` | Terms, explanation, score components, recent metrics |
| `GET /api/v1/topics/{id}/posts?limit=10&offset=0` | Available evidence in the scored window |
| `GET /api/v1/topics/{id}/timeline?limit=30` | Topic window time series |
| `GET /api/v1/search?q=aurora&limit=20&offset=0` | Literal text search across available posts |
| `GET /metrics` | Prometheus text metrics |
| `GET /openapi.json` | Response models and API schema |

Lists validate limits/offsets; topic IDs that do not exist return 404, invalid
parameters return 422, and database failures return sanitized 503 responses.
Only the configured window size is accepted. Deleted/unavailable posts are
excluded from evidence and search, even before the next detection pass.
Responses disable caching. The dashboard polls every 15 seconds and refreshes
on returning to the tab; it handles loading, empty, stale and error states.

Throughput is delivery rate averaged since process start, not unique-post rate.
Lag is the latest persisted event's receipt/source lag and can become stale during
an interruption. p50/p95 receipt-to-raw/database persistence latency uses at most
1,024 recent samples. Checkpoint fsync follows that latency measurement. Prometheus
counters reset on process restart. Health/readiness are separate from connection
availability, which is exposed in pipeline status and the dashboard.

## Configuration and storage policy

See `.env.example` for every variable. Python commands read **exported environment
variables**, not `.env` automatically. Compose reads `.env` for interpolation.

| Setting | Default |
| --- | --- |
| `JETSTREAM_URL` | Current US East v2 subscribeEvents endpoint |
| `DATABASE_URL` | Unset; required for live server mode |
| `RAW_DIR` | `data/raw` |
| `QUEUE_SIZE` / WebSocket frame buffer | 256 / 16 |
| `MAX_MESSAGE_BYTES` | 1 MiB |
| `RAW_MAX_BYTES` / `RAW_ROTATE_SECONDS` | 16 MiB / 300 seconds |
| `RAW_RETENTION_HOURS` / `RAW_BUDGET_BYTES` | 24 hours / 1 GiB |
| `POST_RETENTION_HOURS` | 48 hours since local ingestion |
| `BACKOFF_INITIAL` / `BACKOFF_MAX` | 1 / 30 seconds |
| `DATABASE_RETRIES` / `DATABASE_RETRY_SECONDS` | 5 / 1 second |
| `WINDOW_MINUTES` / `BASELINE_WINDOWS` | 5 / 12 |
| `ALPHA` / `BASELINE_FLOOR` | 2 / 1 |
| `MIN_CURRENT_COUNT` / `MIN_ABSOLUTE_INCREASE` / `MIN_RATE_RATIO` | 10 / 5 / 2 |
| `CANDIDATE_MIN_FREQUENCY` | 3 |
| `DETECT_INTERVAL_SECONDS` / `ANALYSIS_MAX_POSTS` | 15 seconds / 100,000 |
| `METRICS_HOST` / `METRICS_PORT` | `127.0.0.1` / 8000 |

Raw envelopes preserve the exact source message (base64 for binary frames), local
receipt timestamp, validation disposition and parsed metadata. Appends flush and
fsync; new filenames are exclusive and unique. Rotation occurs on the next append
by elapsed time/encoded size. One oversized envelope remains intact if it fits
the total budget. Closed raw files are pruned on new-file creation/rotation by
mtime age and budget, reserving space for the next file. This can retain less than
24 hours at high volume. Quiet/stopped processes do not run scheduled raw purges.

The full runtime runs normalized maintenance hourly: text older than 48 hours is
cleared and made unavailable; metrics/runs older than seven days and old inactive
candidates are removed. Minimal tombstones and event dedup metadata are retained
to preserve ordering/idempotency. Their database growth is not capped and needs
measurement for extended runs. Raw logs may retain subsequently deleted text until
rotation-based retention removes them; there is no immediate raw-log redaction.
Raw references can point to files that have expired. Keep captures local.

Reconnect uses bounded exponential equal jitter and an inclusive persisted cursor.
The checkpoint is `RAW_DIR/checkpoint.json`, source-specific, atomically replaced
and fsynced **after raw and enabled database writes succeed**. Corrupt or mismatched
checkpoints fail closed. A new database requires explicit replay of available raw
files; the checkpoint does not automatically rebuild downstream storage. Recovery
depends on upstream lookback; a forced crash/disk failure can leave an unacknowledged
partial final line or lose unpersisted messages. No exactly-once capture is claimed.
Use one consumer per output directory and one integrated app process.

Connection attempts time out after 10 seconds; socket close timeout is 5 seconds.
Shutdown stops receipt and drains accepted events; duration depends on disk/DB
health and queue depth. Exhausted DB retries fail ingestion and require a process
restart/replay after recovery. Uvicorn may exit 143 after graceful SIGTERM cleanup;
`application_stopped` logs confirm drained counts. No post payloads or source URLs
are included in application error logs.

## Tests and measured results

```sh
uv sync --locked
uv run pytest -q
uv run ruff check src tests migrations tools
uv run ruff format --check src tests migrations tools

# Optional PostgreSQL cases use fresh isolated schemas in this development DB.
TEST_DATABASE_URL='postgresql+psycopg://bluesky:local-development-only@127.0.0.1:5432/bluesky' uv run pytest -q

# Optional browser checks against an already running, labeled demo.
npm ci
npx playwright install chromium
npm run test:ui
```

**Validated: 72 tests plus 32 original subtests with PostgreSQL enabled.** The default offline run passed 57 tests and skipped 15 PostgreSQL cases. Without
`TEST_DATABASE_URL`, PostgreSQL-specific cases skip; the rest use temporary SQLite
files, fixtures and fake WebSockets. No pytest case contacts live Bluesky or services outside the local test environment. UI checks require local Chromium and a running demo, and close
their browser processes afterward.

Coverage includes malformed/unknown events, reconnects, graceful shutdown, bounded
queues/slow writes, fsync/disk failures, corrupt checkpoints, restart/replay,
transient/exhausted DB retries, duplicate/concurrent delivery, out-of-order updates
and deletes, assignment changes, baseline smoothing, low-volume suppression,
synthetic ranking, API validation/pagination, deleted evidence exclusion, data
retention, migration round trips and schema drift. Browser checks cover navigation,
refresh, evidence pagination, chart/table, search, error recovery, missing topics,
script-like source text and desktop/mobile fit. See [UI QA](docs/UI_QA.md).

A live Compose smoke also tested a database outage, bounded backpressure, recovery,
SIGTERM drain and inclusive checkpoint restart. The supplied CI workflow runs
backend/PostgreSQL and browser checks; it was not executed remotely in this session.

[Reproducible measurements](docs/BENCHMARKS.md) include throughput, queue/latency,
CPU/RSS, raw and relation footprint growth, duplicate rate, reconnect recovery and
synthetic detection-to-API latency. The final short PostgreSQL run persisted
**194.2 deliveries/sec**, with 594 unique events from 654 deliveries; this is a
small controlled fixture result, not sustained production capacity.

## Limits and maintenance

Coverage depends on upstream relay, filters, connectivity and retention. Missing
windows and cold-start history are not proof of absent network activity. The
latest completed window introduces up to five minutes of boundary wait, plus the
15-second detector interval and backlog. Late updates/deletes rebuild only the
recent analysis horizon. Candidate grouping is lexical and stop words are mostly
English; unrelated events can share terms. Measure detection quality on more
labeled scenarios before increasing scope.

The detector deliberately fails visibly if its 100,000-post horizon budget is
exceeded; adjust after measurement. Text retention does not cap database metadata,
WAL or all disk growth. This is a single-machine MVP with no auth, multi-node
coordination, embeddings, fact checking or production availability guarantee.

`AGENTS.MD` requires keeping `PROJECT_CONTEXT.MD` current with changes and committing
completed work. This workspace has normal Git history; temporary milestone-1 Git
fallbacks are historical. Credentials, environments, captures and test caches are
ignored. Next recommended work is a longer soak test and labeled quality evaluation,
then tuning retention/horizon budgets from the measurements.

Protocol references: [Jetstream](https://bsky.network/docs/jetstream/),
[legacy event shapes](https://github.com/bluesky-social/jetstream-legacy),
[websockets client](https://websockets.readthedocs.io/en/stable/reference/asyncio/client.html).
