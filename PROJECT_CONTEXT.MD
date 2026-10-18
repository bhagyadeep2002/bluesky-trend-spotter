# Project context

Updated: 2026-10-18. Keep this file current whenever code, configuration,
dependencies, workflows, documentation or validation change. See AGENTS.MD.

## Objective and status

The user authorized completing the full MVP in bluesky_early_warning_codex_brief.md,
testing each stage before advancing. **All functional MVP stages are complete.** The follow-up adds rolling six-hour
retention, stronger filler-word filtering and startup baseline gating. The updated
live Compose app remains on port 8001; the separate demo remains on port 8000.
Final setup, limitations, commands and acceptance evidence are in README.md and
its linked documents. No remote deployment or repository push was requested.

Stage history (tested before advancing): ingestion/checkpoints (36 tests),
PostgreSQL/current-state writes (43), candidates/scoring (49), topic API (55),
integrated dashboard/runtime (60), operations/failure/migration coverage (72).
Each stage was committed separately. The original bluesky_firehose.py is preserved.

## Architecture

Python 3.14, uv/uv.lock, FastAPI, websockets, SQLAlchemy 2.0.55, Alembic 1.20.0,
psycopg 3.3.6 and PostgreSQL 18. SQLite is an isolated test/offline-demo adapter.
No Kafka, embedding service, cloud infrastructure or frontend build is needed.

- src/bluesky_ingest/events.py: legacy/v2 post create/update/delete validation,
  normalization, stable canonical event keys, malformed/unknown handling.
- service.py/storage.py/checkpoint.py: bounded queue and frame/message limits,
  structured payload-free logs, reconnect backoff/jitter, raw JSONL fsync/rotation,
  transient DB retries, atomic source-specific checkpoints after raw + DB success.
- models.py/database.py: current posts, minimal dedup event records, topics/terms,
  assignments, window metrics and detection runs. Source order and tombstones
  protect against stale resurrection; deletes clear current text/assignments.
  Revision 0003 adds a monotonic retention cutoff so expired replay stays rejected
  after old deduplication keys and tombstones are pruned.
- candidates.py/detection.py: capped deterministic hashtags/domains/terms, stop
  words, zero-filled completed windows, smoothed ratio, absolute/volume gates,
  reproducible score components, consecutive-window persistence, run records.
- api.py/schemas.py: response models, input validation, pagination, sanitized DB
  errors, no-store responses, topic/evidence/search routes, health/readiness and
  Prometheus text metrics. Deleted posts never enter new evidence/search results.
- runtime.py/static/: combined app lifecycle, detector/maintenance tasks and local
  responsive HTML/CSS/JS dashboard. Updates every 15s and when a tab regains focus.
  Demo always uses separate data/demo.db and synthetic data with visible labels.
- migrations/: revision 0001 initial schema; 0002 PostgreSQL 64-bit generated
  identifiers AND sequences (SQLite INTEGER rowids already support 64 bits);
  0003 adds retention_state (one monotonic cutoff row).
- maintenance.py: rolling six-hour source-activity expiry, minute cleanup of text,
  assignments, old posts/events/metrics/runs/topics, and a persistent cutoff.
  RetainedSink also runs cleanup on standalone ingestion receipt/idle ticks.

Scoring defaults: five-minute latest completed window, 12 baseline windows,
alpha=2, floor=1, current>=10, increase>=5, ratio>=2; score=increase*log2(ratio)
when eligible. Candidate minimum horizon frequency=3, at most 12 terms/post.
Detector runs every 15 seconds and rejects horizons above 100,000 posts. Typical
timeline contains the latest 13 recomputed windows; baseline counts are stored
in each metric. Rankings are withheld until the earliest observed source window
precedes the current window by 12 baseline windows (roughly one hour of startup
warmup). API exposes baseline_ready, baseline_minutes_required and retention_hours;
the dashboard explains warmup and displays retention. Counts inside that observed
span are still zero-filled; this does not verify uninterrupted network coverage.
Plain-term stop words now include spelled-out numbers and all screenshot fillers;
explicit hashtags (e.g. #Love), domains and terms such as Trump remain eligible.
Detector removes previously extracted filler topics when upgrading existing data.

## Commands

```sh
uv sync --locked
uv run bluesky-serve --demo
# Open http://127.0.0.1:8000

# Full live app with managed local PostgreSQL, migrations and health checks:
docker compose up --build

# Or use a separately configured local database:
export DATABASE_URL='postgresql+psycopg://bluesky:local-development-only@127.0.0.1:5432/bluesky'
uv run alembic upgrade head
uv run bluesky-serve
# --api-only serves existing data without a stream connection.

uv run pytest -q
TEST_DATABASE_URL='postgresql+psycopg://bluesky:local-development-only@127.0.0.1:5432/bluesky' uv run pytest -q
uv run ruff check src tests migrations tools
uv run ruff format --check src tests migrations tools
npm ci && npx playwright install chromium
npm run test:ui  # Requires running, explicitly labeled demo; no live screenshots.
```

Environment variables are listed in .env.example. Python does not automatically
load .env. Without DATABASE_URL the standalone bluesky-ingest command is raw-only;
with it, capture/replay also applies normalized transactions. Replay never advances
a live checkpoint. A new downstream DB requires explicit raw replay; a cursor alone
does not rebuild history. Use a distinct output directory for replay.

## Validated results

- Original MVP full backend suite: **72 passed, 32 original subtests**, PostgreSQL cases
  enabled, in 14.22 seconds. Includes migration upgrade/downgrade and drift checks,
  concurrent duplicates, out-of-order deletes, term reassignment, retries/raw
  recovery, bounded slow writes, analysis budget, retention, API and runtime tests.
- Ruff checks/format pass, uv lock consistency and installed dependency compatibility
  pass, Compose configuration passes. Remote GitHub Actions was not run; supplied
  workflow runs local-equivalent backend/PostgreSQL/browser checks.
- Standalone Playwright Chromium UI checks passed: overview/detail navigation,
  refresh, evidence pagination, timeline table, search/empty state, API docs route,
  error recovery, missing topic, safe script-like source text, desktop/mobile fit.
  Final screenshots were reviewed; selected synthetic-only images are committed in
  docs/screenshots. No horizontal page overflow at 390px. See docs/UI_QA.md.
- The in-app Browser had no available connection and its Node runtime could not
  launch Chromium; standalone npm QA succeeded. npm is optional QA tooling only.
- Live v2 10-event capture passed. Separate Compose stack built, started, migrated
  a populated PostgreSQL DB to 0002, ingested real posts, and passed actual HTTP
  health/readiness/metrics checks. Controlled DB outage returned readiness 503,
  queue peaked at its 256 limit, persistence recovered, SIGTERM drained all accepted
  events, and restart observed an inclusive-cursor duplicate. docs/compose-smoke.json
  records 278 received/persisted at shutdown and one restart duplicate.
- Uvicorn intentionally re-raises SIGTERM after lifespan cleanup; Docker exit 143
  is expected. application_stopped logs showed equal counts and queue depth zero.
- Short real-local-WebSocket benchmark: 594 unique events, 654 deliveries, one forced
  1011 disconnect. PostgreSQL: 194.2 persisted deliveries/sec, p95 348.5ms, peak queue
  64/64, ~104.2MiB RSS. Stable high volume and two-post noise did not outrank the
  4→24 spike. SQLite results and CPU/storage/latency/recovery evidence are in
  docs/BENCHMARKS.md and JSONs. These are short fixture measurements, not sustained
  throughput or complete capture guarantees; source events are backdated.

## Retention and limits

Raw defaults: six-hour file-mtime TTL and 1GiB budget, enforced at rotation and
idle one-minute ticks in the persistence worker. The active file is never unlinked;
idle rotation closes it safely. Chunk expiry can retain text for TTL plus rotation
and cleanup delay, and the byte budget may shorten retention. Raw age is based on
receipt/file time. Stopped processes do not schedule purges; deleted text is not
immediately redacted from raw logs. Raw references can expire.

Normalized retention defaults to six hours of first observed source activity;
updates do not extend text life. The integrated runtime runs maintenance at startup
and every minute. Standalone ingestion uses RetainedSink on receipt and idle minute
ticks. Old post rows, event records, assignments, metrics, runs and topics are
pruned. A recent update/delete can keep only ordering metadata until its latest
source timestamp also expires after six hours. A monotonic, persisted cutoff
rejects expired replay after metadata removal (including non-delete records with
old client createdAt). Increasing retention cannot recover discarded data. Cleanup
can be delayed by backlog, outages or stopped processes. This bounds age, not bytes;
PostgreSQL file size, indexes, WAL and backups are not globally capped. Six-hour
boundary behavior was tested with controlled clocks, not a six-hour live soak.

Coverage depends on relay, filters, connectivity and upstream lookback. Window
boundary wait can be up to five minutes plus detection interval/backlog. Grouping
is lexical with mostly English stop words and can combine unrelated events. Cold
history is gated; missing windows and uninterrupted coverage remain uncertain. No truth/virality/misinformation claims
or sensitive user profiling. Queues and samples are bounded, but long-term data
volume and detector budgets require measurement. A failed disk/forced crash may
leave a partial raw tail or lose unpersisted messages. No exactly-once capture,
multi-process coordination, auth, multi-node availability or production SLO claimed.

Next recommended work: longer soak tests and a broader labeled quality evaluation,
then tune retention/window budgets from measured demand. MVP work is complete.

## Workspace/session state

Normal workspace .git contains all project history on main. Earlier aff4095
milestone1.bundle and /private/tmp Git fallbacks are historical; normal Git writes
now work. No credentials, local environments, captures, node_modules or caches
are committed.

The offline demo was restarted with revision 0003 on 127.0.0.1:8000 (PID 11226).
The user's existing bluesky-firehose Compose stack was found on app port 8001 and
PostgreSQL port 5433, rebuilt with `APP_PORT=8001 POSTGRES_PORT=5433 docker compose
up --build --no-deps -d app`. Existing DB/container volumes were preserved. Migration
0002→0003 succeeded. Live health/readiness returned 200, pipeline connected and
persisted incoming events, detector completed successfully, API reports six-hour
retention and baseline_ready=false during initial history collection. SQL verified
no remaining plain-term candidates from the screenshot's nine filler terms and
no available post text beyond the six-hour-plus-minute cutoff. Existing live data
was younger than six hours, so actual six-hour live expiry was not observed.

Follow-up validation (2026-10-18): **88 passed, 32 original subtests**, including
PostgreSQL integration cases, in 18.71 seconds. The default offline run passed
67 tests plus 32 subtests with 21 PostgreSQL cases skipped (8.12 seconds). Tests cover boundary retention, metadata
pruning, restart/clock-rollback replay rejection, recent deletion ordering,
non-extending updates, old creation dates, receipt/idle cleanup and failure,
populated 0002 migration, filler removal, startup warmup and API fields. Chromium
UI QA passed including warmup and retention label, existing desktop/mobile flows
and error recovery; the updated mobile viewport was reviewed. Ruff, lock check,
installed dependency compatibility, Compose config and git diff whitespace pass.
Historical benchmark figures above were not rerun or represented as performance
measurements of this update. The owned native PostgreSQL test cluster remains at
/private/tmp/bluesky-project-pg on port 55432 and is stopped after final checks.
The user's live Compose PostgreSQL remains running; do not remove its volumes.
