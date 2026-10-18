# Project context

Updated: 2026-10-18. Keep this file current whenever code, configuration,
dependencies, workflows, documentation or validation change. See AGENTS.MD.

## Objective and status

The user authorized completing the full MVP in bluesky_early_warning_codex_brief.md,
testing each stage before advancing. **All functional MVP stages are complete.**
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
  identifiers AND sequences (SQLite INTEGER rowids already support 64 bits).
- maintenance.py: hourly text expiry, old metric/run/inactive-topic cleanup.

Scoring defaults: five-minute latest completed window, 12 baseline windows,
alpha=2, floor=1, current>=10, increase>=5, ratio>=2; score=increase*log2(ratio)
when eligible. Candidate minimum horizon frequency=3, at most 12 terms/post.
Detector runs every 15 seconds and rejects horizons above 100,000 posts. Typical
timeline contains the latest 13 recomputed windows; baseline counts are stored
in each metric. Empty history is zero, so warmup/coverage uncertainty matters.

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

- Final full backend suite: **72 passed, 32 original subtests**, PostgreSQL cases
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

Raw defaults: 24h mtime TTL, 1GiB budget, enforced at new-file creation/rotation;
quiet or stopped processes do not schedule purges. A budget can shorten retention.
Raw logs can retain subsequently deleted text until expiry; immediate raw
redaction is not implemented. Full runtime hourly maintenance expires normalized
text after 48h; standalone ingestion does not run that DB maintenance. Metrics/runs
older than seven days are pruned; minimal event keys and tombstones are kept, so
metadata/WAL/database size is not globally capped. Raw references may expire.

Coverage depends on relay, filters, connectivity and upstream lookback. Window
boundary wait can be up to five minutes plus detection interval/backlog. Grouping
is lexical with mostly English stop words and can combine unrelated events. Cold
history and missing windows are uncertain. No truth/virality/misinformation claims
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

The offline demo is running on 127.0.0.1:8000 (PID 9604 at this update). The isolated
bluesky-validation Compose services and volumes were removed after validation;
no test live capture is left running. Docker Desktop remains available. The owned
native PostgreSQL test cluster /private/tmp/bluesky-project-pg was stopped after
checks; it used port 55432, local trust user bluesky and database bluesky_test.
Its test data is temporary; normal setup should use documented Compose commands.

Final documentation/setup acceptance: uv sync --locked succeeded; with the owned
PostgreSQL cluster stopped and TEST_DATABASE_URL unset, the default offline suite
passed 57 tests plus 32 subtests, skipping the 15 optional PostgreSQL cases. The
full 72-case run above was completed while the isolated PostgreSQL server was up.
The final demo answered HTTP 200 for health, readiness, pipeline status, ranked
#aurora demo data and Prometheus metrics. README local links were verified.
README and docs/BENCHMARKS.md now contain the complete setup, launch, replay,
configuration, measurements and limitations rather than obsolete milestone-1
instructions. All requested work is complete; the demo is the only app left running.
