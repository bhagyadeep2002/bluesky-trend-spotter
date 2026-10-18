# Project context

Updated: 2026-10-18. Keep this file current whenever code, configuration,
dependencies, workflows, documentation or validation change. See AGENTS.MD.

## Objective and status

The user authorized completing the full MVP in bluesky_early_warning_codex_brief.md,
testing each stage before advancing. **All functional MVP stages are complete.** The follow-up adds rolling six-hour
retention, stronger filler-word filtering and startup baseline gating. The updated
live Compose app remains on port 8001; the separate demo remains on port 8000.
The detector/ingestion lag fix is implemented, tested and deployed on port 8001.
Existing DB/raw/checkpoint volumes are preserved. Source backlog remains; a longer
recovery/soak check on a less-contended host is the next operational task.
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
- candidates.py/detection.py: deterministic candidates and unchanged score formula.
  Counts are grouped in SQL under a read snapshot (PostgreSQL REPEATABLE READ,
  SQLite explicit read transaction/WAL), without the ingestion advisory lock.
  Versioned input signatures in metric JSON skip unchanged topics/windows.
  Bulk upserts publish at most 32 topics per transaction, 200 rows per
  executemany batch; obsolete timeline rows are deleted in batches of 1,000.
  Final PostgreSQL publication uses FOR KEY SHARE for topic references, conditional
  active-flag updates, and no ingestion advisory lock. Pruning also omits that lock.
  Existing metrics bootstrap signatures automatically; no schema revision change.
- database.py now batches candidate ID upserts, terms and assignments per post,
  preserving one atomic current-state transaction and matching IDs by candidate key.
- api.py/schemas.py: response models, input validation, pagination, sanitized DB
  errors, no-store responses, topic/evidence/search routes, health/readiness and
  Prometheus text metrics. Deleted posts never enter new evidence/search results.
- runtime.py/static/: combined app lifecycle, detector/maintenance tasks and local
  responsive HTML/CSS/JS dashboard. A persistent, spawned single-worker process runs
  detection with its own DB engines, separate from ingestion's Python interpreter.
  Broken workers are replaced; shutdown awaits outstanding work. Pipeline status
  and Prometheus expose last_detection_duration_seconds. Updates every 15s and
  when a tab regains focus.
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
Detector checks every 15 seconds after the previous run and rejects horizons above
1,000,000 posts by default (ANALYSIS_MAX_POSTS, also passed through Compose). This
ceiling is a guardrail, not a tested capacity promise; the generated load benchmark
used 200,000 posts, 800,000 assignments and 25,000 candidates. The timeline retains
the latest 13 windows; only changed metric inputs are republished. Baseline counts and versioned cache signatures are stored
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

Next task: observe backlog recovery and a longer soak on a less-contended host,
then broaden labeled quality evaluation. Source lag is not yet resolved. Do not
infer sustained full-network capacity or a six-hour retention soak from these
short fixtures and snapshots.

## Workspace/session state

Normal workspace .git contains all project history on main. Earlier aff4095
milestone1.bundle and /private/tmp Git fallbacks are historical; normal Git writes
now work. No credentials, local environments, captures, node_modules or caches
are committed.

The offline demo was restarted with the lag fix on 127.0.0.1:8000 (PID 14421).
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

## Live lag investigation (2026-10-18)

The user asked why processing lag increased. Read-only inspection of the running
Compose app found a concrete throughput bottleneck, without clearing data or
changing runtime configuration. At the sample: 62,992 posts, 81,841 topic candidates,
281,575 assignments and 200,057 window metrics. The latest completed detector run
evaluated 15,389 topics and took 32.302 seconds; earlier recent runs took 28–29
seconds. PostgreSQL showed ingestion actively waiting on advisory lock 847019 while
the detector transaction was idle waiting for application work. The app container
used about 100% of one CPU and 682 MiB RAM; PostgreSQL used about 2% CPU at that
instant. This identifies application-side full-horizon aggregation/metric rebuilding
under the shared writer lock as the primary observed blockage, rather than proving
a disk or database capacity limit.

The pipeline sample reported queue 256/256, 35.2 persisted deliveries/second since
restart, processing lag 2.536 seconds, source lag 350.619 seconds and three reconnects.
Those are point-in-time/cumulative metrics, not sustained capacity measurements;
received rate is backpressured, so it cannot establish the upstream arrival rate.
Processing lag is receipt-to-persistence for the last completed event and updates
only on successful persistence; it excludes time already spent upstream/in socket
buffers. Source lag measures source timestamp age for that event. Neither field
alone describes every queued event's current delay.

Detector currently deletes/rebuilds recent metrics for every eligible topic each
cycle while holding the same advisory lock used by individual event writes.
Candidate volume grows before six-hour expiry can help; the six-hour setting bounds
retention, not per-cycle work or ingest latency. New minute maintenance shares that
lock too, but this sample directly observed a detector blockage. The existing short
594-event benchmark did not establish full-network sustained performance. No code
fix or new performance guarantee was made during this diagnostic question. Project
context was updated and committed; no tests were needed for this documentation-only
change. The live DB, app and volumes were preserved.

## Lag fix implementation and staged validation (2026-10-18)

User authorized implementing the diagnosis. No DB reset or checkpoint skip is used.
Detector reads/computation no longer hold the writer lock for a whole run. A run
publishes changed topic batches from its snapshot; concurrent updates/deletes are
observed by the next run and cannot be lost by clearing a dirty flag. Publication
is atomic per batch, not across the entire ranking. Failed partial publication can
be retried idempotently. Evidence/search remain immediately filtered for deletes.
The integrated app uses its existing minute maintenance rather than duplicate
RetainedSink maintenance on the ingestion path; standalone ingestion retains its
receipt/idle cleanup. Schema head stays 0003.

A generated PostgreSQL benchmark compares old detector commit 980783c with this
intermediate bounded-advisory-lock implementation, running each detector in a
separate process and a concurrent writer
targeting 40 events/sec. On 200,000 posts/800,000 assignments/25,000 candidates:
old detection 43.695s, max writer wait 43.599s; optimized bootstrap 25.960s/max0.376s;
repeat (nearly unchanged) 6.867s/max0.0226s; next window 13.905s/max0.198s. See
`tools/benchmark_detector.py`, `docs/detector-benchmark.json` and docs/BENCHMARKS.md.
This fixture excludes raw/checkpoint fsync and is not a network capacity claim.
Native tests/benchmarks share a database-wide advisory lock despite isolated schemas;
run the long baseline benchmark separately from timing-sensitive tests.

The initial detector-only build was deployed on port 8001 with preserved volumes.
Its first live signature rebuild took ~45s; later sampled runs took 8–22s while
backdated catch-up events changed completed windows. The queue remained full during
this rollout, and source lag stayed near 16 minutes. This prompted a separate
writer batching stage. The pre/post point samples are in docs/lag-validation.json;
restart resets counters. The final batched writer was deployed as a second rollout. Existing DB and raw
volumes and the inclusive checkpoint were preserved across both app restarts.
Historical original benchmark figures above are not substituted for current results.

Full regression before the final lock refinement: **104 passed plus 32 original
subtests**, PostgreSQL enabled, in 39.92 seconds. Final lock refinement passed
**45 targeted SQLite/PostgreSQL tests** in 140.47 seconds, including publication and
pruning while another transaction owns the ingestion advisory lock. Final default
offline run: **76 passed, 30 skipped, 32 subtests**, in 51.19 seconds. New coverage includes unchanged/open-window cache skips, boundary
advance, late data and configuration invalidation, nonblocking consistent reads,
concurrent updates/deletes during a snapshot, retry after partial publication,
replacement of a broken worker and rollback/key-weight matching for batched writes.
Ruff, lock/dependency checks, Compose config and whitespace checks pass. Chromium
UI checks passed again against the updated demo (navigation, refresh, evidence,
search, errors, warmup, retention label, XSS text handling and desktop/mobile fit).
The native PostgreSQL test cluster was stopped after checks; the user's Docker DB
and both app/demo servers remain running.

A sequential before/after ingestion benchmark used the same local real-WebSocket
fixture, raw/checkpoint fsync and current detector/migrations, loading old writer
285b56f via the new --database-revision option. Old writer: 118.0 deliveries/sec,
p95 670ms, CPU4.622s; batched writer: 191.5/sec, p95 384ms, CPU2.825s. Both persisted
654 deliveries/594 unique events with one forced reconnect and the expected spike
ranking. See docs/ingestion-before-batching.json and ingestion-after-batching.json.
These are short fixtures, not a sustained live throughput forecast.


### Final rollout and remaining performance limits

The two staged rollouts (detector process/cache and batched ingestion) were followed
by a final refinement removing ingestion's advisory lock from metric publication
and pruning. PostgreSQL FOR KEY SHARE protects live topic references against
retention deletion; ingestion's non-key updates remain compatible. Active flags
are only updated when they actually differ. The final live app was rebuilt and
restarted without removing its DB/raw/checkpoint volumes; schema stays at 0003.
Latest health/readiness returned 200 and ingestion remained connected, with no
detector error. One final point sample showed 5,196 persisted events, 2.692s local
processing lag, 962.096s source lag and 71.971s detection duration. This is not a
claim that live backlog cleared or full-network sustained capacity is achieved.

The final large benchmark rerun was interrupted during severe host contention:
load averages 143.51/113.46/56.74, ~4.86GiB swap used, physical memory 8GiB. It had
not produced a replacement result file. Its owned temporary schema
(detector_bench_c861f52a00b9471bab3cb4b9295c6a7e) was removed and the native cluster
stopped. No user's Docker DB or other workloads were stopped. The retained 200k
benchmark JSON/table explicitly describe the intermediate implementation; final
lock behavior is validated by the targeted concurrency test. Host contention is
an observed limitation, not proof it explains every remaining live bottleneck.

All final lint/format, dependency/lock, Compose config and whitespace checks pass.
The demo browser checks passed before the final backend-only lock refinement.
The aggregate staged live samples are in docs/lag-validation.json. The code task is
complete; further sustained performance/recovery validation remains an operational
follow-up and should use controlled machine load before stronger guarantees.
