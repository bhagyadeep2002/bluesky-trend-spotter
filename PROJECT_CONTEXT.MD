# Project context

Updated: 2026-10-18. Keep this file current with every project change; see AGENTS.MD.

## Scope and progress

The user authorized completing the full MVP from bluesky_early_warning_codex_brief.md,
testing each stage before advancing. Preserve the original printing demo.

1. Ingestion: complete. Bounded queues, legacy/v2 parser, raw rotation, metrics,
   reconnects, graceful shutdown, offline replay.
2. Durable checkpoint: complete. Atomic replace + fsync, source fingerprint,
   inclusive resume after persisted raw events. Corrupt/mismatched checkpoints
   fail closed. Replay never overwrites live checkpoints.
3. PostgreSQL storage: complete. Eight migrated tables, unique event keys,
   source-ordered current-state writes, cleared deletion text and tombstones,
   bounded database retries. Raw fsync and database transaction precede checkpoints.
4. Detection: complete. Deterministic hashtag/domain/term candidates (up to 12
   per post), stop-word filtering, configurable minimum frequency, zero-filled
   five-minute windows, 12 prior-window baseline, smoothed ratio, absolute/volume
   gates, transparent scores, consecutive-window persistence and run records.
5. API: complete. Health/readiness, pipeline status, ranked spikes, topic
   detail/timeline/evidence and literal text search, response models, pagination,
   input validation, sanitized database failures, no-store responses. Deleted
   posts are excluded even before the next detection pass.
6. Next: integrated runtime and dashboard, then Compose/monitoring/failure
   benchmarks and final documentation.

## Architecture and commands

Python 3.14; src/bluesky_ingest contains the consumer. FastAPI metrics and CLI
already exist. Raw envelopes include source payload, ingestion time, parsed
metadata and stable duplicate keys. Raw data and local environments are ignored.
Checkpoint is RAW_DIR/checkpoint.json. A consumer owns one output directory.

Use uv and pyproject.toml; uv.lock now includes pytest, pytest-asyncio, httpx,
ruff and psutil in the dev group. Existing unittest cases remain discoverable
by pytest. SQLAlchemy 2.0.55, Alembic 1.20.0 and psycopg 3.3.6 are pinned runtime
dependencies. Retired the earlier requirements.lock; uv.lock is authoritative.

    uv sync --locked
    .venv/bin/python -m pytest -q
    .venv/bin/bluesky-ingest --replay tests/fixtures/events.jsonl --no-metrics
    .venv/bin/bluesky-ingest --limit 10 --no-metrics

## Environment and validation

Full filesystem, network and local socket access is now enabled. Restored the
original aff4095 history into normal workspace .git; use normal git commands.
The earlier temporary Git repository and milestone1.bundle are historical
fallbacks, no longer the primary repository.

Original 32 tests passed again. Checkpoint stage tests are passing. A live v2
smoke run received and persisted 10 valid post events, exit 0, no malformed events
or errors; raw files were held in a temporary directory and removed afterward.
This is a short capture check, not a sustained throughput benchmark.

Docker Desktop is installed but its daemon is stopped. Postgres.app supplies
PostgreSQL 18. An isolated development/test cluster has been initialized at
/private/tmp/bluesky-project-pg, bound to 127.0.0.1:55432, database bluesky_test,
user bluesky (local trust authentication, no real credentials). Do not touch
other database clusters. Compose reproducibility will be added later.

## Current limitations and decisions

DATABASE_URL enables normalized database writes in the ingestion CLI; unset
means raw-only capture. Apply migrations with DATABASE_URL=... uv run alembic
upgrade head. No dashboard or integrated server runtime yet. SQLite is a local test
and offline demo adapter; PostgreSQL is the normal deployment database. No retention/redaction
policy yet; raw logs can retain subsequently deleted text and rotation does not
cap total disk growth. No performance/continuous capture guarantees claimed.
Prefer a server-rendered dashboard with local assets and no Node build chain.

Storage-stage validation: 43 tests passed (including actual PostgreSQL schema isolation,
idempotency, out-of-order deletion protection, transient DB failure and raw
recovery). PostgreSQL Alembic upgrade and check passed with no schema drift.
Run integration tests with TEST_DATABASE_URL=postgresql+psycopg://bluesky@127.0.0.1:55432/bluesky_test uv run pytest -q.

Detection validation: synthetic #aurora rose from 4 to 24 posts/window and ranked
above stable #steady at 40/window; tiny two-post noise was excluded. Repeated
detection and duplicate replay keep counts stable; deletes remove assignments
and current scores. Tests passed on SQLite and PostgreSQL; Ruff checks pass.
Windows score the latest completed interval; maximum expected boundary wait is
five minutes plus detector interval. Missing history is represented by zeros,
so baseline warmup and incomplete coverage remain documented limitations.

Latest validation: 55 tests passed, 32 original subtests, with both SQLite and
PostgreSQL fixtures. API checks include stale ranking exclusion, pagination,
invalid inputs, missing topics, deletion exclusion, readiness, safe database
errors and response models. The API serves only latest completed-window scores
as current; older topic details explicitly report data_stale.
