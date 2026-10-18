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
3. Next: PostgreSQL models/migrations and idempotent current-state writes.
4. Then deterministic candidates/aggregates/scoring, API, server-rendered
   dashboard, Compose, failure tests, real benchmarks, final documentation.

## Architecture and commands

Python 3.14; src/bluesky_ingest contains the consumer. FastAPI metrics and CLI
already exist. Raw envelopes include source payload, ingestion time, parsed
metadata and stable duplicate keys. Raw data and local environments are ignored.
Checkpoint is RAW_DIR/checkpoint.json. A consumer owns one output directory.

Use uv and pyproject.toml; uv.lock now includes pytest, pytest-asyncio, httpx,
ruff and psutil in the dev group. Existing unittest cases remain discoverable
by pytest. requirements.lock is the older milestone-1 runtime lock and will be
retired when the remaining runtime dependencies are added.

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

No normalized database, topic detector, dashboard or durable database delivery
yet. Checkpoints currently acknowledge raw writes only; the next stage must
advance them after downstream transactions succeed. No retention/redaction
policy yet; raw logs can retain subsequently deleted text and rotation does not
cap total disk growth. No performance/continuous capture guarantees claimed.
Prefer a server-rendered dashboard with local assets and no Node build chain.
