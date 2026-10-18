# Project context

Updated: 2026-10-18. Read and update this file whenever project changes make it
outdated. `AGENTS.MD` contains the standing maintenance and Git instructions.

## Objective and scope

Follow section 14 of `bluesky_early_warning_codex_brief.md`: implement only
Milestone 1, a reliable Jetstream consumer with validation, rotating JSONL,
bounded exponential reconnect delays, basic metrics, and deterministic tests.
Activity spike detection, database, dashboard, and deployment are future work.

## Initial inspection

The initial workspace contained `bluesky_firehose.py`, the brief, and `.venv`.
The demo is preserved. No Git repository, package configuration, or tests existed.
Available tools: Python 3.14.4, uv 0.11.6, Git 2.54.0, Docker CLI. The original
virtual environment contained websockets 17.2 only.

## Current implementation

- `src/bluesky_ingest/`: settings, legacy/v2 post parser, single-writer rotating
  raw JSONL storage, bounded producer/worker queue, consumer, metrics, CLI.
- New subscriptions default to current Jetstream v2; the legacy demo and v1
  parser remain supported. Current upstream documentation was inspected.
- Raw envelopes preserve each message, receipt time, disposition, and parsed
  post metadata. Malformed and ignored events are retained for investigation.
- fsync precedes persistence acknowledgement; reconnect cursors advance only
  after a supported post event is persisted. No automatic cursor recovery on
  process restart yet. Raw duplicates retain stable event keys.
- Local metrics expose `/healthz` and `/api/v1/pipeline/status`.
- `tests/fixtures/events.jsonl` contains synthetic creates, update, delete,
  identity, malformed JSON, and a v2 post. Deterministic unit, service, ASGI,
  storage, and subprocess CLI tests cover success and failure paths.
- `pyproject.toml` pins Python 3.14+ and runtime dependencies;
  `requirements.lock` pins all runtime transitive dependencies using uv pip
  compile for the installed Python 3.14/macOS environment.

## Environment constraints and decisions

- Shell network requests cannot resolve PyPI. Runtime packages are available in
  the local uv cache; pytest, pytest-asyncio, and httpx are not cached. Use the
  standard-library unittest/IsolatedAsyncioTestCase runner for deterministic
  tests in this milestone so the suite can run offline. This is a documented
  deviation from the brief's preferred test libraries.
- Workspace `.git` writes are protected: `git init` failed with Operation not
  permitted. Preserve commits via a repository in writable `/private/tmp` and
  export a portable Git bundle to the workspace; this does not initialize a
  normal workspace `.git`. The initialized repository is
  `/private/tmp/bluesky-firehose-m1.git` with this workspace as its working tree;
  run `git --git-dir=/private/tmp/bluesky-firehose-m1.git status` to inspect it.
  Export committed history to `milestone1.bundle` (generated/ignored) for
  persistence beyond the temporary repository's lifetime. README includes
  restore and bundle refresh commands.
- Shell socket binding is also blocked: HTTP launch on loopback returned
  Operation not permitted. FastAPI endpoints are verified in-process via ASGI;
  metrics-server startup failures are handled with a drained queue and nonzero
  process exit. Normal HTTP binding needs a less restricted environment.

## Commands

See README.md for installation and all settings. Installed environment:

```sh
.venv/bin/python -m unittest discover -s tests -v
RAW_DIR=data/demo .venv/bin/bluesky-ingest --replay tests/fixtures/events.jsonl --no-metrics
.venv/bin/bluesky-ingest
```

The last command starts live ingestion plus HTTP metrics in an environment with
network and local-port permission. Ctrl+C/SIGTERM stops receipt and drains the
queue. No live Bluesky capture has been attempted or verified here.

## Validation and next work

- Final complete suite: 32 tests passed on Python 3.14.4 in 1.029 seconds,
  including HTTP failure supervision and interruption of a 30-second backoff.
- `uv pip check --python .venv/bin/python`: all 17 installed packages compatible
  (runtime set plus editable project and setuptools).
- Installed CLI replay with `--no-metrics`: exit 0, 6 received and persisted,
  4 valid posts, 1 malformed, 1 ignored; queue empty at shutdown.
- Actual HTTP launch: restricted bind fails with nonzero exit; all 6 accepted
  events drained after converting Uvicorn's SystemExit to a regular task error.
  Final failure smoke run exited 1, recorded state failed/error count 1, and
  reported 6 received/persisted with queue depth 0.
- Tests require neither a live Bluesky connection nor network sockets.
- No continuous-stream, throughput, CPU, memory, or recovery benchmark claimed.

Remaining limits: no persisted restart checkpoint, automatic raw retention or
redaction, normalized database/current-post deletion state, topic detection,
dashboard, or exactly-once delivery. Raw files may contain subsequently deleted
text. Size/time rotation does not bound total disk use. Restart creates unique
files; forced crashes may leave a partial final line. Throughput metrics are
delivery counts averaged since startup, not unique post rates. Lag is the last
persisted supported-event measurement and can go stale while disconnected.

Next recommended task: add a durable restart checkpoint ordered after raw
persistence, with explicit raw-retention decisions, then PostgreSQL models,
migrations, idempotent writes, and current-post updates/deletions.
