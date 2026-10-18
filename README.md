# Bluesky early warning: Milestone 1

A local Jetstream consumer for the first task in
`bluesky_early_warning_codex_brief.md`. It validates public post creates,
updates, and deletes, writes rotating raw JSONL, reconnects with bounded
exponential backoff and jitter, and exposes ingestion metrics.

The original `bluesky_firehose.py` remains available as a small printing demo.
The reliable consumer is the `bluesky-ingest` command below.

## Setup

Use Python 3.14 and uv. Dependencies are pinned in pyproject.toml and uv.lock.

```sh
uv sync --locked
uv run pytest -q
```

To enable normalized PostgreSQL writes, export DATABASE_URL and migrate first:

```sh
export DATABASE_URL=postgresql+psycopg://bluesky:bluesky@127.0.0.1:5432/bluesky
uv run alembic upgrade head
```

Without DATABASE_URL the ingestion command captures raw events only.
Database writes deduplicate event keys, order updates by source time/cursor,
clear deleted text, and retain tombstones against stale replay. Checkpoints
advance after both raw fsync and database commit. Database outages retry a
bounded number of times, apply queue backpressure, then fail with raw data kept
for replay. SQLite is supported for isolated tests and offline demos.

## Run locally

Replay the included synthetic fixtures without a network connection or an HTTP
listener. All six messages are persisted: four post events, one ignored identity
event, and one malformed payload. These are demo fixtures, not real activity.

```sh
RAW_DIR=data/demo .venv/bin/bluesky-ingest --replay tests/fixtures/events.jsonl --no-metrics
```

Consume the live public stream and serve metrics on `127.0.0.1:8000`:

```sh
.venv/bin/bluesky-ingest
```

Stop with Ctrl+C or SIGTERM. Receipt stops, the WebSocket closes, queued
messages drain, and the writer closes. A connection attempt may take up to
10 seconds to time out; socket close timeout is 5 seconds. Drain time depends
on disk health and queue depth. Persistence errors stop the process with a
nonzero exit status. A startup failure of the HTTP server also stops ingestion.

```sh
curl http://127.0.0.1:8000/healthz
curl http://127.0.0.1:8000/api/v1/pipeline/status
```

Use `--limit 100` to stop after 100 received messages (including malformed and
ignored messages); the default `0` runs continuously. Use `--no-metrics` where
binding a local HTTP port is unavailable. Final metrics are always logged.

Replay a saved raw file into a different directory:

```sh
RAW_DIR=data/replayed .venv/bin/bluesky-ingest --replay data/demo/events-REPLACE-WITH-FILENAME.jsonl --no-metrics
```

Replay accepts one file per invocation, either source JSONL or saved envelopes.
Replay files in filename order if processing several rotated files. It creates
new receipt timestamps and retains source event keys; it does not contact
Bluesky. Invalid or truncated source JSON lines are persisted as malformed.

## Configuration

See `.env.example`. Variables must be exported; the CLI does not load `.env`
automatically. Defaults:

| Variable | Default | Meaning |
| --- | --- | --- |
| `JETSTREAM_URL` | Current US East v2 subscribeEvents endpoint | WebSocket source |
| `RAW_DIR` | `data/raw` | Raw envelope directory |
| `QUEUE_SIZE` | `256` | Maximum pending envelopes |
| `MAX_MESSAGE_BYTES` | `1048576` | Maximum source message size |
| `RAW_MAX_BYTES` | `16777216` | Rotate before exceeding this envelope size |
| `RAW_ROTATE_SECONDS` | `300` | Rotate on next append after this age |
| `BACKOFF_INITIAL` | `1` | Initial retry ceiling in seconds |
| `BACKOFF_MAX` | `30` | Maximum retry ceiling in seconds |
| `METRICS_HOST` | `127.0.0.1` | HTTP bind address |
| `METRICS_PORT` | `8000` | HTTP port |

The default subscription filters post commits and negotiates `xrpc.v1.json`.
Legacy `/subscribe` URLs use `wantedCollections` and legacy event parsing, e.g.:

```sh
JETSTREAM_URL=wss://jetstream2.us-east.bsky.network/subscribe .venv/bin/bluesky-ingest
```

Protocol references: [current Jetstream documentation](https://bsky.network/docs/jetstream/),
[legacy event shapes](https://github.com/bluesky-social/jetstream-legacy), and
[websockets client API](https://websockets.readthedocs.io/en/stable/reference/asyncio/client.html).

## Architecture and semantics

Implementation plan: settings and parser → append log → bounded queue and
reconnect supervisor → metrics and CLI → deterministic failure tests.

`service.py` separates receipt from a single persistence worker using a bounded
async queue. The WebSocket frame buffer is also bounded (16 frames), and messages
are size limited. A slow disk applies backpressure. Disk operations run in a
thread so HTTP and socket tasks can continue. Parsing is deterministic and
preserves original text; normalization only applies NFC and collapses whitespace.

Each JSONL line has `schema_version`, local `ingested_at`, `raw_encoding`, exact
source `raw` message, `disposition`, and optional parsed `post` metadata.
Binary frames use base64. Parsed metadata includes record URI, DID, operation,
source time/cursor, languages, and a canonical source-event SHA-256 key.
Deletes produce metadata with no normalized post text. Unknown kinds,
collections, and operations are ignored for post processing but saved in raw
storage; invalid supported event shapes are marked malformed.

Each append flushes and fsyncs before advancing persisted metrics or the
in-memory reconnect cursor. New files use exclusive creation and unique names;
restart does not truncate existing logs. The directory entry is fsynced when a
file is created. Rotation uses local elapsed time and actual encoded bytes; a
single envelope may exceed `RAW_MAX_BYTES` and remains intact in its own file.
There is no file creation just because a quiet time interval elapsed.

Retries use an exponentially growing ceiling capped at `BACKOFF_MAX`, with
equal jitter between half the ceiling and the ceiling. Receipt on a connection
resets the retry sequence. Both clean and unexpected disconnects reconnect.
Reconnect requests the last **persisted supported post** cursor, inclusively.
Raw duplicate deliveries are intentionally preserved with stable event keys;
counts in this milestone count deliveries, not unique posts. Future analytical
storage must deduplicate events and implement current-post state.

`/api/v1/pipeline/status` exposes connection state, received/persisted counts,
valid/malformed/ignored counts, retries, errors, receipt/persistence timestamps,
queue depth/capacity, latest source and processing lag, and throughput averaged
since process start. Source lag uses the latest persisted supported event's
source timestamp; processing lag includes queue and disk time. Lag values are
last-event measurements and can become stale while disconnected. `/healthz`
reports process health, not stream availability. HTTP endpoints have no auth and
default to loopback. Application logs contain event names and error classes,
without source payloads or configured source URLs.

## Tests and validation

```sh
.venv/bin/python -m unittest discover -s tests -v
```

Tests use synthetic fixtures and in-memory fake WebSockets; there is no live
Bluesky dependency, DNS, or bound port in the test suite. HTTP tests exercise the
real FastAPI app through ASGI calls. Coverage includes v1/v2 parsing, unknown
events, malformed payloads, stable duplicate keys, updates/deletes, reconnects,
shutdown, slow writes and bounded queues, disk/fsync failures, size/time rotation,
restart-safe file naming, raw replay, metrics, settings, and CLI exit behavior.

The suite uses pytest and pytest-asyncio, including the original standard-library
unittest cases. PostgreSQL integration cases run when TEST_DATABASE_URL is set;
otherwise those cases skip and all other tests use isolated local SQLite files.

Actual validation results and environment constraints are recorded in
`PROJECT_CONTEXT.MD`. No throughput benchmark or continuous live-capture claim
is made from the small fixture run.

## Limits and next milestone

Coverage depends on the upstream relay, filters, connectivity, and upstream
retention. This consumer does not capture the entire network. It currently provides raw and optional normalized ingestion; the topic
detector, topic API and dashboard are the next stages.

A durable `RAW_DIR/checkpoint.json` resumes from the last acknowledged supported
post cursor on restart. It is source-specific and replaced atomically after
fsync. Corrupt or mismatched checkpoints fail closed. Offline replay never
overwrites the live cursor. Resume is inclusive and limited by upstream lookback;
raw duplicate deliveries remain possible. Next: idempotent current-post storage.

Raw logs can contain text from posts subsequently deleted. This milestone has
no raw-log redaction or automatic retention/purge policy and no current-state
queries. Keep raw logs local, retain only the operational data needed, and
remove obsolete captures. A retention/redaction policy is required before
extended capture or display of retained posts. Rotation alone does not cap
total disk usage. A forced kill, failed disk, or power loss may leave an
unacknowledged partial final line or lose data not yet persisted; there is no
exactly-once or end-to-end crash-recovery guarantee. Run one consumer per output
directory; startup crash repair and multi-process coordination are future work.

## Project context and Git

`AGENTS.MD` requires keeping `PROJECT_CONTEXT.MD` current and committing completed
changes. The user has now authorized completion of the full MVP in tested stages.
The workspace now has a normal `.git` repository restored from the initial
milestone's history. Use ordinary `git status` and `git log` commands.

The original `milestone1.bundle` and temporary Git repository are historical
fallbacks. Generated captures, bundles, credentials and caches remain ignored.
The current reproducible development dependency lock is `uv.lock`:

```sh
uv sync --locked
uv run pytest -q
```

## Detection stage

Candidates are hashtags, URL hostnames, and conservative terms, capped at 12 per
post with common stop words removed. Updates replace assignments and deletes
remove them. The detector recomputes current-state counts across a bounded
window horizon and scores completed windows. Default baseline: 12 prior windows
of five minutes, including zero-count windows. Smoothing alpha=2, baseline
floor=1; gates require at least 10 current posts, an absolute increase of 5 and
a smoothed rate ratio of 2. Score = absolute increase × log2(rate ratio), or zero
when a gate fails. Consecutive eligible windows distinguish sustained activity.
All components and detection runs are persisted for inspection.

A topic starting from absent history can be detected after enough absolute
volume; the model cannot establish network-wide coverage or truth/importance.
Only completed windows are scored; boundary wait can be five minutes. The next
stage exposes these data through the topic API.
