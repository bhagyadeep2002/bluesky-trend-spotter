# Internet Early-Warning System

## Codex Project Brief

## 1. Mission

Build a portfolio-quality data engineering application that consumes
public Bluesky activity in near real time and detects topics whose
activity is growing unusually quickly.

The system should answer:

-   What topics are gaining momentum right now?
-   Which posts are driving each detected spike?
-   How quickly is a topic growing compared with its recent baseline?
-   Is the spike sustained, or was it only a brief burst of activity?

This is an engineering-first project. Prioritize reliable ingestion,
reproducible processing, measurable detection quality, and clear
operational visibility over a complex UI.

## 2. Product scope

### MVP user experience

A user can open a local dashboard and see:

1.  A ranked list of emerging topics.
2.  Each topic's recent post volume and growth relative to its baseline.
3.  A timeline showing the topic's activity over time.
4.  Representative public posts that explain why the topic was detected.
5.  A topic detail page with the keywords/entities and evidence used to
    form the cluster.
6.  Pipeline health: connection status, event rate, processing lag, last
    successful event, and errors.

The app must distinguish **activity spikes** from claims about truth,
importance, or virality. It must not label content as misinformation or
infer sensitive attributes about users.

### Explicitly out of scope for the MVP

-   Archiving all Bluesky content indefinitely.
-   Training a custom language model.
-   Automated fact-checking.
-   Predicting future events with claims of certainty.
-   User-level political, health, or psychological profiling.
-   Multi-node production deployment or cloud infrastructure.
-   Complex authentication and multi-tenant features.

## 3. Proposed architecture

Start with the smallest architecture that is reliable and testable:

``` text
Bluesky Jetstream
      |
      v
Async ingestion service
      |
      +--> Durable raw event files (JSONL, rotated by time/size)
      |
      v
Normalization and filtering
      |
      v
PostgreSQL
  - posts
  - topic assignments
  - time-window aggregates
  - detection runs
      |
      v
Topic detection worker
  - text normalization
  - candidate grouping
  - activity baseline
  - spike scoring
      |
      v
FastAPI
      |
      v
Simple dashboard
```

Use Jetstream as the initial stream source because it provides JSON
events and filtering options. Keep the source integration behind an
interface so it can be replaced later.

Do not add Kafka/Redpanda, Spark, Flink, a vector database, or
Kubernetes in the MVP. Add infrastructure only when a measured
bottleneck or concrete requirement justifies it.

## 4. Technology choices

-   Python 3.12 or a currently supported Python version available in the
    environment.
-   `asyncio` and a maintained WebSocket client for streaming.
-   FastAPI for the API and health endpoints.
-   PostgreSQL for normalized records and aggregates.
-   SQLAlchemy 2.x and Alembic for database access and migrations.
-   `pytest` and `pytest-asyncio` for testing.
-   Docker Compose for PostgreSQL and local reproducibility.
-   A lightweight frontend: use the simplest maintainable option already
    present in the repository. If no frontend exists, choose a minimal
    React + Vite dashboard or server-rendered pages; do not build both.
-   Start with deterministic text-based candidate grouping and
    statistical detection. Embeddings may be added later behind a clear
    interface, not as a prerequisite for the first working version.

Pin dependencies using the repository's existing package manager. If the
repository has no setup, use `pyproject.toml`.

## 5. Stream ingestion requirements

Consume public Bluesky post repository events from Jetstream. Initially
handle post record creates, updates, and deletes.

For each event:

-   Preserve source event metadata needed for debugging and replay.
-   Record the repository DID, record URI or record key, operation,
    event timestamp when supplied, and local ingestion timestamp.
-   Validate the event shape and handle unknown event types without
    crashing the consumer.
-   Normalize post text and extract basic metadata such as language if
    available.
-   Do not assume every event is a new post.
-   Handle reconnects with bounded exponential backoff and jitter.
-   Use bounded queues so a slow database cannot cause unbounded memory
    growth.
-   Log structured errors without logging secrets.
-   Expose connection state, last event time, events received, malformed
    events, retries, and processing lag.
-   Support graceful shutdown and close the WebSocket cleanly.
-   Make ingestion idempotent where possible using stable record
    identifiers and event metadata.
-   Persist raw events to rotating JSONL files before or alongside
    processing so that failures can be investigated and data can be
    replayed locally.

Do not claim complete capture of the whole network. Document that
coverage depends on the upstream relay, filters, connectivity, and
retention.

### Deletions and updates

Keep the current-post view separate from the raw event log. A deletion
must remove or mark the corresponding post as unavailable in the
current-state tables and prevent it from appearing in future
representative-post results. Preserve only the minimum event metadata
necessary for operational correctness and follow platform requirements
and applicable privacy expectations.

## 6. Topic detection approach

Implement a transparent baseline before introducing ML.

### Stage A: Candidate generation

-   Normalize text conservatively; retain original text for display
    where permitted.
-   Extract hashtags, URLs/domains, and salient terms.
-   Exclude common stop words and low-information tokens.
-   Use configurable minimum frequency thresholds.
-   Group posts that share meaningful terms or entities.
-   Avoid grouping every post that contains a generic word.

Keep candidate generation in a separate module so it can later be
replaced with embedding-based clustering.

### Stage B: Time-window aggregation

Maintain counts per candidate for fixed time windows, initially 5
minutes. Also calculate a longer-term baseline using prior windows.

Store enough information to reproduce a score and inspect why a
candidate was selected.

### Stage C: Spike scoring

Build a documented score using at least:

-   Current-window post count.
-   Recent baseline rate.
-   Absolute increase in activity.
-   Minimum-volume threshold.
-   Optional persistence across multiple windows.

Use smoothing and a small baseline floor to avoid unstable ratios when
historical volume is near zero. Do not rank a topic highly based only on
a tiny denominator.

A possible initial approach is a smoothed rate ratio:

`rate_ratio = (current_count + alpha) / (baseline_expected_count + alpha)`

where `alpha` is configurable. Combine this with a minimum current count
and absolute increase. Document all score components and make them
configurable.

Do not call a topic "trending" solely because it has a high lifetime
count. It must be growing relative to its own recent baseline.

### Stage D: Evidence and explanations

For every detected topic, store and expose:

-   Human-readable label.
-   Candidate terms/entities.
-   Detection time and window.
-   Current count and baseline estimate.
-   Absolute increase and rate ratio.
-   Score and score components.
-   A small set of representative posts, selected by relevance and
    recency.
-   A short deterministic explanation such as: "Activity rose from an
    estimated 4 posts per 5 minutes to 23 in the latest window."

Do not generate unsupported narrative explanations. An LLM summary can
be considered later, but it must cite the underlying posts and must not
be required for detection.

## 7. Initial database model

Keep the schema small and evolve it with migrations.

### `posts`

-   `uri` (primary key)
-   `author_did`
-   `text`
-   `created_at`
-   `indexed_at` or source event timestamp where available
-   `ingested_at`
-   `language` nullable
-   `deleted_at` nullable
-   `raw_event_ref` nullable

### `post_events`

-   `id` (database-generated key)
-   `event_key` (stable deduplication key where possible)
-   `uri`
-   `author_did`
-   `operation`
-   `source_time` nullable
-   `ingested_at`
-   `raw_event_ref` nullable

Do not store duplicate full text in every event unless needed for
replay/debugging. Avoid retaining deleted text in the event table after
a deletion if that conflicts with platform requirements.

### `topic_candidates`

-   `id`
-   `label`
-   `normalized_key`
-   `created_at`
-   `last_seen_at`
-   `active`

### `topic_terms`

-   `topic_id`
-   `term`
-   `weight` or count

### `topic_post_assignments`

-   `topic_id`
-   `post_uri`
-   `assigned_at`
-   `relevance_score` nullable

### `topic_window_metrics`

-   `topic_id`
-   `window_start`
-   `window_minutes`
-   `post_count`
-   `baseline_expected_count`
-   `absolute_increase`
-   `rate_ratio`
-   `spike_score`
-   Unique constraint on `(topic_id, window_start, window_minutes)`

### `detection_runs`

-   `id`
-   `started_at`
-   `finished_at`
-   `status`
-   `events_processed`
-   `topics_evaluated`
-   `error_summary` nullable

Add appropriate indexes for timestamps, topic/window lookups, and post
URIs. Avoid premature partitioning; measure table growth first.

## 8. API requirements

Provide at least:

-   `GET /healthz` --- process health.
-   `GET /readyz` --- database and required dependencies ready.
-   `GET /api/v1/pipeline/status` --- connection state, last event,
    throughput, lag, and error counts.
-   `GET /api/v1/topics/trending?window_minutes=5&limit=20` --- ranked
    active spikes.
-   `GET /api/v1/topics/{topic_id}` --- topic metadata, score
    explanation, and recent metrics.
-   `GET /api/v1/topics/{topic_id}/posts` --- representative posts with
    pagination.
-   `GET /api/v1/topics/{topic_id}/timeline` --- time series for the
    topic.
-   `GET /api/v1/search?q=...` --- basic text search across available,
    non-deleted posts.

Use response models, pagination, input validation, sensible error codes,
and API tests. Do not expose internal exception traces to clients.

## 9. Dashboard requirements

The dashboard should prioritize operational clarity over visual
complexity.

### Overview

-   Current pipeline status.
-   Incoming events per second.
-   Recent processing lag and errors.
-   Ranked emerging topics.
-   Current post count versus baseline.
-   "Last updated" timestamp.

### Topic detail

-   Topic label and terms.
-   Activity timeline.
-   Current rate versus baseline.
-   Representative posts with links to the original Bluesky records when
    possible.
-   Explanation of the spike score.
-   Clear indication when sample size is too small.

### UI constraints

-   Keep it responsive and readable.
-   Include loading, empty, and error states.
-   Do not display deleted posts.
-   Do not imply that the dataset is exhaustive.
-   Do not fabricate sample metrics in the live UI. Seed data may be
    used in tests and must be clearly marked as demo data.

## 10. Testing strategy

Use fixtures and a deterministic local event generator. Unit and
integration tests must not depend on a live Bluesky connection.

Cover:

1.  Valid post-create event.
2.  Unknown event type.
3.  Malformed payload.
4.  Duplicate event delivery.
5.  WebSocket disconnect and reconnect.
6.  Graceful shutdown.
7.  Database temporarily unavailable.
8.  Bounded-queue behavior under slow writes.
9.  Update and deletion handling.
10. No deleted post returned by APIs.
11. Baseline calculation and smoothing.
12. Spike detection on synthetic time series.
13. Low-volume false positive suppression.
14. API pagination and invalid inputs.
15. Restart and replay behavior.

Create a synthetic scenario where a candidate rises from a low stable
rate to a clear spike and verify that it is ranked above candidates with
high but stable volume.

## 11. Benchmarking and acceptance criteria

Record actual measurements on the development machine; do not invent
throughput claims.

Track:

-   Events received per second.
-   Events successfully persisted per second.
-   End-to-end processing latency.
-   Queue depth and lag.
-   CPU and memory use.
-   Disk growth per hour.
-   Reconnect count and recovery time.
-   Duplicate rate.
-   Detection latency from the beginning of a synthetic spike to its
    appearance in the API.

Acceptance criteria:

-   The app starts from documented commands.
-   The stream consumer can run continuously and reconnect after
    interruptions.
-   Queues are bounded.
-   Raw events can be inspected and replayed locally.
-   Duplicate delivery does not inflate analytical counts.
-   Deletes are reflected in current-state queries and representative
    posts.
-   The detector finds a controlled synthetic spike and suppresses
    obvious low-volume noise.
-   Health and readiness endpoints work.
-   Tests pass without external network access.
-   Dashboard values come from real application data or are explicitly
    marked as demo data.
-   The README documents limitations and reproducible benchmarks.

## 12. Suggested build sequence

Build in small, runnable increments:

1.  Inspect the existing repository and report its structure before
    making changes.
2.  Create project configuration, settings, logging, and tests.
3.  Implement a fixture-driven Jetstream event parser.
4.  Implement a WebSocket consumer with reconnect behavior.
5.  Add raw JSONL persistence and a durable checkpoint strategy.
6.  Add PostgreSQL models and Alembic migrations.
7.  Add idempotent current-post upserts and deletion handling.
8.  Add deterministic candidate generation and window aggregates.
9.  Add baseline and spike scoring with unit tests.
10. Expose the API and health endpoints.
11. Build the minimal dashboard.
12. Add Docker Compose, monitoring, failure tests, and benchmarks.
13. Update documentation with actual commands and measured results.

After each stage, run the relevant tests and summarize what changed. Do
not implement all stages in one giant patch.

## 13. Coding-agent instructions

-   First inspect the repository, existing conventions, and installed
    tools.
-   If the repository already contains code, preserve useful work and
    adapt the plan rather than replacing files wholesale.
-   State the implementation plan before making broad changes.
-   Implement one vertical slice at a time.
-   Prefer typed Python, small modules, explicit interfaces, and clear
    error handling.
-   Keep configuration in environment variables with a documented
    `.env.example`; never commit credentials.
-   Include migrations and tests for schema changes.
-   Avoid unnecessary abstractions and dependencies.
-   Do not use real network access in unit tests.
-   Do not silently swallow exceptions or treat a failed write as
    success.
-   Do not claim a feature, benchmark, or recovery guarantee works until
    it has been tested.
-   When a requirement is ambiguous, choose the simplest safe option and
    document the decision.
-   At the end, provide setup commands, test results, known limitations,
    and the next recommended task.

## 14. First task for Codex

Do not build the entire application immediately.

First inspect the repository and propose a minimal implementation plan
for **Milestone 1: a reliable Jetstream consumer that validates post
events, writes rotating raw JSONL files, reconnects with bounded
exponential backoff, and exposes basic ingestion metrics**.

Then implement Milestone 1, add deterministic tests using fixtures or a
local fake WebSocket, run the tests, and document how to launch it
locally.

The first milestone is complete only when it can be run and tested
without relying on a live Bluesky connection for its test suite.
