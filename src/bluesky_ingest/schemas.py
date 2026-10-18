from datetime import datetime
from typing import Any

from pydantic import BaseModel


class PostResponse(BaseModel):
    uri: str
    author_did: str
    text: str
    created_at: datetime | None
    indexed_at: datetime
    language: str | None
    original_url: str | None


class PostPage(BaseModel):
    items: list[PostResponse]
    total: int
    limit: int
    offset: int


class MetricResponse(BaseModel):
    window_start: datetime
    window_minutes: int
    post_count: int
    baseline_expected_count: float
    absolute_increase: float
    rate_ratio: float
    spike_score: float
    score_components: dict[str, Any]
    explanation: str
    detected_at: datetime
    sample_sufficient: bool
    persistence: str


class TopicResponse(BaseModel):
    id: int
    label: str
    terms: list[str]
    active: bool


class RankedTopic(BaseModel):
    topic: TopicResponse
    metric: MetricResponse


class TrendingResponse(BaseModel):
    items: list[RankedTopic]
    window_start: datetime
    window_minutes: int
    last_updated: datetime | None
    demo: bool
    coverage_note: str


class TopicDetail(BaseModel):
    topic: TopicResponse
    latest_metric: MetricResponse | None
    recent_metrics: list[MetricResponse]
    data_stale: bool
    demo: bool


class TimelineResponse(BaseModel):
    topic_id: int
    items: list[MetricResponse]


class PipelineResponse(BaseModel):
    connection_state: str
    events_received: int
    events_persisted: int
    events_applied: int
    duplicate_or_stale_events: int
    database_retries: int
    valid_post_events: int
    malformed_events: int
    ignored_events: int
    retries: int
    errors: int
    queue_depth: int
    queue_capacity: int
    last_event_at: str | None
    last_persisted_at: str | None
    source_lag_seconds: float | None
    processing_lag_seconds: float | None
    last_error: str | None
    uptime_seconds: float
    events_per_second: float
    persisted_per_second: float
    last_detection_at: str | None
    detection_error: str | None
    topics_evaluated: int
    latency_p50_seconds: float | None
    latency_p95_seconds: float | None
    latency_sample_count: int
    mode: str


class HealthResponse(BaseModel):
    status: str
