from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field

from fastapi import FastAPI
from fastapi.responses import JSONResponse


@dataclass
class Metrics:
    connection_state: str = "starting"
    events_received: int = 0
    events_persisted: int = 0
    events_applied: int = 0
    duplicate_or_stale_events: int = 0
    database_retries: int = 0
    valid_post_events: int = 0
    malformed_events: int = 0
    ignored_events: int = 0
    retries: int = 0
    errors: int = 0
    queue_depth: int = 0
    queue_capacity: int = 0
    last_event_at: str | None = None
    last_persisted_at: str | None = None
    source_lag_seconds: float | None = None
    processing_lag_seconds: float | None = None
    last_error: str | None = None
    last_detection_at: str | None = None
    detection_error: str | None = None
    topics_evaluated: int = 0
    processing_latencies: list[float] = field(default_factory=list, repr=False)
    started: float = field(default_factory=time.monotonic, repr=False)

    def snapshot(self) -> dict:
        result = asdict(self)
        elapsed = max(time.monotonic() - result.pop("started"), 0.001)
        samples = sorted(result.pop("processing_latencies"))
        result.update(
            uptime_seconds=elapsed,
            events_per_second=self.events_received / elapsed,
            persisted_per_second=self.events_persisted / elapsed,
            latency_p50_seconds=samples[len(samples) // 2] if samples else None,
            latency_p95_seconds=samples[min(len(samples) - 1, int(len(samples) * 0.95))]
            if samples
            else None,
            latency_sample_count=len(samples),
        )
        return result


def create_app(metrics: Metrics) -> FastAPI:
    app = FastAPI(title="Bluesky ingestion metrics")

    @app.get("/healthz")
    async def health():
        failed = metrics.connection_state == "failed"
        return JSONResponse(
            {"status": "failed" if failed else "ok"}, status_code=503 if failed else 200
        )

    @app.get("/api/v1/pipeline/status")
    async def status():
        return metrics.snapshot()

    return app
