from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Settings:
    jetstream_url: str = (
        "wss://jetstream.us-east.bsky.network/xrpc/network.bsky.jetstream.subscribeEvents"
    )
    raw_dir: Path = Path("data/raw")
    queue_size: int = 256
    max_message_bytes: int = 1_048_576
    raw_max_bytes: int = 16_777_216
    raw_rotate_seconds: float = 300
    backoff_initial: float = 1
    backoff_max: float = 30
    metrics_host: str = "127.0.0.1"
    metrics_port: int = 8000
    database_url: str = ""
    database_retries: int = 5
    database_retry_seconds: float = 1
    window_minutes: int = 5
    baseline_windows: int = 12
    alpha: float = 2
    baseline_floor: float = 1
    min_current_count: int = 10
    min_absolute_increase: float = 5
    min_rate_ratio: float = 2
    candidate_min_frequency: int = 3
    detect_interval_seconds: float = 15
    analysis_max_posts: int = 100_000
    raw_retention_hours: float = 6
    raw_budget_bytes: int = 1_073_741_824
    post_retention_hours: float = 6

    def __post_init__(self) -> None:
        url = urlsplit(self.jetstream_url)
        if url.scheme not in {"ws", "wss"} or not url.hostname or url.username or url.password:
            raise ValueError("JETSTREAM_URL must be a ws/wss URL without credentials")
        for name in (
            "queue_size",
            "max_message_bytes",
            "raw_max_bytes",
            "raw_rotate_seconds",
            "backoff_initial",
            "backoff_max",
            "metrics_port",
            "database_retries",
            "database_retry_seconds",
            "window_minutes",
            "baseline_windows",
            "alpha",
            "baseline_floor",
            "min_current_count",
            "min_absolute_increase",
            "min_rate_ratio",
            "candidate_min_frequency",
            "detect_interval_seconds",
            "analysis_max_posts",
            "raw_retention_hours",
            "raw_budget_bytes",
            "post_retention_hours",
        ):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if self.backoff_max < self.backoff_initial:
            raise ValueError("BACKOFF_MAX must be at least BACKOFF_INITIAL")
        if self.metrics_port > 65535:
            raise ValueError("METRICS_PORT must be at most 65535")
        if self.raw_budget_bytes < self.raw_max_bytes:
            raise ValueError("RAW_BUDGET_BYTES must be at least RAW_MAX_BYTES")
        if self.post_retention_hours * 60 <= self.window_minutes * (2 * self.baseline_windows + 2):
            raise ValueError("POST_RETENTION_HOURS must exceed the analysis horizon")

    @classmethod
    def from_env(cls) -> Settings:
        defaults = cls()
        values = {}
        for name in cls.__dataclass_fields__:
            if name.upper() in os.environ:
                values[name] = type(getattr(defaults, name))(os.environ[name.upper()])
        return cls(**values)
