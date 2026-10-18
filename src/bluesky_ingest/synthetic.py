"""A deterministic labeled scenario; never used as unmarked live data."""

import json
from datetime import datetime, timedelta


def make_event(text: str, at: datetime, key: str, operation: str = "create") -> str:
    commit = {
        "operation": operation,
        "collection": "app.bsky.feed.post",
        "rkey": key,
        "record": {
            "$type": "app.bsky.feed.post",
            "text": text,
            "langs": ["en"],
            "createdAt": at.isoformat(),
        },
    }
    if operation == "delete":
        del commit["record"]
    return json.dumps(
        {
            "kind": "commit",
            "did": "did:plc:synthetic",
            "time_us": int(at.timestamp() * 1_000_000),
            "commit": commit,
        }
    )


def scenario(now: datetime, baseline_windows: int = 12, window_minutes: int = 5):
    from .detection import floor_window

    boundary = floor_window(now, window_minutes)
    # Twelve stable baselines; spike begins in the latest completed window.
    for offset in range(baseline_windows, -1, -1):
        start = boundary - timedelta(minutes=window_minutes * (offset + 1))
        for name, count in (
            ("aurora", 24 if offset == 0 else 4),
            ("steady", 40),
            ("tiny", 2 if offset == 0 else 0),
        ):
            for i in range(count):
                yield make_event(
                    f"#{name} Synthetic demonstration evidence {i}",
                    start + timedelta(seconds=10 + i),
                    f"{name}-{offset}-{i}",
                )
