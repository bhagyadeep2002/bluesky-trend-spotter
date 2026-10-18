"""Optional live Docker smoke: bounded DB outage, recovery, SIGTERM shutdown.

This intentionally requires a running Compose stack and external Jetstream.
It is separate from the deterministic offline pytest suite.
"""

import argparse
import json
import subprocess
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="bluesky-validation")
    parser.add_argument("--base-url", default="http://127.0.0.1:18080")
    args = parser.parse_args()
    compose = ["docker", "compose", "-p", args.project]

    def run(*parts):
        subprocess.run([*compose, *parts], check=True, capture_output=True, timeout=40)

    def get(path):
        try:
            with urllib.request.urlopen(args.base_url + path, timeout=4) as response:
                body = response.read().decode()
                return response.status, json.loads(body) if path != "/metrics" else body
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read())

    def wait(predicate, timeout=40):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if predicate():
                    return
            except OSError, urllib.error.URLError:
                pass
            time.sleep(0.1)
        raise RuntimeError("smoke condition did not become true")

    wait(lambda: get("/readyz")[0] == 200)
    wait(lambda: get("/api/v1/pipeline/status")[1]["events_persisted"] >= 10)
    before = get("/api/v1/pipeline/status")[1]
    assert "bluesky_events_persisted_total" in get("/metrics")[1]
    outage_start = time.monotonic()
    try:
        run("stop", "db")
        assert get("/readyz")[0] == 503
        wait(
            lambda: (
                get("/api/v1/pipeline/status")[1]["database_retries"] > before["database_retries"]
            ),
            timeout=2,
        )
        during = get("/api/v1/pipeline/status")[1]
        assert during["queue_depth"] <= during["queue_capacity"]
    finally:
        run("start", "db")
    wait(lambda: get("/readyz")[0] == 200)
    wait(lambda: get("/api/v1/pipeline/status")[1]["events_persisted"] > before["events_persisted"])
    recovered = get("/api/v1/pipeline/status")[1]
    assert recovered["connection_state"] != "failed"
    recovery = time.monotonic() - outage_start
    # Docker sends SIGTERM and waits for the accepted queue to drain.
    run("stop", "app")
    container = subprocess.check_output([*compose, "ps", "-aq", "app"], text=True).strip()
    exit_code = subprocess.check_output(
        ["docker", "inspect", "--format", "{{.State.ExitCode}}", container], text=True
    ).strip()
    # Uvicorn deliberately re-raises SIGTERM after its lifespan cleanup.
    assert exit_code in {"0", "143"}
    logs = subprocess.check_output([*compose, "logs", "--no-log-prefix", "app"], text=True)
    final = [
        json.loads(line)
        for line in logs.splitlines()
        if line.startswith("{") and '"event": "application_stopped"' in line
    ][-1]
    assert final["events_received"] == final["events_persisted"]
    assert final["queue_depth"] == 0
    run("start", "app")
    wait(lambda: get("/readyz")[0] == 200)
    wait(lambda: get("/api/v1/pipeline/status")[1]["duplicate_or_stale_events"] >= 1)
    restart_duplicates = get("/api/v1/pipeline/status")[1]["duplicate_or_stale_events"]
    run("stop", "app")
    result = {
        "measured_at": datetime.now(UTC).isoformat(),
        "live_smoke": "passed",
        "readiness_during_outage": 503,
        "database_retry_increase": recovered["database_retries"] - before["database_retries"],
        "queue_depth_during_outage": during["queue_depth"],
        "queue_capacity": during["queue_capacity"],
        "outage_to_ready_and_progress_seconds": recovery,
        "graceful_container_exit_code": int(exit_code),
        "drained_received": final["events_received"],
        "drained_persisted": final["events_persisted"],
        "restart_duplicate_deliveries": restart_duplicates,
        "note": "Short controlled run with real upstream traffic; not a sustained availability claim.",
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
