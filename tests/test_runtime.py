import asyncio

import httpx
import pytest

from bluesky_ingest.config import Settings
from bluesky_ingest.runtime import application


async def test_offline_demo_lifecycle_and_static_assets(database, tmp_path):
    settings = Settings(
        database_url=database.engine.url.render_as_string(hide_password=False),
        raw_dir=tmp_path / "raw",
        detect_interval_seconds=0.05,
    )
    app = application(settings, demo=True)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            for _ in range(500):
                if app.state.metrics.last_detection_at:
                    break
                await asyncio.sleep(0.01)
            assert app.state.metrics.last_detection_at
            assert app.state.metrics.last_detection_duration_seconds > 0
            assert (await client.get("/readyz")).status_code == 200
            status = (await client.get("/api/v1/pipeline/status")).json()
            assert status["mode"] == "demo"
            assert status["events_received"] == status["events_persisted"]
            data = (await client.get("/api/v1/topics/trending")).json()
            assert data["items"][0]["topic"]["label"] == "#aurora"
            assert data["items"][0]["metric"]["window_start"].endswith("Z")
            for path in ["/", "/topics/1", "/static/app.js", "/static/style.css"]:
                assert (await client.get(path)).status_code == 200


async def test_ingestion_failure_visible_to_readiness(database, tmp_path, monkeypatch):
    class BrokenConsumer:
        def __init__(self, *args, **kwargs):
            pass

        async def run(self, stop):
            raise RuntimeError("synthetic ingestion failure")

    monkeypatch.setattr("bluesky_ingest.runtime.Consumer", BrokenConsumer)
    app = application(
        Settings(
            database_url=database.engine.url.render_as_string(hide_password=False),
            raw_dir=tmp_path,
            detect_interval_seconds=0.05,
        )
    )
    async with app.router.lifespan_context(app):
        for _ in range(100):
            if app.state.metrics.connection_state == "failed":
                break
            await asyncio.sleep(0)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (await client.get("/readyz")).status_code == 503
            assert (await client.get("/healthz")).status_code == 503


def test_live_mode_requires_explicit_database():
    with pytest.raises(ValueError, match="DATABASE_URL"):
        application(Settings())


async def test_broken_detection_worker_is_replaced(database, tmp_path, monkeypatch):
    from concurrent.futures import Executor, Future, ThreadPoolExecutor
    from concurrent.futures.process import BrokenProcessPool

    class BrokenWorker(Executor):
        def submit(self, *args, **kwargs):
            future = Future()
            future.set_exception(BrokenProcessPool("synthetic worker exit"))
            return future

    created = []

    def factory():
        executor = BrokenWorker() if not created else ThreadPoolExecutor(max_workers=1)
        created.append(executor)
        return executor

    monkeypatch.setattr("bluesky_ingest.runtime.detection_worker", factory)
    app = application(
        Settings(
            database_url=database.engine.url.render_as_string(hide_password=False),
            raw_dir=tmp_path / "raw",
            detect_interval_seconds=0.01,
        ),
        demo=True,
    )
    async with app.router.lifespan_context(app):
        for _ in range(500):
            if app.state.metrics.last_detection_at:
                break
            await asyncio.sleep(0.01)
        assert len(created) == 2
        assert app.state.metrics.errors == 1
        assert app.state.metrics.last_detection_at
        assert app.state.metrics.detection_error is None
