from datetime import UTC, datetime, timedelta

import httpx
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from bluesky_ingest.api import create_app
from bluesky_ingest.config import Settings
from bluesky_ingest.detection import detect
from bluesky_ingest.events import parse_event
from bluesky_ingest.models import Topic
from bluesky_ingest.synthetic import make_event


async def test_api_ranking_pagination_validation_and_deleted_exclusion(database):
    now = datetime(2026, 10, 18, 12, 5, tzinfo=UTC)
    settings = Settings()
    for i in range(12):
        database.apply(
            parse_event(
                make_event("#aurora <script>bad()</script>", now - timedelta(minutes=2), str(i))
            ),
            now.isoformat(),
        )
    detect(database, settings, now)
    app = create_app(database, settings, demo=True, clock=lambda: now)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (await client.get("/healthz")).status_code == 200
        assert (await client.get("/readyz")).status_code == 200
        response = await client.get("/api/v1/topics/trending")
        assert response.status_code == 200
        assert response.json()["demo"]
        topic_id = response.json()["items"][0]["topic"]["id"]
        assert (await client.get(f"/api/v1/topics/{topic_id}")).json()["data_stale"] is False
        assert (await client.get(f"/api/v1/topics/{topic_id}/timeline")).json()["items"]
        first = (await client.get(f"/api/v1/topics/{topic_id}/posts?limit=5")).json()
        second = (await client.get(f"/api/v1/topics/{topic_id}/posts?limit=5&offset=5")).json()
        assert first["total"] == 12
        assert {p["uri"] for p in first["items"]}.isdisjoint(p["uri"] for p in second["items"])
        for i in range(12):
            database.apply(parse_event(make_event("", now, str(i), "delete")), now.isoformat())
        assert (await client.get(f"/api/v1/topics/{topic_id}/posts")).json()["total"] == 0
        assert (await client.get("/api/v1/search?q=aurora")).json()["items"] == []
        assert (await client.get("/api/v1/search?q=%25")).json()["items"] == []
        for path in [
            "/api/v1/search?q=",
            "/api/v1/search?q=++",
            "/api/v1/topics/trending?limit=0",
            "/api/v1/topics/trending?window_minutes=4",
            f"/api/v1/topics/{topic_id}/posts?offset=-1",
        ]:
            assert (await client.get(path)).status_code == 422
        assert (await client.get("/api/v1/topics/999999")).status_code == 404
        assert (await client.get("/api/v1/topics/not-a-number")).status_code == 422
        assert response.headers["cache-control"] == "no-store"


async def test_stale_detection_not_ranked_as_current(database):
    now = datetime(2026, 10, 18, 12, 5, tzinfo=UTC)
    for i in range(12):
        database.apply(
            parse_event(make_event("#aurora", now - timedelta(minutes=2), str(i))), now.isoformat()
        )
    detect(database, Settings(), now)
    app = create_app(database, Settings(), clock=lambda: now + timedelta(minutes=10))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (await client.get("/api/v1/topics/trending")).json()["items"] == []
        with Session(database.engine) as session:
            topic_id = session.scalar(select(Topic.id))
        assert (await client.get(f"/api/v1/topics/{topic_id}")).json()["data_stale"] is True


async def test_database_errors_are_sanitized(database, monkeypatch):
    def fail():
        raise OperationalError("secret SQL and credentials", {}, Exception("sensitive"))

    monkeypatch.setattr(database, "ready", fail)
    app = create_app(database, Settings())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/readyz")
        assert response.status_code == 503
        assert "secret" not in response.text and "sensitive" not in response.text
