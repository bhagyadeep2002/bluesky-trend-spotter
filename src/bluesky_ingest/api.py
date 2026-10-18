from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Callable

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from .config import Settings
from .database import Database, utc
from .detection import floor_window
from .metrics import Metrics
from .models import Assignment, DetectionRun, Post, Topic, TopicTerm, WindowMetric
from .schemas import (
    HealthResponse,
    MetricResponse,
    PipelineResponse,
    PostPage,
    PostResponse,
    RankedTopic,
    TimelineResponse,
    TopicDetail,
    TopicResponse,
    TrendingResponse,
)
from .service import log_event

COVERAGE = "Observed public activity only. Coverage depends on upstream relay, filters and connectivity. Activity spikes do not establish truth, importance or virality."


def metric_response(metric: WindowMetric, settings: Settings) -> MetricResponse:
    return MetricResponse(
        **{
            name: getattr(metric, name)
            for name in MetricResponse.model_fields
            if name not in {"sample_sufficient", "persistence"}
        },
        sample_sufficient=metric.post_count >= settings.min_current_count,
        persistence=(
            "sustained"
            if metric.score_components.get("consecutive_spike_windows", 0) >= 2
            else "single window"
            if metric.spike_score > 0
            else "below threshold"
        ),
    )


def topic_response(session: Session, topic: Topic) -> TopicResponse:
    return TopicResponse(
        id=topic.id,
        label=topic.label,
        active=topic.active,
        terms=list(session.scalars(select(TopicTerm.term).where(TopicTerm.topic_id == topic.id))),
    )


def post_response(post: Post) -> PostResponse:
    rkey = post.uri.rsplit("/", 1)[-1]
    original = (
        None
        if post.author_did == "did:plc:synthetic"
        else f"https://bsky.app/profile/{post.author_did}/post/{rkey}"
    )
    return PostResponse(
        uri=post.uri,
        author_did=post.author_did,
        text=post.text or "",
        created_at=post.created_at,
        indexed_at=utc(post.indexed_at),
        language=post.language,
        original_url=original,
    )


def create_app(
    database: Database,
    settings: Settings,
    metrics: Metrics | None = None,
    *,
    demo: bool = False,
    clock: Callable[[], datetime] | None = None,
    lifespan=None,
) -> FastAPI:
    metrics = metrics or Metrics(connection_state="idle")
    now = clock or (lambda: datetime.now(UTC))
    app = FastAPI(
        title="Signal Watch — observed activity spikes", version="1.0.0", lifespan=lifespan
    )
    app.state.database, app.state.metrics, app.state.demo = database, metrics, demo

    @app.middleware("http")
    async def prevent_stale_post_cache(request: Request, call_next):
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @app.exception_handler(SQLAlchemyError)
    async def database_error(request: Request, exc: SQLAlchemyError):
        log_event("api_database_unavailable", error_type=type(exc).__name__)
        return JSONResponse({"detail": "Database temporarily unavailable"}, status_code=503)

    def get_topic(session: Session, topic_id: int) -> Topic:
        topic = session.get(Topic, topic_id)
        if topic is None:
            raise HTTPException(404, "Topic not found")
        return topic

    @app.get("/healthz", response_model=HealthResponse)
    def health():
        if metrics.connection_state == "failed":
            return JSONResponse({"status": "failed"}, status_code=503)
        return {"status": "ok"}

    @app.get("/readyz", response_model=HealthResponse)
    def ready():
        if not database.ready() or metrics.connection_state == "failed" or metrics.detection_error:
            return JSONResponse({"status": "not ready"}, status_code=503)
        return {"status": "ready"}

    @app.get("/api/v1/pipeline/status", response_model=PipelineResponse)
    def pipeline_status():
        return {**metrics.snapshot(), "mode": "demo" if demo else "live"}

    @app.get("/api/v1/topics/trending", response_model=TrendingResponse)
    def trending(
        window_minutes: int = Query(default=settings.window_minutes, ge=1, le=60),
        limit: int = Query(default=20, ge=1, le=100),
    ):
        if window_minutes != settings.window_minutes:
            raise HTTPException(422, "Requested window does not match the configured detector")
        start = floor_window(now(), window_minutes) - timedelta(minutes=window_minutes)
        with Session(database.engine) as session:
            rows = session.execute(
                select(Topic, WindowMetric)
                .join(WindowMetric)
                .where(
                    Topic.active.is_(True),
                    WindowMetric.window_start == start,
                    WindowMetric.window_minutes == window_minutes,
                    WindowMetric.spike_score > 0,
                )
                .order_by(WindowMetric.spike_score.desc(), Topic.id)
                .limit(limit)
            )
            items = [
                RankedTopic(
                    topic=topic_response(session, topic), metric=metric_response(metric, settings)
                )
                for topic, metric in rows
            ]
            last = session.scalar(
                select(func.max(DetectionRun.finished_at)).where(DetectionRun.status == "success")
            )
            return TrendingResponse(
                items=items,
                window_start=start,
                window_minutes=window_minutes,
                last_updated=last,
                demo=demo,
                coverage_note=COVERAGE,
            )

    @app.get("/api/v1/topics/{topic_id}", response_model=TopicDetail)
    def detail(topic_id: int):
        with Session(database.engine) as session:
            topic = get_topic(session, topic_id)
            rows = list(
                session.scalars(
                    select(WindowMetric)
                    .where(
                        WindowMetric.topic_id == topic_id,
                        WindowMetric.window_minutes == settings.window_minutes,
                    )
                    .order_by(WindowMetric.window_start.desc())
                    .limit(100)
                )
            )
            current = floor_window(now(), settings.window_minutes) - timedelta(
                minutes=settings.window_minutes
            )
            return TopicDetail(
                topic=topic_response(session, topic),
                latest_metric=metric_response(rows[0], settings) if rows else None,
                recent_metrics=[metric_response(row, settings) for row in rows],
                data_stale=not rows or utc(rows[0].window_start) < current,
                demo=demo,
            )

    @app.get("/api/v1/topics/{topic_id}/timeline", response_model=TimelineResponse)
    def timeline(topic_id: int, limit: int = Query(default=30, ge=1, le=288)):
        with Session(database.engine) as session:
            get_topic(session, topic_id)
            rows = list(
                session.scalars(
                    select(WindowMetric)
                    .where(
                        WindowMetric.topic_id == topic_id,
                        WindowMetric.window_minutes == settings.window_minutes,
                    )
                    .order_by(WindowMetric.window_start.desc())
                    .limit(limit)
                )
            )
            return TimelineResponse(
                topic_id=topic_id, items=[metric_response(row, settings) for row in reversed(rows)]
            )

    @app.get("/api/v1/topics/{topic_id}/posts", response_model=PostPage)
    def posts(
        topic_id: int,
        limit: int = Query(default=10, ge=1, le=100),
        offset: int = Query(default=0, ge=0, le=100_000),
    ):
        with Session(database.engine) as session:
            get_topic(session, topic_id)
            latest = session.scalar(
                select(WindowMetric)
                .where(
                    WindowMetric.topic_id == topic_id,
                    WindowMetric.window_minutes == settings.window_minutes,
                )
                .order_by(WindowMetric.window_start.desc())
                .limit(1)
            )
            filters = [
                Assignment.topic_id == topic_id,
                Post.deleted_at.is_(None),
                Post.text.is_not(None),
            ]
            if latest:
                filters.extend(
                    [
                        Post.activity_at >= latest.window_start,
                        Post.activity_at
                        < latest.window_start + timedelta(minutes=latest.window_minutes),
                    ]
                )
            query = select(Post).join(Assignment, Assignment.post_uri == Post.uri).where(*filters)
            total = session.scalar(select(func.count()).select_from(query.subquery()))
            records = session.scalars(
                query.order_by(Assignment.relevance_score.desc(), Post.activity_at.desc(), Post.uri)
                .offset(offset)
                .limit(limit)
            )
            return PostPage(
                items=[post_response(post) for post in records],
                total=total,
                limit=limit,
                offset=offset,
            )

    @app.get("/api/v1/search", response_model=PostPage)
    def search(
        q: str = Query(min_length=1, max_length=200),
        limit: int = Query(default=20, ge=1, le=100),
        offset: int = Query(default=0, ge=0, le=100_000),
    ):
        if not q.strip():
            raise HTTPException(422, "Search query must contain non-whitespace text")
        escaped = q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        with Session(database.engine) as session:
            query = select(Post).where(
                Post.deleted_at.is_(None), Post.text.ilike(f"%{escaped}%", escape="\\")
            )
            total = session.scalar(select(func.count()).select_from(query.subquery()))
            records = session.scalars(
                query.order_by(Post.activity_at.desc(), Post.uri).offset(offset).limit(limit)
            )
            return PostPage(
                items=[post_response(post) for post in records],
                total=total,
                limit=limit,
                offset=offset,
            )

    return app
