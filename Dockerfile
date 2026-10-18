FROM ghcr.io/astral-sh/uv:0.11.6 AS uv
FROM python:3.14-slim
COPY --from=uv /uv /uvx /bin/
WORKDIR /app
ENV UV_LINK_MODE=copy PYTHONUNBUFFERED=1
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY . .
RUN uv sync --locked --no-dev
ENV METRICS_HOST=0.0.0.0
EXPOSE 8000
CMD ["sh", "-c", "uv run --no-dev alembic upgrade head && exec uv run --no-dev bluesky-serve"]
