FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.8.14 /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock ./
COPY src src
RUN uv sync --frozen --no-dev

COPY alembic.ini ./
COPY migrations migrations
COPY config config
CMD ["uv", "run", "--no-sync", "newsbot", "worker"]
