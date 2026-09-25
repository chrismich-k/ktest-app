FROM python:3.11-slim AS builder
WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src/ ./src/
RUN uv sync --frozen --no-dev

FROM python:3.11-slim
WORKDIR /app
COPY --from=builder /app/.venv ./.venv
COPY --from=builder /app/src ./src
COPY pyproject.toml ./
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 4266
CMD ["dagster", "api", "grpc", "-m", "ktest.smard.definitions", "-p", "4266", "-h", "0.0.0.0"]
