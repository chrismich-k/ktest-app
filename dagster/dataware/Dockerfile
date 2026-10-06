FROM python:3.11-slim AS builder
WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
ENV PATH="/app/.venv/bin:$PATH"

# dev group included here: `dg` (dagster-dg-cli) is needed below to prepare
# the dbt manifest; it is pruned again before the venv is copied over.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

COPY src/ ./src/
COPY dbt/ ./dbt/
COPY .env.example ./
RUN uv sync --frozen

# Prepare the dbt project state (dbt deps + dbt parse -> manifest) into
# src/ktest/dataware/defs/.local_defs_state/. This needs no database
# connection, only valid-looking DATAWARE_* values, which .env.example
# provides; `dbt deps` needs network access to hub.getdbt.com.
RUN set -a && . ./.env.example && set +a \
    && dg utils refresh-defs-state

RUN uv sync --frozen --no-dev

FROM python:3.11-slim
WORKDIR /app
COPY --from=builder /app/.venv ./.venv
COPY --from=builder /app/src ./src
COPY --from=builder /app/dbt ./dbt
COPY pyproject.toml ./
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 4266
CMD ["dagster", "api", "grpc", "-m", "ktest.dataware.definitions", "-p", "4266", "-h", "0.0.0.0"]
