# syntax=docker/dockerfile:1.7

FROM ghcr.io/astral-sh/uv:0.13.6 AS uv-bin

FROM python:3.13-slim-bookworm AS runtime

ARG AIRFLOW_UID=50000
ARG AIRFLOW_GID=50000

ENV AIRFLOW_HOME=/opt/airflow \
    PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    UV_CACHE_DIR=/tmp/uv-cache \
    UV_LINK_MODE=copy

RUN groupadd --gid "${AIRFLOW_GID}" airflow \
    && useradd --uid "${AIRFLOW_UID}" --gid "${AIRFLOW_GID}" --create-home airflow \
    && mkdir -p /app /opt/airflow/config /opt/airflow/dags /opt/airflow/logs /opt/airflow/runtime \
    && chown -R airflow:airflow /app /opt/airflow

COPY --from=uv-bin /uv /uvx /usr/local/bin/

WORKDIR /app
USER airflow

COPY --chown=airflow:airflow pyproject.toml uv.lock .python-version ./
RUN uv sync --locked --no-dev --no-install-project

COPY --chown=airflow:airflow src ./src
COPY --chown=airflow:airflow docker ./docker
COPY --chown=airflow:airflow dags /opt/airflow/dags
RUN uv sync --locked --no-dev --no-editable \
    && uv cache prune --ci

FROM runtime AS test

COPY --chown=airflow:airflow tests ./tests
COPY --chown=airflow:airflow Dockerfile compose.yaml .dockerignore ./
COPY --chown=airflow:airflow README.md .env.example ./
COPY --chown=airflow:airflow sql ./sql
RUN uv sync --locked --no-editable \
    && uv cache prune --ci

FROM runtime AS final

USER airflow
CMD ["airflow", "version"]
