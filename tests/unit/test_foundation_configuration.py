"""Static contracts for the local reproducible foundation."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_compose_exposes_required_services_and_healthchecks() -> None:
    """Compose must model the minimal Airflow 3 and PostgreSQL topology."""
    document = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))
    services = document["services"]

    expected = {
        "postgres",
        "db-init",
        "airflow-init",
        "airflow-api-server",
        "airflow-scheduler",
        "airflow-dag-processor",
        "test",
    }
    assert expected <= services.keys()

    continuous = {
        "postgres",
        "airflow-api-server",
        "airflow-scheduler",
        "airflow-dag-processor",
    }
    assert all("healthcheck" in services[name] for name in continuous)


def test_local_executor_has_conservative_parallelism() -> None:
    """The local demonstration must not create one worker per host CPU."""
    document = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))
    environment = document["x-airflow-environment"]

    assert environment["AIRFLOW__CORE__PARALLELISM"] == "4"


def test_docker_context_excludes_private_and_runtime_files() -> None:
    """Private inputs and generated data must not enter the Docker context."""
    patterns = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()

    assert "private/" in patterns
    assert "*.pdf" in patterns
    assert ".env" in patterns
    assert "runtime/" in patterns
    assert "logs/" in patterns


def test_foundation_migration_creates_all_logical_schemas() -> None:
    """The first migration must establish the four designed logical areas."""
    sql = (ROOT / "sql/migrations/0001_foundation.sql").read_text(encoding="utf-8")

    for schema in ("ops", "staging", "quality", "curated"):
        assert f"CREATE SCHEMA IF NOT EXISTS {schema}" in sql


def test_dockerfile_pins_runtime_and_uses_non_root_user() -> None:
    """The application image must pin Python/uv and drop root privileges."""
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "ghcr.io/astral-sh/uv:0.13.6" in dockerfile
    assert "python:3.13-slim-bookworm" in dockerfile
    assert "USER airflow" in dockerfile


def test_database_bootstrap_does_not_print_role_passwords() -> None:
    """The role bootstrap query output can contain passwords and must be discarded."""
    script = (ROOT / "docker/postgres/init-databases.sh").read_text(encoding="utf-8")

    first_psql, _ = script.split("psql ", maxsplit=2)[1:]
    assert "--output /dev/null" in first_psql


def test_database_bootstrap_waits_for_postgres_after_restart() -> None:
    """The one-shot initializer must tolerate a simultaneous Compose restart."""
    script = (ROOT / "docker/postgres/init-databases.sh").read_text(encoding="utf-8")

    assert "pg_isready" in script
