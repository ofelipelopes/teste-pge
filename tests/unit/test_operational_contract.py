"""Executable contracts for local operation and handoff documentation."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ENVIRONMENT = {
    "PNCP_DAG_SCHEDULE",
    "PNCP_PAGE_SIZE",
    "PNCP_MAX_PAGES",
    "PNCP_MAX_RECORDS",
    "PNCP_MAX_DURATION_SECONDS",
    "PNCP_MAX_ARTIFACT_BYTES",
    "AIRFLOW_CONN_PNCP_DB",
    "AIRFLOW_VAR_PNCP_API_URL",
    "AIRFLOW_VAR_PNCP_UPDATE_API_URL",
    "AIRFLOW_VAR_PNCP_SCHEDULE_CNPJ",
    "AIRFLOW_VAR_PNCP_SCHEDULE_QUERY_MODE",
    "AIRFLOW_VAR_PNCP_INCREMENTAL_LOOKBACK_DAYS",
    "AIRFLOW_VAR_PNCP_BATCH_SIZE",
}

ENV_TEMPLATE_SETTINGS = {
    "PNCP_API_URL",
    "PNCP_UPDATE_API_URL",
    "PNCP_SCHEDULE_CNPJ",
    "PNCP_DAG_SCHEDULE",
    "PNCP_SCHEDULE_QUERY_MODE",
    "PNCP_INCREMENTAL_LOOKBACK_DAYS",
    "PNCP_PAGE_SIZE",
    "PNCP_MAX_PAGES",
    "PNCP_MAX_RECORDS",
    "PNCP_MAX_DURATION_SECONDS",
    "PNCP_MAX_ARTIFACT_BYTES",
    "PNCP_BATCH_SIZE",
}


def test_pipeline_settings_are_exposed_to_airflow_and_env_template() -> None:
    """Every typed runtime setting must be configurable without editing code."""
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))
    airflow_environment = compose["x-airflow-environment"]
    env_names = {
        line.split("=", maxsplit=1)[0]
        for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#") and "=" in line
    }

    assert airflow_environment.keys() >= PIPELINE_ENVIRONMENT
    assert env_names >= ENV_TEMPLATE_SETTINGS


def test_public_configuration_has_no_executable_password_fallbacks() -> None:
    """Missing secrets fail fast instead of activating repository-known passwords."""
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    runtime = (ROOT / "src/pncp_etl/runtime.py").read_text(encoding="utf-8")
    env_template = (ROOT / ".env.example").read_text(encoding="utf-8")

    assert "dev_only_change_me" not in compose
    assert "dev_only_change_me" not in runtime
    for name in (
        "POSTGRES_SUPERUSER_PASSWORD",
        "AIRFLOW_DB_PASSWORD",
        "AIRFLOW_ADMIN_PASSWORD",
        "AIRFLOW_JWT_SECRET",
        "PNCP_DB_PASSWORD",
    ):
        assert f"${{{name}:?" in compose
        assert f"{name}=\n" in env_template


def test_readme_covers_the_reproducible_operator_journey() -> None:
    """The public handoff must describe only the commands the project supports."""
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    required_fragments = (
        "docker compose up -d --wait",
        "http://localhost:8080",
        "airflow dags trigger pncp_atas_etl",
        "docker compose --profile test run --rm test",
        "docker compose down",
        "permitir_sem_cnpj",
        "ops.pipeline_run",
        "ops.curated_promotion",
        "curated.ata",
        "last_seen_execution_id",
        "python -m pncp_etl.versioning",
        '\"modo_consulta\":\"atualizacao\"',
        "PNCP_INCREMENTAL_LOOKBACK_DAYS",
        'BaseHook.get_connection("pncp_db")',
        "Variable.get(...)",
        "Não existem senhas padrão",
        "Retry de requisição",
        "reprocessamento de um período histórico",
        "vigência",
        "atualização",
        "P1",
    )
    for fragment in required_fragments:
        assert fragment in readme


def test_test_image_contains_files_checked_by_static_contracts() -> None:
    """The Linux suite must see the same public operational files as the host suite."""
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "COPY --chown=airflow:airflow README.md .env.example ./" in dockerfile
