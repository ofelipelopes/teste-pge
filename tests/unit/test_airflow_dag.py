"""Structural tests for the mandatory Airflow DAG."""

from datetime import UTC, datetime
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from airflow.models import DagBag

ROOT = Path(__file__).resolve().parents[2]
DAGS_FOLDER = ROOT / "dags"
if not DAGS_FOLDER.exists():
    DAGS_FOLDER = Path("/opt/airflow/dags")
dag_spec = spec_from_file_location(
    "pncp_atas_etl_test_module", DAGS_FOLDER / "pncp_atas_etl.py"
)
assert dag_spec is not None and dag_spec.loader is not None
dag_module = module_from_spec(dag_spec)
dag_spec.loader.exec_module(dag_module)


def _dag():
    bag = DagBag(dag_folder=str(DAGS_FOLDER))
    assert bag.import_errors == {}
    dag = bag.dags.get("pncp_atas_etl")
    assert dag is not None
    return dag


def test_dag_has_exactly_five_tasks_in_required_chain() -> None:
    """The challenge-mandated stages are visible and ordered in Airflow."""
    dag = _dag()
    expected = ["extract", "validate", "transform", "load_staging", "load_curated"]

    assert list(dag.task_dict) == expected
    for upstream, downstream in zip(expected, expected[1:], strict=False):
        assert dag.get_task(upstream).downstream_task_ids == {downstream}
        assert dag.get_task(downstream).upstream_task_ids == {upstream}


def test_dag_operational_contract_is_conservative() -> None:
    """Schedule, overlap, catchup, params, and task retry policy match the design."""
    dag = _dag()

    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert str(dag.timezone) == "America/Fortaleza"
    assert {
        "data_inicial",
        "data_final",
        "cnpj",
        "codigo_unidade",
        "permitir_sem_cnpj",
        "modo_consulta",
    } <= set(dag.params)
    assert dag.get_task("extract").retries == 1
    assert dag.get_task("validate").retries == 0
    assert dag.get_task("transform").retries == 0
    assert dag.get_task("load_staging").retries == 1
    assert dag.get_task("load_curated").retries == 1


def test_dockerfile_copies_dag_into_airflow_folder() -> None:
    """A successful local build must contain the DAG without a host bind mount."""
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "COPY --chown=airflow:airflow dags /opt/airflow/dags" in dockerfile


def test_manual_context_without_logical_date_uses_run_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Airflow 3 manual DagRuns may omit logical_date but always expose run_after."""
    run_after = datetime(2026, 11, 3, 20, 17, 6, tzinfo=UTC)
    context = {
        "dag": SimpleNamespace(dag_id="pncp_atas_etl"),
        "dag_run": SimpleNamespace(run_type="manual", run_after=run_after),
        "params": {
            "data_inicial": "2026-11-02",
            "data_final": "2026-11-02",
            "cnpj": "00508903000188",
            "modo_consulta": "atualizacao",
        },
        "run_id": "manual__pge_gate_b_1",
    }
    captured: dict[str, Any] = {}
    resolved = object()

    def capture_resolve_run(**kwargs: Any) -> object:
        captured.update(kwargs)
        return resolved

    monkeypatch.setattr(dag_module, "get_current_context", lambda: context)
    monkeypatch.setattr(dag_module, "_airflow_settings", lambda: object())
    monkeypatch.setattr(dag_module, "resolve_run", capture_resolve_run)

    assert dag_module._resolved_context() is resolved
    assert captured["logical_date"] == run_after


def test_runtime_settings_use_airflow_connection_and_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The DAG resolves configuration through Airflow's public SDK abstractions."""
    requested_connections: list[str] = []
    requested_variables: list[str] = []
    values = {
        "pncp_api_url": "https://source.test/v1/atas",
        "pncp_update_api_url": "https://source.test/v1/atas/atualizacao",
        "pncp_schedule_cnpj": "12345678000195",
        "pncp_schedule_query_mode": "atualizacao",
        "pncp_incremental_lookback_days": "3",
        "pncp_batch_size": "25",
    }

    class ConnectionStub:
        def get_uri(self) -> str:
            return "postgresql://resolved-by-airflow"

    def get_connection(connection_id: str) -> ConnectionStub:
        requested_connections.append(connection_id)
        return ConnectionStub()

    def get_variable(key: str, default: Any = None) -> Any:
        requested_variables.append(key)
        return values.get(key, default)

    monkeypatch.setattr(dag_module.BaseHook, "get_connection", get_connection)
    monkeypatch.setattr(dag_module.Variable, "get", get_variable)

    settings = dag_module._airflow_settings()

    assert requested_connections == ["pncp_db"]
    assert set(requested_variables) == set(values)
    assert settings.database_dsn == "postgresql://resolved-by-airflow"
    assert settings.api_url == "https://source.test/v1/atas"
    assert settings.schedule_query_mode.value == "atualizacao"
    assert settings.incremental_lookback_days == 3
    assert settings.batch_size == 25
