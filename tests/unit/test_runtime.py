"""Tests for deterministic runtime and recovery parameters."""

import json
from dataclasses import asdict, replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pncp_etl.contracts import ArtifactRef, SafetyLimits
from pncp_etl.runtime import QueryMode, RuntimeSettings, resolve_run

SETTINGS = RuntimeSettings(
    runtime_root=Path("/tmp/pncp-runtime"),
    database_dsn="postgresql://example",
    api_url="http://mock/v1/atas",
    schedule_cnpj="12345678000195",
    batch_size=100,
    limits=SafetyLimits(
        page_size=10,
        max_pages=10,
        max_records=100,
        max_duration_seconds=60,
        max_artifact_bytes=1_000_000,
    ),
)


def test_scheduled_run_derives_previous_day_and_configured_scope() -> None:
    """The daily schedule is deterministic in its already-localized Airflow context."""
    resolved = resolve_run(
        dag_id="pncp_atas_etl",
        run_id="scheduled__2026-11-02T06:00:00-03:00",
        logical_date=datetime(2026, 11, 2, 6, tzinfo=UTC),
        params={},
        scheduled=True,
        settings=SETTINGS,
    )

    assert resolved.run.window.start.isoformat() == "2026-11-01"
    assert resolved.run.window.end.isoformat() == "2026-11-01"
    assert resolved.run.window.cnpj == "12345678000195"


def test_manual_historical_period_requires_explicit_unscoped_opt_in() -> None:
    """Recovery can select an old period without silently authorizing national scope."""
    base = {
        "data_inicial": "2026-10-01",
        "data_final": "2026-10-03",
    }
    with pytest.raises(ValueError, match="permitir_sem_cnpj"):
        resolve_run(
            dag_id="pncp_atas_etl",
            run_id="manual__recovery",
            logical_date=datetime(2026, 11, 2, tzinfo=UTC),
            params=base,
            scheduled=False,
            settings=SETTINGS,
        )

    resolved = resolve_run(
        dag_id="pncp_atas_etl",
        run_id="manual__recovery",
        logical_date=datetime(2026, 11, 2, tzinfo=UTC),
        params={**base, "permitir_sem_cnpj": True},
        scheduled=False,
        settings=SETTINGS,
    )
    repeated = resolve_run(
        dag_id="pncp_atas_etl",
        run_id="manual__recovery",
        logical_date=datetime(2026, 11, 2, tzinfo=UTC),
        params={**base, "permitir_sem_cnpj": True},
        scheduled=False,
        settings=SETTINGS,
    )

    assert resolved.run.execution_id == repeated.run.execution_id
    assert resolved.run.window.start.isoformat() == "2026-10-01"
    assert resolved.run.window.end.isoformat() == "2026-10-03"


def test_manual_update_mode_reuses_pipeline_with_distinct_endpoint_and_provenance() -> None:
    """Manual incremental extraction changes source semantics, not the five-stage flow."""
    settings = replace(SETTINGS, update_api_url="http://mock/v1/atas/atualizacao")

    resolved = resolve_run(
        dag_id="pncp_atas_etl",
        run_id="manual__incremental",
        logical_date=datetime(2026, 11, 2, 6, tzinfo=UTC),
        params={
            "data_inicial": "2026-10-30",
            "data_final": "2026-11-01",
            "cnpj": "12345678000195",
            "modo_consulta": "atualizacao",
        },
        scheduled=False,
        settings=settings,
    )

    assert resolved.query_mode is QueryMode.GLOBAL_UPDATE
    assert resolved.api_url == "http://mock/v1/atas/atualizacao"
    assert resolved.run.filters["modo_consulta"] == "atualizacao"


def test_scheduled_update_mode_overlaps_source_dates_without_exceeding_limits() -> None:
    """A daily incremental schedule rereads a bounded inclusive lookback window."""
    settings = replace(
        SETTINGS,
        schedule_query_mode=QueryMode.GLOBAL_UPDATE,
        incremental_lookback_days=2,
        update_api_url="http://mock/v1/atas/atualizacao",
    )

    resolved = resolve_run(
        dag_id="pncp_atas_etl",
        run_id="scheduled__incremental",
        logical_date=datetime(2026, 11, 2, 6, tzinfo=UTC),
        params={},
        scheduled=True,
        settings=settings,
    )

    assert resolved.run.window.start.isoformat() == "2026-10-31"
    assert resolved.run.window.end.isoformat() == "2026-11-01"
    assert resolved.query_mode is QueryMode.GLOBAL_UPDATE


def test_incremental_environment_configuration_is_typed_and_bounded() -> None:
    """Mode and overlap errors fail before Airflow can call the source."""
    settings = RuntimeSettings.from_environment(
        {
            "PNCP_UPDATE_API_URL": "http://mock/v1/atas/atualizacao",
            "PNCP_SCHEDULE_QUERY_MODE": "atualizacao",
            "PNCP_INCREMENTAL_LOOKBACK_DAYS": "3",
        },
        database_dsn="postgresql://resolved-by-airflow",
    )
    assert settings.database_dsn == "postgresql://resolved-by-airflow"
    assert settings.update_api_url == "http://mock/v1/atas/atualizacao"
    assert settings.schedule_query_mode is QueryMode.GLOBAL_UPDATE
    assert settings.incremental_lookback_days == 3

    with pytest.raises(ValueError, match="PNCP_SCHEDULE_QUERY_MODE"):
        RuntimeSettings.from_environment(
            {"PNCP_SCHEDULE_QUERY_MODE": "unknown"},
            database_dsn="postgresql://resolved-by-airflow",
        )
    with pytest.raises(ValueError, match="PNCP_INCREMENTAL_LOOKBACK_DAYS"):
        RuntimeSettings.from_environment(
            {"PNCP_INCREMENTAL_LOOKBACK_DAYS": "8"},
            database_dsn="postgresql://resolved-by-airflow",
        )


def test_runtime_database_dsn_must_be_resolved_by_the_orchestrator() -> None:
    """Runtime settings never fall back to a credential embedded in source code."""
    with pytest.raises(TypeError, match="database_dsn"):
        RuntimeSettings.from_environment({})


def test_taskflow_reference_bundle_stays_below_xcom_limit() -> None:
    """Three artifact references fit comfortably below the 4 KiB design ceiling."""
    refs = {
        stage: asdict(
            ArtifactRef(
                execution_id="00000000-0000-0000-0000-000000000000",
                stage=stage,
                manifest_path=f"executions/abc/{stage}/manifest.json",
                record_count=100_000,
                checksum="a" * 64,
            )
        )
        for stage in ("extract", "validate", "transform")
    }

    assert len(json.dumps(refs).encode()) < 4 * 1024
