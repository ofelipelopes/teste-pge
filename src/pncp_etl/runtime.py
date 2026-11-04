"""Runtime configuration and deterministic Airflow run resolution."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from pncp_etl.contracts import QueryWindow, SafetyLimits
from pncp_etl.storage.postgres import PipelineRunSpec


class QueryMode(StrEnum):
    """Supported and semantically distinct PNCP ata queries."""

    VALIDITY = "vigencia"
    GLOBAL_UPDATE = "atualizacao"


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    """Environment-backed local pipeline settings."""

    runtime_root: Path
    database_dsn: str
    api_url: str
    schedule_cnpj: str | None
    batch_size: int
    limits: SafetyLimits
    update_api_url: str = "https://pncp.gov.br/api/consulta/v1/atas/atualizacao"
    schedule_query_mode: QueryMode = QueryMode.VALIDITY
    incremental_lookback_days: int = 2

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
        *,
        database_dsn: str,
    ) -> RuntimeSettings:
        """Load typed settings after the orchestrator resolves the database Connection."""
        values = os.environ if environment is None else environment
        if not database_dsn.strip():
            raise ValueError("database_dsn resolved by Airflow must not be empty")
        return cls(
            runtime_root=Path(values.get("PNCP_RUNTIME_ROOT", "/opt/airflow/runtime")),
            database_dsn=database_dsn,
            api_url=values.get(
                "PNCP_API_URL", "https://pncp.gov.br/api/consulta/v1/atas"
            ),
            schedule_cnpj=_none_if_blank(values.get("PNCP_SCHEDULE_CNPJ")),
            batch_size=_positive_int(values, "PNCP_BATCH_SIZE", 1_000),
            limits=SafetyLimits(
                page_size=_positive_int(values, "PNCP_PAGE_SIZE", 100),
                max_pages=_positive_int(values, "PNCP_MAX_PAGES", 1_000),
                max_records=_positive_int(values, "PNCP_MAX_RECORDS", 100_000),
                max_duration_seconds=_positive_int(
                    values, "PNCP_MAX_DURATION_SECONDS", 1_800
                ),
                max_artifact_bytes=_positive_int(
                    values, "PNCP_MAX_ARTIFACT_BYTES", 2 * 1024 * 1024 * 1024
                ),
            ),
            update_api_url=values.get(
                "PNCP_UPDATE_API_URL",
                "https://pncp.gov.br/api/consulta/v1/atas/atualizacao",
            ),
            schedule_query_mode=_query_mode(
                values.get("PNCP_SCHEDULE_QUERY_MODE", QueryMode.VALIDITY.value),
                "PNCP_SCHEDULE_QUERY_MODE",
            ),
            incremental_lookback_days=_bounded_positive_int(
                values, "PNCP_INCREMENTAL_LOOKBACK_DAYS", 2, maximum=7
            ),
        )


@dataclass(frozen=True, slots=True)
class ResolvedRun:
    """Validated task context shared by all five Airflow stages."""

    run: PipelineRunSpec
    limits: SafetyLimits
    runtime_root: Path
    database_dsn: str
    api_url: str
    batch_size: int
    query_mode: QueryMode = QueryMode.VALIDITY


def resolve_run(
    *,
    dag_id: str,
    run_id: str,
    logical_date: datetime,
    params: Mapping[str, Any],
    scheduled: bool,
    settings: RuntimeSettings,
) -> ResolvedRun:
    """Resolve manual or scheduled parameters into one stable execution identity."""
    query_mode = (
        settings.schedule_query_mode
        if scheduled
        else _query_mode(
            _optional_string(params.get("modo_consulta")) or QueryMode.VALIDITY.value,
            "modo_consulta",
        )
    )
    start_text = _optional_string(params.get("data_inicial"))
    end_text = _optional_string(params.get("data_final"))
    if (start_text is None) != (end_text is None):
        raise ValueError("data_inicial and data_final must be provided together")
    if start_text is None:
        previous_day = logical_date.date() - timedelta(days=1)
        end = previous_day
        if scheduled and query_mode is QueryMode.GLOBAL_UPDATE:
            start = end - timedelta(days=settings.incremental_lookback_days - 1)
        else:
            start = end
    else:
        start = _iso_date(start_text, "data_inicial")
        end = _iso_date(end_text, "data_final")
    if (end - start).days > 6:
        raise ValueError("query window cannot exceed 7 inclusive days")

    cnpj = _optional_string(params.get("cnpj"))
    if cnpj is None and scheduled:
        cnpj = settings.schedule_cnpj
    allow_unscoped = params.get("permitir_sem_cnpj") is True
    if cnpj is None and (scheduled or not allow_unscoped):
        raise ValueError("an unscoped run requires explicit permitir_sem_cnpj=true")
    administrative_unit = _optional_string(params.get("codigo_unidade"))
    window = QueryWindow(
        start=start,
        end=end,
        cnpj=cnpj,
        administrative_unit=administrative_unit,
    )
    execution_id = str(uuid5(NAMESPACE_URL, f"{dag_id}:{run_id}"))
    run = PipelineRunSpec(
        execution_id=execution_id,
        airflow_run_id=run_id,
        dag_id=dag_id,
        window=window,
        filters={
            "cnpj": cnpj,
            "codigo_unidade": administrative_unit,
            "scheduled": scheduled,
            "modo_consulta": query_mode.value,
        },
    )
    return ResolvedRun(
        run=run,
        limits=settings.limits,
        runtime_root=settings.runtime_root,
        database_dsn=settings.database_dsn,
        api_url=(
            settings.update_api_url
            if query_mode is QueryMode.GLOBAL_UPDATE
            else settings.api_url
        ),
        batch_size=settings.batch_size,
        query_mode=query_mode,
    )


def _positive_int(values: Mapping[str, str], name: str, default: int) -> int:
    try:
        value = int(values.get(name, str(default)))
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _bounded_positive_int(
    values: Mapping[str, str],
    name: str,
    default: int,
    *,
    maximum: int,
) -> int:
    value = _positive_int(values, name, default)
    if value > maximum:
        raise ValueError(f"{name} must be at most {maximum}")
    return value


def _query_mode(value: str, name: str) -> QueryMode:
    try:
        return QueryMode(value)
    except ValueError as error:
        raise ValueError(f"{name} must be vigencia or atualizacao") from error


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("optional pipeline parameters must be strings or null")
    return _none_if_blank(value)


def _none_if_blank(value: str | None) -> str | None:
    if value is None or not value.strip():
        return None
    return value.strip()


def _iso_date(value: str, field: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{field} must use YYYY-MM-DD") from error
