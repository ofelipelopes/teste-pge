"""Airflow orchestration for the PNCP price registration minutes pipeline."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pendulum
from airflow.sdk import Param, Variable, dag, get_current_context, task
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk.exceptions import AirflowFailException

from pncp_etl.contracts import ArtifactRef
from pncp_etl.pipeline.extract import extract_to_artifact
from pncp_etl.pipeline.transform import transform_artifact
from pncp_etl.pipeline.validate import validate_artifact
from pncp_etl.pncp.client import PncpClient, default_jitter
from pncp_etl.pncp.errors import ContractError, PermanentSourceError, RateLimitDeferred
from pncp_etl.runtime import ResolvedRun, RuntimeSettings, resolve_run
from pncp_etl.storage.artifacts import ArtifactRepository
from pncp_etl.storage.postgres import PostgresStore

LOGGER = logging.getLogger(__name__)
PNCP_DATABASE_CONNECTION_ID = "pncp_db"


def _airflow_settings() -> RuntimeSettings:
    """Resolve Connection and Variables through Airflow's public runtime SDK."""
    connection = BaseHook.get_connection(PNCP_DATABASE_CONNECTION_ID)
    values = dict(os.environ)
    values.update(
        {
            "PNCP_API_URL": Variable.get(
                "pncp_api_url",
                default="https://pncp.gov.br/api/consulta/v1/atas",
            ),
            "PNCP_UPDATE_API_URL": Variable.get(
                "pncp_update_api_url",
                default="https://pncp.gov.br/api/consulta/v1/atas/atualizacao",
            ),
            "PNCP_SCHEDULE_CNPJ": Variable.get("pncp_schedule_cnpj", default=""),
            "PNCP_SCHEDULE_QUERY_MODE": Variable.get(
                "pncp_schedule_query_mode", default="vigencia"
            ),
            "PNCP_INCREMENTAL_LOOKBACK_DAYS": Variable.get(
                "pncp_incremental_lookback_days", default="2"
            ),
            "PNCP_BATCH_SIZE": Variable.get("pncp_batch_size", default="1000"),
        }
    )
    return RuntimeSettings.from_environment(
        values,
        database_dsn=connection.get_uri(),
    )


def _reference_date(context: dict[str, Any], dag_run: Any) -> datetime:
    """Return Airflow's temporal reference for scheduled or manual runs."""
    reference = context.get("logical_date") or getattr(dag_run, "run_after", None)
    if not isinstance(reference, datetime):
        raise AirflowFailException("DagRun has no logical_date or run_after")
    return reference


def _resolved_context() -> ResolvedRun:
    context = get_current_context()
    dag_run = context["dag_run"]
    run_type = str(getattr(dag_run, "run_type", "")).lower()
    return resolve_run(
        dag_id=context["dag"].dag_id,
        run_id=context["run_id"],
        logical_date=_reference_date(context, dag_run),
        params=context["params"],
        scheduled="scheduled" in run_type,
        settings=_airflow_settings(),
    )


def _repository(resolved: ResolvedRun) -> ArtifactRepository:
    return ArtifactRepository(resolved.runtime_root, now=lambda: datetime.now(UTC))


def _ref(value: dict[str, Any]) -> ArtifactRef:
    return ArtifactRef(**value)


def _complete(stage: str, started: float, **metrics: Any) -> None:
    LOGGER.info(
        "pipeline_stage_complete stage=%s duration_seconds=%.6f metrics=%s",
        stage,
        time.perf_counter() - started,
        metrics,
    )


@dag(
    dag_id="pncp_atas_etl",
    schedule=os.environ.get("PNCP_DAG_SCHEDULE", "0 6 * * *"),
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Fortaleza"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=180),
    params={
        "data_inicial": Param(None, type=["null", "string"]),
        "data_final": Param(None, type=["null", "string"]),
        "cnpj": Param(None, type=["null", "string"]),
        "codigo_unidade": Param(None, type=["null", "string"]),
        "permitir_sem_cnpj": Param(False, type="boolean"),
        "modo_consulta": Param(
            "vigencia", type="string", enum=["vigencia", "atualizacao"]
        ),
    },
    tags=["pncp", "pge-ce", "etl"],
)
def pncp_atas_etl() -> None:
    """Define the mandatory five-stage PNCP workflow."""

    @task(
        task_id="extract",
        retries=1,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=30),
    )
    def extract_task() -> dict[str, Any]:
        started = time.perf_counter()
        resolved = _resolved_context()
        repository = _repository(resolved)
        with httpx.Client(follow_redirects=False) as http_client:
            client = PncpClient(
                http_client=http_client,
                now=lambda: datetime.now(UTC),
                sleep=time.sleep,
                jitter=default_jitter,
                base_url=resolved.api_url,
            )
            try:
                ref = extract_to_artifact(
                    client=client,
                    repository=repository,
                    execution_id=resolved.run.execution_id,
                    window=resolved.run.window,
                    limits=resolved.limits,
                    source=resolved.query_mode.value,
                )
            except RateLimitDeferred as error:
                LOGGER.error("rate_limit_deferred retry_not_before=%s", error.retry_not_before)
                raise AirflowFailException(str(error)) from error
            except (PermanentSourceError, ContractError) as error:
                raise AirflowFailException(str(error)) from error
        _complete("extract", started, records=ref.record_count)
        return asdict(ref)

    @task(task_id="validate", retries=0, execution_timeout=timedelta(minutes=20))
    def validate_task(raw_value: dict[str, Any]) -> dict[str, Any]:
        started = time.perf_counter()
        resolved = _resolved_context()
        try:
            ref = validate_artifact(
                repository=_repository(resolved),
                raw_ref=_ref(raw_value),
                limits=resolved.limits,
            )
        except ContractError as error:
            raise AirflowFailException(str(error)) from error
        _complete("validate", started, valid_records=ref.record_count)
        return {"raw": raw_value, "validation": asdict(ref)}

    @task(task_id="transform", retries=0, execution_timeout=timedelta(minutes=20))
    def transform_task(validation_bundle: dict[str, Any]) -> dict[str, Any]:
        started = time.perf_counter()
        resolved = _resolved_context()
        try:
            ref = transform_artifact(
                repository=_repository(resolved),
                validation_ref=_ref(validation_bundle["validation"]),
                limits=resolved.limits,
            )
        except ContractError as error:
            raise AirflowFailException(str(error)) from error
        _complete("transform", started, candidates=ref.record_count)
        return {
            "raw": validation_bundle["raw"],
            "validation": validation_bundle["validation"],
            "transform": asdict(ref),
        }

    @task(
        task_id="load_staging",
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=30),
    )
    def load_staging_task(
        transform_bundle: dict[str, Any],
    ) -> dict[str, Any]:
        started = time.perf_counter()
        resolved = _resolved_context()
        try:
            result = PostgresStore(resolved.database_dsn).load_staging(
                repository=_repository(resolved),
                run=resolved.run,
                raw_ref=_ref(transform_bundle["raw"]),
                validation_ref=_ref(transform_bundle["validation"]),
                transform_ref=_ref(transform_bundle["transform"]),
                batch_size=resolved.batch_size,
            )
        except ContractError as error:
            raise AirflowFailException(str(error)) from error
        _complete("load_staging", started, **asdict(result))
        return {
            "transform": transform_bundle["transform"],
            "staging": asdict(result),
        }

    @task(
        task_id="load_curated",
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=30),
    )
    def load_curated_task(
        staging_bundle: dict[str, Any],
    ) -> dict[str, Any]:
        started = time.perf_counter()
        resolved = _resolved_context()
        try:
            result = PostgresStore(resolved.database_dsn).promote_curated(
                run=resolved.run,
                transform_ref=_ref(staging_bundle["transform"]),
            )
        except ContractError as error:
            raise AirflowFailException(str(error)) from error
        _complete("load_curated", started, **asdict(result))
        return asdict(result)

    raw = extract_task()
    validated = validate_task(raw)
    transformed = transform_task(validated)
    staged = load_staging_task(transformed)
    load_curated_task(staged)


pncp_atas_etl()
