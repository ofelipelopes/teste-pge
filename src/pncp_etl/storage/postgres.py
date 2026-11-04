"""PostgreSQL staging and curated persistence interfaces."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from itertools import batched
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

import psycopg
from psycopg.types.json import Jsonb

from pncp_etl.contracts import ArtifactRef, QueryWindow
from pncp_etl.pipeline.transform import FRESHNESS_POLICY_VERSION
from pncp_etl.pncp.errors import ContractError
from pncp_etl.quality.mapping import MAPPING_VERSION
from pncp_etl.storage.artifacts import ArtifactRepository

_PROMOTION_LOCK_ID = 7_254_301_129_826_011
_CANONICAL_COLUMNS = (
    "numero_controle_pncp_ata",
    "numero_ata_registro_preco",
    "ano_ata",
    "numero_controle_pncp_compra",
    "cancelado",
    "data_cancelamento_local",
    "data_cancelamento_utc",
    "data_assinatura_local",
    "data_assinatura_utc",
    "vigencia_inicio_local",
    "vigencia_inicio_utc",
    "vigencia_fim_local",
    "vigencia_fim_utc",
    "data_publicacao_pncp_local",
    "data_publicacao_pncp_utc",
    "data_inclusao_local",
    "data_inclusao_utc",
    "data_atualizacao_local",
    "data_atualizacao_utc",
    "data_atualizacao_global_local",
    "data_atualizacao_global_utc",
    "usuario",
    "objeto_contratacao",
    "cnpj_orgao",
    "nome_orgao",
    "codigo_unidade_orgao",
    "nome_unidade_orgao",
    "cnpj_orgao_subrogado",
    "nome_orgao_subrogado",
    "codigo_unidade_orgao_subrogado",
    "nome_unidade_orgao_subrogado",
    "possibilidade_adesao",
)


@dataclass(frozen=True, slots=True)
class PipelineRunSpec:
    """Durable identity and parameters for one orchestration run."""

    execution_id: str
    airflow_run_id: str
    dag_id: str
    window: QueryWindow
    filters: dict[str, Any]


@dataclass(frozen=True, slots=True)
class StagingResult:
    """Reconciled staging counts."""

    raw_records: int
    rejection_evidence: int
    candidates: int
    batches: int


@dataclass(frozen=True, slots=True)
class LoadResult:
    """Immutable logical result of one curated promotion."""

    promotion_id: str
    inserted: int
    updated: int
    unchanged: int
    stale: int
    conflict: int
    replayed: bool = False


class PostgresStore:
    """Persist pipeline artifacts with explicit PostgreSQL transactions."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def load_staging(
        self,
        *,
        repository: ArtifactRepository,
        run: PipelineRunSpec,
        raw_ref: ArtifactRef,
        validation_ref: ArtifactRef,
        transform_ref: ArtifactRef,
        batch_size: int = 1_000,
        after_batch: Callable[[int], None] | None = None,
    ) -> StagingResult:
        """Persist source evidence in retry-safe batches."""
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        _validate_artifact_chain(run, raw_ref, validation_ref, transform_ref)
        repository.read_manifest(raw_ref)
        repository.read_manifest(validation_ref)
        repository.read_manifest(transform_ref)
        artifacts = {
            "raw": _ref_payload(raw_ref),
            "validation": _ref_payload(validation_ref),
            "transform": _ref_payload(transform_ref),
        }
        self._ensure_pipeline_run(run, artifacts)

        batch_number = 0
        rejection_evidence = 0
        for rows in batched(
            repository.iter_jsonl(raw_ref, "raw.jsonl"), batch_size, strict=False
        ):
            self._insert_raw_batch(run.execution_id, rows)
            batch_number += 1
            if after_batch is not None:
                after_batch(batch_number)

        for filename, ref in (
            ("rejected.jsonl", validation_ref),
            ("rejected.jsonl", transform_ref),
        ):
            for rows in batched(
                repository.iter_jsonl(ref, filename), batch_size, strict=False
            ):
                self._insert_rejection_batch(run.execution_id, rows)
                rejection_evidence += len(rows)
                batch_number += 1
                if after_batch is not None:
                    after_batch(batch_number)

        for rows in batched(
            repository.iter_jsonl(transform_ref, "curated.jsonl"),
            batch_size,
            strict=False,
        ):
            self._insert_candidate_batch(run.execution_id, rows)
            batch_number += 1
            if after_batch is not None:
                after_batch(batch_number)

        counts = self._complete_staging(
            run=run,
            artifacts=artifacts,
            expected_raw=raw_ref.record_count,
            expected_rejections=rejection_evidence,
            expected_candidates=transform_ref.record_count,
        )
        return StagingResult(
            raw_records=counts[0],
            rejection_evidence=counts[1],
            candidates=counts[2],
            batches=batch_number,
        )

    def promote_curated(
        self,
        *,
        run: PipelineRunSpec,
        transform_ref: ArtifactRef,
        before_commit: Callable[[], None] | None = None,
        after_commit: Callable[[], None] | None = None,
    ) -> LoadResult:
        """Promote staged candidates under one durable commit protocol."""
        if transform_ref.execution_id != run.execution_id or transform_ref.stage != "transform":
            raise ContractError("promotion input does not match the pipeline run")
        input_fingerprint = _input_fingerprint(transform_ref)
        promotion_id = str(
            uuid5(
                NAMESPACE_URL,
                ":".join(
                    (
                        "pncp-curated",
                        run.execution_id,
                        input_fingerprint,
                        MAPPING_VERSION,
                        FRESHNESS_POLICY_VERSION,
                    )
                ),
            )
        )
        attempt_id = str(uuid4())
        self._start_attempt(
            attempt_id=attempt_id,
            promotion_id=promotion_id,
            execution_id=run.execution_id,
        )
        committed = False
        try:
            with self._connect() as connection:
                with connection.transaction():
                    connection.execute(
                        "SELECT pg_advisory_xact_lock(%s)", (_PROMOTION_LOCK_ID,)
                    )
                    artifacts = self._locked_pipeline_artifacts(connection, run.execution_id)
                    expected_transform = artifacts.get("transform")
                    if expected_transform != _ref_payload(transform_ref):
                        raise ContractError("staging was completed for another transform artifact")

                    existing = connection.execute(
                        """
                        SELECT execution_id, input_fingerprint, mapping_version,
                               freshness_policy_version, load_result
                        FROM ops.curated_promotion
                        WHERE promotion_id = %s
                        """,
                        (promotion_id,),
                    ).fetchone()
                    if existing is not None:
                        _validate_existing_promotion(
                            existing,
                            execution_id=run.execution_id,
                            input_fingerprint=input_fingerprint,
                        )
                        result = _load_result(promotion_id, existing[4], replayed=True)
                        connection.execute(
                            """
                            UPDATE ops.curated_promotion_attempt
                            SET state = 'REPLAYED', finished_at = CURRENT_TIMESTAMP,
                                existing_promotion_id = %s
                            WHERE attempt_id = %s
                            """,
                            (promotion_id, attempt_id),
                        )
                    else:
                        conflicting = connection.execute(
                            """
                            SELECT input_fingerprint, mapping_version, freshness_policy_version
                            FROM ops.curated_promotion
                            WHERE execution_id = %s
                            """,
                            (run.execution_id,),
                        ).fetchone()
                        if conflicting is not None:
                            raise ContractError(
                                "execution already has a different committed promotion"
                            )
                        result = self._apply_candidates(
                            connection=connection,
                            execution_id=run.execution_id,
                            promotion_id=promotion_id,
                        )
                        result_payload = _result_payload(result)
                        connection.execute(
                            """
                            INSERT INTO ops.curated_promotion (
                                promotion_id, execution_id, input_fingerprint,
                                mapping_version, freshness_policy_version, status, load_result
                            ) VALUES (%s, %s, %s, %s, %s, 'COMMITTED', %s)
                            """,
                            (
                                promotion_id,
                                run.execution_id,
                                input_fingerprint,
                                MAPPING_VERSION,
                                FRESHNESS_POLICY_VERSION,
                                Jsonb(result_payload),
                            ),
                        )
                        connection.execute(
                            """
                            UPDATE ops.pipeline_run
                            SET state = 'CURATED_COMMITTED',
                                curated_committed_at = CURRENT_TIMESTAMP,
                                metrics = metrics || %s
                            WHERE execution_id = %s
                            """,
                            (Jsonb({"promotion": result_payload}), run.execution_id),
                        )
                        connection.execute(
                            """
                            UPDATE ops.curated_promotion_attempt
                            SET state = 'COMMITTED', finished_at = CURRENT_TIMESTAMP
                            WHERE attempt_id = %s
                            """,
                            (attempt_id,),
                        )
                        if before_commit is not None:
                            before_commit()
                committed = True
            if after_commit is not None and not result.replayed:
                after_commit()
            return result
        except Exception as error:
            if not committed:
                self._abort_attempt(attempt_id, error)
            raise

    def _connect(self) -> psycopg.Connection[Any]:
        return psycopg.connect(self.dsn, options="-c timezone=UTC")

    def _ensure_pipeline_run(
        self,
        run: PipelineRunSpec,
        artifacts: dict[str, Any],
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO ops.pipeline_run (
                    execution_id, airflow_run_id, dag_id, window_start, window_end,
                    filters, state, mapping_version, freshness_policy_version, artifacts
                ) VALUES (%s, %s, %s, %s, %s, %s, 'LOADING_STAGING', %s, %s, %s)
                ON CONFLICT (execution_id) DO NOTHING
                """,
                (
                    run.execution_id,
                    run.airflow_run_id,
                    run.dag_id,
                    run.window.start,
                    run.window.end,
                    Jsonb(run.filters),
                    MAPPING_VERSION,
                    FRESHNESS_POLICY_VERSION,
                    Jsonb(artifacts),
                ),
            )
            current = connection.execute(
                """
                SELECT airflow_run_id, dag_id, window_start, window_end, filters,
                       mapping_version, freshness_policy_version, artifacts
                FROM ops.pipeline_run
                WHERE execution_id = %s
                """,
                (run.execution_id,),
            ).fetchone()
            expected = (
                run.airflow_run_id,
                run.dag_id,
                run.window.start,
                run.window.end,
                run.filters,
                MAPPING_VERSION,
                FRESHNESS_POLICY_VERSION,
                artifacts,
            )
            if current != expected:
                raise ContractError("execution_id is already bound to different run inputs")

    def _insert_raw_batch(
        self,
        execution_id: str,
        rows: tuple[dict[str, Any], ...],
    ) -> None:
        parameters = []
        for row in rows:
            page_number, record_index = _record_position(row, execution_id)
            payload = row.get("payload")
            collected_at = row.get("collected_at")
            if not isinstance(payload, dict) or not isinstance(collected_at, str):
                raise ContractError("raw staging input has invalid payload or collected_at")
            canonical_bytes = json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
                allow_nan=False,
            ).encode()
            business_key = payload.get("numeroControlePNCPAta")
            if not isinstance(business_key, str) or not business_key.strip():
                business_key = None
            else:
                business_key = business_key.strip()
            parameters.append(
                (
                    execution_id,
                    page_number,
                    record_index,
                    Jsonb(payload),
                    hashlib.sha256(canonical_bytes).hexdigest(),
                    collected_at,
                    business_key,
                )
            )
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.executemany(
                """
                INSERT INTO staging.ata_raw (
                    execution_id, page_number, record_index, payload,
                    payload_sha256, collected_at, business_key
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (execution_id, page_number, record_index) DO UPDATE
                SET payload = staging.ata_raw.payload
                WHERE staging.ata_raw.payload = EXCLUDED.payload
                  AND staging.ata_raw.payload_sha256 = EXCLUDED.payload_sha256
                  AND staging.ata_raw.collected_at = EXCLUDED.collected_at
                """,
                parameters,
            )
            if cursor.rowcount != len(parameters):
                raise ContractError("raw retry conflicts with previously staged content")

    def _insert_rejection_batch(
        self,
        execution_id: str,
        rows: tuple[dict[str, Any], ...],
    ) -> None:
        parameters = []
        for row in rows:
            page_number, record_index = _record_position(row, execution_id)
            stage = row.get("stage")
            reason_code = row.get("reason_code")
            if stage not in {"validate", "transform"} or not isinstance(reason_code, str):
                raise ContractError("rejection artifact has invalid stage or reason_code")
            parameters.append(
                (
                    execution_id,
                    stage,
                    row.get("business_key"),
                    reason_code,
                    row.get("detail"),
                    execution_id,
                    page_number,
                    record_index,
                )
            )
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.executemany(
                """
                INSERT INTO quality.ata_rejection (
                    execution_id, stage, business_key, reason_code, detail, raw_id
                )
                SELECT %s, %s, %s, %s, %s, raw_id
                FROM staging.ata_raw
                WHERE execution_id = %s AND page_number = %s AND record_index = %s
                ON CONFLICT (execution_id, raw_id, stage, reason_code) DO NOTHING
                """,
                parameters,
            )
            if cursor.rowcount < 0 or cursor.rowcount > len(parameters):
                raise ContractError("rejection batch returned an invalid affected-row count")

    def _insert_candidate_batch(
        self,
        execution_id: str,
        rows: tuple[dict[str, Any], ...],
    ) -> None:
        parameters = []
        for row in rows:
            page_number, record_index = _record_position(row, execution_id)
            canonical = row.get("canonical")
            if not isinstance(canonical, dict):
                raise ContractError("transform candidate is not a canonical JSON object")
            business_key = canonical.get("numero_controle_pncp_ata")
            if not isinstance(business_key, str) or not business_key:
                raise ContractError("transform candidate has no business key")
            parameters.append(
                (
                    execution_id,
                    page_number,
                    record_index,
                    business_key,
                    Jsonb(canonical),
                )
            )
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.executemany(
                """
                INSERT INTO staging.ata_candidate (
                    execution_id, page_number, record_index, business_key, canonical_payload
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (execution_id, page_number, record_index) DO UPDATE
                SET canonical_payload = staging.ata_candidate.canonical_payload
                WHERE staging.ata_candidate.business_key = EXCLUDED.business_key
                  AND staging.ata_candidate.canonical_payload = EXCLUDED.canonical_payload
                """,
                parameters,
            )
            if cursor.rowcount != len(parameters):
                raise ContractError("candidate retry conflicts with staged content")

    def _complete_staging(
        self,
        *,
        run: PipelineRunSpec,
        artifacts: dict[str, Any],
        expected_raw: int,
        expected_rejections: int,
        expected_candidates: int,
    ) -> tuple[int, int, int]:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE staging.ata_raw AS raw
                SET validation_status = CASE
                    WHEN EXISTS (
                        SELECT 1 FROM quality.ata_rejection AS rejected
                        WHERE rejected.raw_id = raw.raw_id AND rejected.stage = 'validate'
                    ) THEN 'REJECTED' ELSE 'VALID' END
                WHERE execution_id = %s
                """,
                (run.execution_id,),
            )
            counts = connection.execute(
                """
                SELECT
                    (SELECT count(*) FROM staging.ata_raw WHERE execution_id = %s),
                    (SELECT count(*) FROM quality.ata_rejection
                     WHERE execution_id = %s AND stage IN ('validate', 'transform')),
                    (SELECT count(*) FROM staging.ata_candidate WHERE execution_id = %s)
                """,
                (run.execution_id, run.execution_id, run.execution_id),
            ).fetchone()
            expected = (expected_raw, expected_rejections, expected_candidates)
            if counts != expected:
                raise ContractError(f"staging counts do not reconcile: {counts} != {expected}")
            metrics = {
                "staging": {
                    "raw_records": counts[0],
                    "rejection_evidence": counts[1],
                    "candidates": counts[2],
                }
            }
            connection.execute(
                """
                UPDATE ops.pipeline_run
                SET state = CASE
                        WHEN state = 'CURATED_COMMITTED' THEN state
                        ELSE 'STAGING_COMPLETE'
                    END,
                    staging_completed_at = COALESCE(staging_completed_at, CURRENT_TIMESTAMP),
                    artifacts = %s,
                    metrics = metrics || %s
                WHERE execution_id = %s
                """,
                (Jsonb(artifacts), Jsonb(metrics), run.execution_id),
            )
            return counts

    def _start_attempt(
        self,
        *,
        attempt_id: str,
        promotion_id: str,
        execution_id: str,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO ops.curated_promotion_attempt (
                    attempt_id, promotion_id, execution_id, state
                ) VALUES (%s, %s, %s, 'STARTED')
                """,
                (attempt_id, promotion_id, execution_id),
            )

    @staticmethod
    def _locked_pipeline_artifacts(
        connection: psycopg.Connection[Any], execution_id: str
    ) -> dict[str, Any]:
        row = connection.execute(
            """
            SELECT state, artifacts
            FROM ops.pipeline_run
            WHERE execution_id = %s
            FOR UPDATE
            """,
            (execution_id,),
        ).fetchone()
        if row is None or row[0] not in {"STAGING_COMPLETE", "CURATED_COMMITTED"}:
            raise ContractError("pipeline run is not staging-complete")
        if not isinstance(row[1], dict):
            raise ContractError("pipeline run has invalid artifact metadata")
        return row[1]

    def _apply_candidates(
        self,
        *,
        connection: psycopg.Connection[Any],
        execution_id: str,
        promotion_id: str,
    ) -> LoadResult:
        counts = {key: 0 for key in ("inserted", "updated", "unchanged", "stale", "conflict")}
        candidates = connection.execute(
            """
            SELECT page_number, record_index, business_key, canonical_payload
            FROM staging.ata_candidate
            WHERE execution_id = %s
            ORDER BY business_key
            """,
            (execution_id,),
        )
        candidate_count = 0
        for page_number, record_index, business_key, canonical in candidates:
            candidate_count += 1
            current = connection.execute(
                """
                SELECT canonical_payload
                FROM curated.ata
                WHERE numero_controle_pncp_ata = %s
                FOR UPDATE
                """,
                (business_key,),
            ).fetchone()
            if current is None:
                self._insert_curated(
                    connection, canonical, execution_id=execution_id, promotion_id=promotion_id
                )
                counts["inserted"] += 1
                continue
            existing = current[0]
            if existing == canonical:
                connection.execute(
                    """
                    UPDATE curated.ata
                    SET last_seen_at = CURRENT_TIMESTAMP,
                        last_seen_execution_id = %s
                    WHERE numero_controle_pncp_ata = %s
                    """,
                    (execution_id, business_key),
                )
                counts["unchanged"] += 1
                continue
            freshness = _compare_freshness(existing, canonical)
            if freshness == "newer":
                self._update_curated(
                    connection, canonical, execution_id=execution_id, promotion_id=promotion_id
                )
                counts["updated"] += 1
            elif freshness == "stale":
                self._insert_promotion_rejection(
                    connection,
                    execution_id=execution_id,
                    page_number=page_number,
                    record_index=record_index,
                    business_key=business_key,
                    reason_code="stale_source_version",
                )
                counts["stale"] += 1
            else:
                self._insert_promotion_rejection(
                    connection,
                    execution_id=execution_id,
                    page_number=page_number,
                    record_index=record_index,
                    business_key=business_key,
                    reason_code="ambiguous_source_version",
                )
                counts["conflict"] += 1

        if candidate_count != sum(counts.values()):
            raise ContractError("curated promotion counts do not reconcile")
        return LoadResult(promotion_id=promotion_id, **counts)

    @staticmethod
    def _insert_curated(
        connection: psycopg.Connection[Any],
        canonical: dict[str, Any],
        *,
        execution_id: str,
        promotion_id: str,
    ) -> None:
        columns = ", ".join(_CANONICAL_COLUMNS)
        placeholders = ", ".join("%s" for _ in _CANONICAL_COLUMNS)
        values = [canonical.get(column) for column in _CANONICAL_COLUMNS]
        connection.execute(
            f"""
            INSERT INTO curated.ata (
                {columns}, canonical_payload,
                first_seen_execution_id, last_seen_execution_id,
                last_changed_execution_id, last_promotion_id
            ) VALUES (
                {placeholders}, %s, %s, %s, %s, %s
            )
            """,
            (
                *values,
                Jsonb(canonical),
                execution_id,
                execution_id,
                execution_id,
                promotion_id,
            ),
        )

    @staticmethod
    def _update_curated(
        connection: psycopg.Connection[Any],
        canonical: dict[str, Any],
        *,
        execution_id: str,
        promotion_id: str,
    ) -> None:
        mutable_columns = _CANONICAL_COLUMNS[1:]
        assignments = ", ".join(f"{column} = %s" for column in mutable_columns)
        values = [canonical.get(column) for column in mutable_columns]
        connection.execute(
            f"""
            UPDATE curated.ata
            SET {assignments},
                canonical_payload = %s,
                updated_at = CURRENT_TIMESTAMP,
                last_seen_at = CURRENT_TIMESTAMP,
                last_seen_execution_id = %s,
                last_changed_execution_id = %s,
                last_promotion_id = %s
            WHERE numero_controle_pncp_ata = %s
            """,
            (
                *values,
                Jsonb(canonical),
                execution_id,
                execution_id,
                promotion_id,
                canonical["numero_controle_pncp_ata"],
            ),
        )

    @staticmethod
    def _insert_promotion_rejection(
        connection: psycopg.Connection[Any],
        *,
        execution_id: str,
        page_number: int,
        record_index: int,
        business_key: str,
        reason_code: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO quality.ata_rejection (
                execution_id, raw_id, stage, business_key, reason_code, detail
            )
            SELECT %s, raw_id, 'load_curated', %s, %s,
                   'incoming content cannot safely replace the curated source version'
            FROM staging.ata_raw
            WHERE execution_id = %s AND page_number = %s AND record_index = %s
            ON CONFLICT (execution_id, raw_id, stage, reason_code) DO NOTHING
            """,
            (
                execution_id,
                business_key,
                reason_code,
                execution_id,
                page_number,
                record_index,
            ),
        )

    def _abort_attempt(self, attempt_id: str, error: Exception) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE ops.curated_promotion_attempt
                SET state = 'ABORTED', finished_at = CURRENT_TIMESTAMP, error_class = %s
                WHERE attempt_id = %s AND state = 'STARTED'
                """,
                (type(error).__name__, attempt_id),
            )


def _validate_artifact_chain(
    run: PipelineRunSpec,
    raw_ref: ArtifactRef,
    validation_ref: ArtifactRef,
    transform_ref: ArtifactRef,
) -> None:
    refs = (raw_ref, validation_ref, transform_ref)
    if any(ref.execution_id != run.execution_id for ref in refs):
        raise ContractError("artifact execution does not match the pipeline run")
    if tuple(ref.stage for ref in refs) != ("extract", "validate", "transform"):
        raise ContractError("staging requires extract, validate, and transform artifacts")


def _ref_payload(ref: ArtifactRef) -> dict[str, str | int]:
    return {
        "execution_id": ref.execution_id,
        "stage": ref.stage,
        "manifest_path": ref.manifest_path,
        "record_count": ref.record_count,
        "checksum": ref.checksum,
    }


def _record_position(row: dict[str, Any], execution_id: str) -> tuple[int, int]:
    if row.get("execution_id") != execution_id:
        raise ContractError("staging record belongs to another execution")
    page_number = row.get("page_number")
    record_index = row.get("record_index")
    if isinstance(page_number, bool) or not isinstance(page_number, int) or page_number < 1:
        raise ContractError("staging record has invalid page_number")
    if isinstance(record_index, bool) or not isinstance(record_index, int) or record_index < 0:
        raise ContractError("staging record has invalid record_index")
    return page_number, record_index


def _input_fingerprint(ref: ArtifactRef) -> str:
    content = json.dumps(_ref_payload(ref), separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(content).hexdigest()


def _validate_existing_promotion(
    row: tuple[Any, ...],
    *,
    execution_id: str,
    input_fingerprint: str,
) -> None:
    normalized = (str(row[0]), row[1], row[2], row[3])
    if normalized != (
        execution_id,
        input_fingerprint,
        MAPPING_VERSION,
        FRESHNESS_POLICY_VERSION,
    ):
        raise ContractError("committed promotion fingerprint or versions do not match")


def _result_payload(result: LoadResult) -> dict[str, int | str]:
    return {
        "promotion_id": result.promotion_id,
        "inserted": result.inserted,
        "updated": result.updated,
        "unchanged": result.unchanged,
        "stale": result.stale,
        "conflict": result.conflict,
    }


def _load_result(promotion_id: str, value: Any, *, replayed: bool) -> LoadResult:
    if not isinstance(value, dict) or value.get("promotion_id") != promotion_id:
        raise ContractError("committed promotion has an invalid LoadResult")
    counts: dict[str, int] = {}
    for field in ("inserted", "updated", "unchanged", "stale", "conflict"):
        count = value.get(field)
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ContractError("committed promotion has invalid result counts")
        counts[field] = count
    return LoadResult(promotion_id=promotion_id, replayed=replayed, **counts)


def _compare_freshness(existing: dict[str, Any], incoming: dict[str, Any]) -> str:
    existing_domain, existing_value = _freshness(existing)
    incoming_domain, incoming_value = _freshness(incoming)
    if (
        existing_domain is None
        or incoming_domain is None
        or existing_domain != incoming_domain
        or existing_value is None
        or incoming_value is None
    ):
        return "conflict"
    if incoming_value > existing_value:
        return "newer"
    if incoming_value < existing_value:
        return "stale"
    return "conflict"


def _freshness(canonical: dict[str, Any]) -> tuple[str | None, datetime | None]:
    local = canonical.get("data_atualizacao_global_local")
    utc = canonical.get("data_atualizacao_global_utc")
    if not isinstance(local, str):
        return None, None
    try:
        if isinstance(utc, str):
            return "OFFSET_AWARE", datetime.fromisoformat(utc.replace("Z", "+00:00"))
        return "LOCAL_UNZONED", datetime.fromisoformat(local)
    except ValueError as error:
        raise ContractError("canonical freshness marker is invalid") from error
