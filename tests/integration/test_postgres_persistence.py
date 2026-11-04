"""Integration tests for retry-safe PostgreSQL persistence."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import psycopg
import pytest

from pncp_etl.contracts import ArtifactRef, QueryWindow, SafetyLimits
from pncp_etl.pipeline.extract import extract_to_artifact
from pncp_etl.pipeline.transform import transform_artifact
from pncp_etl.pipeline.validate import validate_artifact
from pncp_etl.pncp.client import PncpClient
from pncp_etl.storage.artifacts import ArtifactRepository
from pncp_etl.storage.postgres import PipelineRunSpec, PostgresStore
from pncp_etl.versioning import ComparisonStatus, compare_staged_versions
from pncp_etl.versioning import main as compare_main

NOW = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)


def _dsn() -> str:
    dsn = os.environ.get("PNCP_TEST_DATABASE_URL") or os.environ.get(
        "AIRFLOW_CONN_PNCP_DB"
    )
    if not dsn:
        pytest.skip("PNCP_TEST_DATABASE_URL or AIRFLOW_CONN_PNCP_DB is required")
    return dsn


@pytest.fixture
def database_cleanup() -> Iterator[tuple[list[str], list[str]]]:
    """Track and remove only rows created by the current test."""
    executions: list[str] = []
    business_keys: list[str] = []
    yield executions, business_keys
    with psycopg.connect(_dsn(), autocommit=True) as connection:
        for key in business_keys:
            connection.execute(
                "DELETE FROM curated.ata WHERE numero_controle_pncp_ata = %s", (key,)
            )
        for execution_id in reversed(executions):
            connection.execute(
                "DELETE FROM quality.ata_rejection WHERE execution_id = %s", (execution_id,)
            )
            connection.execute(
                "DELETE FROM staging.ata_candidate WHERE execution_id = %s", (execution_id,)
            )
            connection.execute(
                "DELETE FROM staging.ata_raw WHERE execution_id = %s", (execution_id,)
            )
            connection.execute(
                "DELETE FROM ops.curated_promotion_attempt WHERE execution_id = %s",
                (execution_id,),
            )
            connection.execute(
                "DELETE FROM ops.curated_promotion WHERE execution_id = %s", (execution_id,)
            )
            connection.execute(
                "DELETE FROM ops.pipeline_run WHERE execution_id = %s", (execution_id,)
            )


def _limits() -> SafetyLimits:
    return SafetyLimits(
        page_size=10,
        max_pages=10,
        max_records=100,
        max_duration_seconds=60,
        max_artifact_bytes=10_000_000,
    )


def _artifacts(
    root: Path,
    execution_id: str,
    records: list[dict[str, Any]],
) -> tuple[ArtifactRepository, ArtifactRef, ArtifactRef, ArtifactRef]:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": records,
                "totalRegistros": len(records),
                "totalPaginas": 1,
                "numeroPagina": 1,
                "paginasRestantes": 0,
                "empty": not records,
            },
        )

    repository = ArtifactRepository(root, now=lambda: NOW)
    client = PncpClient(
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: NOW,
        sleep=lambda _: None,
        jitter=lambda _: 0.0,
    )
    raw_ref = extract_to_artifact(
        client=client,
        repository=repository,
        execution_id=execution_id,
        window=QueryWindow(start=date(2026, 11, 1), end=date(2026, 11, 1)),
        limits=_limits(),
    )
    validation_ref = validate_artifact(
        repository=repository,
        raw_ref=raw_ref,
        limits=_limits(),
    )
    transform_ref = transform_artifact(
        repository=repository,
        validation_ref=validation_ref,
        limits=_limits(),
    )
    return repository, raw_ref, validation_ref, transform_ref


def _run(execution_id: str) -> PipelineRunSpec:
    return PipelineRunSpec(
        execution_id=execution_id,
        airflow_run_id=f"manual__{execution_id}",
        dag_id="pncp_atas_etl",
        window=QueryWindow(start=date(2026, 11, 1), end=date(2026, 11, 1)),
        filters={"test": True},
    )


def _stage(
    *,
    store: PostgresStore,
    root: Path,
    execution_id: str,
    records: list[dict[str, Any]],
    batch_size: int = 2,
) -> tuple[PipelineRunSpec, ArtifactRef]:
    repository, raw_ref, validation_ref, transform_ref = _artifacts(
        root, execution_id, records
    )
    run = _run(execution_id)
    store.load_staging(
        repository=repository,
        run=run,
        raw_ref=raw_ref,
        validation_ref=validation_ref,
        transform_ref=transform_ref,
        batch_size=batch_size,
    )
    return run, transform_ref


def test_staging_recovers_after_failure_between_batches(
    tmp_path: Path,
    database_cleanup: tuple[list[str], list[str]],
) -> None:
    """Confirmed batches remain partial and retry completes without duplicates."""
    executions, business_keys = database_cleanup
    execution_id = str(uuid4())
    executions.append(execution_id)
    keys = [f"TEST-BATCH-{execution_id}-{index}" for index in range(4)]
    business_keys.extend(keys)
    records = [{"numeroControlePNCPAta": key} for key in keys]
    records.insert(2, {"objetoContratacao": "invalid without key"})
    repository, raw_ref, validation_ref, transform_ref = _artifacts(
        tmp_path, execution_id, records
    )
    store = PostgresStore(_dsn())
    run = _run(execution_id)

    def fail_after_first_batch(batch_number: int) -> None:
        if batch_number == 1:
            raise RuntimeError("injected staging failure")

    with pytest.raises(RuntimeError, match="injected staging failure"):
        store.load_staging(
            repository=repository,
            run=run,
            raw_ref=raw_ref,
            validation_ref=validation_ref,
            transform_ref=transform_ref,
            batch_size=2,
            after_batch=fail_after_first_batch,
        )

    with psycopg.connect(_dsn()) as connection:
        state = connection.execute(
            "SELECT state FROM ops.pipeline_run WHERE execution_id = %s", (execution_id,)
        ).fetchone()[0]
        partial_raw = connection.execute(
            "SELECT count(*) FROM staging.ata_raw WHERE execution_id = %s", (execution_id,)
        ).fetchone()[0]
    assert state == "LOADING_STAGING"
    assert 0 < partial_raw < len(records)

    result = store.load_staging(
        repository=repository,
        run=run,
        raw_ref=raw_ref,
        validation_ref=validation_ref,
        transform_ref=transform_ref,
        batch_size=2,
    )
    replay = store.load_staging(
        repository=repository,
        run=run,
        raw_ref=raw_ref,
        validation_ref=validation_ref,
        transform_ref=transform_ref,
        batch_size=2,
    )

    assert result.raw_records == 5
    assert result.rejection_evidence == 1
    assert result.candidates == 4
    assert replay.raw_records == result.raw_records
    with psycopg.connect(_dsn()) as connection:
        assert connection.execute(
            "SELECT count(*) FROM staging.ata_raw WHERE execution_id = %s", (execution_id,)
        ).fetchone()[0] == 5


def test_retry_after_commit_returns_persisted_result_without_second_effect(
    tmp_path: Path,
    database_cleanup: tuple[list[str], list[str]],
) -> None:
    """A lost Airflow acknowledgement is recovered from the committed promotion row."""
    executions, business_keys = database_cleanup
    execution_id = str(uuid4())
    key = f"TEST-ACK-{execution_id}"
    executions.append(execution_id)
    business_keys.append(key)
    store = PostgresStore(_dsn())
    run, transform_ref = _stage(
        store=store,
        root=tmp_path,
        execution_id=execution_id,
        records=[
            {
                "numeroControlePNCPAta": key,
                "objetoContratacao": "committed",
                "dataAtualizacaoGlobal": "2026-11-02T10:00:00Z",
            }
        ],
    )

    def lose_ack() -> None:
        raise RuntimeError("ack lost")

    with pytest.raises(RuntimeError, match="ack lost"):
        store.promote_curated(run=run, transform_ref=transform_ref, after_commit=lose_ack)

    with psycopg.connect(_dsn()) as connection:
        before = connection.execute(
            "SELECT updated_at, last_seen_at FROM curated.ata "
            "WHERE numero_controle_pncp_ata = %s",
            (key,),
        ).fetchone()
        assert connection.execute(
            "SELECT count(*) FROM ops.curated_promotion WHERE execution_id = %s",
            (execution_id,),
        ).fetchone()[0] == 1

    replay = store.promote_curated(run=run, transform_ref=transform_ref)

    with psycopg.connect(_dsn()) as connection:
        after = connection.execute(
            "SELECT updated_at, last_seen_at FROM curated.ata "
            "WHERE numero_controle_pncp_ata = %s",
            (key,),
        ).fetchone()
        replay_states = connection.execute(
            "SELECT count(*) FROM ops.curated_promotion_attempt "
            "WHERE execution_id = %s AND state = 'REPLAYED'",
            (execution_id,),
        ).fetchone()[0]
    assert replay.replayed is True
    assert replay.inserted == 1
    assert after == before
    assert replay_states == 1


def test_failure_before_commit_rolls_back_and_retry_succeeds(
    tmp_path: Path,
    database_cleanup: tuple[list[str], list[str]],
) -> None:
    """No curated data or commit proof survives an injected pre-commit failure."""
    executions, business_keys = database_cleanup
    execution_id = str(uuid4())
    key = f"TEST-PRECOMMIT-{execution_id}"
    executions.append(execution_id)
    business_keys.append(key)
    store = PostgresStore(_dsn())
    run, transform_ref = _stage(
        store=store,
        root=tmp_path,
        execution_id=execution_id,
        records=[{"numeroControlePNCPAta": key}],
    )

    def fail_before_commit() -> None:
        raise RuntimeError("before commit")

    with pytest.raises(RuntimeError, match="before commit"):
        store.promote_curated(
            run=run,
            transform_ref=transform_ref,
            before_commit=fail_before_commit,
        )

    with psycopg.connect(_dsn()) as connection:
        assert connection.execute(
            "SELECT count(*) FROM curated.ata WHERE numero_controle_pncp_ata = %s", (key,)
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT count(*) FROM ops.curated_promotion WHERE execution_id = %s",
            (execution_id,),
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT count(*) FROM ops.curated_promotion_attempt "
            "WHERE execution_id = %s AND state = 'ABORTED'",
            (execution_id,),
        ).fetchone()[0] == 1

    result = store.promote_curated(run=run, transform_ref=transform_ref)
    assert result.inserted == 1


def test_freshness_policy_blocks_stale_and_ambiguous_replays(
    tmp_path: Path,
    database_cleanup: tuple[list[str], list[str]],
) -> None:
    """Only a proven newer source version may replace curated business content."""
    executions, business_keys = database_cleanup
    key = f"TEST-FRESH-{uuid4()}"
    business_keys.append(key)
    store = PostgresStore(_dsn())

    def process(description: str, freshness: str | None) -> Any:
        execution_id = str(uuid4())
        executions.append(execution_id)
        record = {
            "numeroControlePNCPAta": key,
            "objetoContratacao": description,
        }
        if freshness is not None:
            record["dataAtualizacaoGlobal"] = freshness
        run, transform_ref = _stage(
            store=store,
            root=tmp_path,
            execution_id=execution_id,
            records=[record],
        )
        return store.promote_curated(run=run, transform_ref=transform_ref)

    inserted = process("base", "2026-11-02T10:00:00Z")
    updated = process("new", "2026-11-02T11:00:00Z")
    unchanged = process("new", "2026-11-02T11:00:00+00:00")
    stale = process("old replay", "2026-11-02T09:00:00Z")
    conflict = process("ambiguous", "2026-11-02T11:00:00Z")
    missing_freshness = process("no marker", None)

    assert inserted.inserted == 1
    assert updated.updated == 1
    assert unchanged.unchanged == 1
    assert stale.stale == 1
    assert conflict.conflict == 1
    assert missing_freshness.conflict == 1
    with psycopg.connect(_dsn()) as connection:
        row = connection.execute(
            "SELECT objeto_contratacao, data_atualizacao_global_utc "
            "FROM curated.ata WHERE numero_controle_pncp_ata = %s",
            (key,),
        ).fetchone()
    assert row[0] == "new"
    assert row[1] == datetime(2026, 11, 2, 11, 0, tzinfo=UTC)


def test_concurrent_promotions_serialize_and_preserve_newest_version(
    tmp_path: Path,
    database_cleanup: tuple[list[str], list[str]],
) -> None:
    """A global advisory lock makes final content independent of lock acquisition order."""
    executions, business_keys = database_cleanup
    key = f"TEST-CONCURRENT-{uuid4()}"
    business_keys.append(key)
    store = PostgresStore(_dsn())
    promotions = []
    for description, freshness in (
        ("older", "2026-11-02T10:00:00Z"),
        ("newest", "2026-11-02T11:00:00Z"),
    ):
        execution_id = str(uuid4())
        executions.append(execution_id)
        promotions.append(
            _stage(
                store=store,
                root=tmp_path,
                execution_id=execution_id,
                records=[
                    {
                        "numeroControlePNCPAta": key,
                        "objetoContratacao": description,
                        "dataAtualizacaoGlobal": freshness,
                    }
                ],
            )
        )

    def promote(item: tuple[PipelineRunSpec, ArtifactRef]) -> Any:
        run, transform_ref = item
        return store.promote_curated(run=run, transform_ref=transform_ref)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(promote, promotions))

    assert sum(result.inserted for result in results) == 1
    assert sum(result.updated + result.stale for result in results) == 1
    with psycopg.connect(_dsn()) as connection:
        row = connection.execute(
            "SELECT objeto_contratacao, data_atualizacao_global_utc "
            "FROM curated.ata WHERE numero_controle_pncp_ata = %s",
            (key,),
        ).fetchone()
    assert row == ("newest", datetime(2026, 11, 2, 11, 0, tzinfo=UTC))


def test_two_staged_collections_produce_reproducible_version_diff(
    tmp_path: Path,
    database_cleanup: tuple[list[str], list[str]],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Two explicit collection IDs retain provenance and changed canonical fields."""
    executions, business_keys = database_cleanup
    key = f"TEST-VERSION-{uuid4()}"
    business_keys.append(key)
    store = PostgresStore(_dsn())
    collected: list[str] = []
    for description, freshness in (
        ("old", "2026-11-02T10:00:00Z"),
        ("new", "2026-11-02T11:00:00Z"),
    ):
        execution_id = str(uuid4())
        executions.append(execution_id)
        collected.append(execution_id)
        _stage(
            store=store,
            root=tmp_path,
            execution_id=execution_id,
            records=[
                {
                    "numeroControlePNCPAta": key,
                    "objetoContratacao": description,
                    "dataAtualizacaoGlobal": freshness,
                }
            ],
        )

    result = compare_staged_versions(
        dsn=_dsn(),
        business_key=key,
        left_execution_id=collected[0],
        right_execution_id=collected[1],
    )

    assert result.status is ComparisonStatus.CHANGED
    assert result.newer_execution_id == collected[1]
    assert result.left.airflow_run_id == f"manual__{collected[0]}"
    assert result.right.airflow_run_id == f"manual__{collected[1]}"
    assert {change.field for change in result.changes} >= {
        "objeto_contratacao",
        "data_atualizacao_global_local",
    }

    monkeypatch.setenv("AIRFLOW_CONN_PNCP_DB", _dsn())
    assert compare_main([key, collected[0], collected[1]]) == 0
    cli_result = json.loads(capsys.readouterr().out)
    assert cli_result["status"] == "CHANGED"
    assert cli_result["newer_execution_id"] == collected[1]
