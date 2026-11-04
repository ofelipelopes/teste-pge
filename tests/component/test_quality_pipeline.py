"""Component tests for validation and transformation artifacts."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from pncp_etl.contracts import QueryWindow, SafetyLimits
from pncp_etl.pipeline.extract import extract_to_artifact
from pncp_etl.pipeline.transform import transform_artifact
from pncp_etl.pipeline.validate import validate_artifact
from pncp_etl.pncp.client import PncpClient
from pncp_etl.pncp.errors import ContractError
from pncp_etl.storage.artifacts import ArtifactRepository

NOW = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)


def _limits() -> SafetyLimits:
    return SafetyLimits(
        page_size=10,
        max_pages=10,
        max_records=100,
        max_duration_seconds=60,
        max_artifact_bytes=1_000_000,
    )


def _extract(
    tmp_path: Path,
    records: list[dict[str, Any]],
    *,
    execution_id: str,
) -> tuple[ArtifactRepository, Any]:
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

    client = PncpClient(
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: NOW,
        sleep=lambda _: None,
        jitter=lambda _: 0.0,
    )
    repository = ArtifactRepository(tmp_path, now=lambda: NOW)
    raw_ref = extract_to_artifact(
        client=client,
        repository=repository,
        execution_id=execution_id,
        window=QueryWindow(start=date(2026, 11, 1), end=date(2026, 11, 1)),
        limits=_limits(),
    )
    return repository, raw_ref


def test_raw_to_validation_to_transform_reconciles_counts(tmp_path: Path) -> None:
    """Valid records continue, invalid records are evidenced, and newer versions win."""
    records = [
        {
            "numeroControlePNCPAta": "ATA-A",
            "objetoContratacao": "old",
            "dataAtualizacaoGlobal": "2026-11-02T09:00:00Z",
        },
        {"objetoContratacao": "missing key"},
        {
            "numeroControlePNCPAta": "ATA-A",
            "objetoContratacao": "new",
            "dataAtualizacaoGlobal": "2026-11-02T10:00:00Z",
        },
        {"numeroControlePNCPAta": "ATA-B", "objetoContratacao": "only"},
    ]
    repository, raw_ref = _extract(tmp_path, records, execution_id="quality-1")

    validation_ref = validate_artifact(
        repository=repository,
        raw_ref=raw_ref,
        limits=_limits(),
    )
    validation_manifest = repository.read_manifest(validation_ref)
    valid_lines = list(repository.iter_jsonl(validation_ref, "valid.jsonl"))
    validation_rejections = list(
        repository.iter_jsonl(validation_ref, "rejected.jsonl")
    )

    assert validation_manifest["metrics"]["input_records"] == 4
    assert validation_manifest["metrics"]["valid_records"] == 3
    assert validation_manifest["metrics"]["rejected_records"] == 1
    assert len(valid_lines) == 3
    assert len(validation_rejections) == 1
    assert validation_rejections[0]["reason_code"] == "missing_business_key"
    assert "payload" not in validation_rejections[0]

    transform_ref = transform_artifact(
        repository=repository,
        validation_ref=validation_ref,
        limits=_limits(),
    )
    transform_manifest = repository.read_manifest(transform_ref)
    curated_lines = list(repository.iter_jsonl(transform_ref, "curated.jsonl"))

    assert transform_manifest["metrics"]["input_records"] == 3
    assert transform_manifest["metrics"]["transformed_records"] == 2
    assert transform_manifest["metrics"]["superseded_versions"] == 1
    assert transform_manifest["metrics"]["conflict_records"] == 0
    assert transform_manifest["metrics"]["input_records"] == (
        transform_manifest["metrics"]["transformed_records"]
        + transform_manifest["metrics"]["identical_duplicates"]
        + transform_manifest["metrics"]["superseded_versions"]
        + transform_manifest["metrics"]["conflict_records"]
    )
    assert [line["canonical"]["numero_controle_pncp_ata"] for line in curated_lines] == [
        "ATA-A",
        "ATA-B",
    ]
    assert curated_lines[0]["canonical"]["objeto_contratacao"] == "new"


def test_transform_quarantines_equal_timestamp_conflict(tmp_path: Path) -> None:
    """Divergent content at equal source freshness never reaches curated output."""
    records = [
        {
            "numeroControlePNCPAta": "ATA-A",
            "objetoContratacao": "one",
            "dataAtualizacaoGlobal": "2026-11-02T09:00:00Z",
        },
        {
            "numeroControlePNCPAta": "ATA-A",
            "objetoContratacao": "two",
            "dataAtualizacaoGlobal": "2026-11-02T09:00:00+00:00",
        },
    ]
    repository, raw_ref = _extract(tmp_path, records, execution_id="quality-conflict")
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

    assert list(repository.iter_jsonl(transform_ref, "curated.jsonl")) == []
    rejections = list(repository.iter_jsonl(transform_ref, "rejected.jsonl"))
    assert len(rejections) == 2
    assert {item["reason_code"] for item in rejections} == {"conflicting_duplicate"}


def test_validation_rejects_corrupted_raw_before_publishing(tmp_path: Path) -> None:
    """A consumer validates the producer checksum before reading any record."""
    repository, raw_ref = _extract(
        tmp_path,
        [{"numeroControlePNCPAta": "ATA-A"}],
        execution_id="quality-corrupt",
    )
    raw_manifest = repository.read_manifest(raw_ref)
    raw_path = tmp_path / raw_manifest["files"][0]["path"]
    content = bytearray(raw_path.read_bytes())
    content[0] = ord("X")
    raw_path.write_bytes(content)

    with pytest.raises(ContractError, match="checksum"):
        validate_artifact(repository=repository, raw_ref=raw_ref, limits=_limits())

    assert not list(tmp_path.rglob("validate/manifest.json"))


def test_validation_retry_reuses_complete_artifact(tmp_path: Path) -> None:
    """A retry verifies and reuses the committed validation result."""
    repository, raw_ref = _extract(
        tmp_path,
        [{"numeroControlePNCPAta": "ATA-A"}],
        execution_id="quality-retry",
    )
    first = validate_artifact(repository=repository, raw_ref=raw_ref, limits=_limits())
    valid_path = next(tmp_path.rglob("validate/valid.jsonl"))
    first_mtime = valid_path.stat().st_mtime_ns

    second = validate_artifact(repository=repository, raw_ref=raw_ref, limits=_limits())

    assert second == first
    assert valid_path.stat().st_mtime_ns == first_mtime
