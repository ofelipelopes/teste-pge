"""Validation stage interface."""

from typing import Any

from pncp_etl.contracts import ArtifactRef, SafetyLimits
from pncp_etl.pncp.errors import ContractError
from pncp_etl.quality.mapping import MAPPING_VERSION
from pncp_etl.quality.records import validate_and_transform
from pncp_etl.storage.artifacts import ArtifactRepository


def validate_artifact(
    *,
    repository: ArtifactRepository,
    raw_ref: ArtifactRef,
    limits: SafetyLimits,
) -> ArtifactRef:
    """Validate a raw artifact and publish valid and rejected JSONL files."""
    if raw_ref.stage != "extract":
        raise ContractError("validation input must be an extract artifact")
    repository.read_manifest(raw_ref)
    reused = repository.reuse_stage(
        execution_id=raw_ref.execution_id,
        stage="validate",
        input_ref=raw_ref,
    )
    if reused is not None:
        return reused

    input_records = 0
    valid_records = 0
    rejected_records = 0
    rejection_evidence = 0
    rejection_counts: dict[str, int] = {}
    with repository.stage_writer(
        execution_id=raw_ref.execution_id,
        stage="validate",
        filenames=("valid.jsonl", "rejected.jsonl"),
        input_ref=raw_ref,
        max_artifact_bytes=limits.max_artifact_bytes,
    ) as writer:
        for raw_line in repository.iter_jsonl(raw_ref, "raw.jsonl"):
            page_number, record_index, payload = _raw_record(raw_line, raw_ref.execution_id)
            input_records += 1
            outcome = validate_and_transform(payload)
            if not outcome.issues:
                writer.write("valid.jsonl", raw_line)
                valid_records += 1
                continue
            rejected_records += 1
            for issue in outcome.issues:
                writer.write(
                    "rejected.jsonl",
                    {
                        "execution_id": raw_ref.execution_id,
                        "stage": "validate",
                        "page_number": page_number,
                        "record_index": record_index,
                        "business_key": _available_business_key(payload),
                        "reason_code": issue.reason_code,
                        "field": issue.field,
                        "detail": issue.detail,
                    },
                )
                rejection_evidence += 1
                rejection_counts[issue.reason_code] = (
                    rejection_counts.get(issue.reason_code, 0) + 1
                )

        if input_records != raw_ref.record_count:
            raise ContractError("raw input count does not match extract artifact reference")
        return writer.commit(
            record_count=valid_records,
            metrics={
                "input_records": input_records,
                "valid_records": valid_records,
                "rejected_records": rejected_records,
                "rejection_evidence": rejection_evidence,
                "rejection_counts": rejection_counts,
            },
            metadata={"mapping_version": MAPPING_VERSION},
        )


def _raw_record(
    raw_line: dict[str, Any], execution_id: str
) -> tuple[int, int, dict[str, Any]]:
    if raw_line.get("execution_id") != execution_id:
        raise ContractError("raw record belongs to another execution")
    page_number = raw_line.get("page_number")
    record_index = raw_line.get("record_index")
    payload = raw_line.get("payload")
    collected_at = raw_line.get("collected_at")
    if isinstance(page_number, bool) or not isinstance(page_number, int) or page_number < 1:
        raise ContractError("raw record has invalid page_number")
    if isinstance(record_index, bool) or not isinstance(record_index, int) or record_index < 0:
        raise ContractError("raw record has invalid record_index")
    if not isinstance(payload, dict):
        raise ContractError("raw record payload must be a JSON object")
    if not isinstance(collected_at, str):
        raise ContractError("raw record has invalid collected_at")
    return page_number, record_index, payload


def _available_business_key(payload: dict[str, Any]) -> str | None:
    value = payload.get("numeroControlePNCPAta")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None
