"""Transformation stage interface."""

from pncp_etl.contracts import ArtifactRef, SafetyLimits
from pncp_etl.pipeline.validate import _raw_record
from pncp_etl.pncp.errors import ContractError
from pncp_etl.quality.deduplication import RawRecordRef, SqliteDeduplicator
from pncp_etl.quality.mapping import MAPPING_VERSION
from pncp_etl.quality.records import validate_and_transform
from pncp_etl.storage.artifacts import ArtifactRepository

FRESHNESS_POLICY_VERSION = "1"


def transform_artifact(
    *,
    repository: ArtifactRepository,
    validation_ref: ArtifactRef,
    limits: SafetyLimits,
) -> ArtifactRef:
    """Transform validated records and publish deduplicated candidates."""
    if validation_ref.stage != "validate":
        raise ContractError("transform input must be a validation artifact")
    validation_manifest = repository.read_manifest(validation_ref)
    metadata = validation_manifest.get("metadata")
    if not isinstance(metadata, dict) or metadata.get("mapping_version") != MAPPING_VERSION:
        raise ContractError("validation artifact mapping version is incompatible")
    reused = repository.reuse_stage(
        execution_id=validation_ref.execution_id,
        stage="transform",
        input_ref=validation_ref,
    )
    if reused is not None:
        return reused

    with repository.stage_writer(
        execution_id=validation_ref.execution_id,
        stage="transform",
        filenames=("curated.jsonl", "rejected.jsonl"),
        input_ref=validation_ref,
        max_artifact_bytes=limits.max_artifact_bytes,
    ) as writer:
        sqlite_part = writer.part_path("dedup-index.sqlite")
        with SqliteDeduplicator(sqlite_part) as deduplicator:
            input_records = 0
            for valid_line in repository.iter_jsonl(validation_ref, "valid.jsonl"):
                page_number, record_index, payload = _raw_record(
                    valid_line, validation_ref.execution_id
                )
                outcome = validate_and_transform(payload)
                if outcome.canonical is None or outcome.issues:
                    raise ContractError("validation artifact contains an invalid record")
                deduplicator.add(
                    RawRecordRef(page_number, record_index),
                    outcome.canonical,
                )
                input_records += 1
            if input_records != validation_ref.record_count:
                raise ContractError("valid input count does not match validation artifact")

            deduplication = deduplicator.resolve()
            for winner in deduplicator.iter_winners():
                writer.write(
                    "curated.jsonl",
                    {
                        "execution_id": validation_ref.execution_id,
                        "page_number": winner.raw_ref.page_number,
                        "record_index": winner.raw_ref.record_index,
                        "canonical": winner.values,
                    },
                )
            for conflict in deduplicator.iter_conflicts():
                writer.write(
                    "rejected.jsonl",
                    {
                        "execution_id": validation_ref.execution_id,
                        "stage": "transform",
                        "page_number": conflict.raw_ref.page_number,
                        "record_index": conflict.raw_ref.record_index,
                        "business_key": conflict.identity,
                        "reason_code": conflict.reason_code,
                        "field": "numeroControlePNCPAta",
                        "detail": "duplicate identity has no safely ordered source version",
                    },
                )

        writer.register_external_part("dedup-index.sqlite")
        return writer.commit(
            record_count=deduplication.output_records,
            metrics={
                "input_records": deduplication.input_records,
                "transformed_records": deduplication.output_records,
                "identical_duplicates": deduplication.identical_duplicates,
                "superseded_versions": deduplication.superseded_versions,
                "conflict_identities": deduplication.conflict_identities,
                "conflict_records": deduplication.conflict_records,
            },
            metadata={
                "mapping_version": MAPPING_VERSION,
                "freshness_policy_version": FRESHNESS_POLICY_VERSION,
            },
        )
