"""Read-only comparison of two explicitly selected staged observations."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import Any

import psycopg


class ComparisonStatus(StrEnum):
    """Safe conclusion supported by source content and freshness."""

    UNCHANGED = "UNCHANGED"
    CHANGED = "CHANGED"
    TEMPORAL_CONFLICT = "TEMPORAL_CONFLICT"


@dataclass(frozen=True, slots=True)
class VersionObservation:
    """One canonical ata observation plus collection provenance."""

    execution_id: str
    airflow_run_id: str
    window_start: date
    window_end: date
    collected_at: datetime
    canonical_payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class FieldChange:
    """Null-safe before/after values for one canonical field."""

    field: str
    left: Any
    right: Any


@dataclass(frozen=True, slots=True)
class VersionComparison:
    """Reproducible comparison without collection-order assumptions."""

    business_key: str
    left: VersionObservation
    right: VersionObservation
    status: ComparisonStatus
    changes: tuple[FieldChange, ...]
    newer_execution_id: str | None
    conflict_reason: str | None


def compare_versions(
    left: VersionObservation,
    right: VersionObservation,
) -> VersionComparison:
    """Compare two observations using only comparable source freshness."""
    if left.execution_id == right.execution_id:
        raise ValueError("version comparison requires distinct executions")
    left_key = _business_key(left)
    right_key = _business_key(right)
    if left_key != right_key:
        raise ValueError("version observations must identify the same ata")

    changes = tuple(
        FieldChange(
            field=field,
            left=left.canonical_payload.get(field),
            right=right.canonical_payload.get(field),
        )
        for field in sorted(left.canonical_payload.keys() | right.canonical_payload.keys())
        if left.canonical_payload.get(field) != right.canonical_payload.get(field)
    )
    if not changes:
        return VersionComparison(
            business_key=left_key,
            left=left,
            right=right,
            status=ComparisonStatus.UNCHANGED,
            changes=(),
            newer_execution_id=None,
            conflict_reason=None,
        )

    left_domain, left_freshness = _freshness(left.canonical_payload)
    right_domain, right_freshness = _freshness(right.canonical_payload)
    if left_freshness is None or right_freshness is None:
        return _conflict(left_key, left, right, changes, "missing_freshness")
    if left_domain != right_domain:
        return _conflict(
            left_key,
            left,
            right,
            changes,
            "incomparable_freshness_domain",
        )
    if left_freshness == right_freshness:
        return _conflict(
            left_key,
            left,
            right,
            changes,
            "equal_freshness_divergent_content",
        )
    newer_execution_id = (
        left.execution_id if left_freshness > right_freshness else right.execution_id
    )
    return VersionComparison(
        business_key=left_key,
        left=left,
        right=right,
        status=ComparisonStatus.CHANGED,
        changes=changes,
        newer_execution_id=newer_execution_id,
        conflict_reason=None,
    )


def compare_staged_versions(
    *,
    dsn: str,
    business_key: str,
    left_execution_id: str,
    right_execution_id: str,
) -> VersionComparison:
    """Load two staged observations from PostgreSQL and compare them."""
    if not business_key.strip():
        raise ValueError("business_key must not be empty")
    if left_execution_id == right_execution_id:
        raise ValueError("version comparison requires distinct executions")
    with psycopg.connect(dsn) as connection:
        left = _load_observation(connection, business_key, left_execution_id)
        right = _load_observation(connection, business_key, right_execution_id)
    return compare_versions(left, right)


def comparison_as_json(comparison: VersionComparison) -> str:
    """Serialize a comparison as stable operator-facing JSON."""
    return json.dumps(
        asdict(comparison),
        default=_json_default,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _business_key(observation: VersionObservation) -> str:
    value = observation.canonical_payload.get("numero_controle_pncp_ata")
    if not isinstance(value, str) or not value:
        raise ValueError("version observation has no canonical business key")
    return value


def _freshness(payload: dict[str, Any]) -> tuple[str | None, datetime | None]:
    local = payload.get("data_atualizacao_global_local")
    utc = payload.get("data_atualizacao_global_utc")
    if not isinstance(local, str):
        return None, None
    try:
        if isinstance(utc, str):
            return "OFFSET_AWARE", datetime.fromisoformat(utc.replace("Z", "+00:00"))
        return "LOCAL_UNZONED", datetime.fromisoformat(local)
    except ValueError as error:
        raise ValueError("canonical freshness marker is invalid") from error


def _conflict(
    business_key: str,
    left: VersionObservation,
    right: VersionObservation,
    changes: tuple[FieldChange, ...],
    reason: str,
) -> VersionComparison:
    return VersionComparison(
        business_key=business_key,
        left=left,
        right=right,
        status=ComparisonStatus.TEMPORAL_CONFLICT,
        changes=changes,
        newer_execution_id=None,
        conflict_reason=reason,
    )


def _load_observation(
    connection: psycopg.Connection[Any],
    business_key: str,
    execution_id: str,
) -> VersionObservation:
    row = connection.execute(
        """
        SELECT run.airflow_run_id, run.window_start, run.window_end,
               raw.collected_at, candidate.canonical_payload
        FROM staging.ata_candidate AS candidate
        JOIN ops.pipeline_run AS run
          ON run.execution_id = candidate.execution_id
        JOIN staging.ata_raw AS raw
          ON raw.execution_id = candidate.execution_id
         AND raw.page_number = candidate.page_number
         AND raw.record_index = candidate.record_index
        WHERE candidate.business_key = %s
          AND candidate.execution_id = %s
        """,
        (business_key, execution_id),
    ).fetchone()
    if row is None:
        raise ValueError(
            f"staged observation not found for execution {execution_id} and business key"
        )
    airflow_run_id, window_start, window_end, collected_at, canonical = row
    if not isinstance(canonical, dict):
        raise ValueError("staged observation has an invalid canonical payload")
    return VersionObservation(
        execution_id=execution_id,
        airflow_run_id=airflow_run_id,
        window_start=window_start,
        window_end=window_end,
        collected_at=collected_at,
        canonical_payload=canonical,
    )


def _json_default(value: Any) -> str:
    if isinstance(value, date | datetime):
        return value.isoformat()
    raise TypeError(f"value of type {type(value).__name__} is not JSON serializable")


def main(argv: Sequence[str] | None = None) -> int:
    """Compare two staged observations and print JSON without writing to the database."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("business_key")
    parser.add_argument("left_execution_id")
    parser.add_argument("right_execution_id")
    arguments = parser.parse_args(argv)
    dsn = os.environ.get("AIRFLOW_CONN_PNCP_DB")
    if not dsn:
        parser.error("AIRFLOW_CONN_PNCP_DB is required")
    try:
        result = compare_staged_versions(
            dsn=dsn,
            business_key=arguments.business_key,
            left_execution_id=arguments.left_execution_id,
            right_execution_id=arguments.right_execution_id,
        )
    except ValueError as error:
        parser.error(str(error))
    print(comparison_as_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
