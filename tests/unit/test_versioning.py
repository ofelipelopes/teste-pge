"""Tests for conservative comparison of staged ata observations."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta

import pytest

from pncp_etl.quality.records import validate_and_transform
from pncp_etl.versioning import (
    ComparisonStatus,
    VersionObservation,
    compare_versions,
    comparison_as_json,
)


def _canonical(*, description: str, freshness: str | None) -> dict[str, object]:
    payload: dict[str, object] = {
        "numeroControlePNCPAta": "ATA-VERSION-1",
        "objetoContratacao": description,
    }
    if freshness is not None:
        payload["dataAtualizacaoGlobal"] = freshness
    outcome = validate_and_transform(payload)
    assert outcome.canonical is not None
    return outcome.canonical


def _observation(
    execution_id: str,
    *,
    description: str,
    freshness: str | None,
    collected_at: datetime,
) -> VersionObservation:
    return VersionObservation(
        execution_id=execution_id,
        airflow_run_id=f"manual__{execution_id}",
        window_start=date(2026, 11, 1),
        window_end=date(2026, 11, 1),
        collected_at=collected_at,
        canonical_payload=_canonical(description=description, freshness=freshness),
    )


def test_comparable_change_names_fields_and_proven_newer_execution() -> None:
    """Source freshness, not collection order, proves which content is newer."""
    collected = datetime(2026, 11, 2, 12, tzinfo=UTC)
    left = _observation(
        "left-collected-later",
        description="old",
        freshness="2026-11-02T09:00:00Z",
        collected_at=collected + timedelta(hours=1),
    )
    right = _observation(
        "right-source-newer",
        description="new",
        freshness="2026-11-02T10:00:00+00:00",
        collected_at=collected,
    )

    result = compare_versions(left, right)
    changed = {change.field: (change.left, change.right) for change in result.changes}

    assert result.status is ComparisonStatus.CHANGED
    assert result.newer_execution_id == right.execution_id
    assert result.conflict_reason is None
    assert changed["objeto_contratacao"] == ("old", "new")
    assert "data_atualizacao_global_local" in changed


def test_identical_content_is_unchanged_but_keeps_both_provenances() -> None:
    """A repeated observation remains attributable to two distinct runs."""
    collected = datetime(2026, 11, 2, 12, tzinfo=UTC)
    left = _observation(
        "left",
        description="same",
        freshness="2026-11-02T10:00:00Z",
        collected_at=collected,
    )
    right = _observation(
        "right",
        description="same",
        freshness="2026-11-02T10:00:00+00:00",
        collected_at=collected + timedelta(minutes=5),
    )

    result = compare_versions(left, right)

    assert result.status is ComparisonStatus.UNCHANGED
    assert result.changes == ()
    assert result.newer_execution_id is None
    assert result.left.execution_id == "left"
    assert result.right.execution_id == "right"


@pytest.mark.parametrize(
    ("left_freshness", "right_freshness", "reason"),
    [
        (
            "2026-11-02T10:00:00Z",
            "2026-11-02T10:00:00+00:00",
            "equal_freshness_divergent_content",
        ),
        (None, "2026-11-02T10:00:00Z", "missing_freshness"),
        (
            "2026-11-02T10:00:00",
            "2026-11-02T10:00:00Z",
            "incomparable_freshness_domain",
        ),
    ],
)
def test_ambiguous_change_never_invents_a_newer_version(
    left_freshness: str | None,
    right_freshness: str | None,
    reason: str,
) -> None:
    """Ties, missing markers, and mixed domains remain explicit conflicts."""
    collected = datetime(2026, 11, 2, 12, tzinfo=UTC)
    left = _observation(
        "left", description="one", freshness=left_freshness, collected_at=collected
    )
    right = _observation(
        "right", description="two", freshness=right_freshness, collected_at=collected
    )

    result = compare_versions(left, right)

    assert result.status is ComparisonStatus.TEMPORAL_CONFLICT
    assert result.newer_execution_id is None
    assert result.conflict_reason == reason
    assert result.changes


def test_same_execution_is_rejected_and_json_is_stable() -> None:
    """A comparison requires two collections and has deterministic JSON output."""
    observation = _observation(
        "same",
        description="one",
        freshness="2026-11-02T10:00:00Z",
        collected_at=datetime(2026, 11, 2, 12, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="distinct executions"):
        compare_versions(observation, observation)

    second = _observation(
        "second",
        description="two",
        freshness="2026-11-02T11:00:00Z",
        collected_at=datetime(2026, 11, 2, 13, tzinfo=UTC),
    )
    serialized = comparison_as_json(compare_versions(observation, second))

    assert json.loads(serialized)["status"] == "CHANGED"
    assert serialized == comparison_as_json(compare_versions(observation, second))
