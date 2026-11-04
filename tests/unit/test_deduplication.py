"""Tests for disk-backed deterministic duplicate resolution."""

from pathlib import Path

from pncp_etl.quality.deduplication import RawRecordRef, SqliteDeduplicator
from pncp_etl.quality.records import validate_and_transform


def _canonical(
    *,
    key: str = "ATA-1",
    description: str = "object",
    freshness: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "numeroControlePNCPAta": key,
        "objetoContratacao": description,
    }
    if freshness is not None:
        payload["dataAtualizacaoGlobal"] = freshness
    outcome = validate_and_transform(payload)
    assert outcome.canonical is not None
    return outcome.canonical


def test_identical_duplicates_emit_one_candidate(tmp_path: Path) -> None:
    """Repeated content is counted but only one deterministic candidate survives."""
    with SqliteDeduplicator(tmp_path / "dedup.sqlite") as deduplicator:
        deduplicator.add(RawRecordRef(1, 1), _canonical())
        deduplicator.add(RawRecordRef(1, 0), _canonical())
        metrics = deduplicator.resolve()

        winners = list(deduplicator.iter_winners())
        conflicts = list(deduplicator.iter_conflicts())

    assert len(winners) == 1
    assert winners[0].raw_ref == RawRecordRef(1, 0)
    assert metrics.input_records == 2
    assert metrics.output_records == 1
    assert metrics.identical_duplicates == 1
    assert metrics.superseded_versions == 0
    assert conflicts == []


def test_newer_comparable_version_wins(tmp_path: Path) -> None:
    """The greatest global update within one temporal domain wins."""
    with SqliteDeduplicator(tmp_path / "dedup.sqlite") as deduplicator:
        deduplicator.add(
            RawRecordRef(1, 0),
            _canonical(description="old", freshness="2026-11-02T09:00:00Z"),
        )
        deduplicator.add(
            RawRecordRef(2, 0),
            _canonical(description="new", freshness="2026-11-02T10:00:00+00:00"),
        )
        metrics = deduplicator.resolve()
        winners = list(deduplicator.iter_winners())

    assert len(winners) == 1
    assert winners[0].values["objeto_contratacao"] == "new"
    assert metrics.superseded_versions == 1
    assert metrics.conflict_records == 0


def test_resolution_is_independent_of_input_order(tmp_path: Path) -> None:
    """Reversing ingestion order does not change the selected business content."""
    candidates = [
        (
            RawRecordRef(1, 0),
            _canonical(description="old", freshness="2026-11-02T09:00:00"),
        ),
        (
            RawRecordRef(2, 0),
            _canonical(description="new", freshness="2026-11-02T10:00:00"),
        ),
    ]

    selected: list[dict[str, object]] = []
    for index, ordered in enumerate((candidates, list(reversed(candidates)))):
        with SqliteDeduplicator(tmp_path / f"dedup-{index}.sqlite") as deduplicator:
            for raw_ref, values in ordered:
                deduplicator.add(raw_ref, values)
            deduplicator.resolve()
            selected.append(list(deduplicator.iter_winners())[0].values)

    assert selected[0] == selected[1]


def test_equal_timestamp_with_divergent_content_rejects_all(tmp_path: Path) -> None:
    """Arrival order cannot break a source timestamp tie."""
    with SqliteDeduplicator(tmp_path / "dedup.sqlite") as deduplicator:
        deduplicator.add(
            RawRecordRef(1, 0),
            _canonical(description="one", freshness="2026-11-02T09:00:00Z"),
        )
        deduplicator.add(
            RawRecordRef(1, 1),
            _canonical(description="two", freshness="2026-11-02T09:00:00+00:00"),
        )
        metrics = deduplicator.resolve()
        winners = list(deduplicator.iter_winners())
        conflicts = list(deduplicator.iter_conflicts())

    assert winners == []
    assert [conflict.raw_ref for conflict in conflicts] == [
        RawRecordRef(1, 0),
        RawRecordRef(1, 1),
    ]
    assert {conflict.reason_code for conflict in conflicts} == {"conflicting_duplicate"}
    assert metrics.conflict_identities == 1
    assert metrics.conflict_records == 2


def test_divergent_content_without_freshness_rejects_all(tmp_path: Path) -> None:
    """Collection time is never used as a substitute for source freshness."""
    with SqliteDeduplicator(tmp_path / "dedup.sqlite") as deduplicator:
        deduplicator.add(RawRecordRef(1, 0), _canonical(description="one"))
        deduplicator.add(RawRecordRef(2, 0), _canonical(description="two"))
        deduplicator.resolve()

        assert list(deduplicator.iter_winners()) == []
        assert len(list(deduplicator.iter_conflicts())) == 2


def test_mixed_freshness_domains_reject_all(tmp_path: Path) -> None:
    """Offset-aware and local-unzoned source versions are not comparable."""
    with SqliteDeduplicator(tmp_path / "dedup.sqlite") as deduplicator:
        deduplicator.add(
            RawRecordRef(1, 0),
            _canonical(description="one", freshness="2026-11-02T09:00:00Z"),
        )
        deduplicator.add(
            RawRecordRef(2, 0),
            _canonical(description="two", freshness="2026-11-02T10:00:00"),
        )
        deduplicator.resolve()

        assert list(deduplicator.iter_winners()) == []
        assert len(list(deduplicator.iter_conflicts())) == 2
