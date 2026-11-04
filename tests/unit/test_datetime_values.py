"""Tests for lossless source date-time parsing."""

from datetime import UTC, datetime

import pytest

from pncp_etl.quality.datetime_values import (
    DateTimeParseError,
    SourceDateTimeDomain,
    parse_source_datetime,
)


def test_offset_datetime_preserves_wall_time_and_normalizes_utc() -> None:
    """An explicit offset creates both local components and an ordered instant."""
    value = parse_source_datetime("2026-11-02T09:30:15.123456-03:00")

    assert value.lexeme == "2026-11-02T09:30:15.123456-03:00"
    assert value.local == datetime(2026, 11, 2, 9, 30, 15, 123456)
    assert value.utc == datetime(2026, 11, 2, 12, 30, 15, 123456, tzinfo=UTC)
    assert value.domain is SourceDateTimeDomain.OFFSET_AWARE


def test_unzoned_datetime_never_invents_utc() -> None:
    """A local source value remains explicitly unzoned."""
    value = parse_source_datetime("2026-11-02T09:30:15")

    assert value.local == datetime(2026, 11, 2, 9, 30, 15)
    assert value.utc is None
    assert value.domain is SourceDateTimeDomain.LOCAL_UNZONED


def test_full_date_is_preserved_as_unzoned_midnight() -> None:
    """The date-only shape observed in PNCP remains local and has no invented UTC."""
    value = parse_source_datetime("2026-11-02")

    assert value.lexeme == "2026-11-02"
    assert value.local == datetime(2026, 11, 2)
    assert value.utc is None
    assert value.domain is SourceDateTimeDomain.LOCAL_UNZONED


def test_fraction_above_microseconds_is_rejected_without_truncation() -> None:
    """Precision unsupported by Python/PostgreSQL is quarantined, not rounded."""
    with pytest.raises(DateTimeParseError) as raised:
        parse_source_datetime("2026-11-02T09:30:15.1234567Z")

    assert raised.value.reason_code == "unsupported_datetime_precision"


@pytest.mark.parametrize(
    "lexeme",
    [
        "2026-11-02 09:30:15",
        "2026-02-30T09:30:15",
        "2026-11-02T25:30:15",
        "2026-11-02T09:30:15+24:00",
    ],
)
def test_invalid_datetime_has_stable_reason(lexeme: str) -> None:
    """Malformed date-times share one safe rejection code."""
    with pytest.raises(DateTimeParseError) as raised:
        parse_source_datetime(lexeme)

    assert raised.value.reason_code == "invalid_datetime"
