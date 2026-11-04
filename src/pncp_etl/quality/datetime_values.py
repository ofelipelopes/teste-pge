"""Source date-time value interfaces."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

_SOURCE_DATETIME_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}"
    r"(?:\.(?P<fraction>\d+))?(?:Z|[+-]\d{2}:\d{2})?)?$"
)


class SourceDateTimeDomain(StrEnum):
    """Whether a source date-time proves an instant."""

    OFFSET_AWARE = "OFFSET_AWARE"
    LOCAL_UNZONED = "LOCAL_UNZONED"


@dataclass(frozen=True, slots=True)
class SourceDateTime:
    """Lossless source temporal value plus normalized projections."""

    lexeme: str
    local: datetime
    utc: datetime | None
    domain: SourceDateTimeDomain


class DateTimeParseError(ValueError):
    """A safe temporal validation failure."""

    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


def parse_source_datetime(lexeme: str) -> SourceDateTime:
    """Parse one PNCP date or date-time without inventing a timezone."""
    match = _SOURCE_DATETIME_PATTERN.fullmatch(lexeme)
    if match is None:
        raise DateTimeParseError("invalid_datetime")
    fraction = match.group("fraction")
    if fraction is not None and len(fraction) > 6:
        raise DateTimeParseError("unsupported_datetime_precision")
    try:
        parsed = datetime.fromisoformat(lexeme.replace("Z", "+00:00"))
    except ValueError as error:
        raise DateTimeParseError("invalid_datetime") from error

    local = parsed.replace(tzinfo=None)
    if parsed.tzinfo is None:
        return SourceDateTime(
            lexeme=lexeme,
            local=local,
            utc=None,
            domain=SourceDateTimeDomain.LOCAL_UNZONED,
        )
    return SourceDateTime(
        lexeme=lexeme,
        local=local,
        utc=parsed.astimezone(UTC),
        domain=SourceDateTimeDomain.OFFSET_AWARE,
    )
