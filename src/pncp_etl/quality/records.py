"""Record validation and canonical transformation interface."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pncp_etl.quality.datetime_values import (
    DateTimeParseError,
    SourceDateTime,
    parse_source_datetime,
)
from pncp_etl.quality.mapping import FIELD_SPECS, FieldKind, FieldSpec, validate_mapping


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    """One safe, stable reason for quarantining a record."""

    reason_code: str
    field: str | None = None
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class ValidationOutcome:
    """Either a complete canonical record or its validation issues."""

    canonical: dict[str, Any] | None
    issues: tuple[ValidationIssue, ...]


def validate_and_transform(payload: dict[str, Any]) -> ValidationOutcome:
    """Validate one raw payload and return an all-or-nothing canonical record."""
    validate_mapping(FIELD_SPECS)
    canonical: dict[str, Any] = {}
    temporal: dict[str, SourceDateTime] = {}
    issues: list[ValidationIssue] = []

    for spec in FIELD_SPECS:
        raw_value = payload.get(spec.source)
        if raw_value is None:
            if spec.required:
                issues.append(
                    ValidationIssue(
                        reason_code="missing_business_key",
                        field=spec.source,
                        detail="required business key is absent or null",
                    )
                )
            _set_null(canonical, spec)
            continue
        try:
            _set_value(canonical, temporal, spec, raw_value)
        except _FieldValidationError as error:
            issues.append(
                ValidationIssue(
                    reason_code=error.reason_code,
                    field=spec.source,
                    detail=error.detail,
                )
            )
            _set_null(canonical, spec)

    _validate_validity_range(temporal, issues)
    if issues:
        return ValidationOutcome(canonical=None, issues=_unique_issues(issues))
    return ValidationOutcome(canonical=canonical, issues=())


class _FieldValidationError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(detail)
        self.reason_code = reason_code
        self.detail = detail


def _set_null(canonical: dict[str, Any], spec: FieldSpec) -> None:
    if spec.kind is FieldKind.DATETIME:
        canonical[f"{spec.target}_local"] = None
        canonical[f"{spec.target}_utc"] = None
    else:
        canonical[spec.target] = None


def _set_value(
    canonical: dict[str, Any],
    temporal: dict[str, SourceDateTime],
    spec: FieldSpec,
    raw_value: Any,
) -> None:
    if spec.kind is FieldKind.STRING:
        if not isinstance(raw_value, str):
            raise _FieldValidationError("invalid_type", "expected a JSON string")
        normalized = raw_value.strip()
        if spec.required and not normalized:
            raise _FieldValidationError(
                "missing_business_key", "required business key is empty after trim"
            )
        canonical[spec.target] = normalized
        return
    if spec.kind is FieldKind.INTEGER:
        if isinstance(raw_value, bool) or not isinstance(raw_value, int):
            raise _FieldValidationError("invalid_type", "expected a JSON integer")
        if spec.source == "anoAta" and not 1800 <= raw_value <= 9999:
            raise _FieldValidationError("invalid_type", "year is outside the accepted range")
        canonical[spec.target] = raw_value
        return
    if spec.kind is FieldKind.BOOLEAN:
        if not isinstance(raw_value, bool):
            raise _FieldValidationError("invalid_type", "expected a JSON boolean")
        canonical[spec.target] = raw_value
        return
    if spec.kind is FieldKind.CNPJ:
        if not isinstance(raw_value, str):
            raise _FieldValidationError("invalid_type", "expected CNPJ as a JSON string")
        normalized_cnpj = _normalize_cnpj(raw_value)
        if not _is_valid_cnpj(normalized_cnpj):
            raise _FieldValidationError("invalid_cnpj", "CNPJ must contain valid check digits")
        canonical[spec.target] = normalized_cnpj
        return
    if spec.kind is FieldKind.DATETIME:
        if not isinstance(raw_value, str):
            raise _FieldValidationError("invalid_type", "expected date-time as a JSON string")
        try:
            value = parse_source_datetime(raw_value)
        except DateTimeParseError as error:
            raise _FieldValidationError(error.reason_code, "invalid source date-time") from error
        temporal[spec.source] = value
        canonical[f"{spec.target}_local"] = value.local.isoformat()
        canonical[f"{spec.target}_utc"] = _format_utc(value.utc)
        return
    raise AssertionError(f"unsupported field kind: {spec.kind}")


def _validate_validity_range(
    temporal: dict[str, SourceDateTime],
    issues: list[ValidationIssue],
) -> None:
    start = temporal.get("vigenciaInicio")
    end = temporal.get("vigenciaFim")
    if start is None or end is None:
        return
    if start.domain is not end.domain:
        issues.append(
            ValidationIssue(
                reason_code="incomparable_temporal_domain",
                field="vigenciaInicio,vigenciaFim",
                detail="validity endpoints use different temporal domains",
            )
        )
        return
    start_value = start.utc if start.utc is not None else start.local
    end_value = end.utc if end.utc is not None else end.local
    if start_value > end_value:
        issues.append(
            ValidationIssue(
                reason_code="invalid_validity_range",
                field="vigenciaInicio,vigenciaFim",
                detail="validity start is after validity end",
            )
        )


def _normalize_cnpj(value: str) -> str:
    return value.translate(str.maketrans("", "", ". /-"))


def _is_valid_cnpj(value: str) -> bool:
    if len(value) != 14 or not value.isdigit() or len(set(value)) == 1:
        return False
    first_weights = (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
    second_weights = (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)

    def check_digit(digits: str, weights: tuple[int, ...]) -> str:
        remainder = sum(
            int(digit) * weight for digit, weight in zip(digits, weights, strict=True)
        ) % 11
        return "0" if remainder < 2 else str(11 - remainder)

    first = check_digit(value[:12], first_weights)
    second = check_digit(value[:12] + first, second_weights)
    return value[-2:] == first + second


def _format_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat().replace("+00:00", "Z")


def _unique_issues(issues: list[ValidationIssue]) -> tuple[ValidationIssue, ...]:
    return tuple(dict.fromkeys(issues))
