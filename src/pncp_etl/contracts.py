"""Typed contracts shared by the extraction pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any


@dataclass(frozen=True, slots=True)
class QueryWindow:
    """Source query window for price registration minutes."""

    start: date
    end: date
    cnpj: str | None = None
    administrative_unit: str | None = None
    user_id: int | None = None

    def __post_init__(self) -> None:
        """Reject invalid source parameters before any HTTP request."""
        if self.start > self.end:
            raise ValueError("start must be before or equal to end")
        if self.cnpj is not None and (len(self.cnpj) != 14 or not self.cnpj.isdigit()):
            raise ValueError("cnpj must contain exactly 14 digits")
        if self.administrative_unit is not None and not (
            1 <= len(self.administrative_unit) <= 30
        ):
            raise ValueError("administrative_unit must contain 1 to 30 characters")
        if self.user_id is not None and self.user_id < 0:
            raise ValueError("user_id must be non-negative")

    def query_params(self, *, page: int, page_size: int) -> dict[str, str | int]:
        """Build query parameters using the names documented by PNCP."""
        params: dict[str, str | int] = {
            "dataInicial": self.start.strftime("%Y%m%d"),
            "dataFinal": self.end.strftime("%Y%m%d"),
            "pagina": page,
            "tamanhoPagina": page_size,
        }
        if self.cnpj is not None:
            params["cnpj"] = self.cnpj
        if self.administrative_unit is not None:
            params["codigoUnidadeAdministrativa"] = self.administrative_unit
        if self.user_id is not None:
            params["idUsuario"] = self.user_id
        return params


@dataclass(frozen=True, slots=True)
class SafetyLimits:
    """Hard ceilings that bound one extraction execution."""

    page_size: int
    max_pages: int
    max_records: int
    max_duration_seconds: float
    max_artifact_bytes: int

    def __post_init__(self) -> None:
        """Validate finite positive guardrails."""
        if not 10 <= self.page_size <= 500:
            raise ValueError("page_size must be between 10 and 500")
        for field_name in ("max_pages", "max_records", "max_artifact_bytes"):
            if getattr(self, field_name) <= 0:
                raise ValueError(f"{field_name} must be positive")
        if self.max_duration_seconds <= 0:
            raise ValueError("max_duration_seconds must be positive")


@dataclass(frozen=True, slots=True)
class SourcePage:
    """One validated source page."""

    number: int
    total_pages: int
    total_records: int
    records: tuple[dict[str, Any], ...]


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    """Small reference passed between pipeline stages."""

    execution_id: str
    stage: str
    manifest_path: str
    record_count: int
    checksum: str
