"""Disk-backed duplicate resolution interface."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, Self

from pncp_etl.pncp.errors import ContractError

_BUSINESS_KEY = "numero_controle_pncp_ata"
_FRESHNESS_LOCAL = "data_atualizacao_global_local"
_FRESHNESS_UTC = "data_atualizacao_global_utc"


@dataclass(frozen=True, order=True, slots=True)
class RawRecordRef:
    """Stable position of a source record inside an extraction."""

    page_number: int
    record_index: int


@dataclass(frozen=True, slots=True)
class DeduplicatedRecord:
    """One selected canonical record and its winning provenance."""

    raw_ref: RawRecordRef
    values: dict[str, Any]


@dataclass(frozen=True, slots=True)
class DuplicateConflict:
    """One raw record rejected because its identity is ambiguous."""

    raw_ref: RawRecordRef
    reason_code: str
    identity: str


@dataclass(frozen=True, slots=True)
class DeduplicationMetrics:
    """Reconciled counts from one deterministic resolution."""

    input_records: int
    output_records: int
    identical_duplicates: int
    superseded_versions: int
    conflict_identities: int
    conflict_records: int


class SqliteDeduplicator:
    """Resolve identities using a reconstructible local SQLite index."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(path)
        self._resolved = False
        self._connection.executescript(
            """
            PRAGMA journal_mode = DELETE;
            PRAGMA synchronous = FULL;
            PRAGMA temp_store = FILE;
            DROP TABLE IF EXISTS winners;
            DROP TABLE IF EXISTS conflicts;
            DROP TABLE IF EXISTS candidates;
            CREATE TABLE candidates (
                page_number INTEGER NOT NULL,
                record_index INTEGER NOT NULL,
                identity TEXT NOT NULL,
                canonical_json TEXT NOT NULL,
                freshness_domain TEXT,
                freshness_value TEXT,
                PRIMARY KEY (page_number, record_index)
            );
            CREATE INDEX candidates_identity_idx ON candidates(identity);
            CREATE TABLE winners (
                identity TEXT PRIMARY KEY,
                canonical_json TEXT NOT NULL,
                page_number INTEGER NOT NULL,
                record_index INTEGER NOT NULL
            );
            CREATE TABLE conflicts (
                identity TEXT PRIMARY KEY,
                reason_code TEXT NOT NULL
            );
            """
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self._connection.close()

    def add(self, raw_ref: RawRecordRef, values: dict[str, Any]) -> None:
        """Add one fully validated canonical record."""
        if self._resolved:
            raise ContractError("cannot add candidates after duplicate resolution")
        if raw_ref.page_number < 1 or raw_ref.record_index < 0:
            raise ContractError("raw record reference is invalid")
        identity = values.get(_BUSINESS_KEY)
        if not isinstance(identity, str) or not identity:
            raise ContractError("canonical candidate has no business identity")
        canonical_json = json.dumps(
            values,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        )
        freshness_domain, freshness_value = _freshness(values)
        try:
            self._connection.execute(
                """
                INSERT INTO candidates (
                    page_number,
                    record_index,
                    identity,
                    canonical_json,
                    freshness_domain,
                    freshness_value
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    raw_ref.page_number,
                    raw_ref.record_index,
                    identity,
                    canonical_json,
                    freshness_domain,
                    freshness_value,
                ),
            )
        except sqlite3.IntegrityError as error:
            raise ContractError("duplicate raw record reference in transform input") from error

    def resolve(self) -> DeduplicationMetrics:
        """Resolve all identities independently of insertion order."""
        if self._resolved:
            raise ContractError("duplicate resolution was already completed")
        self._connection.commit()
        input_records = 0
        output_records = 0
        identical_duplicates = 0
        superseded_versions = 0
        conflict_identities = 0
        conflict_records = 0

        identities = self._connection.execute(
            """
            SELECT identity, COUNT(*) AS record_count,
                   COUNT(DISTINCT canonical_json) AS variant_count
            FROM candidates
            GROUP BY identity
            ORDER BY identity
            """
        )
        while batch := identities.fetchmany(1_000):
            for identity, record_count, variant_count in batch:
                input_records += record_count
                if variant_count == 1:
                    identical_duplicates += record_count - 1
                    self._insert_winner(identity, freshness_value=None)
                    output_records += 1
                    continue

                missing_freshness, domain_count, maximum = self._freshness_summary(identity)
                if missing_freshness or domain_count != 1 or maximum is None:
                    self._insert_conflict(identity)
                    conflict_identities += 1
                    conflict_records += record_count
                    continue
                maximum_variants = self._connection.execute(
                    """
                    SELECT COUNT(DISTINCT canonical_json)
                    FROM candidates
                    WHERE identity = ? AND freshness_value = ?
                    """,
                    (identity, maximum),
                ).fetchone()[0]
                if maximum_variants != 1:
                    self._insert_conflict(identity)
                    conflict_identities += 1
                    conflict_records += record_count
                    continue
                self._insert_winner(identity, freshness_value=maximum)
                output_records += 1
                identical_duplicates += record_count - variant_count
                superseded_versions += variant_count - 1

        self._connection.commit()
        self._resolved = True
        if input_records != (
            output_records
            + identical_duplicates
            + superseded_versions
            + conflict_records
        ):
            raise ContractError("duplicate resolution counts do not reconcile")
        return DeduplicationMetrics(
            input_records=input_records,
            output_records=output_records,
            identical_duplicates=identical_duplicates,
            superseded_versions=superseded_versions,
            conflict_identities=conflict_identities,
            conflict_records=conflict_records,
        )

    def iter_winners(self) -> Iterator[DeduplicatedRecord]:
        """Yield selected records sorted by business identity."""
        self._require_resolved()
        rows = self._connection.execute(
            """
            SELECT page_number, record_index, canonical_json
            FROM winners
            ORDER BY identity
            """
        )
        for page_number, record_index, canonical_json in rows:
            values: Any = json.loads(canonical_json)
            if not isinstance(values, dict):
                raise ContractError("SQLite winner is not a canonical JSON object")
            yield DeduplicatedRecord(
                raw_ref=RawRecordRef(page_number, record_index),
                values=values,
            )

    def iter_conflicts(self) -> Iterator[DuplicateConflict]:
        """Yield every raw record belonging to an unresolved identity."""
        self._require_resolved()
        rows = self._connection.execute(
            """
            SELECT c.page_number, c.record_index, x.reason_code, c.identity
            FROM candidates AS c
            JOIN conflicts AS x USING (identity)
            ORDER BY c.identity, c.page_number, c.record_index
            """
        )
        for page_number, record_index, reason_code, identity in rows:
            yield DuplicateConflict(
                raw_ref=RawRecordRef(page_number, record_index),
                reason_code=reason_code,
                identity=identity,
            )

    def _freshness_summary(self, identity: str) -> tuple[int, int, str | None]:
        row = self._connection.execute(
            """
            SELECT
                SUM(CASE WHEN freshness_value IS NULL THEN 1 ELSE 0 END),
                COUNT(DISTINCT freshness_domain),
                MAX(freshness_value)
            FROM candidates
            WHERE identity = ?
            """,
            (identity,),
        ).fetchone()
        return row[0], row[1], row[2]

    def _insert_winner(self, identity: str, *, freshness_value: str | None) -> None:
        if freshness_value is None:
            freshness_clause = ""
            parameters: tuple[str, ...] = (identity,)
        else:
            freshness_clause = "AND freshness_value = ?"
            parameters = (identity, freshness_value)
        row = self._connection.execute(
            f"""
            SELECT canonical_json, page_number, record_index
            FROM candidates
            WHERE identity = ? {freshness_clause}
            ORDER BY canonical_json, page_number, record_index
            LIMIT 1
            """,
            parameters,
        ).fetchone()
        if row is None:
            raise ContractError("duplicate resolver could not locate its winner")
        self._connection.execute(
            """
            INSERT INTO winners (identity, canonical_json, page_number, record_index)
            VALUES (?, ?, ?, ?)
            """,
            (identity, row[0], row[1], row[2]),
        )

    def _insert_conflict(self, identity: str) -> None:
        self._connection.execute(
            "INSERT INTO conflicts (identity, reason_code) VALUES (?, ?)",
            (identity, "conflicting_duplicate"),
        )

    def _require_resolved(self) -> None:
        if not self._resolved:
            raise ContractError("duplicate resolution must finish before reading results")


def _freshness(values: dict[str, Any]) -> tuple[str | None, str | None]:
    local = values.get(_FRESHNESS_LOCAL)
    utc = values.get(_FRESHNESS_UTC)
    if local is None:
        return None, None
    if not isinstance(local, str):
        raise ContractError("canonical freshness local value must be a string or null")
    if utc is None:
        return "LOCAL_UNZONED", local
    if not isinstance(utc, str):
        raise ContractError("canonical freshness UTC value must be a string or null")
    return "OFFSET_AWARE", utc
