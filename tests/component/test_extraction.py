"""Component tests for incremental raw artifact publication."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from pncp_etl.contracts import QueryWindow, SafetyLimits
from pncp_etl.pipeline.extract import extract_to_artifact
from pncp_etl.pncp.client import PncpClient
from pncp_etl.pncp.errors import ContractError, TransientSourceError
from pncp_etl.storage.artifacts import ArtifactRepository

NOW = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)


def _envelope(page: int, data: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "data": data,
        "totalRegistros": 3,
        "totalPaginas": 2,
        "numeroPagina": page,
        "paginasRestantes": 2 - page,
        "empty": False,
    }


def _window() -> QueryWindow:
    return QueryWindow(start=date(2026, 11, 1), end=date(2026, 11, 1))


def _limits() -> SafetyLimits:
    return SafetyLimits(
        page_size=10,
        max_pages=10,
        max_records=100,
        max_duration_seconds=60,
        max_artifact_bytes=1_000_000,
    )


def test_extract_publishes_jsonl_then_atomic_manifest(tmp_path: Path) -> None:
    """A complete extraction is checksummed, attributable, and consumible."""

    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["pagina"])
        records = [{"id": 1}, {"id": 2}] if page == 1 else [{"id": 3}]
        return httpx.Response(200, json=_envelope(page, records))

    client = PncpClient(
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: NOW,
        sleep=lambda _: None,
        jitter=lambda _: 0.0,
    )
    repository = ArtifactRepository(tmp_path, now=lambda: NOW)

    ref = extract_to_artifact(
        client=client,
        repository=repository,
        execution_id="execution-1",
        window=_window(),
        limits=_limits(),
    )

    manifest_path = tmp_path / ref.manifest_path
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw_path = tmp_path / manifest["files"][0]["path"]
    raw_bytes = raw_path.read_bytes()
    lines = [json.loads(line) for line in raw_bytes.splitlines()]

    assert ref.record_count == 3
    assert manifest["complete"] is True
    assert manifest["record_count"] == 3
    assert manifest["page_count"] == 2
    assert manifest["files"][0]["sha256"] == hashlib.sha256(raw_bytes).hexdigest()
    assert [line["payload"]["id"] for line in lines] == [1, 2, 3]
    assert [(line["page_number"], line["record_index"]) for line in lines] == [
        (1, 0),
        (1, 1),
        (2, 0),
    ]
    assert all(line["execution_id"] == "execution-1" for line in lines)
    assert not list(tmp_path.rglob("*.part"))


def test_extract_failure_leaves_no_consumable_manifest(tmp_path: Path) -> None:
    """A failure after one page leaves only an explicit partial artifact."""

    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["pagina"])
        if page == 1:
            return httpx.Response(200, json=_envelope(1, [{"id": 1}, {"id": 2}]))
        return httpx.Response(503)

    client = PncpClient(
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: NOW,
        sleep=lambda _: None,
        jitter=lambda _: 0.0,
    )
    repository = ArtifactRepository(tmp_path, now=lambda: NOW)

    with pytest.raises(TransientSourceError):
        extract_to_artifact(
            client=client,
            repository=repository,
            execution_id="execution-2",
            window=_window(),
            limits=_limits(),
        )

    assert list(tmp_path.rglob("raw.jsonl.part"))
    assert not list(tmp_path.rglob("manifest.json"))


def test_artifact_byte_ceiling_prevents_manifest_publication(tmp_path: Path) -> None:
    """Crossing the byte ceiling leaves an incomplete raw file and fails the stage."""

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": [{"id": 1, "description": "x" * 200}],
                "totalRegistros": 1,
                "totalPaginas": 1,
                "numeroPagina": 1,
                "paginasRestantes": 0,
                "empty": False,
            },
        )

    client = PncpClient(
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: NOW,
        sleep=lambda _: None,
        jitter=lambda _: 0.0,
    )
    repository = ArtifactRepository(tmp_path, now=lambda: NOW)
    limits = SafetyLimits(
        page_size=10,
        max_pages=10,
        max_records=100,
        max_duration_seconds=60,
        max_artifact_bytes=64,
    )

    with pytest.raises(ContractError, match="max_artifact_bytes"):
        extract_to_artifact(
            client=client,
            repository=repository,
            execution_id="execution-too-large",
            window=_window(),
            limits=limits,
        )

    assert list(tmp_path.rglob("raw.jsonl.part"))
    assert not list(tmp_path.rglob("manifest.json"))


def test_retry_same_execution_replaces_partial_artifact(tmp_path: Path) -> None:
    """A task retry rebuilds only its own partial output and then commits normally."""
    should_fail = True

    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["pagina"])
        if page == 2 and should_fail:
            return httpx.Response(503)
        records = [{"id": 1}, {"id": 2}] if page == 1 else [{"id": 3}]
        return httpx.Response(200, json=_envelope(page, records))

    client = PncpClient(
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: NOW,
        sleep=lambda _: None,
        jitter=lambda _: 0.0,
    )
    repository = ArtifactRepository(tmp_path, now=lambda: NOW)

    with pytest.raises(TransientSourceError):
        extract_to_artifact(
            client=client,
            repository=repository,
            execution_id="execution-retry",
            window=_window(),
            limits=_limits(),
        )

    should_fail = False
    ref = extract_to_artifact(
        client=client,
        repository=repository,
        execution_id="execution-retry",
        window=_window(),
        limits=_limits(),
    )

    assert ref.record_count == 3
    assert not list(tmp_path.rglob("*.part"))
    assert len(list(tmp_path.rglob("manifest.json"))) == 1


def test_retry_after_complete_manifest_reuses_verified_artifact(tmp_path: Path) -> None:
    """A retry after publication returns the durable result without querying PNCP again."""
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        page = int(request.url.params["pagina"])
        records = [{"id": 1}, {"id": 2}] if page == 1 else [{"id": 3}]
        return httpx.Response(200, json=_envelope(page, records))

    client = PncpClient(
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: NOW,
        sleep=lambda _: None,
        jitter=lambda _: 0.0,
    )
    repository = ArtifactRepository(tmp_path, now=lambda: NOW)
    first = extract_to_artifact(
        client=client,
        repository=repository,
        execution_id="execution-complete-retry",
        window=_window(),
        limits=_limits(),
        source="atualizacao",
    )
    first_call_count = calls
    second = extract_to_artifact(
        client=client,
        repository=repository,
        execution_id="execution-complete-retry",
        window=_window(),
        limits=_limits(),
        source="atualizacao",
    )

    assert second == first
    assert calls == first_call_count
    manifest = json.loads((tmp_path / first.manifest_path).read_text(encoding="utf-8"))
    assert manifest["source"] == "atualizacao"
    with pytest.raises(ContractError, match="query source"):
        extract_to_artifact(
            client=client,
            repository=repository,
            execution_id="execution-complete-retry",
            window=_window(),
            limits=_limits(),
            source="vigencia",
        )
    assert calls == first_call_count
