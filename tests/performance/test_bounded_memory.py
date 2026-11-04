"""Synthetic evidence that application memory is bounded by page/index batches."""

from __future__ import annotations

import gc
import tracemalloc
from datetime import UTC, date, datetime
from pathlib import Path

import httpx

from pncp_etl.contracts import QueryWindow, SafetyLimits
from pncp_etl.pipeline.extract import extract_to_artifact
from pncp_etl.pipeline.transform import transform_artifact
from pncp_etl.pipeline.validate import validate_artifact
from pncp_etl.pncp.client import PncpClient
from pncp_etl.storage.artifacts import ArtifactRepository

NOW = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
PAGE_SIZE = 50


def _run_synthetic(root: Path, *, pages: int) -> tuple[int, int]:
    total_records = pages * PAGE_SIZE

    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["pagina"])
        start = (page - 1) * PAGE_SIZE
        records = [
            {
                "numeroControlePNCPAta": f"SYNTHETIC-{start + offset:08d}",
                "objetoContratacao": "x" * 500,
                "dataAtualizacaoGlobal": "2026-11-02T12:00:00Z",
            }
            for offset in range(PAGE_SIZE)
        ]
        return httpx.Response(
            200,
            json={
                "data": records,
                "totalRegistros": total_records,
                "totalPaginas": pages,
                "numeroPagina": page,
                "paginasRestantes": pages - page,
                "empty": False,
            },
        )

    limits = SafetyLimits(
        page_size=PAGE_SIZE,
        max_pages=pages,
        max_records=total_records,
        max_duration_seconds=120,
        max_artifact_bytes=100_000_000,
    )
    repository = ArtifactRepository(root, now=lambda: NOW)
    client = PncpClient(
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: NOW,
        sleep=lambda _: None,
        jitter=lambda _: 0.0,
    )

    gc.collect()
    tracemalloc.start()
    raw_ref = extract_to_artifact(
        client=client,
        repository=repository,
        execution_id=f"synthetic-{pages}",
        window=QueryWindow(start=date(2026, 11, 1), end=date(2026, 11, 1)),
        limits=limits,
    )
    validation_ref = validate_artifact(
        repository=repository,
        raw_ref=raw_ref,
        limits=limits,
    )
    transform_ref = transform_artifact(
        repository=repository,
        validation_ref=validation_ref,
        limits=limits,
    )
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return transform_ref.record_count, peak


def test_tenfold_volume_keeps_sublinear_peak_memory(tmp_path: Path) -> None:
    """A 10x period grows files/SQLite, not the live Python record set."""
    small_count, small_peak = _run_synthetic(tmp_path / "small", pages=2)
    large_count, large_peak = _run_synthetic(tmp_path / "large", pages=20)

    assert small_count == 100
    assert large_count == 1_000
    assert large_peak < small_peak * 3
    assert large_peak < 20 * 1024 * 1024
