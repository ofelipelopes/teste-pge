"""Raw extraction stage interface."""

from __future__ import annotations

from pncp_etl.contracts import ArtifactRef, QueryWindow, SafetyLimits
from pncp_etl.pncp.client import PncpClient
from pncp_etl.storage.artifacts import ArtifactRepository


def extract_to_artifact(
    *,
    client: PncpClient,
    repository: ArtifactRepository,
    execution_id: str,
    window: QueryWindow,
    limits: SafetyLimits,
    source: str = "vigencia",
) -> ArtifactRef:
    """Extract a source window and publish its raw artifact."""
    return repository.publish_raw(
        execution_id=execution_id,
        window=window,
        limits=limits,
        pages=client.iter_pages(window, limits),
        source=source,
    )
