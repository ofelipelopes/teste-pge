"""Smoke tests for pinned runtime dependencies."""

from __future__ import annotations

import importlib.metadata


def test_airflow_and_direct_dependencies_import() -> None:
    """Airflow and the direct runtime dependencies must load in the target runtime."""
    import airflow
    import httpx
    import psycopg

    assert airflow.__version__ == "3.3.2"
    assert importlib.metadata.version("apache-airflow-providers-postgres") == "7.0.2"
    assert httpx.__version__ == "0.28.1"
    assert psycopg.__version__ == "3.3.5"
