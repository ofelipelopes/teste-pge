"""Static contracts for the PostgreSQL persistence schema."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_persistence_migration_declares_transactional_tables() -> None:
    """The versioned DDL must contain every durable state used by DB-001."""
    sql = (ROOT / "sql/migrations/0002_pipeline.sql").read_text(encoding="utf-8")

    for table in (
        "ops.pipeline_run",
        "ops.curated_promotion_attempt",
        "ops.curated_promotion",
        "staging.ata_raw",
        "staging.ata_candidate",
        "quality.ata_rejection",
        "curated.ata",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
    assert "TIMESTAMP(6) WITHOUT TIME ZONE" in sql
    assert "TIMESTAMPTZ(6)" in sql
    assert "REPLAYED" in sql


def test_database_bootstrap_applies_every_ordered_migration() -> None:
    """Adding a migration must not require editing the bootstrap command again."""
    script = (ROOT / "docker/postgres/init-databases.sh").read_text(encoding="utf-8")

    assert 'for migration in /migrations/*.sql' in script
    assert '--file "${migration}"' in script
