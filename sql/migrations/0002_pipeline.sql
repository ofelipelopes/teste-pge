\set ON_ERROR_STOP on

BEGIN;

SET ROLE :"app_user";

CREATE TABLE IF NOT EXISTS ops.pipeline_run (
    execution_id UUID PRIMARY KEY,
    airflow_run_id TEXT NOT NULL UNIQUE,
    dag_id TEXT NOT NULL,
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    filters JSONB NOT NULL DEFAULT '{}'::jsonb,
    state TEXT NOT NULL CHECK (
        state IN ('LOADING_STAGING', 'STAGING_COMPLETE', 'CURATED_COMMITTED')
    ),
    mapping_version TEXT NOT NULL,
    freshness_policy_version TEXT NOT NULL,
    artifacts JSONB NOT NULL DEFAULT '{}'::jsonb,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    staging_completed_at TIMESTAMPTZ(6),
    curated_committed_at TIMESTAMPTZ(6),
    CHECK (window_start <= window_end)
);

CREATE TABLE IF NOT EXISTS ops.curated_promotion (
    promotion_id UUID PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES ops.pipeline_run(execution_id),
    input_fingerprint TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    freshness_policy_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status = 'COMMITTED'),
    load_result JSONB NOT NULL,
    committed_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (execution_id, input_fingerprint, mapping_version, freshness_policy_version)
);

CREATE TABLE IF NOT EXISTS ops.curated_promotion_attempt (
    attempt_id UUID PRIMARY KEY,
    promotion_id UUID NOT NULL,
    execution_id UUID NOT NULL REFERENCES ops.pipeline_run(execution_id),
    task_try_number INTEGER NOT NULL DEFAULT 1 CHECK (task_try_number > 0),
    state TEXT NOT NULL CHECK (state IN ('STARTED', 'ABORTED', 'COMMITTED', 'REPLAYED')),
    error_class TEXT,
    existing_promotion_id UUID REFERENCES ops.curated_promotion(promotion_id),
    started_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ(6)
);

CREATE TABLE IF NOT EXISTS staging.ata_raw (
    raw_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES ops.pipeline_run(execution_id),
    page_number INTEGER NOT NULL CHECK (page_number > 0),
    record_index INTEGER NOT NULL CHECK (record_index >= 0),
    payload JSONB NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (length(payload_sha256) = 64),
    collected_at TIMESTAMPTZ(6) NOT NULL,
    business_key TEXT,
    validation_status TEXT NOT NULL DEFAULT 'UNKNOWN' CHECK (
        validation_status IN ('UNKNOWN', 'VALID', 'REJECTED')
    ),
    UNIQUE (execution_id, page_number, record_index)
);

CREATE TABLE IF NOT EXISTS staging.ata_candidate (
    execution_id UUID NOT NULL REFERENCES ops.pipeline_run(execution_id),
    page_number INTEGER NOT NULL,
    record_index INTEGER NOT NULL,
    business_key TEXT NOT NULL,
    canonical_payload JSONB NOT NULL,
    PRIMARY KEY (execution_id, page_number, record_index),
    UNIQUE (execution_id, business_key),
    FOREIGN KEY (execution_id, page_number, record_index)
        REFERENCES staging.ata_raw(execution_id, page_number, record_index)
);

CREATE TABLE IF NOT EXISTS quality.ata_rejection (
    rejection_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES ops.pipeline_run(execution_id),
    raw_id BIGINT NOT NULL REFERENCES staging.ata_raw(raw_id),
    stage TEXT NOT NULL CHECK (stage IN ('validate', 'transform', 'load_curated')),
    business_key TEXT,
    reason_code TEXT NOT NULL,
    detail TEXT,
    created_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (execution_id, raw_id, stage, reason_code)
);

CREATE TABLE IF NOT EXISTS curated.ata (
    numero_controle_pncp_ata TEXT PRIMARY KEY,
    numero_ata_registro_preco TEXT,
    ano_ata INTEGER,
    numero_controle_pncp_compra TEXT,
    cancelado BOOLEAN,
    data_cancelamento_local TIMESTAMP(6) WITHOUT TIME ZONE,
    data_cancelamento_utc TIMESTAMPTZ(6),
    data_assinatura_local TIMESTAMP(6) WITHOUT TIME ZONE,
    data_assinatura_utc TIMESTAMPTZ(6),
    vigencia_inicio_local TIMESTAMP(6) WITHOUT TIME ZONE,
    vigencia_inicio_utc TIMESTAMPTZ(6),
    vigencia_fim_local TIMESTAMP(6) WITHOUT TIME ZONE,
    vigencia_fim_utc TIMESTAMPTZ(6),
    data_publicacao_pncp_local TIMESTAMP(6) WITHOUT TIME ZONE,
    data_publicacao_pncp_utc TIMESTAMPTZ(6),
    data_inclusao_local TIMESTAMP(6) WITHOUT TIME ZONE,
    data_inclusao_utc TIMESTAMPTZ(6),
    data_atualizacao_local TIMESTAMP(6) WITHOUT TIME ZONE,
    data_atualizacao_utc TIMESTAMPTZ(6),
    data_atualizacao_global_local TIMESTAMP(6) WITHOUT TIME ZONE,
    data_atualizacao_global_utc TIMESTAMPTZ(6),
    usuario TEXT,
    objeto_contratacao TEXT,
    cnpj_orgao VARCHAR(14),
    nome_orgao TEXT,
    codigo_unidade_orgao TEXT,
    nome_unidade_orgao TEXT,
    cnpj_orgao_subrogado VARCHAR(14),
    nome_orgao_subrogado TEXT,
    codigo_unidade_orgao_subrogado TEXT,
    nome_unidade_orgao_subrogado TEXT,
    possibilidade_adesao BOOLEAN,
    canonical_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    first_seen_execution_id UUID NOT NULL REFERENCES ops.pipeline_run(execution_id),
    last_seen_execution_id UUID NOT NULL REFERENCES ops.pipeline_run(execution_id),
    last_changed_execution_id UUID NOT NULL REFERENCES ops.pipeline_run(execution_id),
    last_promotion_id UUID NOT NULL,
    CHECK (data_cancelamento_utc IS NULL OR data_cancelamento_local IS NOT NULL),
    CHECK (data_assinatura_utc IS NULL OR data_assinatura_local IS NOT NULL),
    CHECK (vigencia_inicio_utc IS NULL OR vigencia_inicio_local IS NOT NULL),
    CHECK (vigencia_fim_utc IS NULL OR vigencia_fim_local IS NOT NULL),
    CHECK (data_publicacao_pncp_utc IS NULL OR data_publicacao_pncp_local IS NOT NULL),
    CHECK (data_inclusao_utc IS NULL OR data_inclusao_local IS NOT NULL),
    CHECK (data_atualizacao_utc IS NULL OR data_atualizacao_local IS NOT NULL),
    CHECK (data_atualizacao_global_utc IS NULL OR data_atualizacao_global_local IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS ata_raw_execution_idx
    ON staging.ata_raw(execution_id);
CREATE INDEX IF NOT EXISTS ata_rejection_execution_reason_idx
    ON quality.ata_rejection(execution_id, reason_code);
CREATE INDEX IF NOT EXISTS promotion_attempt_promotion_started_idx
    ON ops.curated_promotion_attempt(promotion_id, started_at);

INSERT INTO ops.schema_migration (version)
VALUES ('0002_pipeline')
ON CONFLICT (version) DO NOTHING;

RESET ROLE;

COMMIT;
