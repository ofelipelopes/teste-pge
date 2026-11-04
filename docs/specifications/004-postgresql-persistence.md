# DB-001 — Persistência PostgreSQL e promoção idempotente

| Campo | Valor |
|---|---|
| Status | **IMPLEMENTED AND VERIFIED — POSTGRESQL REAL/LINUX ARM64** |
| Requisitos | FR-008/009/013/014; NFR-006/007/010; AC-09–14/16/17 |
| Dependência | VAL-001 verificada |

## Objetivo

Persistir raw, rejeições e candidatos em batches recuperáveis e promover a curated em uma transação serializada, com prova durável de commit, replay sem segundo efeito e proteção contra versões obsoletas/ambíguas.

## Contratos do incremento

- DDL versionado para `ops`, `staging`, `quality` e `curated`, aplicado idempotentemente;
- staging usa batches configuráveis, `ON CONFLICT` por posição da coleta e estado `LOADING_STAGING` até reconciliar tudo;
- falha entre batches deixa evidência parcial recuperável; retry completa sem duplicar;
- promoção deriva `promotion_id` do execution/fingerprint/versões e registra tentativa separada;
- advisory lock transacional serializa promoções; `ops.curated_promotion=COMMITTED` nasce na mesma transação do efeito curated;
- retry pós-commit retorna o `LoadResult` persistido e termina a nova tentativa como `REPLAYED`;
- conteúdo igual é `unchanged`; versão maior comparável atualiza; menor é `stale`; empate divergente, ausência ou domínio misto é `conflict`;
- `created_at` não muda; `updated_at` muda somente com conteúdo aceito diferente; ordem de coleta nunca decide frescor;
- SQL é parametrizado e timestamps técnicos usam UTC.

## Aceite

Testes de integração com PostgreSQL real cobrem múltiplos batches, falha/retry de staging, falha pré-commit, falha pós-commit/pré-ack, replay, janela sobreposta, `inserted/updated/unchanged/stale/conflict` e contagens. A suíte offline não consulta PNCP.

## Evidência do incremento

- RED estático: 2/2 falhas antes da migração/runner ordenado;
- RED de integração: 4/4 falhas comportamentais com `PostgresStore` ainda vazio;
- primeira execução GREEN parcial: 3/4; defeito isolado na comparação `UUID`/`str` do replay e corrigido sem alterar o teste;
- integração final host: 5/5 aprovados com PostgreSQL 16 real em 0,84 s;
- suíte integral host: 71 aprovados em 1,58 s; Ruff aprovado;
- suíte integral Linux ARM64/Python 3.13.18 + PostgreSQL real: 71 aprovados em 2,37 s; Ruff aprovado;
- comprovados: batch parcial/retry, pre-commit rollback, post-commit/pre-ack, `REPLAYED`, sobreposição, marcador ausente, replay obsoleto, empate divergente e concorrência serializada.

O batch size de produção permanece configurável (default 1.000). Benchmark de volume/memória e E2E da DAG pertencem aos gates seguintes.
