# FND-001 — Fundação executável

| Campo | Valor |
|---|---|
| Status | **IMPLEMENTED AND VERIFIED — Linux ARM64** |
| Requisitos | NFR-001–005, NFR-006, NFR-012; preparação de FR-010/011 |
| Dependência | System Design aprovado em `docs/architecture/system-design.md` |

## Objetivo

Criar o menor ambiente reproduzível capaz de importar o pacote Python, resolver dependências com Python 3.13/`uv`, executar Pytest e Ruff, construir uma imagem única e iniciar PostgreSQL e os processos mínimos do Airflow 3 por Docker Compose.

## Escopo

- `pyproject.toml`, `.python-version` e `uv.lock`;
- pacote `src/pncp_etl` importável e tipado;
- Pytest e Ruff configurados;
- Dockerfile não-root usando o lock;
- Compose com PostgreSQL, inicialização idempotente e Airflow API server, scheduler e DAG processor;
- SQL inicial versionado para os bancos/roles necessários;
- healthchecks e teste de conexão PostgreSQL;
- `.env.example` e `.dockerignore` sem material privado.

Ficam fora deste incremento: cliente PNCP, regras de dados, tabelas finais do pipeline, DAG completa e qualquer consulta real à API.

## Aceite verificável

1. teste escrito antes do pacote falha por `ModuleNotFoundError` e passa após a implementação mínima;
2. Python executado pelo projeto é 3.13.x;
3. `uv lock --check`, `uv sync --locked` e verificação de dependências passam;
4. `uv run --locked pytest` e `uv run --locked ruff check .` passam;
5. `docker compose config` valida a configuração;
6. a imagem constrói com o lock e importa Airflow, provider PostgreSQL, Psycopg, HTTPX e `pncp_etl`;
7. PostgreSQL fica healthy, os dois bancos necessários existem e a aplicação abre conexão real;
8. migração do Airflow conclui e API server, scheduler e DAG processor iniciam sem erro fatal;
9. arquivos privados, PDF, `.env`, runtime e logs não entram no contexto Docker.

## Estratégia test-first

- **RED:** teste de contrato do pacote antes de existir `src/pncp_etl`.
- **GREEN:** projeto/package mínimo, lock e imports.
- **REFACTOR:** consolidar configuração, imagem e Compose; repetir testes/lint/build/health.

Falha real de resolução ou runtime em Python 3.13 será reproduzida antes de considerar o fallback 3.12.

## Evidência do incremento

- RED do pacote: 2 coletados, 1 erro esperado por `ModuleNotFoundError`;
- GREEN final: 10 testes aprovados no host e 10 na imagem Linux, sem falhas/skip;
- `uv lock --check`, sync locked e dependency check: 140 pacotes resolvidos, 139 instalados compatíveis;
- Ruff: aprovado;
- build Linux ARM64 e imports de Airflow/provider/Psycopg/HTTPX/pacote: aprovados;
- PostgreSQL, API server, scheduler e DAG processor: healthy;
- `db-init`/`airflow-init`: exit 0 e idempotência confirmada;
- restart: dados/migração preservados e todos os serviços retornaram a healthy;
- import errors de DAG: `[]` (a DAG funcional ainda pertence a incremento posterior).

Limitação: build/smoke AMD64 permanece pendente; o host possui `uv` 0.12.18, por isso a validação local executou o `uv` 0.13.6 isolado, enquanto a imagem já o contém diretamente.
