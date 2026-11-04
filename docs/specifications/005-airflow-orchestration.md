# DAG-001 — Orquestração Airflow e E2E local

| Campo | Valor |
|---|---|
| Status | **IMPLEMENTED AND VERIFIED — AIRFLOW/LINUX ARM64** |
| Requisitos | FR-010–014; NFR-003/004/009/011; AC-12–19 |
| Dependência | DB-001 verificada |

## Objetivo

Expor os módulos implementados em uma DAG Airflow com exatamente cinco tasks, parâmetros manuais/agenda determinísticos, XCom pequeno, retry coerente com a classificação de erro e execução completa contra fonte simulada e PostgreSQL real.

## Contratos do incremento

- DAG `pncp_atas_etl`, timezone `America/Fortaleza`, `catchup=False`, `max_active_runs=1`;
- cadeia exata `extract → validate → transform → load_staging → load_curated`;
- DAG apenas resolve contexto/configuração e chama módulos; mapping/SQL/regras HTTP permanecem fora;
- execução manual aceita janela/filtros/ceiling; agenda deriva o dia civil anterior e exige CNPJ configurado;
- execução manual sem CNPJ exige opt-in explícito e continua sob ceilings;
- XCom contém somente referências/resultados JSON pequenos;
- permanente/contrato não repete request; transitório pode usar o retry da task; rate-limit longo registra prazo e não requisita cedo;
- ambiente da imagem contém a DAG e `list-import-errors` permanece vazio.

## Aceite

Testes do `DagBag` verificam import, IDs, dependências, parâmetros e retries. E2E offline executa as cinco funções com HTTP simulado/PostgreSQL real, reconcilia contagens, repete o período sem duplicar curated e consulta métricas/estados.

## Evidência do incremento

- RED inicial corrigiu duas incompatibilidades do próprio teste com Airflow 3.3 antes de contar a ausência da feature;
- RED comportamental: DAG/Dockerfile ausentes; após implementação, o teste detectou e removeu arestas laterais de XCom;
- `DagBag`: exatamente cinco tasks e cadeia linear obrigatória, 3/3 testes aprovados;
- parâmetros manuais, janela histórica, opt-in sem CNPJ e XCom < 4 KiB comprovados;
- suíte integral final host: 91 aprovados em 2,05 s; Ruff aprovado;
- suíte integral final Linux ARM64/Python 3.13.18/PostgreSQL real: 91 aprovados em 3,32 s; Ruff aprovado;
- Airflow real: PostgreSQL/API server/scheduler/DAG processor healthy, import errors `[]` e `pncp_atas_etl` listada em `/opt/airflow/dags/pncp_atas_etl.py`;
- E2E offline percorre os cinco módulos com `httpx.MockTransport` e PostgreSQL real; não consulta a API pública.
- teste sintético com volume 10 vezes maior comprova crescimento sublinear e teto de memória;
- README/configuração operacional possuem contrato automatizado; imagem final auditada sem material privado.

Uma execução limitada contra PNCP real permanece deliberadamente separada e só deve ocorrer com configuração/autorização explícita.
