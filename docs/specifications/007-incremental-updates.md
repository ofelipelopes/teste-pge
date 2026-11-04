# INC-001 — Consulta incremental por atualização global

| Campo | Valor |
|---|---|
| Status | **VERIFIED WITH CONTROLLED REAL SMOKE** |
| Prioridade | P1 condicional |
| Dependência | P0 verificado; contrato oficial consultado em 03/11/2026 |

## Objetivo

Reutilizar a DAG e todo o pipeline P0 para consultar opcionalmente
`/v1/atas/atualizacao`, sem confundir atualização global com vigência e sem criar uma
segunda pipeline.

## Contrato

- `modo_consulta=vigencia` continua sendo o padrão e usa `/v1/atas`;
- `modo_consulta=atualizacao` usa `/v1/atas/atualizacao`, cuja janela oficial é filtrada
  por data de atualização global no formato `yyyyMMdd`;
- execução agendada escolhe o modo por ambiente; no modo incremental, relê de um a sete
  dias até ontem, com dois dias inclusivos por padrão;
- execução manual aceita modo e datas explícitos; escopo/ceilings continuam obrigatórios;
- modo, endpoint lógico e janela ficam na proveniência, inclusive no manifesto raw e em
  `ops.pipeline_run.filters`;
- retry não pode reutilizar raw produzido por outro modo;
- validação, transformação, staging, promoção e proteção de frescor são as mesmas do P0.

## Aceite

Testes offline comprovam seleção do endpoint, parâmetros, sobreposição configurável,
proveniência, rejeição de configuração inválida e isolamento de retry. DagBag continua
com exatamente cinco tasks. A validação real permanece sob demanda e fora da suíte para
não introduzir dependência externa nos testes normais.

## Evidência

- OpenAPI oficial confirmou parâmetros, paginação e resposta do endpoint em 03/11/2026;
- RED focal: 6 falhas comportamentais e 5 testes existentes aprovados;
- GREEN focal: 11/11 testes aprovados em 0,94 s;
- divergência real corrigida por TDD: a fonte enviou datas `YYYY-MM-DD` apesar do formato
  `date-time` no OpenAPI; o mapping v2 aceita a data local sem inventar UTC;
- regressão host/Python 3.13/PostgreSQL: 92/92 em 2,66 s;
- regressão Linux ARM64/Python 3.13.18/PostgreSQL: 92/92 em 4,18 s e Ruff aprovado;
- Airflow AS BUILT: exatamente cinco tasks, DAG pausada e zero erro de importação;
- smoke real: janela 02/11/2026, CNPJ `00508903000188`, 1 página/1 registro; duas DagRuns
  completas produziram `inserted=1` e depois `unchanged=1`, zero rejeições e uma linha
  final em curated. A evidência não garante disponibilidade contínua da fonte.
