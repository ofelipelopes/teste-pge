# VER-001 — Comparação mínima entre versões coletadas

| Campo | Valor |
|---|---|
| Status | **IMPLEMENTED AND TESTED — POSTGRESQL REAL** |
| Prioridade | P1 de baixo custo |
| Dependência | P0 verificado; staging e proveniência existentes |

## Objetivo

Comparar duas coletas explicitamente escolhidas da mesma ata, informando os campos
canônicos alterados e sua proveniência sem usar ordem de coleta como versão de negócio.

## Contrato

- a entrada identifica `numeroControlePNCPAta` e dois `execution_id` distintos;
- ambas as observações devem existir em `staging.ata_candidate` e pertencer à mesma ata;
- conteúdo idêntico resulta em `UNCHANGED`;
- conteúdo divergente com `dataAtualizacaoGlobal` comparável e diferente resulta em
  `CHANGED`, indicando qual execução é comprovadamente mais nova;
- empate, marcador ausente ou domínios temporais incompatíveis resultam em
  `TEMPORAL_CONFLICT`, sem eleger versão mais nova;
- o resultado lista valores antes/depois por campo e a proveniência de cada execução;
- uma CLI somente leitura torna a comparação reproduzível sem modificar a DAG P0.

## Aceite

Testes unitários cobrem conteúdo igual, ordem comprovada, empate, ausência e domínios
mistos. Teste com PostgreSQL real carrega duas coletas, consulta o resultado e comprova
proveniência e diff. A CLI retorna JSON e falha explicitamente para execução ausente ou
igual.

## Evidência

- RED: 6/6 testes falharam em `NotImplementedError`, com imports e fixtures válidos;
- GREEN unitário: 6/6 aprovados em 0,05 s;
- integração PostgreSQL/CLI: 1/1 aprovado em 0,25 s;
- Ruff aprovado nos módulos e testes afetados;
- nenhuma migração, task ou serviço foi acrescentado.
