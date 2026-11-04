# EXT-001 — Cliente PNCP e extração raw

| Campo | Valor |
|---|---|
| Status | **IMPLEMENTED AND VERIFIED — OFFLINE/LINUX ARM64** |
| Requisitos | FR-001–004, FR-012/013; NFR-005/008/009; AC-01–04/12/15/18/19 |
| Dependência | FND-001 verificada |

## Objetivo

Consultar `/v1/atas` sequencialmente, com janela/limites tipados, paginação completa e retries finitos, publicando raw JSONL e manifesto/checksum atômicos sem acumular o período em memória.

## Contratos do incremento

- `QueryWindow` e `SafetyLimits` rejeitam configuração inválida antes da rede;
- `PncpClient.iter_pages` mantém no máximo uma página e valida os metadados do envelope;
- 400/401/422 e demais 4xx permanentes fazem uma chamada; 429/5xx/transporte têm no máximo três tentativas;
- `Retry-After` válido nunca é antecipado; acima do orçamento inline produz `RateLimitDeferred` com `retry_not_before`;
- 204 inicial e envelope inicial vazio são sucesso vazio; vazio intermediário, JSON inválido e metadados contraditórios falham;
- `extract_to_artifact` escreve `raw.jsonl.part`, faz checksum/rename e publica `manifest.json` por último;
- falha ou ceiling não publica manifesto completo; retry da mesma execução pode reconstruir o `.part`;
- cada linha raw preserva payload e proveniência de execução/página/posição/coleta.

## Aceite

Testes offline com `httpx.MockTransport` cobrem duas páginas, vazios, 400/401/422, 429/5xx, timeout/transporte, JSON inválido, contradição e limites. Testes de filesystem comprovam checksum, contagens, atomicidade e parcial não consumível. Pytest e Ruff devem permanecer verdes.

## Evidência do incremento

- RED comportamental: 21 testes coletados e 21 falhas nas interfaces sem implementação;
- GREEN/REFACTOR: 24 testes de componente aprovados, incluindo retry após manifesto já publicado sem nova chamada HTTP;
- suíte integral no host: 34 aprovados, sem falhas ou skips, em 0,87 s;
- suíte integral na imagem Linux ARM64/Python 3.13.18: 34 aprovados em 1,72 s;
- Ruff integral: aprovado;
- testes não consultam a API pública; `httpx.MockTransport` reproduz respostas e falhas deterministicamente;
- ambiente persistente permaneceu healthy: PostgreSQL, Airflow API server, scheduler e DAG processor.

Uma consulta PNCP real e limitada pertence ao E2E posterior. Este status não alega validação online nem elimina a limitação de snapshot entre páginas descrita no System Design.
