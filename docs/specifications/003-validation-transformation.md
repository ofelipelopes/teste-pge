# VAL-001 — Validação e transformação determinística

| Campo | Valor |
|---|---|
| Status | **IMPLEMENTED AND VERIFIED — OFFLINE/LINUX ARM64** |
| Requisitos | FR-005–007, FR-012/013; NFR-008/009; AC-05–08/12/15/18 |
| Dependência | EXT-001 verificada |

## Objetivo

Ler o raw incrementalmente, separar registros válidos e rejeitados, aplicar o mapping canônico sem alterar o payload original e resolver duplicatas por identidade com índice SQLite reconstruível.

## Contratos do incremento

- mapping único e versionado; origem/destino duplicado é erro estrutural antes dos registros;
- `numeroControlePNCPAta` é chave local obrigatória; opcionais ausentes/nulos permanecem `None`;
- tipos JSON não são coercidos silenciosamente (`bool` não é `int`, número não vira texto);
- `SourceDateTime` preserva lexema/local e só produz UTC quando a fonte informa offset;
- fração acima de seis dígitos, date-only, formato/horário inválido e domínios de vigência incompatíveis geram códigos estáveis de rejeição;
- CNPJ pontuado válido é normalizado para 14 dígitos; valor inválido vai à quarentena;
- extras permanecem exclusivamente no raw;
- SQLite resolve por identidade independentemente da ordem: igual colapsa; maior `dataAtualizacaoGlobal` comparável vence; empate divergente/ausência/domínio misto rejeita conflito;
- `validate` e `transform` publicam manifestos por último e nunca tornam `.part` consumível.

## Aceite

Testes unitários cobrem mapping, tipos, nulos, extras, temporalidade e intervalos. Testes de componente cobrem válido + inválido no mesmo raw, deduplicação igual, atualização ordenável, conflito ambíguo, ordem de entrada, contagens/checksums e retry. Pytest e Ruff permanecem verdes.

## Evidência do incremento

- RED do núcleo: 20 testes coletados e 20 falhas comportamentais antes do parser/mapping;
- RED do SQLite: 6 testes coletados e 6 falhas antes do resolvedor;
- RED vertical: 4 testes coletados e 4 falhas antes dos artefatos/tasks;
- suíte integral no host: 64 aprovados, sem falhas ou skips, em 0,93 s;
- suíte integral Linux ARM64/Python 3.13.18: 64 aprovados em 1,85 s;
- Ruff integral: aprovado nos dois ambientes;
- `raw → validate → transform` comprovado offline com válido, inválido, versão ordenável, conflito, corrupção e retry;
- manifesto de `transform` inclui `curated.jsonl`, rejeições, índice SQLite, checksums e equação reconciliada de contagens.

A política entre execuções ainda pertence à promoção PostgreSQL; este incremento prova somente resolução determinística dentro da execução.
