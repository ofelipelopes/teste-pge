# System Design — Pipeline ETL de Atas do PNCP

| Metadado | Valor |
|---|---|
| Status | **APPROVED WITH MINOR CORRECTIONS — correction incorporated** |
| Data | 02/11/2026 |
| PRD de origem | PRD canônico v0.3, preservado como artefato interno de requisitos |
| Fonte normativa | PDF local do desafio técnico da PGE-CE, 5 páginas |
| Escopo | Arquitetura aprovada e posteriormente implementada; estado operacional no `README.md` |
| Estado AS BUILT | DAG, Compose, SQL e testes confirmados na auditoria final de entrega |

> A revisão independente concluiu `APPROVED WITH MINOR CORRECTIONS`, com SDR-001 a
> SDR-006 verificados como resolvidos e a correção LOW incorporada. Este documento registra
> o desenho aprovado; comandos e evidências atuais de execução ficam no `README.md` e nos
> testes automatizados.

## 1. Contexto e objetivos

O produto é um pipeline ETL local que consulta Atas de Registro de Preços (ARP) no endpoint público `GET /v1/atas` do PNCP, valida e normaliza os registros e persiste evidência bruta, rejeições e uma visão tratada em PostgreSQL. O Apache Airflow orquestra as cinco etapas exigidas, e o Docker Compose reproduz o ambiente.

O desenho busca, simultaneamente:

1. completar uma consulta selecionada ou falhar explicitamente;
2. limitar memória em todas as etapas, e não só no HTTP;
3. preservar o dado recebido e sua proveniência;
4. impedir duplicações lógicas na curated após reexecução;
5. separar falha estrutural de registro individual inválido;
6. permitir teste sem rede pública e recuperação por tarefa;
7. manter a solução explicável e executável em uma máquina local.

Uma ARP não comprova gasto, compra ou pagamento. A solução organiza publicações do PNCP e não executa análise jurídica.

## 2. Requisitos arquiteturalmente significativos

| Tema | Requisito determinante | Consequência arquitetural |
|---|---|---|
| Orquestração | cinco tarefas visíveis e ordenadas | uma DAG `extract -> validate -> transform -> load_staging -> load_curated` |
| Volume | o contrato permite paginação extensa; 528.120 e 499.534 são observações históricas não reproduzidas neste gate, não capacidade ou benchmark | iteração por página/lote, índice de deduplicação em disco e guardrails antes de avançar |
| XCom | datasets grandes proibidos | XCom com referência de manifesto, checksum e contagens, limitado por contrato a poucos KiB |
| Rastreabilidade | aceitos e rejeitados ligados ao bruto | JSONL imutável por execução e chaves de proveniência em PostgreSQL |
| Idempotência | reexecução não duplica curated | chave candidata validada, `UPSERT`, comparação nula-segura e transações |
| Falha parcial | retry sem sucesso falso | manifestos atômicos, estados de execução e promoção final transacional |
| Fonte mutável | paginação pode mudar durante a consulta | invariantes de metadados, deduplicação e limitação documentada; nenhuma promessa de snapshot |
| Testabilidade | suíte padrão sem PNCP ao vivo | cliente injetável com `httpx.MockTransport`, regras puras e PostgreSQL real em integração |
| Reprodutibilidade | `uv`, Compose e versões compatíveis | `pyproject.toml`, `uv.lock`, constraints do Airflow e uma imagem construída pelo projeto |
| Segurança | sem credenciais em código/Git | `.env.example`, Airflow Connections/ambiente, usuários separados e logs sem payload integral |

## 3. Restrições

- Python, Apache Airflow, PostgreSQL, Docker Compose, Pytest, Ruff e `uv` são obrigatórios.
- O endpoint P0 é `/v1/atas`, cuja janela representa vigência, não publicação nem atualização.
- `/v1/atas/atualizacao` era P1 condicional no gate P0 e foi incorporado depois como
  modo opcional AS BUILT, reutilizando a mesma DAG; `/v1/atas` permanece o padrão.
- Não há Kafka, Spark, Kubernetes, Redis, Celery, microsserviços, dashboard, API própria ou SCD2 completo.
- O projeto não transporta páginas ou registros por XCom.
- O desenho prevê SQL versionado, implementado em `sql/migrations/`.
- O roteiro de vídeo permanece reservado ao último gate; arquitetura, specifications e implementação não dependem dele.

## 4. Visão da solução

A aplicação Python será um pacote modular usado pela DAG, sem regra de negócio dentro do arquivo de orquestração. O Airflow 3 executará as tarefas no `LocalExecutor`; isso mantém os processos no container do scheduler e elimina broker e workers separados. Um volume nomeado compartilhado guarda artefatos intermediários entre tarefas e sobrevive a reinícios de containers.

Um único serviço PostgreSQL conterá dois bancos lógicos:

- `airflow`: metadados do orquestrador;
- `pncp`: dados do pipeline, separados nos schemas `ops`, `staging`, `quality` e `curated`.

Os componentes do Airflow serão `api-server`, `scheduler` e `dag-processor`, mínimos obrigatórios no Airflow 3. O triggerer não será iniciado porque o P0 não usa tarefas deferrable. O ambiente utilizará Simple Auth Manager somente para desenvolvimento/demonstração local.

Diagramas relacionados:

- [Visão de componentes](diagrams/componentes.md)
- [Fluxo ETL](diagrams/fluxo-etl.md)
- [Falhas e recuperação](diagrams/falhas-e-recuperacao.md)

As decisões de arquitetura local modular, artefatos incrementais e persistência
idempotente estão incorporadas neste desenho. Seus registros decisórios detalhados são
artefatos internos e não integram a distribuição pública.

## 5. Componentes e responsabilidades

| Componente | Responsabilidade | Não faz |
|---|---|---|
| DAG Airflow | parâmetros, dependências, timeouts, retries e tradução de erros para estados Airflow | HTTP, mapping, validação ou SQL de negócio |
| Cliente PNCP | request síncrono, timeout, paginação e classificação HTTP | regra de curated ou conexão com Airflow |
| Extração | percorrer páginas e produzir raw JSONL + manifesto | acumular o período ou carregar banco |
| Validação | validar registros, separar válidos/rejeitados e reconciliar contagens | alterar o raw ou deduplicar globalmente em RAM |
| Transformação/mapping | converter nomes/tipos e resolver duplicatas de modo determinístico | acessar rede, Airflow ou PostgreSQL |
| Repositório de artefatos | escrita atômica, checksum, leitura iterativa e retenção | ser fonte final de consulta |
| Persistência PostgreSQL | staging, rejeições, execução e `UPSERT` curated | decidir semântica de transformação |
| PostgreSQL | metadados Airflow e dados do pipeline em bancos separados | armazenar arquivos intermediários do Airflow |
| Docker Compose | serviços, rede, healthchecks, volumes e inicialização | orquestração distribuída de produção |

### 5.1 Contratos internos pequenos

Os contratos serão `dataclasses` tipadas, sem framework adicional:

- `QueryWindow`: datas, filtros e escopo declarado;
- `SafetyLimits`: página, páginas, registros, duração e bytes de artefato;
- `ArtifactRef`: `execution_id`, etapa, caminho do manifesto, contagem e checksum;
- `StageManifest`: versão do formato, parâmetros, estado `complete`, arquivos, contagens e checksums;
- `ValidationResult` e `LoadResult`: somente contagens e referências;
- exceções de domínio: `PermanentSourceError`, `TransientSourceError`, `ContractError`, `ArtifactIntegrityError` e `PersistenceError`.

Esses tipos não criam interfaces abstratas para implementações únicas. Eles tornam explícitas as fronteiras realmente usadas pela DAG e pelos testes.

## 6. Fluxo de dados

1. A DAG resolve a janela agendada ou os parâmetros manuais e valida os limites antes da rede.
2. `extract` consulta sequencialmente cada página, mantém apenas uma resposta na memória e escreve um JSONL `.part` no namespace da execução.
3. Ao terminar e reconciliar todas as páginas, `extract` fecha o arquivo, calcula checksum, renomeia atomicamente e grava o manifesto por último.
4. `validate` lê raw linha a linha e produz `valid.jsonl` e `rejected.jsonl` com manifestos próprios.
5. `transform` lê válidos, aplica mapping e usa SQLite local como índice temporário em disco para resolver identidades sem conjunto global em RAM; emite `curated.jsonl` e rejeições de conflito.
6. `load_staging` persiste raw e todas as rejeições em batches idempotentes e marca a execução `STAGING_COMPLETE` somente após reconciliar contagens.
7. `load_curated` deriva um `promotion_id` determinístico do `execution_id`, fingerprint do manifesto curated e versões do mapping/política; registra a tentativa, serializa promoções no PostgreSQL e consulta o registro de commit antes de escrever.
8. Se a operação ainda não foi confirmada, `load_curated` copia os candidatos progressivamente para tabela temporária, classifica `inserted/updated/unchanged/stale/conflict`, aplica somente updates comprovadamente mais novos e persiste o resultado da promoção na mesma transação das mudanças curated.
9. O commit produz `CURATED_COMMITTED` no PostgreSQL. O sucesso da task continua sendo estado do Airflow e pode ser reconhecido depois; um retry pós-commit recupera o resultado persistido sem repetir efeitos.
10. Artefato incompleto, limite excedido, conflito de fingerprint ou qualquer falha estrutural impede a promoção e nunca é convertido em sucesso.

### 6.1 Namespace e arquivos intermediários

O caminho lógico será:

`/opt/airflow/runtime/<execution_key>/<stage>/...`

`execution_key` será derivado por hash SHA-256 do `dag_id + run_id`, enquanto o `run_id` original ficará no manifesto e no banco. Isso evita caracteres inseguros em caminhos e mantém correlação.

Arquivos previstos:

| Etapa | Artefatos |
|---|---|
| `extract` | `raw.jsonl`, `manifest.json` |
| `validate` | `valid.jsonl`, `rejected.jsonl`, `manifest.json` |
| `transform` | `curated.jsonl`, `rejected.jsonl`, `dedup-index.sqlite`, `manifest.json` |

Arquivos `.part` não são consumíveis. O manifesto `complete=true`, gravado por último, é o commit do artefato. O consumidor valida versão, parâmetros, existência, tamanho e SHA-256 antes de ler.

### 6.2 Retenção e limpeza

- sucesso: elegível a limpeza após 7 dias;
- falha: elegível após 14 dias para favorecer diagnóstico;
- `.part` da própria execução: substituído com segurança no retry da tarefa produtora;
- limpeza de outras execuções: somente por comando operacional futuro que confirme estado terminal no Airflow, nunca por glob amplo;
- não haverá uma sexta tarefa na DAG apenas para limpeza.

Os valores são defaults locais e poderão ser ajustados por configuração após medição de espaço. A documentação final deverá alertar que a limpeza é destrutiva e distinguir `down` de remoção de volumes.

## 7. Design da DAG

### 7.1 Identidade e agenda

| Propriedade | Decisão |
|---|---|
| DAG ID proposto | `pncp_atas_etl` |
| Timezone | `America/Fortaleza` |
| Agenda default | `0 6 * * *`, configurável por ambiente |
| Janela agendada | vigência: dia civil anterior; atualização: lookback inclusivo de 1–7 dias até ontem, default 2 |
| Execução manual | `data_inicial`, `data_final`, `cnpj`, `codigo_unidade` e `modo_consulta` opcionais, sujeitos aos ceilings |
| `catchup` | `False` |
| concorrência | `max_active_runs=1` |
| executor | `LocalExecutor` |
| timeout de DAG | 180 minutos, configurável |

Como uma observação histórica registrou 499.534 itens para uma janela de vigência de um dia, a operação agendada P0 deverá possuir um `cnpj` de escopo configurado. Esse número não foi reproduzido neste gate e pode mudar; ele motiva cautela, não dimensiona capacidade. Ausência do escopo em execução agendada falha antes da rede. Execução manual sem CNPJ exige confirmação explícita por parâmetro e permanece sujeita aos guardrails; jamais é apresentada como demonstração integral se interrompida por limite.

### 7.2 Cinco tarefas obrigatórias

| Task | Entrada | Responsabilidade | Saída por XCom | Retry/timeout inicial |
|---|---|---|---|---|
| `extract` | janela, filtros, ceilings | validar parâmetros; consultar todas as páginas selecionadas; escrever raw/manifesto | `ArtifactRef` do raw e contagens | 1 retry após 5 min; timeout 30 min; HTTP possui retries próprios |
| `validate` | referência raw | conferir integridade; validar registros; emitir válidos/rejeitados | referência do manifesto de validação | 0 retries/20 min |
| `transform` | referência de válidos | mapping; tipos; deduplicação/conflitos em disco | referência do manifesto transformado | 0 retries/20 min |
| `load_staging` | manifestos anteriores | persistir raw e rejeições em batches idempotentes | `execution_id`, status e contagens | 1 retry após 1 min; timeout 30 min |
| `load_curated` | curated + staging completa | reconciliar `promotion_id`; serializar; aplicar frescor; `UPSERT`; persistir commit/resultado | `LoadResult` pequeno, novo ou recuperado do registro durável | 1 retry após 1 min; timeout 30 min |

Timeouts são defaults de proteção para a demonstração local, não SLA. O limite da DAG supera a soma sequencial dos timeouts base das cinco tarefas (130 minutos), com margem para intervalos de retry, sem prometer que toda repetição caberá no mesmo run. Eles serão calibrados em E2E; qualquer alteração deve manter falha explícita e ser documentada.

### 7.3 Política de retry em duas camadas

- O cliente HTTP faz até 3 tentativas totais por request somente para timeout/transporte, 429 e 5xx transitórios.
- A task `extract` tem uma repetição adicional para indisponibilidade prolongada depois do esgotamento do cliente.
- 400, 401, 422 e demais 4xx não classificados como 429 geram `PermanentSourceError`.
- A DAG converte erro permanente/contrato em `AirflowFailException`, que no Airflow ignora retries restantes.
- `validate` e `transform` não repetem erros determinísticos; a correção deve ocorrer na entrada/configuração.
- cargas podem repetir porque seus writes possuem chaves de idempotência e transações; `load_curated` não repete o efeito se o mesmo `promotion_id` já estiver confirmado.

### 7.4 XCom

O retorno de cada task deve serializar para JSON e permanecer abaixo de 4 KiB. Ele contém somente IDs, caminhos relativos sob a raiz aprovada, contagens, versão e checksum. O consumidor rejeita caminho absoluto externo, `..` e manifesto de outro `execution_id`.

Nenhuma lista de registros, página HTTP, DataFrame ou conteúdo JSONL é retornado pela TaskFlow API.

## 8. Integração com o PNCP

### 8.1 Endpoint e parâmetros

- Base URL: `https://pncp.gov.br/api/consulta`.
- P0: `GET /v1/atas`.
- P1 AS BUILT opcional: `GET /v1/atas/atualizacao`, selecionado por modo explícito e
  usando janela sobreposta; paginação, filtros e resposta são reutilizados.
- Datas: `yyyyMMdd`, com `dataInicial <= dataFinal`.
- Página inicial: 1.
- `tamanhoPagina`: 100 por default; permitido entre 10 e 500.
- Filtros opcionais: `cnpj`, `codigoUnidadeAdministrativa` e `idUsuario`, quando configurados.
- Requisições sequenciais; concorrência HTTP igual a 1.

O default 100 equilibra o número de chamadas e o tamanho de cada página enquanto não há benchmark de bytes. A specifications poderá elevar até 500 somente após medir memória e latência.

### 8.2 Guardrails iniciais

| Limite | Default local | Comportamento ao exceder |
|---|---:|---|
| janela | 1 dia para agenda/demo; máximo configurável de 7 dias | falha antes da rede |
| páginas | 1.000 | falha/incompleta; nenhuma curated |
| registros | 100.000 | preflight pela primeira página ou falha durante extração |
| duração de extração | 30 min | timeout e execução falha |
| artefato raw | 2 GiB | encerra `.part` como incompleto e falha |
| requests concorrentes | 1 | configuração acima disso é rejeitada no P0 |

Esses ceilings protegem o ambiente local e não descrevem capacidade de produção. O primeiro envelope permite falhar cedo quando `totalPaginas` ou `totalRegistros` já exceder o teto. Uma execução limitada é registrada como `FAILED_LIMIT` no manifesto/log da etapa e como falha no Airflow, nunca `SUCCEEDED` ou “carga integral”. Os números devem ser calibrados em specs/E2E sem enfraquecer essa semântica.

### 8.3 Timeout, retry e backoff HTTP

- `connect=10s`, `read=60s`, `write=10s`, `pool=10s`;
- 3 tentativas totais por request;
- backoff exponencial com jitter, base 1s e teto 30s;
- `Retry-After` válido representa um prazo mínimo, nunca um valor a truncar: aceita `delay-seconds` não negativo ou `HTTP-date`, calculado com relógio UTC controlável;
- a espera inline máxima inicial é 60s apenas para evitar bloquear o worker; se o prazo válido for maior ou ultrapassar o orçamento da task/DAG, a tentativa encerra sem novo request como `RATE_LIMIT_DEFERRED`, registra `retry_not_before` e desativa retry automático antecipado;
- sem header, com header inválido ou com data já passada, aplica-se o backoff local exponencial com jitter positivo, dentro do orçamento restante;
- `User-Agent` identifica o projeto sem dados pessoais;
- redirects não são seguidos automaticamente; qualquer 3xx é tratado como mudança de contrato/permanente, sem reenviar a requisição a outro host.

Não foi encontrado rate limit oficial. Esses valores são política local defensiva, não alegação sobre a fonte. `Retry-After` segue o RFC 9110 §10.2.3; HTTP 429 segue o RFC 6585 §4. Reexecução manual ou agendada após `RATE_LIMIT_DEFERRED` só pode ocorrer em ou depois de `retry_not_before`. O desenho não adiciona triggerer/deferrable task: uma espera longa falha explicitamente para liberação do worker, em vez de dormir ou requisitar cedo.

### 8.4 Paginação e completude

Para cada página, o extrator valida:

- `data` é coleção;
- `numeroPagina` corresponde à página solicitada;
- `totalPaginas`, `totalRegistros`, `paginasRestantes` e `empty` têm tipos coerentes;
- metadados totais não mudam silenciosamente em relação à primeira página;
- uma página intermediária não é vazia;
- a quantidade final lida é reconciliável com o envelope, ressalvadas duplicatas de origem contabilizadas.

HTTP 204 ou envelope vazio é sucesso vazio somente na primeira página e com estado coerente. Vazio após página não vazia é falha de completude.

### 8.5 Classificação de erros

| Classe | Exemplos | Ação |
|---|---|---|
| transitório | timeout, reset, 429, 500–599 aplicável | retry HTTP finito; depois erro transitório da task |
| permanente | 400, 401, 404, 422 e outro 4xx exceto 429 | uma chamada; erro permanente; nenhuma task retry |
| contrato/lote | 2xx com JSON inválido, envelope ausente ou metadado contraditório | falha não retentável e artefato incompleto |
| vazio válido | 204 inicial ou envelope inicial vazio/coerente | artefato completo com zero registros |
| registro inválido | campo individual ausente/incompatível | quarentena; demais válidos continuam |

### 8.6 Conjunto mutável entre páginas

A API não documenta snapshot consistente nem ordenação estável para `/v1/atas`. Comparar metadados e deduplicar detecta várias anomalias, mas não elimina a possibilidade de um registro entrar/sair entre páginas sem contradição observável. Por isso:

- não haverá paralelismo de páginas;
- a execução registra início/fim e metadados de cada página;
- duplicatas são resolvidas deterministicamente;
- lacuna invisível não será apresentada como impossível;
- o modo opcional por `/v1/atas/atualizacao` reduz o volume ao filtrar atualização
  global, mas continua sem snapshot e usa overlap civil, não cursor de alta precisão.

## 9. Modelo de dados

### 9.1 Mapping canônico

O mapping será uma coleção versionada de `FieldSpec`, importável por validação e transformação. Antes de ler registros, a configuração será validada quanto a origem/destino duplicados e colisões; qualquer inconsistência é falha estrutural da etapa inteira. Campo extra fica apenas no raw. Campo opcional ausente/nulo permanece `None`. Campo presente com tipo inválido é rejeitado, não zerado nem convertido silenciosamente.

| Campo PNCP | Campo canônico | Tipo Python | Tipo SQL curated | Regra |
|---|---|---|---|---|
| `numeroControlePNCPAta` | `numero_controle_pncp_ata` | `str` | `TEXT` | obrigatório local; trim; vazio rejeita |
| `numeroAtaRegistroPreco` | `numero_ata_registro_preco` | `Optional[str]` | `TEXT NULL` | trim; opcional |
| `anoAta` | `ano_ata` | `Optional[int]` | `INTEGER NULL` | inteiro, faixa plausível validada sem corrigir |
| `numeroControlePNCPCompra` | `numero_controle_pncp_compra` | `Optional[str]` | `TEXT NULL` | preservar texto |
| `cancelado` | `cancelado` | `Optional[bool]` | `BOOLEAN NULL` | somente boolean JSON |
| `dataCancelamento` | `data_cancelamento` | `Optional[SourceDateTime]` | par temporal descrito em §9.1.1 | nulo permitido; nunca truncar hora |
| `dataAssinatura` | `data_assinatura` | `Optional[SourceDateTime]` | par temporal descrito em §9.1.1 | preservar data, hora, fração e offset |
| `vigenciaInicio` | `vigencia_inicio` | `Optional[SourceDateTime]` | par temporal descrito em §9.1.1 | sem reduzir a `DATE` |
| `vigenciaFim` | `vigencia_fim` | `Optional[SourceDateTime]` | par temporal descrito em §9.1.1 | ordem validada somente entre valores comparáveis |
| `dataPublicacaoPncp` | `data_publicacao_pncp` | `Optional[SourceDateTime]` | par temporal descrito em §9.1.1 | não é filtro P0 |
| `dataInclusao` | `data_inclusao` | `Optional[SourceDateTime]` | par temporal descrito em §9.1.1 | sem confundir com publicação |
| `dataAtualizacao` | `data_atualizacao` | `Optional[SourceDateTime]` | par temporal descrito em §9.1.1 | atualização do próprio registro |
| `dataAtualizacaoGlobal` | `data_atualizacao_global` | `Optional[SourceDateTime]` | par temporal descrito em §9.1.1 | marcador preferencial de frescor; regras em §10.4 |
| `usuario` | `usuario` | `Optional[str]` | `TEXT NULL` | trim |
| `objetoContratacao` | `objeto_contratacao` | `Optional[str]` | `TEXT NULL` | preservar conteúdo, apenas trim externo |
| `cnpjOrgao` | `cnpj_orgao` | `Optional[str]` | `VARCHAR(14) NULL` | somente dígitos; pontuação removível de forma segura |
| `nomeOrgao` | `nome_orgao` | `Optional[str]` | `TEXT NULL` | trim |
| `codigoUnidadeOrgao` | `codigo_unidade_orgao` | `Optional[str]` | `TEXT NULL` | nunca converter para número |
| `nomeUnidadeOrgao` | `nome_unidade_orgao` | `Optional[str]` | `TEXT NULL` | trim |
| `cnpjOrgaoSubrogado` | `cnpj_orgao_subrogado` | `Optional[str]` | `VARCHAR(14) NULL` | mesma regra de CNPJ |
| `nomeOrgaoSubrogado` | `nome_orgao_subrogado` | `Optional[str]` | `TEXT NULL` | trim |
| `codigoUnidadeOrgaoSubrogado` | `codigo_unidade_orgao_subrogado` | `Optional[str]` | `TEXT NULL` | preservar texto |
| `nomeUnidadeOrgaoSubrogado` | `nome_unidade_orgao_subrogado` | `Optional[str]` | `TEXT NULL` | trim |
| `possibilidadeAdesao` | `possibilidade_adesao` | `Optional[bool]` | `BOOLEAN NULL` | boolean ou nulo |

O OpenAPI oficial consultado em 02/11/2026 declara os oito campos acima como `string/date-time` e apresenta exemplos com hora. A observação histórica de dez itens que teria retornado somente `YYYY-MM-DD` não é reproduzível nos artefatos deste gate, não substitui o contrato e não fundamenta `DATE`.

Fornecedor, valor monetário e número do processo não integram o DTO P0 confirmado e não aparecem no mapping.

#### 9.1.1 Contrato temporal sem perda silenciosa

`SourceDateTime` será um valor tagged e imutável com o lexema original, o horário local (`datetime` sem `tzinfo`), o instante UTC quando houver `Z`/offset e a classificação `OFFSET_AWARE` ou `LOCAL_UNZONED`. O parser não atribui timezone ausente. Os oito campos `date-time` usam o mesmo padrão físico:

- `<campo>_local TIMESTAMP(6) WITHOUT TIME ZONE NULL`: componentes de calendário/hora exatamente como publicados;
- `<campo>_utc TIMESTAMPTZ(6) NULL`: instante normalizado para UTC somente quando a fonte trouxer `Z` ou offset;
- constraint lógica: `_utc` exige `_local`; ausência de `_utc` significa explicitamente “timezone não informado”, nunca UTC implícito;
- o lexema e qualquer precisão além do tipo normalizado permanecem no `staging.ata_raw.payload`, ligado à curated por proveniência.

PostgreSQL e `datetime` preservam microssegundos. Fração com até seis casas é aceita sem arredondamento; mais de seis casas não é truncada: o raw é preservado e o registro vai para quarentena com `unsupported_datetime_precision` até existir decisão explícita. Valor apenas com data em campo `date-time`, offset inválido, horário impossível ou formato incompatível recebe `invalid_datetime`; nulo opcional permanece nulo.

| Dado temporal | Tipo na API/origem | Python | PostgreSQL | Normalização e comparação |
|---|---|---|---|---|
| `dataCancelamento` | `date-time` | `Optional[SourceDateTime]` | `data_cancelamento_local TIMESTAMP(6)` + `data_cancelamento_utc TIMESTAMPTZ(6)` | UTC somente com offset; não participa do frescor |
| `dataAssinatura` | `date-time` | `Optional[SourceDateTime]` | par `data_assinatura_local/_utc` | mesma regra; sem inferir meia-noite ou fuso |
| `vigenciaInicio` | `date-time` | `Optional[SourceDateTime]` | par `vigencia_inicio_local/_utc` | comparação com fim apenas se ambos forem do mesmo domínio temporal |
| `vigenciaFim` | `date-time` | `Optional[SourceDateTime]` | par `vigencia_fim_local/_utc` | offset-aware compara em UTC; dois unzoned comparam wall time; mistura é ambígua |
| `dataPublicacaoPncp` | `date-time` | `Optional[SourceDateTime]` | par `data_publicacao_pncp_local/_utc` | não substitui data de atualização |
| `dataInclusao` | `date-time` | `Optional[SourceDateTime]` | par `data_inclusao_local/_utc` | semântica própria de inclusão |
| `dataAtualizacao` | `date-time` | `Optional[SourceDateTime]` | par `data_atualizacao_local/_utc` | atualização do registro; não substitui automaticamente a global |
| `dataAtualizacaoGlobal` | `date-time` | `Optional[SourceDateTime]` | par `data_atualizacao_global_local/_utc` | marcador preferencial; somente valores válidos e comparáveis ordenam versões |
| `dataInicial`, `dataFinal` da consulta | `yyyyMMdd` (data civil) | `date` | `DATE` em `ops.pipeline_run` | preserva semântica de data, no timezone da DAG; nunca vira timestamp da fonte |
| `collected_at`, `created_at`, `updated_at`, `committed_at` | relógio do pipeline | `datetime` aware | `TIMESTAMPTZ(6)` | gerados em UTC; não provam frescor de negócio |

Os casos `LOCAL_UNZONED` são “campos sem informação temporal suficiente para determinar um instante”, mas continuam sendo date-time; não são rebaixados a data. Comparações entre `OFFSET_AWARE` e `LOCAL_UNZONED` são proibidas e resultam em conflito/quarentena, não em ordenação presumida.

### 9.2 Tabelas planejadas

#### `ops.pipeline_run`

Uma linha por execução com `execution_id UUID`, `airflow_run_id`, DAG, janela, filtros JSONB, estado, versões de contrato/mapping, contagens, páginas esperadas/processadas, caminhos/checksums de manifestos, início e fim. `airflow_run_id` é único.

Estados mínimos do banco: `LOADING_STAGING`, `STAGING_COMPLETE`, `CURATED_COMMITTED`. `SUCCEEDED` permanece estado do Airflow e não é gravado antecipadamente como se o PostgreSQL pudesse confirmar o ack do orquestrador. Falhas anteriores ao staging ficam no Airflow/manifests; falha antes do commit deixa `STAGING_COMPLETE`; falha depois do commit deixa `CURATED_COMMITTED`, mesmo se o Airflow ainda mostrar falha/retry.

#### `ops.curated_promotion_attempt`

Uma linha auditável por tentativa de task, com `attempt_id`, `promotion_id`, `execution_id`, início/fim, estado `STARTED/ABORTED/COMMITTED/REPLAYED`, task try number e erro seguro. `STARTED` sem registro correspondente em `ops.curated_promotion` não prova commit: pode representar transação ativa ou tentativa interrompida. A linha é criada em transação curta antes da promoção e é observabilidade, não fonte de verdade sobre os dados. Quando a tentativa encontra uma promoção idêntica já confirmada, ela termina como `REPLAYED`, registra `finished_at` e referencia o commit existente; isso não autoriza COPY/UPSERT nem alteração de timestamps de negócio.

#### `ops.curated_promotion`

Registro imutável de commit, criado **na mesma transação** do UPSERT e das rejeições de frescor. Contém `promotion_id` como PK, `execution_id`, checksum/contagem do manifesto curated, versões do mapping e da política de frescor, `status=COMMITTED`, `committed_at` e o `LoadResult` final (`inserted`, `updated`, `unchanged`, `stale`, `conflict`). Uma unique adicional sobre a identidade lógica da operação impede que o mesmo `execution_id` seja associado silenciosamente a outro fingerprint.

Ausência da linha significa apenas “sem commit confirmado”. Presença com fingerprint e versões idênticos autoriza replay sem escrita; divergência para a mesma identidade de operação é erro estrutural. Essa tabela, não o estado do Airflow nem inferência a partir da curated, é a fonte de verdade sobre a promoção.

#### `staging.ata_raw`

- `raw_id BIGINT` técnico;
- `execution_id` FK;
- página e índice na página;
- `payload JSONB` fiel ao registro recebido;
- hash da representação canônica do payload;
- instante de coleta `TIMESTAMPTZ`;
- chave de negócio disponível e status de validação;
- `UNIQUE(execution_id, page_number, record_index)` para retry.

#### `quality.ata_rejection`

- `rejection_id BIGINT`;
- `execution_id`, `raw_id`, etapa e chave disponível;
- `reason_code`, detalhe seguro e instante;
- `UNIQUE(execution_id, raw_id, stage, reason_code)`.

O payload não é duplicado: a rejeição referencia o raw. Para conflito com vários registros, cada `raw_id` envolvido recebe evidência própria.

#### `curated.ata`

- chave de negócio validada;
- todos os campos canônicos da tabela de mapping;
- `created_at`, `updated_at`, `last_seen_at` como `TIMESTAMPTZ` técnicos;
- `first_seen_execution_id`, `last_seen_execution_id`, `last_changed_execution_id`;
- `last_promotion_id` para vincular a mudança aceita ao commit durável;
- constraints de tipo/nulabilidade e índices apenas para consultas demonstradas.

`updated_at` muda somente quando conteúdo canônico muda; `last_seen_at` muda a cada observação. Assim, reexecução idêntica é observável sem fingir alteração do dado.

Timestamps técnicos serão gravados em UTC e a sessão PostgreSQL da aplicação usará UTC. Isso não atribui UTC aos campos de negócio da PNCP, que seguem o contrato variante de §9.1.1.

### 9.3 Chave de negócio e fallback

Plano principal, condicionado à validação: `numero_controle_pncp_ata TEXT PRIMARY KEY`.

Antes do DDL final, uma amostra representativa e fixtures devem verificar:

1. ausência/nulidade;
2. duplicatas iguais;
3. mesma chave associada a diferentes compra, número/ano da ata ou órgão;
4. comportamento entre páginas/janelas.

Ausência sempre vai para quarentena. Duplicata da mesma ata não invalida a chave; é resolvida pela política de atualização. Se houver colisão entre identidades distintas, o DDL é bloqueado e a alternativa proporcional é validar a composição:

`numeroControlePNCPAta + numeroControlePNCPCompra + numeroAtaRegistroPreco + anoAta`.

Só haverá hash técnico dessa composição depois de provar presença e unicidade. Se a
composição também falhar, os afetados permanecem em quarentena e a decisão de identidade
deve ser revista; não será criada chave sintética silenciosa para promover dados ambíguos.

### 9.4 Índices planejados

- PK/unique da identidade curated validada;
- `staging.ata_raw(execution_id)`;
- `quality.ata_rejection(execution_id, reason_code)`;
- `ops.curated_promotion(promotion_id)` PK e unique de fingerprint por execução/versões;
- `ops.curated_promotion_attempt(promotion_id, started_at)`;
- índice em `curated.ata(data_atualizacao_global_utc)` ou `_local` somente se uma consulta real o utilizar; a PK é suficiente para o predicado de UPSERT por identidade;
- `ops.pipeline_run(airflow_run_id)` único.

Índice em todo campo é rejeitado. O DDL versionado só será produzido após os padrões de consulta e a chave serem confirmados.

### 9.5 Retenção no PostgreSQL

Curated não terá expiração automática no P0. Staging, rejeições e `pipeline_run` permanecem por default para todas as execuções demonstrativas; eventual purge será um comando explícito por `execution_id`, com FK/ordem controladas e sem `CASCADE` amplo. Uma política automática só será adicionada após medir crescimento e preservar as evidências necessárias à apresentação.

## 10. Qualidade de dados

### 10.1 Níveis de validação

| Nível | Exemplo | Resultado |
|---|---|---|
| configuração | data inválida, janela invertida, ceiling inválido ou mapping colidente | falha antes da rede ou dos registros |
| resposta/lote | JSON inválido, `data` ausente, metadados contraditórios | falha da execução; nenhuma carga final |
| registro rejeitado | chave ausente, tipo incompatível, data inválida, conflito irresolúvel | raw preservado + quarentena; válidos continuam |
| recuperável | espaços externos, CNPJ pontuado válido | normalização determinística + raw preservado |
| válido opcional | campo ausente ou nulo permitido | canônico nulo |
| extra | campo novo não mapeado | permanece no raw; métrica de campo desconhecido |

### 10.2 Códigos de rejeição

O conjunto inicial será estável e pequeno: `missing_business_key`, `invalid_type`, `invalid_datetime`, `unsupported_datetime_precision`, `incomparable_temporal_domain`, `invalid_cnpj`, `invalid_validity_range`, `conflicting_duplicate`, `stale_source_version` e `ambiguous_source_version`. Colisão na definição do mapping não é rejeição individual: é erro estrutural e bloqueia a transformação. Detalhes registram campo e regra, nunca o payload integral ou segredo.

### 10.3 Duplicatas sem memória proporcional

`transform` utilizará `sqlite3` da biblioteca padrão como índice temporário no volume da execução. Para cada identidade, armazena conteúdo canônico, data global, referências raw e estado de conflito. A decisão é:

1. mesmo conteúdo: um candidato, duplicata contabilizada;
2. conteúdo diferente com `dataAtualizacaoGlobal` válida, do mesmo domínio temporal e distinta: vence a maior;
3. empate, ausência, mistura `OFFSET_AWARE/LOCAL_UNZONED` ou valor inválido que impeça decisão segura: todos os candidatos conflitantes são rejeitados;
4. saída final ordenada por identidade, independente da ordem de entrada.

SQLite é um arquivo interno de trabalho, não novo serviço nem fonte oficial. Ele evita manter centenas de milhares de chaves em memória e pode ser reconstruído deterministicamente.

### 10.4 Frescor entre execuções

O contrato oficial descreve `dataAtualizacaoGlobal` como a última atualização global da ata, incluindo dependentes, e o endpoint incremental é filtrado por esse campo. Por isso ele é o marcador preferencial de versão da fonte, sem transformar `collected_at` em atualização de negócio. A garantia é conservadora:

1. **identidade nova:** insere mesmo sem marcador de frescor, pois não há estado anterior a regredir;
2. **mesmo conteúdo canônico:** classifica `unchanged`; pode registrar uma nova observação da execução, mas não altera a versão da fonte nem `updated_at`;
3. **conteúdo divergente e marcador comparável maior:** atualiza campos, marcador, `updated_at`, proveniência e `last_promotion_id`;
4. **conteúdo divergente e marcador comparável menor:** classifica `stale`, mantém curated intacta e grava rejeição/auditoria ligada ao raw;
5. **marcador igual e conteúdo divergente:** classifica `conflict`; não escolhe por ordem de chegada;
6. **marcador ausente em qualquer lado, inválido ou de domínio temporal incompatível:** classifica `conflict/ambiguous_source_version`; curated não muda;
7. `dataAtualizacao` pode apoiar diagnóstico, mas não substitui automaticamente `dataAtualizacaoGlobal`; qualquer fallback futuro exige evidência contratual e revisão desta política;
8. `collected_at`, ordem do run, `run_id` e instante de ingestão nunca provam que o conteúdo é mais novo na origem.

A classificação e o `UPDATE ... WHERE` lógico são avaliados atomicamente no PostgreSQL sob lock transacional. Uma chave única evita duplicidade; o predicado de frescor evita regressão. A política deliberadamente prefere não atualizar a sobrescrever com versão ambígua. Isso não é SCD2: versões rejeitadas/stale ficam auditáveis em staging/quality, enquanto curated mantém uma linha aceita por identidade.

### 10.5 Métricas de qualidade

Por execução: extraídos, válidos, rejeitados por código/etapa, duplicatas idênticas, conflitos, transformados únicos, campos extras e nulos relevantes. As equações mínimas são:

- `extraidos = validos_individuais + rejeitados_validacao`;
- `validos_individuais = transformados_unicos + duplicatas_idênticas + rejeitados_conflito`.

Na promoção final também se exige `candidatos = inserted + updated + unchanged + stale + conflict`. As contagens do `LoadResult` ficam imutáveis em `ops.curated_promotion` e são recuperadas em retry pós-commit.

Cada manifesto e a linha `pipeline_run` devem carregar as contagens correspondentes.

## 11. Idempotência

Duas execuções equivalentes podem criar duas evidências de coleta na staging, mas produzem a mesma cardinalidade e o mesmo conteúdo de negócio na curated.

Cada promoção possui:

- `input_fingerprint`: SHA-256 do manifesto curated completo, incluindo checksum do arquivo, contagem, janela/filtros e versão do formato;
- `promotion_id`: UUID v5 ou digest determinístico sobre `execution_id + input_fingerprint + mapping_version + freshness_policy_version`;
- `LoadResult`: resultado imutável persistido no commit.

O `UPSERT` terá cinco resultados lógicos:

- **inserted:** identidade não existia; define `created_at`, `updated_at` e `last_seen_at`;
- **updated:** identidade existia, conteúdo difere e a versão da fonte é comprovadamente maior; preserva `created_at`, muda `updated_at` e proveniência;
- **unchanged:** conteúdo igual; preserva `updated_at` e versão da fonte, registrando no máximo uma observação por `promotion_id`;
- **stale:** conteúdo difere e a versão recebida é comprovadamente menor; curated não muda;
- **conflict:** conteúdo difere e não existe ordenação segura; curated não muda e a divergência vai para quality.

A comparação de conteúdo será feita por tupla nula-segura no PostgreSQL, sem depender do hash P1. O hash de conteúdo permanece otimização opcional, não requisito para a correção P0. A comparação de versão segue §10.4 e é parte do predicado transacional; `collected_at` não aparece nesse predicado.

Retry de staging usa suas uniques por execução. Retry de curated primeiro consulta `ops.curated_promotion`: se o mesmo `promotion_id` estiver `COMMITTED` e fingerprint/versões coincidirem, retorna o `LoadResult` persistido sem executar COPY/UPSERT nem alterar `last_seen_at`; se divergirem, falha. Assim, o efeito é idempotente mesmo quando o PostgreSQL confirmou o commit e o Airflow não recebeu o retorno.

## 12. Transações e atomicidade

### 12.1 Staging

Staging é escrita em batches configuráveis, inicialmente 1.000 registros. Cada batch é uma transação curta com `ON CONFLICT` idempotente. A linha `pipeline_run` é criada/atualizada como `LOADING_STAGING`; somente depois de todos os batches e da reconciliação passa a `STAGING_COMPLETE`.

Uma falha pode deixar evidência parcial de staging, explicitamente marcada como incompleta. Isso é aceitável e recuperável; não autoriza curated.

### 12.2 Curated

`load_curated` usa uma tentativa auditável e uma transação de promoção:

1. valida `STAGING_COMPLETE`, manifesto, checksums, contagens e versões; deriva `input_fingerprint` e `promotion_id`;
2. em transação curta, registra `ops.curated_promotion_attempt=STARTED`; isso não autoriza sucesso;
3. abre a transação final em `READ COMMITTED` e adquire um PostgreSQL transaction-scoped advisory lock fixo para a promoção PNCP, serializando inclusive chamadas que escapem de `max_active_runs=1`;
4. consulta `ops.curated_promotion` pelo `promotion_id` e pela identidade lógica da execução;
5. se já houver `COMMITTED` idêntico, encerra a tentativa como `REPLAYED` em uma transação curta de auditoria, sem writes de negócio, e retorna o `LoadResult` persistido; fingerprint/versão divergente falha como `PromotionConflict`;
6. sem commit prévio, cria tabela temporária e carrega candidatos progressivamente com `COPY`/batches;
7. classifica cada identidade sob o estado corrente da curated e aplica somente `inserted/updated/unchanged`, com predicado de frescor atômico; `stale/conflict` geram evidência em quality e não alteram curated;
8. reconcilia todas as contagens e persiste `ops.curated_promotion=COMMITTED`, `LoadResult`, `pipeline_run=CURATED_COMMITTED` e `promotion_attempt=COMMITTED` na mesma transação;
9. confirma a transação e só então retorna ao Airflow.

Falha antes do passo 9 desfaz tabela temporária, mudanças curated, rejeições inter-run e registro de commit; a tentativa pode permanecer `STARTED` e será tratada como interrompida. Depois do passo 9, o PostgreSQL já tem prova suficiente para o retry retornar o mesmo resultado. A unique da identidade protege cardinalidade; o advisory lock e o predicado de frescor protegem ordem/conteúdo. O estado do Airflow não é fonte de verdade sobre o commit.

### 12.3 Estados observáveis na fronteira de commit

| Situação | `promotion_attempt` | `curated_promotion` | Curated | Ação do retry |
|---|---|---|---|---|
| nunca iniciada | ausente | ausente | sem efeito dessa operação | executar normalmente |
| iniciada, transação ativa | `STARTED` | ausente/invisível | alteração não confirmada | advisory lock aguarda; depois reconsulta |
| interrompida antes do commit | `STARTED` sem heartbeat/fim | ausente | rollback; sem efeito confirmado | repetir com o mesmo `promotion_id` |
| commit confirmado no PostgreSQL | `COMMITTED` | `COMMITTED` + resultado | efeito integral | retornar resultado persistido, sem write de negócio |
| commit confirmado, Airflow registrou falha/retry | tentativa original `COMMITTED`; retry termina `REPLAYED` | `COMMITTED` + resultado | efeito integral | registrar somente auditoria do replay e retornar o mesmo resultado |

Não se declara sucesso pela mera presença de linhas curated, por tentativa `STARTED` ou por estado anterior do Airflow. Só o registro `COMMITTED` com fingerprint/versões idênticos, gravado junto ao efeito, confirma a promoção.

A transação única aumenta duração/uso de WAL para volume muito grande. Esse trade-off é
aceito no P0 pela garantia de promoção atômica e pela janela demonstrativa pequena. Se
benchmarks mostrarem inviabilidade, deverá ser avaliada uma work table persistente seguida
de merge curto, mediante revisão da decisão arquitetural.

## 13. Falhas e recuperação

| Falha | Estado durável | Recuperação |
|---|---|---|
| timeout/5xx durante página | `.part` da execução + logs | retry HTTP; depois retry da task recomeça/reutiliza apenas artefato íntegro |
| 400/401/422 | logs e falha Airflow | corrigir configuração/contrato; sem retry automático |
| envelope/JSON inválido | manifesto incompleto/logs | corrigir contrato/fonte e limpar/reexecutar `extract` |
| crash após `extract` | raw + manifesto completo | `validate` pode ser reexecutada |
| artefato ausente/corrompido | checksum incompatível | falha consumidor; limpar/reexecutar produtor |
| registro inválido | raw + rejeição futura | válidos seguem; revisar motivo sem editar bruto |
| crash em staging | batches confirmados + `LOADING_STAGING` | retry idempotente completa os batches |
| falha antes do commit curated | staging completa; attempt `STARTED`; curated anterior intacta | retry repete a transação com o mesmo `promotion_id` |
| falha após commit e antes do ack Airflow | `curated_promotion=COMMITTED`, resultado e curated integral | retry reconhece fingerprint e retorna resultado sem segundo efeito |
| replay comprovadamente antigo | staging/quality da nova tentativa; curated mais nova intacta | classifica `stale`, não atualiza e audita |
| versão igual/divergente ou frescor ausente/ambíguo | conflito em quality; curated intacta | corrigir/inspecionar; ordem de chegada não decide |
| duas promoções concorrentes | uma possui advisory lock; a outra aguarda | segunda reavalia estado após lock e aplica frescor atomicamente |
| reinício de container | volumes nomeados | serviços voltam; Airflow reexecuta a task necessária |
| ceiling atingido | artefato incompleto + Airflow failed | reduzir escopo ou aumentar limite com justificativa; nunca promover parcial |

O operador deverá limpar a própria task produtora e dependentes quando o artefato precisar ser reconstruído. Limpar somente `load_curated` é suficiente quando staging e transform estão íntegros.

## 14. Memória, desempenho e volume

### 14.1 Limite de memória por etapa

| Etapa | Estado máximo mantido em memória |
|---|---|
| extração | uma página (default 100 registros) + buffers de escrita |
| validação | um registro + batch de escrita |
| transformação | um registro + batch; chaves globais em SQLite |
| staging | um batch (default 1.000) |
| curated | um batch Python; conjunto completo na tabela temporária do PostgreSQL, não na aplicação |

Nenhuma função pública deve aceitar/retornar `list` do período completo. Interfaces recebem iteradores ou caminhos de artefatos.

### 14.2 Prova futura de escalabilidade

AC-18 será provado por:

1. dataset sintético gerado sob demanda, sem arquivo gigante versionado;
2. execuções com 1x, 5x e 10x páginas mantendo page/batch size;
3. contagem exata e sentinelas nas fronteiras dos batches;
4. instrumentação do maior buffer vivo, que nunca excede página + batch configurados;
5. medição em subprocesso com `tracemalloc` e RSS para demonstrar crescimento sublinear, com tolerância definida após baseline controlado;
6. interrupção injetada entre batches, que não termina em `SUCCEEDED`;
7. reexecução que preserva cardinalidade curated;
8. inspeção do XCom, que permanece referência pequena.

Não será fixado requisito de RAM/throughput antes do benchmark. A propriedade obrigatória é memória da aplicação limitada pelo batch, não pelo total acumulado.

### 14.3 Demonstração local

O perfil de demonstração usará:

- janela de um dia;
- filtro `cnpj` explícito;
- page size e ceilings exibidos nos logs;
- execução integral desse escopo;
- identificação textual de que se trata de recorte demonstrativo, não carga nacional.

### 14.4 Alternativas de processamento

| Estratégia | Vantagem | Custo/risco | Decisão |
|---|---|---|---|
| JSONL + SQLite temporário | simples, retomável, streaming, biblioteca padrão | espaço em disco e duas passagens | principal |
| PostgreSQL desde `extract` | elimina arquivos | acopla tarefas iniciais ao banco e confunde `load_staging` | rejeitada no P0 |
| lista/Pandas completa | código curto | memória proporcional e OOM | rejeitada |
| Parquet | compacto e tipado | dependência PyArrow e schema antecipado para raw | adiado |
| Spark | paralelismo distribuído | cluster/runtime desnecessário e operação maior | fora do P0 |

## 15. Stack e versões

Versões e documentação verificadas em 02/11/2026:

| Item | Versão decidida | Justificativa |
|---|---|---|
| Python | **3.13.x alvo** (`requires-python = "==3.13.*"`) | Airflow 3.3.2 testa 3.13; há imagem e constraints específicos; preferência do candidato sem incompatibilidade documental comprovada |
| Apache Airflow | 3.3.2 | versão fixada pelo desenho; testa Python 3.10–3.14 e Linux AMD64/ARM64 |
| Provider PostgreSQL | **7.0.2** | suporta Python 3.13 e é o pin do constraints oficial 3.3.2/3.13; 7.1.1 é compatível documentalmente, mas não traz necessidade P0 que justifique sair do conjunto testado |
| PostgreSQL | 16.15, imagem `postgres:16.15-bookworm` | major maduro, suportado até 2028 e testado pelo Airflow |
| Psycopg | `psycopg[binary]==3.3.5` | geração atual, binário Linux sem toolchain/libpq local e pin oficial do constraints; 3.3.6 suporta 3.13, porém fica fora do conjunto sem benefício exigido |
| HTTP | `httpx==0.28.1` | requer Python 3.8+, consta no constraints oficial e atende cliente síncrono/mock transport |
| Pytest | `pytest==9.2.0` | grupo dev; requer Python >=3.10 e declara 3.13; Airflow não inclui dependências de teste em constraints desde 3.1 |
| Ruff | `ruff==0.17.0` | grupo dev; reconhece `py313`; pin exato porque minor pode conter breaking change |
| uv | **0.13.6 como ferramenta externa** | gerenciador local/build, copiado da imagem oficial; não será dependência runtime, portanto o `uv==0.12.13` incidental do constraints Airflow não cria conflito |
| Cryptography | **46.0.3** | workaround empírico local: 48.0.0, 49.0.0 e 50.0.2 terminaram com `SIGILL` durante o import no Linux ARM64; 46.0.3 importou corretamente. A causa raiz não foi determinada e a versão possui advisories aplicáveis, aceitos apenas para esta entrega local |
| validação | biblioteca padrão | dataclasses/parsers bastam; Pydantic não agrega valor proporcional |
| DataFrame/ORM | não adotados | fluxo registro a registro e SQL PostgreSQL explícito são mais diretos |

### 15.1 Classificação da compatibilidade Python 3.13

| Verificação | Evidência documental | Classificação neste gate |
|---|---|---|
| Airflow 3.3.2 + Python 3.13 | prerequisites lista 3.13 como testado | **suporte oficial documentado** |
| Linux/Docker | Airflow suporta Linux e testa Debian Bookworm; imagens são AMD64/ARM64 | **suporte oficial documentado** |
| imagem Airflow | padrão `apache/airflow:3.3.2-python3.13` e variante slim publicados para versões suportadas; SBOM 3.3.2/3.13 existe | **disponibilidade oficial documentada; pull não executado** |
| constraints | `constraints-3.3.2/constraints-3.13.txt` existe e foi gerado para o release | **conjunto oficialmente testado disponível** |
| provider 7.0.2 | pacote declara Python 3.13 e aparece no constraints | **suporte oficial + pin testado** |
| provider 7.1.1 | requer Airflow >=2.11 e Psycopg >=3.2.9 em Python <3.14 | **compatibilidade documental; fora do constraints do release** |
| Psycopg 3.3.5 | `psycopg`/`psycopg-binary` 3.3.5 aparecem no constraints 3.13 | **pin testado pelo conjunto Airflow; runtime não testado aqui** |
| Psycopg 3.3.6 | projeto suporta Python 3.10–3.15/Linux e publica binários 3.13 | **compatibilidade documental; fora do constraints** |
| HTTPX 0.28.1 | requer Python >=3.8 e aparece no constraints | **compatibilidade documental + pin do conjunto** |
| Pytest 9.2.0 | requer Python >=3.10 e classifica 3.13 | **compatibilidade documental do grupo dev; resolução não testada** |
| Ruff 0.17.0 | `target-version` aceita `py313`; wheel/binário pré-compilado é o caminho normal | **compatibilidade documental do grupo dev; execução não testada** |
| uv 0.13.6 | Tier 1 para Python 3.13, lock/sync e imagem oficial Linux | **suporte oficial da ferramenta; resolução do projeto não testada** |
| transitivas críticas | constraints fixa common-compat 1.19.0, common-sql 2.1.1, SQLAlchemy 2.0.52, Pendulum 3.2.0, AnyIO 4.15.1, HTTP Core 1.0.9, Psycopg Binary 3.3.5 | **resolvidas no lock; Cryptography diverge conforme evidência ARM64** |

Conclusão atualizada por FND-001: Python 3.13 foi comprovado por resolução, build, imports e smoke Linux ARM64; AMD64 permanece pendente. A incompatibilidade observada durante imports com versões posteriores de Cryptography foi contornada pelo pin 46.0.3, sem causa raiz estabelecida e sem atribuí-la ao pacote. O pin não é solução de produção: advisories oficiais afetam essa versão e exigem revalidação posterior. Python 3.12 não foi necessário.

Fontes oficiais principais:

- [Airflow 3.3.2 — pré-requisitos](https://airflow.apache.org/docs/apache-airflow/3.3.2/installation/prerequisites.html)
- [Airflow 3.3.2 — instalação e constraints](https://airflow.apache.org/docs/apache-airflow/3.3.2/installation/installing-from-pypi.html)
- [Constraints Airflow 3.3.2/Python 3.13](https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.13.txt)
- [Imagens oficiais e variantes por Python](https://airflow.apache.org/docs/docker-stack/index.html)
- [Provider PostgreSQL 7.1.1 — requisitos](https://airflow.apache.org/docs/apache-airflow-providers-postgres/7.1.1/index.html)
- [Provider PostgreSQL 7.0.2 — metadados e Python suportado](https://pypi.org/project/apache-airflow-providers-postgres/7.0.2/)
- [Serviços mínimos do Airflow 3](https://airflow.apache.org/docs/apache-airflow/3.3.2/howto/run-with-systemd.html)
- [Psycopg 3 — sistemas suportados](https://www.psycopg.org/psycopg3/docs/basic/install.html)
- [HTTPX 0.28.1](https://pypi.org/project/httpx/0.28.1/)
- [Pytest — política de versões Python](https://docs.pytest.org/en/stable/backwards-compatibility.html)
- [Ruff — `target-version`](https://docs.astral.sh/ruff/settings/#target-version)
- [uv — suporte a Python](https://docs.astral.sh/uv/reference/policies/python/)
- [Política de versões PostgreSQL](https://www.postgresql.org/support/versioning/)
- [uv em Docker](https://docs.astral.sh/uv/guides/integration/docker/)
- [Lock e sync do uv](https://docs.astral.sh/uv/concepts/projects/sync/)
- [Advisories oficiais do Cryptography](https://github.com/pyca/cryptography/security/advisories)

### 15.2 Estratégia `uv`, constraints e lock

Duas alternativas oficiais/documentadas foram comparadas:

| Alternativa | Fluxo | Vantagem | Limitação | Decisão |
|---|---|---|---|---|
| A — instalação em duas fases | `uv pip install` de Airflow com `constraints-3.13.txt`, depois dependências próprias sem o constraints e com Airflow repinado | replica diretamente o fluxo recomendado pelo Airflow | divide a resolução em passos e não usa `uv.lock` como única fonte do ambiente final; exige freeze/constraints próprios para reprodução | válida como diagnóstico/fallback de resolução, não fluxo principal |
| B — projeto gerenciado pelo uv | dependências e grupo dev no `pyproject.toml`; pins críticos alinhados ao constraints; restrições transitivas em `[tool.uv].constraint-dependencies`; `uv.lock` único | mesmo lock em local, imagem e CI; sem modificação manual pós-build | não é bit a bit o arquivo integral do Airflow; exige provar que o lock final continua coerente | **escolhida** |

Regras da alternativa B:

- `pyproject.toml` é a única declaração de dependências.
- `uv.lock` é versionado e contém runtime e grupo `dev`.
- `project.requires-python` será `==3.13.*`; runtime direto pinado: Airflow 3.3.2, provider 7.0.2, Psycopg/binary 3.3.5 e HTTPX 0.28.1; dev: Pytest 9.2.0 e Ruff 0.17.0.
- `[tool.uv].constraint-dependencies` expressa somente transitivas críticas alcançadas pelo grafo: pins oficiais para common-compat, common-sql, SQLAlchemy, Pendulum, AnyIO e HTTP Core, além do pin empírico de Cryptography 46.0.3 que foi o único candidato testado com import bem-sucedido no Linux ARM64 local. Isso não estabelece a causa do `SIGILL` observado nos demais candidatos nem elimina os advisories dessa versão. Incluir constraint não instala pacote por si só.
- O arquivo integral do Airflow é baseline de comparação/auditoria, não argumento de `uv lock`. **`uv lock --constraint` não existe e não será documentado.**
- `uv lock`, `uv lock --check`, `uv sync --locked` e `uv run --locked` são os comandos do fluxo de projeto; `uv pip check` verifica requisitos instalados após o sync.
- `required-environments` exigirá resolução para Linux `x86_64` e `aarch64`, sem afirmar build multi-arch antes de executá-lo.
- Desenvolvimento/CI sincronizam runtime + dev; a imagem usa `uv sync --locked --no-dev --no-editable`.
- A imagem será construída sobre `python:3.13-slim-bookworm`, com `uv` 0.13.6 copiado da imagem oficial. A imagem Airflow `3.3.2-python3.13` existe e será referência/fallback, mas não é a base principal para evitar dois ambientes Python concorrentes e manter um único `uv.lock`.
- `PATH` apontará para `/app/.venv/bin`; todos os processos Airflow e pipeline usarão exatamente o mesmo ambiente resolvido.
- A imagem base e o binário `uv` deverão receber digest após o primeiro build multi-arquitetura validado.

O constraints oficial também contém `uv==0.12.13`, mas esse pin só afetaria um `uv` instalado como pacote do ambiente Airflow. O projeto usa o binário 0.13.6 externamente e não declara `uv` em runtime, evitando uma falsa colisão. Psycopg2/Asyncpg eventualmente trazidos como requisitos do provider não serão usados diretamente nem recebem extras adicionais.

### 15.3 Plano obrigatório de validação no gate de implementação

Estas etapas pertencem à implementação. FND-001 já executou 1–11 em Linux ARM64; build/smoke AMD64 e digest multi-arquitetura do item 12 permanecem pendentes:

1. confirmar `uv --version` e a versão GIL-enabled de `python --version` como 3.13.x;
2. criar o `pyproject.toml` conforme a decisão e executar `uv lock` em ambiente limpo;
3. comparar os pins críticos do `uv.lock` com `constraints-3.13.txt` e justificar qualquer diferença;
4. executar `uv lock --check`, `uv sync --locked` e `uv pip check`;
5. executar `uv run --locked python -c` para registrar versões de Airflow/provider/Psycopg/HTTPX e importações básicas;
6. construir a imagem Linux sem cache e repetir `python --version`, versões e importações dentro dela;
7. iniciar PostgreSQL e executar a migração do metadata DB do Airflow;
8. iniciar API server, scheduler e DAG processor e verificar health;
9. importar/listar a DAG e confirmar ausência de import errors;
10. resolver a Airflow Connection e abrir conexão real com o PostgreSQL de negócio;
11. executar smoke test controlado do fluxo, incluindo uma consulta mockada/local, sem depender do PNCP ao vivo;
12. quando disponível, repetir build/smoke em AMD64 e ARM64 e registrar digest/evidência.

Falha de resolução ou runtime deve ser reproduzida e classificada. Só se houver conflito real relevante, sem solução proporcional, que comprometa prazo ou confiabilidade, o fallback será Python 3.12. O fallback mantém Airflow 3.3.2, troca para `constraints-3.12.txt`, `python:3.12-slim-bookworm`, `requires-python ==3.12.*` e `py312`, regenera o lock e repete toda a validação; não autoriza mudar Airflow ou adicionar framework para “salvar” 3.13.

## 16. Estrutura do projeto

Estrutura planejada, sem criar módulos vazios neste gate:

```text
.
├── dags/
│   └── pncp_atas_etl.py
├── src/pncp_etl/
│   ├── config.py
│   ├── contracts.py
│   ├── observability.py
│   ├── pncp/
│   │   └── client.py
│   ├── pipeline/
│   │   ├── extract.py
│   │   ├── validate.py
│   │   ├── mapping.py
│   │   └── transform.py
│   └── storage/
│       ├── artifacts.py
│       └── postgres.py
├── sql/
│   └── migrations/
├── tests/
│   ├── unit/
│   ├── component/
│   ├── integration/
│   ├── dag/
│   ├── e2e/
│   └── fixtures/
├── docs/architecture/
├── compose.yaml
├── Dockerfile
├── pyproject.toml
└── uv.lock
```

Responsabilidades:

- `config`: parsing/validação de ambiente, Params e ceilings;
- `contracts`: dataclasses e exceções sem dependência de Airflow;
- `pncp.client`: HTTP/paginação;
- `pipeline.*`: regras puras e iterativas;
- `storage.artifacts`: JSONL/manifesto/checksum/SQLite temporário;
- `storage.postgres`: SQL parametrizado, COPY, transações e resultados;
- `observability`: contexto de logs/contagens;
- `dags`: somente composição e tradução de exceções.

Não haverá repositório genérico, service locator, factory abstrata ou camada “use case” sem necessidade concreta.

## 17. Estratégia de testes

| Grupo | Objetivo/fronteira | Dependências/fixtures | Mocks/containers | Critério de sucesso |
|---|---|---|---|---|
| unitário temporal/mapping | FR-005/006/007, AC-05/06/17 | `date`, `date-time` local, `Z`, offsets, frações 0–6/>6, nulo, inválido, ausente, extras, colisões e duplicatas | sem container; sem rede; relógio controlado | raw/lexema intacto; componentes locais e UTC corretos; sem timezone inventado nem perda silenciosa; comparação/ordenação só entre domínios compatíveis |
| componente HTTP | FR-001/002/003, AC-01–04/15/19 | envelopes de 2 páginas, 204, 200 vazio, 400/401/422/429/5xx, JSON inválido; `Retry-After` delta/data futura/passada/inválida/ausente | `httpx.MockTransport`; relógio e espera injetados | chamadas/parâmetros/tentativas exatos; permanente uma vez; parcial falha; nunca requisita antes de prazo válido e espera longa vira `RATE_LIMIT_DEFERRED` |
| artefatos/volume | FR-004/012/013, AC-12/18 | gerador sintético 1x/5x/10x, corrupção e interrupção | filesystem temporário; subprocesso | checksums/atomicidade; buffer limitado; fronteiras/contagens exatas; XCom leve |
| integração PostgreSQL | FR-008/009/013, NFR-007, AC-05/07–09/14/18 | insert/update/unchanged/stale/conflict; marcador menor/igual/ausente; janelas sobrepostas; duas conexões; falha antes e depois do commit | PostgreSQL 16.15 real em Compose; barreiras de concorrência e fault injection | rollback; unicidade; update condicional; serialização; resultado durável; retry pós-commit sem segundo efeito nem regressão |
| DAG | FR-010/011/012, AC-10–12/19 | ambiente mínimo e módulos mockados | Airflow instalado; sem PNCP | import sem erro; uma DAG; cinco IDs/arestas; Params, timeouts e retry permanente corretos |
| E2E local | AC-09/10/13/14 | fixture/servidor HTTP local e janela controlada | Compose completo + PostgreSQL real | health, pipeline completo, SQL auditável, reexecução, restart e recuperação |
| segurança/reprodução | NFR-002/004/006/012, AC-14/16 | `.env.example`, lock e árvore de arquivos | build Compose limpo | lock check, Ruff, pytest offline, ausência de segredo/privado na lista pública |
| live opt-in | verificar drift do PNCP | janela mínima e page size 10 | rede explícita; não roda por default | apenas contrato/amostra; nunca substitui mocks ou integração |

### 17.1 Falhas injetadas

- interrupção após N páginas;
- checksum inválido entre tasks;
- falha no batch N de staging;
- erro antes do commit curated;
- encerramento do processo imediatamente depois de o PostgreSQL confirmar o commit e antes do retorno ao Airflow;
- replay de artefato antigo depois de versão nova, inclusive em ordem inversa e janela sobreposta;
- duas promoções concorrentes da mesma identidade, com marcadores maior, menor, igual e ambíguo;
- `Retry-After` válido maior que o orçamento de espera inline e maior que o tempo restante da task;
- 400/401/422 no cliente e na policy da task;
- restart após staging;
- conjunto de metadados que muda entre páginas.

Os testes de fronteira do commit devem provar cinco fatos separados: rollback antes do commit; linha `curated_promotion` invisível antes do commit; `LoadResult` e efeito curated visíveis juntos após o commit; retry que recupera o resultado persistido sem modificar `updated_at`, `last_seen_at` ou contagens; e todas as tentativas encerradas coerentemente, incluindo `REPLAYED` no retry pós-commit. Os testes de frescor devem cobrir os dez cenários de §10.4/§13, inclusive timestamp igual com conteúdo divergente e ausência do marcador.

### 17.2 Comandos planejados

O README final usará somente comandos verificados. A intenção é padronizar `uv sync --locked`, `uv lock --check`, `uv run --locked pytest` e `uv run --locked ruff check .`, além de comandos Compose validados posteriormente. Nenhum resultado é alegado neste gate.

## 18. Ambiente local e Docker Compose

### 18.1 Serviços

| Serviço | Tipo | Papel |
|---|---|---|
| `postgres` | contínuo | bancos `airflow` e `pncp`; health por `pg_isready` |
| `db-init` | one-shot | cria banco/roles necessários e aplica migrations SQL versionadas |
| `airflow-init` | one-shot | executa migrações do metadata DB e configuração local inicial |
| `airflow-api-server` | contínuo | UI e API na porta 8080 |
| `airflow-scheduler` | contínuo | agenda e executa tasks no LocalExecutor |
| `airflow-dag-processor` | contínuo | parse/serialização independente das DAGs no Airflow 3 |
| `test` | profile/one-shot | suíte/lint/diagnóstico no target `test` do mesmo Dockerfile/lock; dependências dev ficam fora do target Airflow |

Não haverá Redis, Celery worker, triggerer ou serviço Python separado. Os processos Airflow usam o mesmo target final; o profile de teste deriva do mesmo Dockerfile e lock, adicionando somente o grupo dev.

### 18.2 Dependências e health

1. `postgres` precisa estar healthy;
2. `db-init` e `airflow-init` terminam com sucesso;
3. scheduler, dag processor e API server iniciam;
4. API server expõe health e UI;
5. DAG só executa quando o volume runtime é gravável e a conexão `pncp` está disponível.

Todos os serviços Airflow usam o mesmo target final Linux/Python 3.13. O profile de teste usa um target derivado do mesmo Dockerfile e `uv.lock`, adicionando apenas o grupo dev. A imagem oficial `apache/airflow:3.3.2-python3.13` permanece fallback de base, não um segundo ambiente executado em paralelo.

### 18.3 Volumes e rede

- `postgres_data`: dados dos dois bancos;
- `airflow_logs`: logs persistentes da demonstração;
- `pipeline_runtime`: artefatos JSONL/manifests/SQLite;
- DAGs/código são empacotados na imagem para reprodução; bind mount opcional fica em override de desenvolvimento, não no caminho principal.

Todos os serviços usam a rede privada do Compose. Somente UI Airflow e, se necessário para inspeção, PostgreSQL têm portas host configuráveis.

### 18.4 Configuração

`.env.example` terá placeholders para usuários/senhas locais, portas, schedule, CNPJ agendado, ceilings e Airflow secret. O `.env` real permanece ignorado. A conexão PNCP de dados será injetada por `AIRFLOW_CONN_PNCP_DB` ou backend de ambiente equivalente; a API PNCP não usa token.

A precedência será: ceilings e conexão definidos pelo ambiente (não podem ser ampliados por um run), defaults operacionais não sensíveis lidos de Airflow Variables em runtime, e Params/`dag_run.conf` usados apenas para reduzir o escopo ou escolher a janela/filtros permitidos. A DAG não consulta Variables durante import, evitando dependência do metadata DB no parse.

O Simple Auth Manager é aceitável apenas no ambiente local. Não se afirma que essa topologia é produção corporativa.

## 19. Segurança

- preservar todos os padrões atuais do `.gitignore`;
- manter `.dockerignore` protegendo `.git`, `.venv`, `private`, PDF, prompts, `.env`,
  runtime, logs e documentos internos;
- executar containers como usuário não-root;
- separar usuário do metadata DB e usuário da aplicação;
- runtime da aplicação recebe DML; DDL fica no serviço de inicialização;
- SQL sempre parametrizado; identificadores dinâmicos vêm de allowlist interna;
- validar que referências de artefato permanecem sob a raiz autorizada;
- não registrar DSN, senha, headers de autenticação ou payload integral;
- `.env.example` contém somente nomes/placeholders;
- antes de publicação: inventário Git, `check-ignore`, diff staged e secret scan;
- PDF, `private/`, prompts, dados brutos, artefatos e logs não serão publicados.

## 20. Observabilidade

O projeto usará `logging` da biblioteca padrão com mensagem curta e campos estruturados em formato `key=value` ou JSON simples, sem dependência de observabilidade distribuída.

Campos comuns:

- `execution_id`, `promotion_id`, `input_fingerprint`, `airflow_run_id`, `task_id`, `event`;
- janela e filtros não sensíveis;
- página/tentativa/status HTTP;
- `retry_not_before` e forma interpretada de `Retry-After`, sem copiar headers sensíveis;
- contagens extraídas, válidas, rejeitadas, transformadas, staged, inserted, updated, unchanged, stale e conflict;
- marcador de versão da fonte, domínio temporal e decisão de frescor, sem payload integral;
- duração em milissegundos;
- `error_class`, status e motivo seguro;
- checksum/manifesto por nome, sem conteúdo.

Eventos mínimos: `stage_started`, `page_fetched`, `stage_completed`, `record_rejected_summary`, `retry_scheduled`, `rate_limit_deferred`, `guardrail_exceeded`, `artifact_verified`, `promotion_started`, `promotion_committed`, `promotion_replayed`, `source_version_stale`, `source_version_conflict` e `stage_failed`. `promotion_committed` significa confirmação do PostgreSQL; não significa que o Airflow já registrou sucesso.

Airflow continua sendo a interface de logs/estado das tasks; PostgreSQL mantém contagens finais e staging consultável. Prometheus, Grafana e OpenTelemetry não entram no P0.

## 21. Documentação

Artefatos públicos AS BUILT:

- README operacional com objetivo, stack, pré-requisitos, configuração, Compose, Airflow, parâmetros, testes, SQL de verificação, idempotência, logs, recuperação, shutdown e limitações;
- este System Design, revisado conforme AS BUILT;
- três diagramas Mermaid;
- specifications enxutas dos sete incrementos implementados;
- type hints e docstrings Google Style em APIs públicas não óbvias;
- referência ao Swagger/OpenAPI PNCP com data de verificação.

O PRD canônico e os registros decisórios detalhados permanecem como material interno e
não integram a distribuição pública.

O repositório público não será transformado em documentação de IA. O roteiro de vídeo não pertence a este gate.

## 22. Trade-offs e alternativas

| Decisão | Benefício | Custo aceito | Alternativa rejeitada/agendada |
|---|---|---|---|
| LocalExecutor | poucos serviços, operação simples | scheduler compartilha recursos com tasks | Celery/Redis fora do P0 |
| um PostgreSQL, dois bancos | menor operação com separação lógica | falha do serviço afeta ambos | dois containers sem benefício local |
| JSONL | streaming e inspeção simples | mais espaço que Parquet | Parquet se medição justificar |
| SQLite temporário | dedup global em disco sem serviço | arquivo de trabalho adicional | set Python O(n) em RAM |
| SQL explícito/Psycopg | controle de COPY/UPSERT/transação | SQL PostgreSQL específico | ORM completo desnecessário |
| transação curated + registro de commit no mesmo commit | efeito e prova durável aparecem juntos; retry pós-commit é idempotente | WAL/duração e tabela de controle | confiar no estado Airflow ou gravar commit depois deixa janela de ambiguidade |
| lock consultivo global + predicado de frescor | concorrência simples e nenhuma regressão silenciosa comprovável | serializa promoções curated e coloca divergências ambíguas em quality | `max_active_runs=1` isolado não protege chamadas externas/concorrentes |
| Python 3.13 alvo condicionado | atende preferência e possui suporte/constraints/imagem do Airflow 3.3.2 | resolução, build e runtime ainda precisam de prova | Python 3.12 é fallback somente sob conflito relevante reproduzido |
| sem Kafka | fonte REST batch e um consumidor/destino | sem buffer/event log distribuído | reconsiderar com eventos e múltiplos consumidores |
| sem Spark | transformação registro a registro cabe em máquina com spill | sem cluster para histórico pesado | reconsiderar com benchmark, joins/agregações grandes |

## 23. Riscos e mitigação

| Risco | Probabilidade/impacto | Mitigação | Evidência futura |
|---|---|---|---|
| volume enorme mesmo em um dia | alta/alta | CNPJ agendado, preflight e ceilings | AC-15/18 |
| chave candidata colide | média/alta | validação antes do DDL; fallback condicionado; quarentena | integração + amostra opt-in |
| API muda | média/alta | contrato estrito, campos extras medidos, fixtures e data de verificação | AC-04/17 |
| fonte muda entre páginas | média/média | sequencial, invariantes, dedup e limitação explícita | teste de metadados mutáveis |
| `date-time` perde hora/offset/precisão | baixa/alta após correção | raw imutável, `SourceDateTime`, colunas local/UTC e quarentena >6 casas | testes temporais + PG real |
| replay antigo regressa curated | baixa/alta após correção | marcador global da fonte, predicado condicional, lock e conflito conservador | matriz de frescor/concorrência |
| commit ocorre e Airflow não recebe ack | média/alta | registro `COMMITTED` e resultado na mesma transação; retry consulta PostgreSQL | fault injection pós-commit |
| disco de artefatos esgota | média/alta | teto de bytes, espaço verificado e retenção | teste de guardrail |
| transação curated longa | média/média | demo pequena, COPY e benchmark; revisar work table | integração de volume |
| matriz Python 3.13/Airflow/uv conflita ou falha em runtime | média/alta | pins alinhados ao constraints 3.13, lock, build e smoke; fallback 3.12 condicionado | resolução, `uv pip check`, import DAG e E2E |
| espera longa de `Retry-After` ocupa worker ou retry antecipa prazo | baixa/média | orçamento inline; estado `RATE_LIMIT_DEFERRED`; nenhuma chamada antes do prazo | relógio controlado/mocks |
| retry permanente indevido | baixa/alta | exceção tipada + `AirflowFailException` | AC-19 |
| segredo/privado publicado | baixa/alta | ignore, `.dockerignore`, inventário e scan | AC-16 |
| limpeza remove execução ativa | baixa/alta | elegibilidade por estado terminal e caminho explícito | teste de cleanup futuro |

## 24. Decisões adiadas e pendências

### 24.1 Resolver antes da implementação

1. validar presença/unicidade/semântica de `numeroControlePNCPAta` e congelar a PK;
2. materializar o `pyproject.toml`, gerar `uv.lock` pela estratégia §15.2 e confirmar a resolução exata em Linux/arm64 e amd64;
3. executar o plano §15.3 com Python 3.13, Airflow 3.3.2, provider 7.0.2 e Psycopg 3.3.5;
4. verificar amostra controlada dos oito `date-time` sem substituir o contrato oficial: formatos realmente observados, offsets e precisão alimentam fixtures, mas não autorizam redução para `DATE`;
5. validar o mecanismo exato de Simple Auth/segredos do Compose;
6. confirmar a distribuição real de `dataAtualizacaoGlobal` em amostra limitada; o
   contrato do endpoint incremental foi validado, mas a política conservadora continua
   sem sobrescrever quando a ordem não for comprovável.

### 24.2 Hipóteses para validação empírica

- bordas inclusivas de vigência;
- vazio real 204 e/ou 200 vazio;
- rate limit/headers e comportamento 429;
- campos omitidos versus nulos;
- tamanho médio/máximo de página e artefato;
- tolerância numérica do teste de memória;
- desempenho da transação curated com volume sintético.
- distribuição real dos domínios temporais (`LOCAL_UNZONED` versus `OFFSET_AWARE`) e da precisão fracionária;
- disponibilidade operacional dos tags/digests exatos em AMD64 e ARM64 no momento do build.

### 24.3 Deliberadamente adiadas

- page size acima de 100 e batch diferente de 1.000;
- digest multi-arquitetura das imagens;
- hash curated e cursor incremental de alta precisão; o modo por janela civil já está
  implementado com overlap configurável;
- checkpoint por página reutilizável entre DAG runs;
- Parquet/work table persistente;
- endpoint complementar para fornecedor/valores/processo.

### 24.4 Melhorias opcionais

- scan de vulnerabilidade/SBOM;
- cobertura de testes como métrica informativa;
- comando de limpeza assistida de artefatos;
- índices adicionais comprovados por `EXPLAIN`.

## 25. Matriz de rastreabilidade PRD → arquitetura

### 25.1 Requisitos funcionais

| Requisito | Cobertura arquitetural | Evidência futura |
|---|---|---|
| FR-001 | §§7–8, cliente/configuração | parâmetros HTTP mockados |
| FR-002 | §§8.2–8.4, 14 | paginação, vazio, ceilings e completude |
| FR-003 | §§7.3, 8.3–8.5, 13 | retries, `Retry-After` com relógio controlado e AC-19 |
| FR-004 | §§6, 9.2 | raw/manifests e vínculo staging |
| FR-005 | §§9.1, 10.1–10.2 | unitários temporais + rejeição em PG |
| FR-006 | §§10.3–10.5, 11 | duplicatas e conflitos de versão determinísticos |
| FR-007 | §§9.1, 10 | AC-17; mapping temporal sem truncamento |
| FR-008 | §§9.2, 12.1 | consultas staging/quality por execução |
| FR-009 | §§9.2–9.4, 10.4, 11–12 | UPSERT condicional, commit durável, rollback e concorrência em PG real |
| FR-010 | §7.2, §16 | import e grafo das cinco tasks |
| FR-011 | §7.1, §18.4 | execução manual/agendada e config |
| FR-012 | §§6.1, 7.4 | XCom pequeno e checksum |
| FR-013 | §§11–13 | falha antes/depois do commit, retry e replay antigo |
| FR-014 | §§10.5, 12, 20 | logs, promoção e contagens reconciliadas |

### 25.2 Requisitos não funcionais

| Requisito | Cobertura arquitetural | Evidência futura |
|---|---|---|
| NFR-001 | §§5, 16, 21 | estrutura, type hints, docstrings e Ruff |
| NFR-002 | §§15.2–15.3 | `uv.lock`, check/sync/run locked e comparação com constraints |
| NFR-003 | §§15.1–15.3, 18 | build e smoke da matriz Python 3.13/Linux |
| NFR-004 | §18 | Compose limpo, health e restart |
| NFR-005 | §17 | suíte offline + PG real + falhas/concorrência controladas |
| NFR-006 | §§18.4, 19 | configuração e scan |
| NFR-007 | §§10.4, 11–13 | transação, commit durável, frescor atômico e SQL parametrizado |
| NFR-008 | §§6, 8.2, 14 | AC-18 e guardrails |
| NFR-009 | §§8–10, 15 | OpenAPI/contrato temporal e fontes oficiais de compatibilidade |
| NFR-010 | §§4, 21 | README futuro + diagramas |
| NFR-011 | §3 e §21 | compromisso final preservado; fora do gate arquitetural |
| NFR-012 | §19 | inventário público/privado |

### 25.3 Critérios de aceite

| AC | Elemento de design | Gate de evidência |
|---|---|---|
| AC-01 | paginação sequencial e manifesto (§8.4) | componente HTTP |
| AC-02 | vazio inicial válido (§8.4) | componente HTTP |
| AC-03 | retries transitórios e `Retry-After` sem antecipação (§8.3/8.5) | componente HTTP com relógio controlado |
| AC-04 | contrato/lote falha (§8.5) | componente HTTP |
| AC-05 | quarentena ligada ao raw (§9.2/10) | unitário + PG |
| AC-06 | SQLite/dedup e ordenação temporal segura (§10.3) | unitário/componente |
| AC-07 | UPSERT nula-seguro e condicionado por frescor (§10.4/11) | PG real |
| AC-08 | atualização comprovadamente mais nova; stale/conflito não regressa (§10.4/11) | PG real |
| AC-09 | staging, promoção durável e retry pós-commit (§12–13) | integração/DAG com fault injection |
| AC-10 | agenda/manual (§7.1) | smoke/E2E |
| AC-11 | cinco tasks (§7.2) | teste DAG |
| AC-12 | `ArtifactRef` < 4 KiB (§7.4) | teste DAG/componente |
| AC-13 | volumes nomeados (§18.3) | E2E restart |
| AC-14 | `uv.lock` + offline + PG real (§15/17) | suíte/build limpo |
| AC-15 | preflight/ceiling (§8.2) | componente |
| AC-16 | proteção/scan (§19) | gate de publicação |
| AC-17 | mapping temporal explícito, raw/local/UTC (§9.1) | unitário + PG |
| AC-18 | streaming/SQLite/COPY (§14) | componente + integração |
| AC-19 | permanente sem retry (§7.3/8.5) | HTTP + DAG |
| AC-20 | obrigação final sem roteiro agora (§3/21) | gate final de entrega |

### 25.4 Seção 7 do PDF

| Item oficial | Cobertura de design |
|---|---|
| unitários de transformação/validação | §17, grupo unitário |
| paginação/tratamento de respostas | §17, componente HTTP |
| idempotência PostgreSQL | §17, integração PostgreSQL |
| vazios/incompletos/HTTP | §§8.4–8.5 e testes |
| mocks sem API pública | `httpx.MockTransport`; live separado |

### 25.5 Seção 8 do PDF

| Entregável oficial | Preparação arquitetural |
|---|---|
| código modular + DAG | §§7 e 16 |
| Docker Compose/configuração | §18 |
| scripts SQL | §§9/12; criação adiada ao gate autorizado |
| testes automatizados | §17 |
| dependências | §15 e `uv` |
| README | §21; criação final adiada |
| diagrama | três Mermaid coerentes em `docs/architecture/diagrams/` |

## 26. Verificação histórica do gate de design

- [x] 14 FRs, 12 NFRs e 20 ACs possuem destino arquitetural.
- [x] Seções 7 e 8 do PDF estão mapeadas.
- [x] A DAG mantém exatamente cinco tarefas de negócio visíveis.
- [x] Nenhuma etapa acumula o dataset completo em memória.
- [x] Idempotência, frescor, atomicidade, concorrência, falha parcial e recuperação pós-commit têm limites explícitos.
- [x] XCom contém somente referências/metadados pequenos.
- [x] Stack/compatibilidade foram verificadas em fontes oficiais.
- [x] Diagramas e decisões arquiteturais correspondem ao texto.
- [x] No gate de design, ainda não havia código; a implementação posterior preservou as
  decisões arquiteturais relevantes.
- [x] A aprovação independente foi registrada antes do início da implementação.

### 26.1 Correção menor pós-re-review

- **RR-SD-LOW-001 — incorporado:** uma tentativa que reconhece `ops.curated_promotion=COMMITTED` agora termina explicitamente como `REPLAYED`, com `finished_at`, sem repetir COPY/UPSERT ou alterar timestamps e contagens de negócio.
- **Resultado independente registrado:** `APPROVED WITH MINOR CORRECTIONS`.

**Status final:** `APPROVED WITH MINOR CORRECTIONS — correction incorporated`.

**Estado posterior:** implementação test-first concluída; operação e limitações estão no
`README.md`.
