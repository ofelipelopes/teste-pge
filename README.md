# ETL de Atas de Registro de Preços do PNCP

Pipeline local que extrai Atas de Registro de Preços da API pública do PNCP, valida e
normaliza os registros e os persiste em camadas `staging`, `quality` e `curated` no
PostgreSQL. O Apache Airflow orquestra as cinco etapas obrigatórias:

`extract → validate → transform → load_staging → load_curated`

O escopo implementado cobre o P0 do desafio: paginação, falhas HTTP, quarentena,
artefatos incrementais, lotes, memória limitada, promoção transacional, idempotência,
recuperação, métricas e testes offline. A suíte não depende da disponibilidade da API
pública. Dois diferenciais P1 de baixo custo reutilizam essa base: comparação entre
coletas e consulta opcional por atualização global.

## Arquitetura

1. `extract` percorre `/v1/atas`, grava JSONL bruto imutável e publica manifesto atômico
   com checksum.
2. `validate` preserva registros válidos e rejeições com motivo, sem carregar o período
   inteiro em memória.
3. `transform` aplica mapping explícito, preserva a semântica temporal e deduplica com
   SQLite scratch reconstruível.
4. `load_staging` grava raw, rejeições e candidatos em lotes configuráveis.
5. `load_curated` usa lock consultivo e uma transação PostgreSQL para aplicar regras de
   frescor, registrar a promoção durável e evitar replay obsoleto.

As regras ficam nos módulos Python; a DAG apenas resolve configuração e orquestra. XCom
transporta referências pequenas, não os dados. Consulte o
[System Design](docs/architecture/system-design.md) e os
[diagramas](docs/architecture/diagrams/componentes.md) para o detalhamento.

Tecnologias: Python 3.13, Apache Airflow 3.3.2, PostgreSQL 16, Docker Compose, uv,
HTTPX, Psycopg, SQLite, Pytest e Ruff.

## Pré-requisitos e configuração

- Docker Engine/Desktop com Compose v2;
- aproximadamente 4 GB livres para imagens, volumes e artefatos de demonstração;
- opcional para execução no host: Python 3.13 e uv 0.13.6.

Crie a configuração local e preencha todos os segredos que estão vazios. O arquivo
`.env` é ignorado pelo Git e não deve ser publicado.

```bash
cp .env.example .env
```

Antes da primeira inicialização, gere valores independentes para
`POSTGRES_SUPERUSER_PASSWORD`, `AIRFLOW_DB_PASSWORD`, `AIRFLOW_ADMIN_PASSWORD`,
`AIRFLOW_JWT_SECRET` e `PNCP_DB_PASSWORD`. Uma opção é executar
`openssl rand -hex 24` uma vez para cada variável e copiar cada resultado para o `.env`;
um gerenciador de senhas também pode ser usado. Valores hexadecimais evitam a necessidade
de escapar caracteres nas URLs de conexão. Não imprima nem compartilhe o arquivo.

Não existem senhas padrão no código ou no Compose. As cinco variáveis acima são
obrigatórias: `docker compose config --quiet` falha antes de criar containers quando
alguma delas está ausente ou vazia. Nomes de bancos e usuários possuem defaults locais,
mas não são segredos.

### Connections, Variables e Params do Airflow

O Compose converte a configuração privada do `.env` nas abstrações oficiais do Airflow:

- a Connection `pncp_db` é fornecida por `AIRFLOW_CONN_PNCP_DB`; a DAG a resolve em
  runtime com `BaseHook.get_connection("pncp_db")` e nunca contém uma DSN ou senha padrão;
- as Variables `pncp_api_url`, `pncp_update_api_url`, `pncp_schedule_cnpj`,
  `pncp_schedule_query_mode`, `pncp_incremental_lookback_days` e `pncp_batch_size` são
  fornecidas por `AIRFLOW_VAR_*` e lidas com `Variable.get(...)` dentro das tasks;
- `data_inicial`, `data_final`, `cnpj`, `codigo_unidade`, `permitir_sem_cnpj` e
  `modo_consulta` são Params de cada DagRun e podem variar por execução;
- o schedule, o diretório de runtime e os ceilings máximos permanecem no ambiente do
  container: o schedule precisa existir durante o parse da DAG e uma DagRun não pode
  ampliar os limites de segurança.

Connections e Variables fornecidas pelo backend de ambiente são resolvidas pelo Airflow,
mas não aparecem nas telas de administração porque não são copiadas para o metadata
database. Isso evita um bootstrap manual pela interface e mantém a execução reproduzível.
Os valores sensíveis permanecem somente no `.env` privado; a Connection não é enviada por
XCom nem registrada nos logs.

`PNCP_SCHEDULE_CNPJ` pode permanecer vazio somente enquanto a DAG estiver pausada. No
Airflow 3, uma DagRun manual disparada nesse estado permanece em `queued`; portanto,
preencha um CNPJ de 14 dígitos antes de remover a pausa, mesmo que a execução manual
informe seu próprio CNPJ no JSON. URLs e portas podem manter os valores do template
quando não houver conflito local.

Os ceilings `PNCP_MAX_PAGES`, `PNCP_MAX_RECORDS`, `PNCP_MAX_DURATION_SECONDS` e
`PNCP_MAX_ARTIFACT_BYTES` limitam uma extração; `PNCP_PAGE_SIZE` e `PNCP_BATCH_SIZE`
controlam páginas e cargas. `PNCP_SCHEDULE_QUERY_MODE` aceita `vigencia` (padrão) ou
`atualizacao`; `PNCP_INCREMENTAL_LOOKBACK_DAYS` aceita de 1 a 7 dias inclusivos e vale
somente para agenda incremental. Valores inválidos são rejeitados antes da rede.

Para a primeira consulta real controlada, use no `.env`:

```dotenv
PNCP_PAGE_SIZE=10
PNCP_MAX_PAGES=1
PNCP_MAX_RECORDS=10
PNCP_MAX_DURATION_SECONDS=180
PNCP_MAX_ARTIFACT_BYTES=1048576
PNCP_BATCH_SIZE=10
```

Esses limites não são necessários para a suíte offline, mas impedem que uma demonstração
consuma um período extenso. A paginação com várias páginas é coberta pelos testes; amplie
os ceilings somente quando a consulta real exigir isso conscientemente.

Os ceilings são guardrails: se a resposta exigir mais páginas, registros, tempo ou bytes,
a execução falha explicitamente e não publica um artefato truncado como completo. Se o
`.env` for alterado após a criação dos containers, aplique a nova configuração com
`docker compose up -d --wait --wait-timeout 180 --force-recreate`.

Defina as senhas antes do primeiro `docker compose up`. O PostgreSQL preserva as
credenciais no volume `postgres_data`; editar o `.env` depois não rotaciona a senha já
gravada. Se isso acontecer, restaure o valor anterior ou altere a senha no banco. Remover
o volume reinicializa tudo, mas também apaga os dados e só deve ser feito em ambiente
descartável.

## Iniciar e verificar o ambiente

```bash
docker compose config --quiet
docker compose up -d --wait --wait-timeout 180 --build
docker compose ps
curl -fsS http://localhost:8080/api/v2/monitor/health
docker compose exec -T airflow-scheduler airflow dags list-import-errors --output json
```

`postgres`, `airflow-api-server`, `airflow-scheduler` e `airflow-dag-processor` devem
aparecer saudáveis; os dois serviços de inicialização devem terminar com código zero. O
último comando deve retornar `[]`. A interface fica em
[http://localhost:8080](http://localhost:8080). Use o usuário `admin` e o valor de
`AIRFLOW_ADMIN_PASSWORD` definido no `.env`. Por padrão, o Airflow publica a porta `8080`
e o PostgreSQL a `55432` no host; ambas podem ser alteradas no `.env`.

Para acompanhar ou diagnosticar os serviços sem expor o `.env`:

```bash
docker compose logs --tail=100 airflow-scheduler airflow-dag-processor
docker compose restart airflow-scheduler airflow-dag-processor
docker compose ps
```

O restart não recria volumes. Depois dele, aguarde os healthchecks antes de disparar uma
nova execução.

A DAG nasce pausada por segurança. Após configurar `PNCP_SCHEDULE_CNPJ`, habilite-a
pela interface ou por:

```bash
docker compose exec -T airflow-scheduler airflow dags unpause --yes pncp_atas_etl
```

O cron padrão é `0 6 * * *` em `America/Fortaleza`; em `vigencia`, a agenda consulta o
dia civil anterior. Em `atualizacao`, termina ontem e relê dois dias por padrão para
absorver sobreposição com segurança. `catchup` está desligado e apenas uma execução fica
ativa por vez.

## Executar uma janela manual

Use uma janela pequena e um CNPJ conhecido. O intervalo inclusivo máximo é de sete dias.
Este comando consulta a API real e, por isso, deve ser disparado conscientemente:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger pncp_atas_etl --conf '{"data_inicial":"2026-10-01","data_final":"2026-10-01","cnpj":"00000000000000"}'
```

Substitua o CNPJ de exemplo. Uma consulta nacional exige o parâmetro explícito
`"permitir_sem_cnpj":true`; ela continua sujeita aos ceilings, mas não é recomendada
para a primeira demonstração. `codigo_unidade` é opcional.

### Modo incremental por atualização global

O [Swagger oficial](https://pncp.gov.br/api/consulta/swagger-ui/index.html) define
`/v1/atas/atualizacao` com `dataInicial`, `dataFinal`, filtros e paginação equivalentes,
mas por data de atualização global. O
[comunicado oficial](https://www.gov.br/pncp/pt-br/central-de-conteudo/comunicados/2025/no-01-2025-implantacao-da-consulta-as-atas-de-registro-de-precos-por-data-de-atualizacao-no-pncp)
confirma o uso incremental por intervalo.

Para uma execução manual pequena, acrescente o modo explícito à mesma DAG:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger pncp_atas_etl --conf '{"data_inicial":"2026-10-31","data_final":"2026-11-01","cnpj":"00000000000000","modo_consulta":"atualizacao"}'
```

Para agenda incremental, configure `PNCP_SCHEDULE_QUERY_MODE=atualizacao` e mantenha
`PNCP_INCREMENTAL_LOOKBACK_DAYS=2` no `.env`, depois recrie os serviços. A sobreposição
é intencional: staging preserva cada coleta e a curated idempotente só aceita conteúdo
com frescor comprovadamente maior. Recuperações mais antigas continuam sendo execuções
manuais por data.

Uma validação real controlada foi executada em 03/11/2026 para a janela de 02/11/2026,
com o CNPJ `00508903000188`, página de 10 e ceilings de 1 página/10 registros/180 s/1 MiB.
A API informou e entregou 1 registro. A carga inicial inseriu a ata e reexecuções da mesma
janela resultaram em `unchanged=1`, sem rejeições e com uma única linha em curated. Uma
das extrações enfrentou latência/retry e terminou em cerca de 151 s, justificando o teto
de 180 s para o smoke. Essa é evidência pontual de integração, não garantia de
disponibilidade futura da fonte.

Os logs e estados das cinco tasks ficam na interface do Airflow. Os artefatos ficam no
volume `pipeline_runtime`; o PostgreSQL usa o volume `postgres_data`.

## Recuperar um período

Há quatro mecanismos distintos:

- **Retry de requisição:** o cliente repete apenas falhas HTTP/transporte transitórias e
  respeita `Retry-After` válido.
- **Retry de tarefa:** Airflow repete `extract`, `load_staging` ou `load_curated` sem
  republicar artefato válido nem duplicar a promoção.
- **Reexecução da DAG:** uma nova execução da mesma janela reconcilia `curated` pela
  chave de negócio e pelo frescor da origem.
- **reprocessamento de um período histórico:** identifique a janela no Airflow ou em
  `ops.pipeline_run` e dispare a DAG manualmente com as mesmas datas.

Falhas antes do commit são revertidas. Se o worker perder a confirmação depois do
commit, o retry encontra `ops.curated_promotion` como `COMMITTED` e retorna o resultado
durável como replay. Registros mais antigos não sobrescrevem conteúdo novo.

## Consultar dados, métricas e qualidade

Os comandos abaixo usam os nomes padrão de banco e superusuário. Ajuste-os caso tenha
alterado `PNCP_DB_NAME` ou `POSTGRES_SUPERUSER`.

O banco do pipeline é separado do banco de metadados do Airflow. `staging.ata_raw`
preserva o JSON recebido e sua posição; `staging.ata_candidate` guarda os candidatos
canônicos por execução; `quality.ata_rejection` registra descartes e conflitos;
`curated.ata` mantém uma linha por `numeroControlePNCPAta`; e `ops` guarda a execução,
tentativas e promoções.

```bash
docker compose exec -T postgres psql -U postgres -d pncp -c "SELECT count(*) AS atas_curated FROM curated.ata;"
docker compose exec -T postgres psql -U postgres -d pncp -c "SELECT numero_controle_pncp_ata, numero_ata_registro_preco, ano_ata, cnpj_orgao, nome_orgao, vigencia_inicio_local, vigencia_fim_local, data_atualizacao_global_local, last_seen_execution_id FROM curated.ata ORDER BY updated_at DESC LIMIT 10;"
docker compose exec -T postgres psql -U postgres -d pncp -c "SELECT execution_id, page_number, record_index, collected_at, business_key, validation_status, payload FROM staging.ata_raw ORDER BY raw_id DESC LIMIT 5;"
docker compose exec -T postgres psql -U postgres -d pncp -c "SELECT execution_id, business_key, canonical_payload FROM staging.ata_candidate ORDER BY execution_id DESC, page_number, record_index LIMIT 5;"
docker compose exec -T postgres psql -U postgres -d pncp -c "SELECT execution_id, state, window_start, window_end, metrics, started_at, staging_completed_at, curated_committed_at FROM ops.pipeline_run ORDER BY started_at DESC LIMIT 10;"
docker compose exec -T postgres psql -U postgres -d pncp -c "SELECT promotion_id, execution_id, load_result, committed_at FROM ops.curated_promotion ORDER BY committed_at DESC LIMIT 10;"
docker compose exec -T postgres psql -U postgres -d pncp -c "SELECT execution_id, stage, business_key, reason_code, detail, created_at FROM quality.ata_rejection ORDER BY created_at DESC LIMIT 10;"
```

Uma consulta vazia em `quality.ata_rejection` significa apenas que as execuções observadas
não produziram rejeições; não indica erro. O JSON de `staging` é evidência bruta e pode
conter campos opcionais nulos, enquanto as colunas de `curated` refletem o mapping
normalizado.

Os manifestos guardam contagens de entrada, válidos, rejeitados, deduplicados e
conflitos. `ops.pipeline_run.metrics` guarda reconciliação de staging e promoção;
`load_result` informa inseridos, atualizados, sem alteração, obsoletos e conflitos. Os
logs estruturados `pipeline_stage_complete` registram duração e volume por task; a
duração e o resultado final da DAG também ficam nos metadados do Airflow.

Para demonstrar idempotência, anote a contagem de `curated.ata`, repita a mesma janela e
consulte novamente. A nova coleta permanece auditável em staging, enquanto atas sem
mudança não são duplicadas em curated.

## Comparar duas versões coletadas

Localize duas execuções que observaram a mesma chave:

```bash
docker compose exec -T postgres psql -U postgres -d pncp -c "SELECT candidate.business_key, candidate.execution_id, run.airflow_run_id, run.started_at FROM staging.ata_candidate AS candidate JOIN ops.pipeline_run AS run USING (execution_id) WHERE candidate.business_key = 'NUMERO_CONTROLE_PNCP_ATA' ORDER BY run.started_at;"
```

Compare explicitamente os dois `execution_id` encontrados:

```bash
docker compose exec -T airflow-scheduler python -m pncp_etl.versioning 'NUMERO_CONTROLE_PNCP_ATA' 'UUID_EXECUCAO_ESQUERDA' 'UUID_EXECUCAO_DIREITA'
```

O JSON devolve proveniência, valores diferentes por campo e um dos estados `UNCHANGED`,
`CHANGED` ou `TEMPORAL_CONFLICT`. `newer_execution_id` só é preenchido quando os dois
valores de `dataAtualizacaoGlobal` são comparáveis e diferentes; horário da coleta e
ordem dos argumentos nunca elegem uma versão mais nova.

## Testes e qualidade

A forma mais reproduzível executa Python 3.13, PostgreSQL real, testes offline e Ruff na
imagem Linux:

```bash
docker compose --profile test build test
docker compose --profile test run --rm test
```

No host, com Python 3.13 e uv 0.13.6:

```bash
uv lock --check
uv sync --locked
AIRFLOW_HOME=/tmp/pncp-airflow-test uv run --locked pytest
uv run --locked ruff check .
```

HTTP é simulado nos testes normais. Os testes de integração usam PostgreSQL real e
cobrem batches, rollback, perda de confirmação pós-commit, replay, janela sobreposta,
concorrência e proteção por frescor. O teste sintético aumenta o volume em 10 vezes e
verifica que o pico de memória continua sublinear e abaixo do teto de teste.

## Limitações e pressupostos

- `/v1/atas` filtra por **vigência**; `/v1/atas/atualizacao` filtra por **atualização**
  global. O modo precisa ser explícito e fica na proveniência; trocar o endpoint não muda
  retrospectivamente a semântica de uma execução.
- O incremental usa datas civis e sobreposição, não cursor de alta precisão. Atualizações
  anteriores ao lookback exigem reprocessamento manual; a suíte normal permanece offline
  e o smoke real pontual não comprova disponibilidade contínua do PNCP.
- A paginação não oferece snapshot documentado. Mudanças na fonte durante a navegação
  podem deslocar registros entre páginas; checksums e reconciliação detectam artefatos
  inconsistentes localmente, não criam isolamento na API.
- Rate limits formais não são publicados no contrato consultado. O cliente aplica retry
  limitado, backoff com jitter e `Retry-After`, sem alegar uma cota específica.
- Date-times com offset preservam o valor local e a projeção UTC. Valores sem offset
  permanecem locais, sem timezone inventado; precisão acima de seis casas é rejeitada.
  Embora o OpenAPI marque esses campos como `date-time`, a resposta real observada usou
  `YYYY-MM-DD`: o raw preserva esse lexema e a projeção local usa meia-noite, com UTC nulo.
- Campos opcionais podem ser nulos. Identidade ausente, tipos inválidos e datas inválidas
  vão para quarentena; uma página fora do contrato interrompe a extração.
- A idempotência usa `numeroControlePNCPAta`, versão do mapping, fingerprint e frescor.
  Timestamp igual com conteúdo divergente ou frescor ausente vira conflito auditável;
  replay obsoleto não atualiza curated.
- A demonstração é local, usa `LocalExecutor`, uma execução ativa e lock consultivo. Os
  ceilings protegem disco/memória, mas não caracterizam escala distribuída.
- `cryptography==46.0.3` é uma dependência transitiva do Airflow e permanece fixada porque
  foi o workaround que importou corretamente no Linux ARM64 local; versões posteriores
  testadas terminaram com `SIGILL`, mas a causa raiz não foi determinada. A auditoria
  direcionada encontrou sete avisos aplicáveis (GHSA-r6ph-v2qm-q3c2,
  GHSA-m959-cc7f-wv43, GHSA-p423-j2cm-9vmq, GHSA-537c-gmf6-5ccf,
  GHSA-jwv3-5hgf-82ww, GHSA-m2h6-j472-rp4c e GHSA-g6cj-pr64-35w5). O código do pipeline
  não importa `cryptography` diretamente, porém o risco transitivo permanece e uma versão
  corrigida precisa ser revalidada antes de uso em produção.

## Diferenciais

| Diferencial | Prioridade | Evidência atual |
|---|---|---|
| Recuperação de períodos | P0 | parâmetros manuais, replay e E2E idempotente |
| Métricas | P0 | logs, Airflow, manifestos e tabelas `ops` |
| Carga em lotes | P0 | lote configurável, testes de fronteira/falha e memória |
| Qualidade de código | P0 | uv lockado, Pytest, Ruff, tipos e separação de módulos |
| Limitações/pressupostos | P0 | esta seção alinhada ao comportamento testado |
| Comparação entre versões | P1 | CLI JSON somente leitura, diff, proveniência e conflito temporal testados |
| Incremental por atualização | P1 | mesma DAG; smoke real com 2 DagRuns, 1 inserção e 1 registro sem alteração |

Os P1 são opcionais e não alteram os critérios nem o modo padrão do pipeline P0.

## Encerrar

```bash
docker compose down
```

Esse comando preserva os volumes. Removê-los apaga banco, logs e artefatos e deve ser
uma decisão explícita do operador. Somente quando a perda de todos os dados locais estiver
confirmada, use:

```bash
docker compose down --volumes
```

Esse segundo comando é destrutivo e não tem recuperação automática pelo projeto.
