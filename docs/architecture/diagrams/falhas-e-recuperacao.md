# Diagrama de falhas e recuperação

> Estado: arquitetura aprovada. O diagrama mostra o limite de sucesso, não comandos operacionais.

```mermaid
flowchart TD
    start[Task inicia] --> kind{Tipo de falha?}

    kind -->|timeout, transporte, 429 ou 5xx| httpRetry[Retry HTTP finito]
    httpRetry -->|Retry-After maior que orçamento| deferred[RATE_LIMIT_DEFERRED<br/>gravar retry_not_before e liberar worker]
    deferred --> failed[Airflow FAILED<br/>nova execução somente após o prazo]
    httpRetry -->|recuperou| resume[Continuar página ou batch]
    httpRetry -->|esgotou| taskRetry[Uma task retry quando configurada]
    taskRetry -->|recuperou| resume
    taskRetry -->|esgotou| failed

    kind -->|400, 401, 422 ou contrato inválido| permanent[AirflowFailException<br/>sem retry da task]
    permanent --> failed

    kind -->|registro individual inválido| reject[Preservar raw<br/>registrar motivo]
    reject --> resume

    kind -->|artefato ausente ou checksum inválido| rebuild[Reexecutar a task produtora]
    rebuild --> failed

    kind -->|falha durante staging| partial[Estado LOADING_STAGING<br/>batches podem existir]
    partial --> stagingRetry[Retry idempotente<br/>ON CONFLICT]
    stagingRetry --> stagingComplete[STAGING_COMPLETE]

    kind -->|falha antes do commit curated| rollback[ROLLBACK integral<br/>sem promotion COMMITTED]
    rollback --> curatedRetry[Retry com mesmo promotion_id]
    curatedRetry --> lock[Advisory lock<br/>aguardar e reconsultar]
    lock --> committed{promotion_id já<br/>COMMITTED?}
    committed -->|sim, fingerprint igual| replay[Marcar tentativa REPLAYED<br/>recuperar LoadResult sem write de negócio]
    committed -->|não| classify{Versão da fonte}
    classify -->|maior comparável| apply[Insert ou update condicionado]
    classify -->|igual e mesmo conteúdo| unchanged[Unchanged]
    classify -->|menor| stale[Stale<br/>curated intacta]
    classify -->|igual divergente ou ambígua| conflict[Conflict em quality<br/>curated intacta]
    apply --> commit[Mesmo commit:<br/>curated + quality + LoadResult<br/>+ promotion COMMITTED]
    unchanged --> commit
    stale --> commit
    conflict --> commit
    kind -->|falha após commit e antes do ack| postCommit[PostgreSQL permanece<br/>CURATED_COMMITTED]
    postCommit --> curatedRetry

    resume --> next[Próxima etapa]
    stagingComplete --> next
    commit --> ack[Retorno ao Airflow]
    replay --> ack
    ack --> success[Airflow SUCCEEDED]
```

O commit de PostgreSQL produz `CURATED_COMMITTED`, não `SUCCEEDED`. Só o retorno posterior permite ao Airflow marcar sucesso; se essa comunicação falhar, o retry recupera o resultado durável. Staging parcial, tentativa `STARTED` e artefato incompleto nunca confirmam promoção.
