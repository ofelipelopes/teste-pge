# Diagrama de componentes

> Estado: atualizado para o ambiente AS BUILT validado.

```mermaid
flowchart LR
    operator[Operador ou avaliador]
    pncp[API pública PNCP<br/>GET /v1/atas<br/>GET /v1/atas/atualizacao opcional]

    subgraph compose[Docker Compose local]
        subgraph airflow[Apache Airflow 3.3.2]
            api[API Server e UI]
            scheduler[Scheduler<br/>LocalExecutor]
            processor[DAG Processor]
            code[Pacote Python 3.13 + DAG<br/>imagem única com uv.lock]
        end

        runtime[(Volume pipeline_runtime<br/>JSONL, manifestos e SQLite)]
        logs[(Volume airflow_logs)]

        subgraph postgres[PostgreSQL 16.15]
            metadata[(Banco airflow<br/>metadados)]
            pipeline[(Banco pncp<br/>ops + promotion commit<br/>staging, quality, curated)]
        end
    end

    operator -->|UI, API e execução manual| api
    api <--> metadata
    scheduler <--> metadata
    processor <--> metadata
    processor --> code
    scheduler --> code
    scheduler -->|requests sequenciais| pncp
    scheduler <--> runtime
    scheduler --> pipeline
    scheduler --> logs
    api --> logs
```

Decisões expressas:

- as tasks rodam em processos locais do scheduler, não em workers Celery;
- o dado intermediário passa pelo volume, não por XCom;
- metadados Airflow e dados de negócio usam bancos distintos no mesmo serviço PostgreSQL;
- os processos Airflow usam o mesmo ambiente Python 3.13 travado, validado em Linux ARM64;
- PostgreSQL, não o estado do Airflow, confirma atomicamente promoção, resultado e versão aceita da fonte;
- não há Redis, Kafka, Spark, triggerer ou serviço Python separado no P0.
