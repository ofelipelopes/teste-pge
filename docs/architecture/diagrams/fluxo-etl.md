# Diagrama do fluxo ETL

> Estado: atualizado para o fluxo AS BUILT validado.

```mermaid
flowchart TD
    params[Janela, modo, filtros e ceilings]
    source[PNCP /v1/atas<br/>ou /v1/atas/atualizacao]

    subgraph dag[DAG pncp_atas_etl]
        extract[extract]
        validate[validate]
        transform[transform]
        loadStaging[load_staging]
        loadCurated[load_curated]
    end

    raw[(raw.jsonl<br/>manifesto completo)]
    checked[(valid.jsonl<br/>rejected.jsonl)]
    index[(dedup-index.sqlite<br/>spill em disco)]
    ready[(curated.jsonl<br/>fingerprint e versões da fonte)]
    staging[(staging.ata_raw)]
    quality[(quality.ata_rejection)]
    run[(ops.pipeline_run)]
    promotion[(ops.curated_promotion<br/>COMMITTED + LoadResult)]
    curated[(curated.ata)]

    params --> extract
    source -->|página por página| extract
    extract --> raw
    raw --> validate
    validate --> checked
    checked --> transform
    transform <--> index
    transform --> ready
    raw --> loadStaging
    checked --> loadStaging
    ready --> loadStaging
    loadStaging --> staging
    loadStaging --> quality
    loadStaging -->|STAGING_COMPLETE| run
    loadStaging --> loadCurated
    ready --> loadCurated
    promotion -->|retry consulta antes de escrever| loadCurated
    loadCurated -->|advisory lock + frescor atômico<br/>insert, update ou unchanged| curated
    loadCurated -->|stale ou conflito ambíguo| quality
    loadCurated -->|mesma transação| promotion
    loadCurated -->|CURATED_COMMITTED| run
```

Cada seta entre tasks representa somente uma referência pequena em XCom. Os arquivos permanecem no volume compartilhado e são verificados por manifesto/checksum. O commit final torna visíveis juntos os efeitos curated/quality, `LoadResult`, `promotion=COMMITTED` e `pipeline_run=CURATED_COMMITTED`; o estado `SUCCEEDED` pertence posteriormente ao Airflow.
