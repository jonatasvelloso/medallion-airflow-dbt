-- Maior tabela da plataforma: incremental, processa só o que mudou desde a
-- última execução. Um `dbt build --full-refresh` reconstrói do zero.
{{
    config(
        materialized='incremental',
        unique_key='ordem_servico_sk',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

with unificado as (
    {{ unir_origens('ordens_servico') }}
)

select
    ordem_servico_sk,
    contrato_sk,
    origem,
    tipo,
    aberta_em,
    fechada_em,
    atualizado_em,
    _ingested_at
from unificado
{% if is_incremental() %}
where _ingested_at > (select coalesce(max(_ingested_at), '1900-01-01') from {{ this }})
{% endif %}
