select
    {{ chave('alfa', 'id') }}             as ordem_servico_sk,
    {{ chave('alfa', 'contrato_id') }}    as contrato_sk,
    'alfa'::text                          as origem,
    lower(tipo)                           as tipo,
    aberta_em::timestamp                  as aberta_em,
    fechada_em::timestamp                 as fechada_em,
    atualizado_em::timestamp              as atualizado_em,
    _ingested_at
from {{ source('bronze_alfa', 'ordens_servico') }}
