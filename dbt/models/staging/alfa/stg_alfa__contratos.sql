select
    {{ chave('alfa', 'id') }}             as contrato_sk,
    {{ chave('alfa', 'cliente_id') }}     as cliente_sk,
    'alfa'::text                          as origem,
    {{ normalizar_nome('plano') }}        as plano,
    valor_mensal::numeric(10, 2)          as valor_mensal,
    lower(status)                         as status,
    data_ativacao::date                   as data_ativacao,
    data_cancelamento::date               as data_cancelamento,
    atualizado_em::timestamp              as atualizado_em,
    _ingested_at
from {{ source('bronze_alfa', 'contratos') }}
