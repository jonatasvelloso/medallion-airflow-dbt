select
    {{ chave('beta', 'ticket_id') }}         as ordem_servico_sk,
    {{ chave('beta', 'subscription_id') }}   as contrato_sk,
    'beta'::text                             as origem,
    case ticket_type
        when 'install' then 'instalacao'
        when 'repair'  then 'reparo'
        when 'removal' then 'retirada'
    end                                      as tipo,
    {{ timestamp_utc('opened_at') }}         as aberta_em,
    {{ timestamp_utc('closed_at') }}         as fechada_em,
    {{ timestamp_utc('updated_at') }}        as atualizado_em,
    _ingested_at
from {{ source('bronze_beta', 'ordens_servico') }}
