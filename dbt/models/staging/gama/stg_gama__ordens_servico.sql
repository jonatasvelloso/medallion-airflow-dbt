select
    {{ chave('gama', 'cod_os') }}         as ordem_servico_sk,
    {{ chave('gama', 'cod_contrato') }}   as contrato_sk,
    'gama'::text                          as origem,
    case tp_os
        when 'I' then 'instalacao'
        when 'R' then 'reparo'
        when 'D' then 'retirada'
    end                                   as tipo,
    dt_abertura::timestamp                as aberta_em,
    dt_fechamento::timestamp              as fechada_em,
    dt_alteracao::timestamp               as atualizado_em,
    _ingested_at
from {{ source('bronze_gama', 'ordens_servico') }}
