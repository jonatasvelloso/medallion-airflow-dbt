select
    {{ chave('gama', 'cod_contrato') }}   as contrato_sk,
    {{ chave('gama', 'cod_cliente') }}    as cliente_sk,
    'gama'::text                          as origem,
    {{ normalizar_nome('desc_plano') }}   as plano,
    -- legado grava "119,90" (vírgula decimal, como texto)
    replace(vlr_mensal, ',', '.')::numeric(10, 2) as valor_mensal,
    case
        when fl_ativo = 'N'    then 'cancelado'
        when fl_suspenso = 'S' then 'suspenso'
        else 'ativo'
    end                                   as status,
    {{ data_br('dt_ativacao') }}          as data_ativacao,
    {{ data_br('dt_cancelamento') }}      as data_cancelamento,
    dt_alteracao::timestamp               as atualizado_em,
    _ingested_at
from {{ source('bronze_gama', 'contratos') }}
