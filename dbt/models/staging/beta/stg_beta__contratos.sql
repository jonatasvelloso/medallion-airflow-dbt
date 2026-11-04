select
    {{ chave('beta', 'subscription_id') }}   as contrato_sk,
    {{ chave('beta', 'customer_id') }}       as cliente_sk,
    'beta'::text                             as origem,
    {{ normalizar_nome('plan_name') }}       as plano,
    -- Beta guarda valores em centavos
    (monthly_fee_cents::integer / 100.0)::numeric(10, 2) as valor_mensal,
    case status_code
        when '1' then 'ativo'
        when '2' then 'suspenso'
        when '3' then 'cancelado'
    end                                      as status,
    activated_on::date                       as data_ativacao,
    canceled_on::date                        as data_cancelamento,
    {{ timestamp_utc('updated_at') }}        as atualizado_em,
    _ingested_at
from {{ source('bronze_beta', 'contratos') }}
