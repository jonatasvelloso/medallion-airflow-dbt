with unificado as (
    {{ unir_origens('contratos') }}
)

select
    contrato_sk,
    cliente_sk,
    origem,
    plano,
    valor_mensal,
    status,
    data_ativacao,
    -- contrato cancelado sem data na origem: usa a última alteração
    case
        when status = 'cancelado' then coalesce(data_cancelamento, atualizado_em::date)
    end             as data_cancelamento,
    atualizado_em,
    _ingested_at
from unificado
