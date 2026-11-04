select
    c.contrato_sk,
    cli.pessoa_id,
    {{ dbt.hash('c.plano') }}                                       as plano_id,
    c.origem,
    c.status,
    c.valor_mensal,
    c.data_ativacao,
    c.data_cancelamento,
    coalesce(c.data_cancelamento, current_date) - c.data_ativacao  as permanencia_dias,
    c.status = 'cancelado'
        and c.data_cancelamento - c.data_ativacao <= {{ var('dias_churn_precoce') }} as churn_precoce
from {{ ref('silver_contratos') }} c
join {{ ref('silver_clientes') }} cli using (cliente_sk)
