-- Indicadores mensais por empresa: base ativa, ativações, cancelamentos,
-- churn e receita recorrente (MRR). É a tabela que alimenta o painel executivo.
with meses as (
    select distinct inicio_mes, fim_mes, competencia
    from {{ ref('dim_calendario') }}
    where inicio_mes <= current_date
),

contratos as (
    select origem, valor_mensal, data_ativacao, data_cancelamento
    from {{ ref('fct_contratos') }}
),

por_mes as (
    select
        m.competencia,
        m.inicio_mes,
        c.origem,
        count(*) filter (
            where c.data_ativacao < m.inicio_mes
              and (c.data_cancelamento is null or c.data_cancelamento >= m.inicio_mes)
        )                                                           as base_inicio,
        count(*) filter (where c.data_ativacao between m.inicio_mes and m.fim_mes)      as ativacoes,
        count(*) filter (where c.data_cancelamento between m.inicio_mes and m.fim_mes)  as cancelamentos,
        count(*) filter (
            where c.data_ativacao <= m.fim_mes
              and (c.data_cancelamento is null or c.data_cancelamento > m.fim_mes)
        )                                                           as base_fim,
        coalesce(sum(c.valor_mensal) filter (
            where c.data_ativacao <= m.fim_mes
              and (c.data_cancelamento is null or c.data_cancelamento > m.fim_mes)
        ), 0)                                                       as mrr
    from meses m
    cross join contratos c
    group by 1, 2, 3
)

select
    competencia,
    inicio_mes,
    origem,
    base_inicio,
    ativacoes,
    cancelamentos,
    base_fim,
    round(cancelamentos::numeric / nullif(base_inicio, 0), 4)       as taxa_churn,
    mrr
from por_mes
where base_inicio > 0 or ativacoes > 0
