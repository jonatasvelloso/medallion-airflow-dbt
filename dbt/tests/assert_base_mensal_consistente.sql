-- Fechamento da base: início + ativações - cancelamentos = fim.
-- Se não bater, há contrato contado em mês errado.
select competencia, origem, base_inicio, ativacoes, cancelamentos, base_fim
from {{ ref('mart_base_mensal') }}
where base_inicio + ativacoes - cancelamentos <> base_fim
   or taxa_churn not between 0 and 1
