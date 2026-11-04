select
    {{ dbt.hash('plano') }}                                         as plano_id,
    plano,
    case when plano ilike 'empresarial%' then 'empresarial' else 'residencial' end as segmento,
    case
        when plano ~* '(\d+)\s*giga' then (substring(plano from '(?i)(\d+)\s*giga'))::int * 1000
        else (substring(plano from '(?i)(\d+)\s*mega'))::int
    end                                                             as velocidade_mbps
from (select distinct plano from {{ ref('silver_contratos') }}) planos
