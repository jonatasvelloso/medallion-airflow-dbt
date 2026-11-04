{#-
    Gera o UNION ALL das models de staging de todas as origens listadas em
    var('origens'). Todas as staging de uma entidade expõem as mesmas colunas
    na mesma ordem: esse é o contrato que torna a Silver genérica.
-#}
{% macro unir_origens(entidade) -%}
    {%- for origem in var('origens') %}
    select * from {{ ref('stg_' ~ origem ~ '__' ~ entidade) }}
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor %}
{%- endmacro %}
