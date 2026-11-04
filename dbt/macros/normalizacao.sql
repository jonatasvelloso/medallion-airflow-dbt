{#- Regras de limpeza aplicadas igualmente às três origens. -#}

{% macro apenas_digitos(coluna) -%}
    nullif(regexp_replace({{ coluna }}, '\D', '', 'g'), '')
{%- endmacro %}

{% macro normalizar_nome(coluna) -%}
    initcap(lower(trim({{ coluna }})))
{%- endmacro %}

{% macro normalizar_email(coluna) -%}
    nullif(lower(trim({{ coluna }})), '')
{%- endmacro %}

{% macro data_br(coluna) -%}
    {#- Sistema legado grava datas como texto DD/MM/AAAA. -#}
    to_date(nullif(trim({{ coluna }}), ''), 'DD/MM/YYYY')
{%- endmacro %}

{% macro timestamp_utc(coluna) -%}
    ({{ coluna }}::timestamptz at time zone 'UTC')
{%- endmacro %}

{% macro chave(origem, coluna) -%}
    '{{ origem }}-' || {{ coluna }}
{%- endmacro %}
