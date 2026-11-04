-- Uma linha por pessoa, consolidando os cadastros das três empresas.
-- O CPF não chega à Gold (LGPD): a chave é o hash calculado na Silver.
with cadastros as (
    select
        *,
        row_number() over (partition by pessoa_id order by cadastrado_em desc) as mais_recente
    from {{ ref('silver_clientes') }}
)

select
    pessoa_id,
    max(nome)  filter (where mais_recente = 1)            as nome,
    max(email) filter (where mais_recente = 1)            as email,
    max(cidade) filter (where mais_recente = 1)           as cidade,
    max(uf)    filter (where mais_recente = 1)            as uf,
    min(cadastrado_em)                                    as cliente_desde,
    count(distinct origem)                                as qtd_empresas,
    string_agg(distinct origem, ', ' order by origem)     as empresas,
    count(distinct origem) > 1                            as cliente_multiempresa
from cadastros
group by pessoa_id
