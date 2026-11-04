-- Clientes das três empresas em um único modelo.
-- pessoa_id identifica a mesma pessoa em empresas diferentes (mesmo CPF/CNPJ):
-- é a chave que permite enxergar o cliente consolidado após a fusão.
with unificado as (
    {{ unir_origens('clientes') }}
)

select
    cliente_sk,
    md5(documento)  as pessoa_id,
    origem,
    cliente_id_origem,
    nome,
    documento,
    email,
    cidade,
    uf,
    cadastrado_em,
    _ingested_at
from unificado
