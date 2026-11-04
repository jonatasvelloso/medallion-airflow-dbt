select
    {{ chave('alfa', 'id') }}             as cliente_sk,
    'alfa'::text                          as origem,
    id                                    as cliente_id_origem,
    {{ normalizar_nome('nome') }}         as nome,
    {{ apenas_digitos('cpf_cnpj') }}      as documento,
    {{ normalizar_email('email') }}       as email,
    {{ normalizar_nome('cidade') }}       as cidade,
    upper(trim(uf))                       as uf,
    criado_em::timestamp                  as cadastrado_em,
    _ingested_at
from {{ source('bronze_alfa', 'clientes') }}
