select
    {{ chave('beta', 'customer_id') }}    as cliente_sk,
    'beta'::text                          as origem,
    customer_id                           as cliente_id_origem,
    {{ normalizar_nome('full_name') }}    as nome,
    {{ apenas_digitos('document') }}      as documento,
    {{ normalizar_email('email') }}       as email,
    {{ normalizar_nome('city') }}         as cidade,
    upper(trim(state))                    as uf,
    {{ timestamp_utc('created_at') }}     as cadastrado_em,
    _ingested_at
from {{ source('bronze_beta', 'clientes') }}
