select
    {{ chave('gama', 'cod_cliente') }}    as cliente_sk,
    'gama'::text                          as origem,
    cod_cliente                           as cliente_id_origem,
    {{ normalizar_nome('nome_razao') }}   as nome,
    {{ apenas_digitos('num_doc') }}       as documento,
    {{ normalizar_email('email') }}       as email,
    {{ normalizar_nome('municipio') }}    as cidade,
    upper(trim(uf))                       as uf,
    {{ data_br('dt_cadastro') }}::timestamp as cadastrado_em,
    _ingested_at
from {{ source('bronze_gama', 'clientes') }}
