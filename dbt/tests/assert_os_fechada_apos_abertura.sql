-- Ordem de serviço não pode fechar antes de abrir.
select ordem_servico_sk, aberta_em, fechada_em
from {{ ref('silver_ordens_servico') }}
where fechada_em < aberta_em
