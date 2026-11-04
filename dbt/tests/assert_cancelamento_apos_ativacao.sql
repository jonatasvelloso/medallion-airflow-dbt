-- Nenhum contrato pode ser cancelado antes de ser ativado.
select contrato_sk, data_ativacao, data_cancelamento
from {{ ref('silver_contratos') }}
where data_cancelamento < data_ativacao
