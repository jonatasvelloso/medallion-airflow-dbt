select
    os.ordem_servico_sk,
    os.contrato_sk,
    os.origem,
    os.tipo,
    os.aberta_em,
    os.fechada_em,
    os.fechada_em is null                                           as em_aberto,
    round((extract(epoch from os.fechada_em - os.aberta_em) / 3600)::numeric, 1) as horas_atendimento,
    sla.sla_horas,
    case
        when os.fechada_em is null then null
        else os.fechada_em - os.aberta_em <= make_interval(hours => sla.sla_horas)
    end                                                             as dentro_sla
from {{ ref('silver_ordens_servico') }} os
left join {{ ref('sla_ordens_servico') }} sla using (tipo)
