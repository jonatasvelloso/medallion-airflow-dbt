select
    dia::date                                   as data,
    extract(year from dia)::int                 as ano,
    extract(month from dia)::int                as mes,
    date_trunc('month', dia)::date              as inicio_mes,
    (date_trunc('month', dia) + interval '1 month - 1 day')::date as fim_mes,
    to_char(dia, 'YYYY-MM')                     as competencia,
    extract(isodow from dia)::int               as dia_semana,
    extract(isodow from dia) in (6, 7)          as fim_de_semana
from generate_series('2023-01-01'::date, (current_date + interval '1 year')::date, interval '1 day') as dia
