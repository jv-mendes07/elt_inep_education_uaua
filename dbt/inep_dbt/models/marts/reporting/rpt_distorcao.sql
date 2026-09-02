-- Dataset Superset "Distorção idade-série": Uauá + municípios pares. Sem
-- benchmark Brasil/UF ainda (ver TODO em fct_distorcao.sql).

select
    f.ano,
    f.etapa_codigo,
    e.etapa_nome,
    f.nivel_agregacao,
    f.unidade_geografica,
    f.localizacao,
    f.dependencia,
    m.is_target,
    m.is_peer,
    f.pc_distorcao
from {{ ref('fct_distorcao') }} f
join {{ ref('dim_etapa_ensino') }} e on e.etapa_codigo = f.etapa_codigo
left join {{ ref('dim_municipio') }} m on m.co_municipio = f.co_municipio
where m.is_target or m.is_peer
