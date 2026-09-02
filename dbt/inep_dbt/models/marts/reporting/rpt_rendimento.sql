-- Dataset Superset "Rendimento": Uauá + municípios pares (is_target/is_peer)
-- junto com os benchmarks Brasil e Bahia.

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
    f.pc_aprovacao,
    f.pc_reprovacao,
    f.pc_abandono
from {{ ref('fct_rendimento') }} f
join {{ ref('dim_etapa_ensino') }} e on e.etapa_codigo = f.etapa_codigo
left join {{ ref('dim_municipio') }} m on m.co_municipio = f.co_municipio
where f.nivel_agregacao = 'BRASIL'
   or (f.nivel_agregacao = 'UF' and f.unidade_geografica = 'Bahia')
   or (f.nivel_agregacao = 'MUNICIPIO' and (m.is_target or m.is_peer))
