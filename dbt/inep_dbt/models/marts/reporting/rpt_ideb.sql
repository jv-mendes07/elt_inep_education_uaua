-- Dataset Superset "IDEB": Uauá + municípios pares (modalidades padrão) e
-- os benchmarks Brasil/Bahia (modalidade ensino médio integrado — a única
-- fonte Brasil/UF confirmada hoje; ver TODO em stg_ideb_brasil_uf.sql).

select
    i.ano,
    i.modalidade,
    i.nivel_agregacao,
    i.unidade_geografica,
    i.rede,
    m.is_target,
    m.is_peer,
    i.nota_ideb,
    i.indicador_rendimento,
    i.nota_saeb_media
from {{ ref('fct_ideb') }} i
left join {{ ref('dim_municipio') }} m on m.co_municipio = i.co_municipio
where i.nivel_agregacao = 'BRASIL'
   or (i.nivel_agregacao = 'UF' and i.unidade_geografica = 'Bahia')
   or (i.nivel_agregacao = 'MUNICIPIO' and (m.is_target or m.is_peer))
