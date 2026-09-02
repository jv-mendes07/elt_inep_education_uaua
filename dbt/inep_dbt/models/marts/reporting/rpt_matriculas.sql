-- Modelo denormalizado para o Superset — dataset "Matrículas". Uma linha por
-- escola x etapa x ano, já com nome de município, etapa e flags de
-- comparação (is_target/is_peer) resolvidos, para manter o SQL dos gráficos
-- simples.

select
    f.ano,
    f.etapa_codigo,
    e.etapa_nome,
    e.nivel_agregado,
    m.co_municipio,
    m.no_municipio,
    m.is_target,
    m.is_peer,
    esc.co_entidade,
    esc.no_entidade,
    esc.tp_dependencia,
    esc.tp_localizacao,
    f.qt_matriculas,
    f.qt_docentes,
    f.qt_turmas
from {{ ref('fct_matriculas') }} f
join {{ ref('dim_escola') }} esc
    on esc.co_entidade = f.co_entidade and esc.ano_censo = f.ano
join {{ ref('dim_municipio') }} m
    on m.co_municipio = f.co_municipio
join {{ ref('dim_etapa_ensino') }} e
    on e.etapa_codigo = f.etapa_codigo
where m.is_target or m.is_peer
