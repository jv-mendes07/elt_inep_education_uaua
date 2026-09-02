-- Grão: escola x etapa de ensino x ano.

select
    co_entidade,
    co_municipio,
    ano_censo as ano,
    etapa_codigo,
    qt_matriculas,
    qt_docentes,
    qt_turmas
from {{ ref('stg_matriculas_long') }}
