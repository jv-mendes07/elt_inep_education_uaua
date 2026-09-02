-- Grão: escola x etapa de ensino x ano. Linhas com qt_matriculas nulo
-- (etapa não ofertada pela escola) são descartadas — a ausência da etapa já
-- é representável via a ausência da linha, evitando zeros/NULLs espúrios
-- nos gráficos de matrículas do Superset.

with unpivoted as (
    {{ unpivot_matriculas(ref('stg_escolas')) }}
)

select *
from unpivoted
where qt_matriculas is not null
