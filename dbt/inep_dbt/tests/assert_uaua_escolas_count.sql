-- Regression guard: Uauá (CO_MUNICIPIO=2932002) deve resolver para
-- exatamente 38 escolas no censo 2024 em dim_escola, o número já verificado
-- manualmente em notebooks/inep_qedu_data_dev.ipynb. dbt tests falham quando
-- a query retorna QUALQUER linha, então esta query só retorna linhas quando
-- a contagem diverge do esperado.

with contagem as (
    select count(*) as qt_escolas
    from {{ ref('dim_escola') }}
    where co_municipio = {{ var('co_municipio_alvo') }}
      and ano_censo = 2024
)

select *
from contagem
where qt_escolas != 38
