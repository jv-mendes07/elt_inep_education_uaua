-- Regression guard: Uauá (CO_MUNICIPIO=2932002) deve resolver para
-- exatamente 38 escolas no censo do ano de referência (var
-- ano_referencia_pares, 2024) em dim_escola — número verificado manualmente
-- em notebooks/inep_qedu_data_dev.ipynb. dbt tests falham quando a query
-- retorna QUALQUER linha, então esta só retorna linhas quando a contagem
-- diverge do esperado.

with contagem as (
    select count(*) as qt_escolas
    from {{ ref('dim_escola') }}
    where co_municipio = {{ var('co_municipio_alvo') }}
      and ano_censo = {{ var('ano_referencia_pares') }}
)

select *
from contagem
where qt_escolas != 38
