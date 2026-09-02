-- Confirmed against tx_rend_municipios_2024.xlsx — município grain, has
-- CO_MUNICIPIO/NO_MUNICIPIO. This is the source that actually brings Uauá
-- (and its peer municípios) into fct_rendimento/rpt_rendimento.

select
    ano,
    co_municipio,
    unidade_geografica,
    localizacao,
    dependencia,
    etapa_codigo,
    pc_aprovacao,
    pc_reprovacao,
    pc_abandono,
    'MUNICIPIO' as nivel_agregacao
from (
    {{ unpivot_taxas_rendimento_municipios(source('raw', 'taxas_rendimento_municipios')) }}
) unpivoted
where pc_aprovacao is not null or pc_reprovacao is not null or pc_abandono is not null
