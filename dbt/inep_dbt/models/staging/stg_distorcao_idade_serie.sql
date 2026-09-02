-- Confirmed against TDI_MUNICIPIOS_2024.xlsx — município grain, has
-- CO_MUNICIPIO/NO_MUNICIPIO (Uauá present: CO_MUNICIPIO=2932002). No
-- Brasil/UF equivalent confirmed yet (see docs/data_sources.md) — if one
-- turns up, mirror the stg_taxas_rendimento_uf/_municipios split.

select
    ano,
    co_municipio,
    unidade_geografica,
    localizacao,
    dependencia,
    etapa_codigo,
    pc_distorcao,
    'MUNICIPIO' as nivel_agregacao
from (
    {{ unpivot_distorcao_municipios(source('raw', 'distorcao_idade_serie_municipios')) }}
) unpivoted
where pc_distorcao is not null
