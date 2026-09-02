{#
    Unpivots raw.distorcao_idade_serie_municipios' wide {etapa}_CAT_0 columns
    (confirmed against TDI_MUNICIPIOS_2024.xlsx) into one row per
    (município x localização x dependência x ano x etapa). Unlike taxas de
    rendimento, distortion has a single indicator (no aprovação/reprovação/
    abandono split), hence one column suffix per etapa rather than a
    {1|2|3}_ prefix.
#}

{% macro unpivot_distorcao_municipios(source_relation) %}
{% set _etapas = {'fund_ai': 'FUN_AI', 'fund_af': 'FUN_AF', 'med': 'MED'} %}
{% for etapa_codigo, col_prefix in _etapas.items() %}
select
    "NU_ANO_CENSO"::numeric::int as ano,
    "CO_MUNICIPIO"::numeric::int as co_municipio,
    "NO_MUNICIPIO" as unidade_geografica,
    "NO_CATEGORIA" as localizacao,
    "NO_DEPENDENCIA" as dependencia,
    '{{ etapa_codigo }}' as etapa_codigo,
    nullif(nullif(nullif("{{ col_prefix }}_CAT_0"::text, '-'), '--'), '')::numeric as pc_distorcao
from {{ source_relation }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
{% endmacro %}
