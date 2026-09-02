{#
    Unpivots the wide {1|2|3}_CAT_{etapa} columns (1=aprovação, 2=reprovação,
    3=abandono — confirmed from tx_rend_brasil_regioes_ufs_2024.xlsx and
    tx_rend_municipios_2024.xlsx, both share this code layout) into one row
    per (unidade x localização x dependência x ano x etapa), with
    pc_aprovacao/pc_reprovacao/pc_abandono as columns.

    Two variants because the identifying columns differ: the Brasil/Região/UF
    file has a single free-text UNIDGEO column and no CO_MUNICIPIO, while the
    Município file has NO_REGIAO/SG_UF/CO_MUNICIPIO/NO_MUNICIPIO instead.

    Only the three etapa aggregates that match dim_etapa_ensino's grain are
    unpivoted here (fund_ai, fund_af, med) — the source also has a per-grade/
    série breakdown (*_01.._09, MED_01..04) which is out of v1 scope; add a
    second macro call if per-série drilldown becomes a requirement.

    '-' and '--' are both placeholders INEP uses for "sem dado" — both
    nullif'd before the numeric cast. ano/co_municipio are cast via
    `::numeric::int` rather than `::int` because raw.* columns are TEXT and
    an ID column can serialize as "1100015.0" (pandas float-upcasts a
    column the moment any row — even an unrelated footer row at the bottom
    of the xlsx — has a NaN in it); `numeric` parses that fine, `int` alone
    does not.
#}

{% macro _rendimento_rate_cols(col_suffix) %}
    nullif(nullif(nullif("1_CAT_{{ col_suffix }}"::text, '-'), '--'), '')::numeric as pc_aprovacao,
    nullif(nullif(nullif("2_CAT_{{ col_suffix }}"::text, '-'), '--'), '')::numeric as pc_reprovacao,
    nullif(nullif(nullif("3_CAT_{{ col_suffix }}"::text, '-'), '--'), '')::numeric as pc_abandono
{% endmacro %}

{% macro unpivot_taxas_rendimento_uf(source_relation) %}
{% set _etapas = {'fund_ai': 'FUN_AI', 'fund_af': 'FUN_AF', 'med': 'MED'} %}
{% for etapa_codigo, col_suffix in _etapas.items() %}
select
    "NU_ANO_CENSO"::numeric::int as ano,
    cast(null as int) as co_municipio,
    "UNIDGEO" as unidade_geografica,
    "NO_CATEGORIA" as localizacao,
    "NO_DEPENDENCIA" as dependencia,
    '{{ etapa_codigo }}' as etapa_codigo,
    {{ _rendimento_rate_cols(col_suffix) }}
from {{ source_relation }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
{% endmacro %}

{% macro unpivot_taxas_rendimento_municipios(source_relation) %}
{% set _etapas = {'fund_ai': 'FUN_AI', 'fund_af': 'FUN_AF', 'med': 'MED'} %}
{% for etapa_codigo, col_suffix in _etapas.items() %}
select
    "NU_ANO_CENSO"::numeric::int as ano,
    "CO_MUNICIPIO"::numeric::int as co_municipio,
    "NO_MUNICIPIO" as unidade_geografica,
    "NO_CATEGORIA" as localizacao,
    "NO_DEPENDENCIA" as dependencia,
    '{{ etapa_codigo }}' as etapa_codigo,
    {{ _rendimento_rate_cols(col_suffix) }}
from {{ source_relation }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
{% endmacro %}
