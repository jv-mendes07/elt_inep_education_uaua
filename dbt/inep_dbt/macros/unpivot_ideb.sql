{#
    Unpivots the wide VL_OBSERVADO_<edicao>/VL_INDICADOR_REND_<edicao>/
    VL_NOTA_MEDIA_<edicao> columns (confirmed against
    divulgacao_brasil_Ensino_Medio_integrado_ideb_2025.xlsx and
    divulgacao_anos_iniciais_municipios_2025.xlsx — same code pattern in
    both) into one row per (unidade x rede x edição). VL_OBSERVADO is the
    actual IDEB score (0-10 scale); VL_INDICADOR_REND is the rendimento
    component (P) and VL_NOTA_MEDIA the mean Saeb score (N) that combine to
    produce it — kept alongside for drilldown/debugging.

    Two variants because the identifying columns differ: brasil/uf files
    have UNIDADE_GEOGRAFICA/REDE (renamed from blank headers in
    inep_pipeline/extract.py) and no CO_MUNICIPIO; município files have
    SG_UF/CO_MUNICIPIO/NO_MUNICIPIO/REDE with real codes already.

    Editions unpivoted = every VL_OBSERVADO_<ano> column actually present on
    `source_relation` (_ideb_editions introspects it), NOT a hardcoded list —
    confirmed 2026-09-10 that coverage genuinely differs per (nível,
    modalidade), not just a brasil/ufs quirk:
      brasil_ensino_medio_integrado:            2019,2021,2023,2025
      ufs_ensino_medio_integrado:                2017,2019,2021,2023,2025
      municipios_anos_iniciais_fundamental:      2005,2007,...,2025 (11 edições)
      municipios_anos_finais_fundamental:        2005,2007,...,2025 (11 edições)
      municipios_ensino_medio:                   2017,2019,2021,2023,2025
    A hardcoded list would either error on a table missing a column (as
    "brasil" does for 2017) or silently drop editions a table does have (as
    the previous [2019,2021,2023,2025]-only scope did for the fundamental
    tables' 2005-2017). Introspecting means a future INEP edition (2027+)
    is picked up automatically without touching this file.
#}

{% macro _ideb_editions(source_relation) %}
    {%- set columns = adapter.get_columns_in_relation(source_relation) -%}
    {%- set editions = [] -%}
    {%- for col in columns -%}
        {%- if col.column.upper().startswith('VL_OBSERVADO_') -%}
            {%- do editions.append(col.column.split('_')[-1] | int) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(editions | sort) }}
{% endmacro %}

{% macro _ideb_edition_cols(edicao) %}
    nullif(nullif(nullif("VL_OBSERVADO_{{ edicao }}"::text, '-'), '--'), '')::numeric as nota_ideb,
    nullif(nullif(nullif("VL_INDICADOR_REND_{{ edicao }}"::text, '-'), '--'), '')::numeric as indicador_rendimento,
    nullif(nullif(nullif("VL_NOTA_MEDIA_{{ edicao }}"::text, '-'), '--'), '')::numeric as nota_saeb_media
{% endmacro %}

{% macro unpivot_ideb_brasil_uf(source_relation) %}
{% set _edicoes = _ideb_editions(source_relation) %}
{% for edicao in _edicoes %}
select
    cast(null as int) as co_municipio,
    "UNIDADE_GEOGRAFICA" as unidade_geografica,
    "REDE" as rede,
    -- Normalized to the same BRASIL/REGIAO/UF/MUNICIPIO/ESCOLA convention
    -- used by fct_rendimento/fct_distorcao — "NIVEL_AGREGACAO" here still
    -- holds the raw Python string the DAG tagged it with ("brasil"/"ufs").
    case "NIVEL_AGREGACAO"
        when 'brasil' then 'BRASIL'
        when 'ufs' then 'UF'
        else upper("NIVEL_AGREGACAO")
    end as nivel_agregacao,
    "MODALIDADE" as modalidade,
    {{ edicao }} as ano_edicao,
    {{ _ideb_edition_cols(edicao) }}
from {{ source_relation }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
{% endmacro %}

{% macro unpivot_ideb_municipios(source_relation) %}
{#- NIVEL_AGREGACAO/MODALIDADE are added by the DAG before load (same as
    for the brasil/uf table — see dags/inep_etl_dag.py load_raw_ideb), so
    they're read from the source here rather than hardcoded, keeping this
    macro identical for every município modalidade table. -#}
{% set _edicoes = _ideb_editions(source_relation) %}
{% for edicao in _edicoes %}
select
    "CO_MUNICIPIO"::numeric::int as co_municipio,
    "NO_MUNICIPIO" as unidade_geografica,
    "REDE" as rede,
    -- Always 'municipios' in the raw column (config.IDEB_FONTES) — 'MUNICIPIO'
    -- (singular) matches the BRASIL/REGIAO/UF/MUNICIPIO/ESCOLA convention
    -- used elsewhere (fct_rendimento/fct_distorcao), not the raw value itself.
    'MUNICIPIO' as nivel_agregacao,
    "MODALIDADE" as modalidade,
    {{ edicao }} as ano_edicao,
    {{ _ideb_edition_cols(edicao) }}
from {{ source_relation }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
{% endmacro %}
