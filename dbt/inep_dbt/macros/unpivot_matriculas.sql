{#
    Unpivots stg_escolas' wide qt_mat_*/qt_doc_*/qt_tur_* columns into one row
    per (co_entidade, ano_censo, etapa_codigo). Kept as a plain Jinja list
    rather than querying seed_etapa_ensino_mapping at parse time (dbt seeds
    aren't reliably queryable before they're built) — if you add/remove a
    stage here, mirror the change in
    dbt/inep_dbt/seeds/seed_etapa_ensino_mapping.csv so dim_etapa_ensino stays
    in sync.
#}

{% macro unpivot_matriculas(source_relation) %}

{%- set etapas = [
    'inf_cre', 'inf_pre',
    'fund_ai', 'fund_af',
    'med',
    'prof_tec',
    'eja_fund', 'eja_med',
    'esp_cc', 'esp_ce'
] -%}

{% for etapa in etapas %}
select
    co_entidade,
    co_municipio,
    nu_ano_censo as ano_censo,
    '{{ etapa }}' as etapa_codigo,
    qt_mat_{{ etapa }} as qt_matriculas,
    qt_doc_{{ etapa }} as qt_docentes,
    qt_tur_{{ etapa }} as qt_turmas
from {{ source_relation }}
{% if not loop.last %}union all{% endif %}
{% endfor %}

{% endmacro %}
