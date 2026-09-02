{#
    Typed, cleaned pass-through of raw.escolas, restricted to the Região
    Geográfica Intermediária de Juazeiro (CO_REGIAO_GEOG_INTERM = 2908) —
    Uauá plus its candidate peer municípios (see marts/dim_municipio.sql).
    raw.escolas itself keeps the full national census, per the existing
    notebook's load pattern; narrowing here keeps every downstream
    staging/marts model small.

    Source columns are quoted with their exact original case ("NU_ANO_CENSO")
    because pandas' to_sql / psycopg2 COPY preserved the census CSV's
    upper-case headers verbatim when raw.escolas was created — unquoted
    identifiers would be folded to lowercase by Postgres and fail to match.

    Every raw.* column is TEXT (see dags/inep_pipeline/load.py —
    _create_table_from_df types everything TEXT to sidestep a pandas/
    SQLAlchemy version conflict), so integer columns are cast via
    `::numeric::int` rather than `::int` directly: a direct `::int` cast
    rejects "1100015.0", which is what an ID column serializes as once
    pandas silently upcasts it to float64 (any NaN elsewhere in that column
    forces the whole column to float) — `numeric` parses the decimal form
    fine first.
#}

select
    "NU_ANO_CENSO"::numeric::int as nu_ano_censo,
    "CO_MUNICIPIO"::numeric::int as co_municipio,
    "NO_MUNICIPIO" as no_municipio,
    "SG_UF" as sg_uf,
    "CO_UF"::numeric::int as co_uf,
    "CO_REGIAO_GEOG_INTERM"::numeric::int as co_regiao_geog_interm,
    "NO_REGIAO_GEOG_INTERM" as no_regiao_geog_interm,
    "CO_ENTIDADE"::numeric::bigint as co_entidade,
    "NO_ENTIDADE" as no_entidade,
    "TP_DEPENDENCIA"::numeric::int as tp_dependencia,
    "TP_LOCALIZACAO"::numeric::int as tp_localizacao,

    -- matrículas por etapa (grão: escola x ano) — desempilhadas em
    -- stg_matriculas_long via macro unpivot_matriculas()
    "QT_MAT_INF_CRE"::numeric as qt_mat_inf_cre,
    "QT_MAT_INF_PRE"::numeric as qt_mat_inf_pre,
    "QT_MAT_FUND_AI"::numeric as qt_mat_fund_ai,
    "QT_MAT_FUND_AF"::numeric as qt_mat_fund_af,
    "QT_MAT_MED"::numeric as qt_mat_med,
    "QT_MAT_PROF_TEC"::numeric as qt_mat_prof_tec,
    "QT_MAT_EJA_FUND"::numeric as qt_mat_eja_fund,
    "QT_MAT_EJA_MED"::numeric as qt_mat_eja_med,
    "QT_MAT_ESP_CC"::numeric as qt_mat_esp_cc,
    "QT_MAT_ESP_CE"::numeric as qt_mat_esp_ce,

    -- docentes por etapa
    "QT_DOC_INF_CRE"::numeric as qt_doc_inf_cre,
    "QT_DOC_INF_PRE"::numeric as qt_doc_inf_pre,
    "QT_DOC_FUND_AI"::numeric as qt_doc_fund_ai,
    "QT_DOC_FUND_AF"::numeric as qt_doc_fund_af,
    "QT_DOC_MED"::numeric as qt_doc_med,
    "QT_DOC_PROF_TEC"::numeric as qt_doc_prof_tec,
    "QT_DOC_EJA_FUND"::numeric as qt_doc_eja_fund,
    "QT_DOC_EJA_MED"::numeric as qt_doc_eja_med,
    "QT_DOC_ESP_CC"::numeric as qt_doc_esp_cc,
    "QT_DOC_ESP_CE"::numeric as qt_doc_esp_ce,

    -- turmas por etapa
    "QT_TUR_INF_CRE"::numeric as qt_tur_inf_cre,
    "QT_TUR_INF_PRE"::numeric as qt_tur_inf_pre,
    "QT_TUR_FUND_AI"::numeric as qt_tur_fund_ai,
    "QT_TUR_FUND_AF"::numeric as qt_tur_fund_af,
    "QT_TUR_MED"::numeric as qt_tur_med,
    "QT_TUR_PROF_TEC"::numeric as qt_tur_prof_tec,
    "QT_TUR_EJA_FUND"::numeric as qt_tur_eja_fund,
    "QT_TUR_EJA_MED"::numeric as qt_tur_eja_med,
    "QT_TUR_ESP_CC"::numeric as qt_tur_esp_cc,
    "QT_TUR_ESP_CE"::numeric as qt_tur_esp_ce

from {{ source('raw', 'escolas') }}
where "CO_REGIAO_GEOG_INTERM"::numeric::int = {{ var('co_regiao_geog_interm_alvo') }}
