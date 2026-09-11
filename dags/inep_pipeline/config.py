"""Central configuration for the INEP Uauá-BA pipeline.

Every value that could change between environments (Postgres connection,
INEP/IBGE URLs, landing-zone paths) is read from Airflow Connections/
Variables here rather than hardcoded in the DAG or task code — see
docs/data_sources.md for how each Variable is populated.
"""
from __future__ import annotations

import json
from pathlib import Path

from airflow.hooks.base import BaseHook
from airflow.models import Variable

# Airflow connection id for the "inep" Postgres database. Defined via the
# AIRFLOW_CONN_POSTGRES_INEP env var on the airflow-webserver/-scheduler
# services in docker-compose.yml (Airflow's native env-var connection
# convention: AIRFLOW_CONN_<UPPER_CASE_CONN_ID>).
POSTGRES_CONN_ID = "postgres_inep"

# IBGE código do município-alvo (Uauá - BA).
CO_MUNICIPIO_ALVO = 2932002

# Ano de referência do projeto — o "snapshot" usado pela seleção de
# municípios pares (marts/dim_municipio.sql) e pelo regression check das 38
# escolas de Uauá. Sempre deve estar contido em get_anos_ingestao().
NU_ANO_CENSO = 2024

# --- Anos a ingerir ---------------------------------------------------
#
# O DAG faz dynamic task mapping sobre esta lista: uma instância de
# extract_*/load_raw_* por ano. Default = só o ano de referência; um
# backfill multi-ano é feito sem alterar código:
#
#   airflow variables set inep_anos_ingestao '[2019,2020,2021,2022,2023,2024,2025]'
#
# Faixa suportada por template de URL puro: 2019+ (ver docs/data_sources.md
# — anos anteriores usam outro padrão de caminho no download.inep.gov.br).
# O Censo só vai até 2024 (2025 ainda não publicado na data da última
# verificação); anos sem arquivo simplesmente falham a task daquele ano,
# sem afetar os demais.
VAR_ANOS_INGESTAO = "inep_anos_ingestao"
DEFAULT_ANOS_INGESTAO = [NU_ANO_CENSO]
CENSO_ANO_MAXIMO = 2024

# Taxas de rendimento: anos anteriores a estes usam um template de planilha
# mais antigo sem a linha de código machine-readable esperada em
# TAXAS_RENDIMENTO_HEADER_ROW (colunas "ano"/"TIPOLOCA"/"DEPENDAD"/"tap_*"
# em vez de "NU_ANO_CENSO"/"UNIDGEO"/"1_CAT_*") — confirmado 2026-09-10
# inspecionando os arquivos reais ano a ano. O corte NÃO é o mesmo para as
# duas fontes: Município já usa o template novo a partir de 2020, mas
# Brasil/UF só migra em 2021. Excluídos explicitamente aqui (em vez de
# deixar cair no filtro de NULL do unpivot) para que a tabela raw nunca
# seja criada a partir do schema antigo — com tasks paralelas, não há
# garantia de qual ano roda primeiro, e se um ano do template antigo criar
# a tabela, TODOS os anos do template novo quebram (colunas divergentes).
TAXAS_RENDIMENTO_MUNICIPIOS_ANO_MINIMO = 2020
TAXAS_RENDIMENTO_UF_ANO_MINIMO = 2021

# --- Download URLs -------------------------------------------------------
#
# INEP's download pages are JS-rendered and couldn't be resolved
# automatically, but the file URLs themselves follow a stable, predictable
# pattern (confirmed by range-request probing every year 2019-2025 on
# 2026-09-09 — see docs/data_sources.md). Templated by {ano} here; a single
# year still resolves to exactly the URL that was hardcoded before.
#
# Per-year override: `airflow variables set <prefixo>_<ano> '<url>'` (e.g.
# `inep_taxas_rendimento_municipios_url_2018`) — used when a specific year's
# file doesn't match the template (older years use a different path).

VAR_TAXAS_RENDIMENTO_UF_URL_PREFIX = "inep_taxas_rendimento_uf_url_"
TAXAS_RENDIMENTO_UF_URL_TMPL = (
    "https://download.inep.gov.br/informacoes_estatisticas/"
    "indicadores_educacionais/{ano}/tx_rend_brasil_regioes_ufs_{ano}.zip"
)

VAR_TAXAS_RENDIMENTO_MUNICIPIOS_URL_PREFIX = "inep_taxas_rendimento_municipios_url_"
TAXAS_RENDIMENTO_MUNICIPIOS_URL_TMPL = (
    "https://download.inep.gov.br/informacoes_estatisticas/"
    "indicadores_educacionais/{ano}/tx_rend_municipios_{ano}.zip"
)

VAR_DISTORCAO_MUNICIPIOS_URL_PREFIX = "inep_distorcao_municipios_url_"
DISTORCAO_MUNICIPIOS_URL_TMPL = (
    "https://download.inep.gov.br/informacoes_estatisticas/"
    "indicadores_educacionais/{ano}/TDI_{ano}_MUNICIPIOS.zip"
)

VAR_CENSO_URL_PREFIX = "inep_censo_escolar_url_"
CENSO_URL_TMPL = (
    "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_{ano}.zip"
)

VAR_IDEB_URL_PREFIX = "inep_ideb_url_"  # + "<nivel>_<modalidade>", see get_ideb_url
DEFAULT_IDEB_URLS = {
    "brasil_ensino_medio_integrado": (
        "https://download.inep.gov.br/ideb/resultados/"
        "divulgacao_brasil_Ensino_Medio_integrado_ideb_2025.zip"
    ),
    "ufs_ensino_medio_integrado": (
        "https://download.inep.gov.br/ideb/resultados/"
        "divulgacao_ufs_Ensino_Medio_integrado_ideb_2025.zip"
    ),
    "municipios_anos_iniciais_fundamental": (
        "https://download.inep.gov.br/ideb/resultados/"
        "divulgacao_anos_iniciais_municipios_2025.zip"
    ),
    "municipios_anos_finais_fundamental": (
        "https://download.inep.gov.br/ideb/resultados/"
        "divulgacao_anos_finais_municipios_2025.zip"
    ),
    "municipios_ensino_medio": (
        "https://download.inep.gov.br/ideb/resultados/"
        "divulgacao_ensino_medio_municipios_2025.zip"
    ),
}

# IDEB is NOT published one file per edition — each file already contains
# every edition as wide VL_OBSERVADO_<ano> columns (2005-2025 for the
# município files!), unpivoted to long format in the stg_ideb_* models down
# to the 4 editions this project tracks (see unpivot_ideb.sql). Files are
# instead split by nível de agregação x modalidade.
#
# "escolas" (per-school IDEB) still isn't in this list — INEP's per-school
# results now live behind an interactive BI panel rather than a guaranteed
# bulk file (see the IDEB note in docs/data_sources.md); add it here once/if
# a bulk file turns up.
IDEB_FONTES = [
    ("brasil", "ensino_medio_integrado"),
    ("ufs", "ensino_medio_integrado"),
    ("municipios", "anos_iniciais_fundamental"),
    ("municipios", "anos_finais_fundamental"),
    ("municipios", "ensino_medio"),
]


def ideb_raw_table(nivel: str, modalidade: str) -> str:
    """Raw table name for one (nível, modalidade) IDEB source.

    Every (nivel, modalidade) combination gets its own dedicated raw table —
    initially "brasil" and "ufs" were assumed to share one table (identical
    column layout), but that turned out to be wrong: the "ufs" file has an
    extra 2017 edition the "brasil" file doesn't, so their column counts
    differ and a shared table breaks COPY (confirmed by running the DAG —
    see docs/data_sources.md). "municipios" modalidades were already known
    to differ (anos_iniciais/anos_finais/ensino_medio have different
    per-série columns), so one table per source turned out to be the right
    call everywhere, not just there.
    """
    return f"ideb_{nivel}_{modalidade}"


# The SIDRA/IBGE population API is a stable public endpoint. The period is
# built from the ingestion year range (get_ibge_populacao_url) so a
# multi-year run pulls the matching population series in one call; SIDRA
# tolerates a range that overshoots what it has (table 6579 has no 2022/2023
# — Census years) and just returns the years it does have. Still fully
# overridable via Variable.
VAR_IBGE_POPULACAO_URL = "ibge_populacao_sidra_url"
IBGE_POPULACAO_URL_TMPL = (
    "https://apisidra.ibge.gov.br/values/t/6579/n6/all/v/9324/p/{periodo}"
)

# Header row (0-indexed, matches pandas.read_excel(header=...)) of the
# machine-readable column-code row in each INEP xlsx — every file buries it
# under several title/merged-header rows. Confirmed empirically against all
# 8 example files downloaded so far (see docs/data_sources.md): every
# taxas-de-rendimento/distorção file (Brasil/UF and Municípios alike) has it
# at spreadsheet row 9 (index 8); every IDEB divulgacao_*.xlsx file (Brasil/
# UF and Municípios alike) has it at row 10 (index 9). Re-verify if INEP
# changes the template.
TAXAS_RENDIMENTO_HEADER_ROW = 8
DISTORCAO_HEADER_ROW = 8
IDEB_HEADER_ROW = 9

# Landing zone for raw downloads, following the convention already used by
# notebooks/data/microdados_censo_escolar_2024_defeso/.
DATA_DIR = Path("/opt/airflow/notebooks/data")

# Colunas do Censo Escolar que a camada dbt realmente consome (ver
# stg_escolas.sql). raw.escolas é reduzida a estas ~40 colunas na extração
# em vez das ~400 do arquivo original: protege contra drift de schema entre
# anos (uma coluna nova/removida em um ano não quebra o COPY) e mantém a
# tabela pequena num backfill de 6 anos. Todas confirmadas presentes em
# 2019-2024 (docs/data_sources.md). CO_REGIAO_GEOG_INTERM NÃO está aqui de
# propósito — não existe antes de 2023; a região vem do seed
# seed_municipios_regiao_alvo no dbt.
CENSO_COLUNAS = [
    "NU_ANO_CENSO",
    "CO_MUNICIPIO", "NO_MUNICIPIO", "SG_UF", "CO_UF",
    "CO_ENTIDADE", "NO_ENTIDADE",
    "TP_DEPENDENCIA", "TP_LOCALIZACAO",
    "QT_MAT_INF_CRE", "QT_MAT_INF_PRE", "QT_MAT_FUND_AI", "QT_MAT_FUND_AF",
    "QT_MAT_MED", "QT_MAT_PROF_TEC", "QT_MAT_EJA_FUND", "QT_MAT_EJA_MED",
    "QT_MAT_ESP_CC", "QT_MAT_ESP_CE",
    "QT_DOC_INF_CRE", "QT_DOC_INF_PRE", "QT_DOC_FUND_AI", "QT_DOC_FUND_AF",
    "QT_DOC_MED", "QT_DOC_PROF_TEC", "QT_DOC_EJA_FUND", "QT_DOC_EJA_MED",
    "QT_DOC_ESP_CC", "QT_DOC_ESP_CE",
    "QT_TUR_INF_CRE", "QT_TUR_INF_PRE", "QT_TUR_FUND_AI", "QT_TUR_FUND_AF",
    "QT_TUR_MED", "QT_TUR_PROF_TEC", "QT_TUR_EJA_FUND", "QT_TUR_EJA_MED",
    "QT_TUR_ESP_CC", "QT_TUR_ESP_CE",
]

# Nome-base do CSV principal dentro do zip do Censo — o caminho interno
# completo varia por ano (pasta com acento em 2022, .CSV maiúsculo em 2020,
# etc., ver docs/data_sources.md), então a extração faz glob por este sufixo
# em vez de montar o caminho por template.
CENSO_MEMBRO_REGEX = r"dados/microdados_ed_basica_\d{4}\.csv$"


def censo_local_glob(ano: int) -> str:
    """Glob (relativo a DATA_DIR) para um CSV do Censo já baixado à mão —
    a extração procura isto antes de baixar da web. Cobre o layout do
    exemplo (`microdados_censo_escolar_2024_defeso/`) e o do zip oficial
    (`microdados_censo_escolar_<ano>/`)."""
    return f"*censo*{ano}*/**/microdados_ed_basica_{ano}.csv"


def get_postgres_uri() -> str:
    """Resolve the inep Postgres connection to a SQLAlchemy/psycopg2 URI."""
    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    return (
        f"postgresql+psycopg2://{conn.login}:{conn.password}"
        f"@{conn.host}:{conn.port}/{conn.schema}"
    )


def get_variable_url(var_name: str, default: str | None = None) -> str:
    """Fetch a URL Variable, falling back to `default` (a confirmed URL
    baked in above) when unset, or raising an actionable error when neither
    is available.

    Airflow Variables are the operator-facing override surface for these
    URLs — INEP doesn't expose a stable API, so `airflow variables set
    <nome> '<url>'` is how an operator points at a newer year without a
    code change.
    """
    value = Variable.get(var_name, default_var=default)
    if not value:
        raise ValueError(
            f"Airflow Variable '{var_name}' is not set and no default is "
            f"known. Confirm the download URL manually (see "
            f"docs/data_sources.md) and set it with: "
            f"airflow variables set {var_name} '<url>'"
        )
    return value


def get_ideb_url(nivel: str, modalidade: str) -> str:
    key = f"{nivel}_{modalidade}"
    return get_variable_url(f"{VAR_IDEB_URL_PREFIX}{key}", DEFAULT_IDEB_URLS.get(key))


def get_anos_ingestao() -> list[int]:
    """Lista de anos a ingerir, do Variable `inep_anos_ingestao` (JSON) ou
    o default. Ordenada, deduplicada, só ints. Garante que NU_ANO_CENSO
    (ano de referência do projeto) esteja sempre presente."""
    raw = Variable.get(VAR_ANOS_INGESTAO, default_var=None)
    if raw is None:
        anos = list(DEFAULT_ANOS_INGESTAO)
    else:
        parsed = raw if isinstance(raw, list) else json.loads(raw)
        anos = [int(a) for a in parsed]
    return sorted(set(anos) | {NU_ANO_CENSO})


def _get_templated_url(var_prefix: str, template: str, ano: int) -> str:
    """URL de uma fonte anual: Variable `<prefixo><ano>` se existir, senão o
    template preenchido com o ano."""
    return get_variable_url(f"{var_prefix}{ano}", template.format(ano=ano))


def get_taxas_rendimento_uf_url(ano: int) -> str:
    return _get_templated_url(
        VAR_TAXAS_RENDIMENTO_UF_URL_PREFIX, TAXAS_RENDIMENTO_UF_URL_TMPL, ano
    )


def get_taxas_rendimento_municipios_url(ano: int) -> str:
    return _get_templated_url(
        VAR_TAXAS_RENDIMENTO_MUNICIPIOS_URL_PREFIX,
        TAXAS_RENDIMENTO_MUNICIPIOS_URL_TMPL,
        ano,
    )


def get_distorcao_municipios_url(ano: int) -> str:
    return _get_templated_url(
        VAR_DISTORCAO_MUNICIPIOS_URL_PREFIX, DISTORCAO_MUNICIPIOS_URL_TMPL, ano
    )


def get_censo_url(ano: int) -> str:
    return _get_templated_url(VAR_CENSO_URL_PREFIX, CENSO_URL_TMPL, ano)


def get_ibge_populacao_url(anos: list[int] | None = None) -> str:
    """URL da SIDRA com o período cobrindo `anos` (ou só NU_ANO_CENSO).
    Um Variable explícito, se setado, tem precedência sobre o template."""
    override = Variable.get(VAR_IBGE_POPULACAO_URL, default_var=None)
    if override:
        return override
    anos = anos or [NU_ANO_CENSO]
    periodo = f"{min(anos)}-{max(anos)}" if len(anos) > 1 else str(anos[0])
    return IBGE_POPULACAO_URL_TMPL.format(periodo=periodo)
