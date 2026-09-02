"""Central configuration for the INEP Uauá-BA pipeline.

Every value that could change between environments (Postgres connection,
INEP/IBGE URLs, landing-zone paths) is read from Airflow Connections/
Variables here rather than hardcoded in the DAG or task code — see
docs/data_sources.md for how each Variable is populated.
"""
from __future__ import annotations

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
NU_ANO_CENSO = 2024

# --- Download URLs -------------------------------------------------------
#
# Confirmed 2026-09-01 by opening INEP's download pages directly (they're
# JS-rendered, so this couldn't be resolved automatically — see the
# "manual confirmation" note in docs/data_sources.md). Set as Variable
# *defaults* rather than hardcoded constants so a future year can override
# them without a code change: `airflow variables set <nome> '<url>'`.
#
# INEP's URLs follow a predictable /<ano>/<arquivo>_<ano>.zip pattern —
# tempting to template by year for an automatic multi-year backfill, but
# that's deliberately deferred until this single-year pipeline is validated
# end-to-end (per plan decision). Revisit once v1 is confirmed working.

VAR_TAXAS_RENDIMENTO_UF_URL = "inep_taxas_rendimento_uf_url"
DEFAULT_TAXAS_RENDIMENTO_UF_URL = (
    "https://download.inep.gov.br/informacoes_estatisticas/"
    "indicadores_educacionais/2024/tx_rend_brasil_regioes_ufs_2024.zip"
)

VAR_TAXAS_RENDIMENTO_MUNICIPIOS_URL = "inep_taxas_rendimento_municipios_url"
DEFAULT_TAXAS_RENDIMENTO_MUNICIPIOS_URL = (
    "https://download.inep.gov.br/informacoes_estatisticas/"
    "indicadores_educacionais/2024/tx_rend_municipios_2024.zip"
)

VAR_DISTORCAO_MUNICIPIOS_URL = "inep_distorcao_municipios_url"
DEFAULT_DISTORCAO_MUNICIPIOS_URL = (
    "https://download.inep.gov.br/informacoes_estatisticas/"
    "indicadores_educacionais/2024/TDI_2024_MUNICIPIOS.zip"
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


# The SIDRA/IBGE population API is a stable public endpoint — a sane default
# is provided, but it's still overridable via Variable for flexibility.
VAR_IBGE_POPULACAO_URL = "ibge_populacao_sidra_url"
DEFAULT_IBGE_POPULACAO_URL = (
    "https://apisidra.ibge.gov.br/values/t/6579/n6/all/v/9324/p/last%201"
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
CENSO_CSV_PATH = (
    DATA_DIR
    / "microdados_censo_escolar_2024_defeso"
    / "dados"
    / "microdados_ed_basica_2024.csv"
)


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


def get_ibge_populacao_url() -> str:
    return Variable.get(
        VAR_IBGE_POPULACAO_URL, default_var=DEFAULT_IBGE_POPULACAO_URL
    )
