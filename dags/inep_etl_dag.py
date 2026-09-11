"""INEP Uauá-BA education data pipeline.

Ingests, for every year in the `inep_anos_ingestao` Airflow Variable
(default `[2024]`), INEP's school census, taxas de rendimento (Brasil/UF +
município), taxa de distorção idade-série, plus IDEB 2019-2025 (edition-wide
files, independent of the year list) and IBGE população. A multi-year
backfill is `airflow variables set inep_anos_ingestao '[2019,...,2025]'`
followed by a re-trigger — the per-year extract/load tasks are dynamically
mapped, so adding years adds task instances, not code.

The year list is read inside a task (`anos_ingestao`), NOT at DAG-parse
time: `.expand()` over a plain module-level list bakes in whatever the
Variable held at the scheduler's LAST parse of this file, which is stale
the moment you change the Variable and trigger without waiting for (or
forcing) a re-parse — silently running fewer years than requested, with no
error. Mapping over `anos_ingestao()`'s XCom output instead re-resolves the
Variable fresh on every run.

extract_* (mapped per year) -> matching load_raw_* (mapped) -> dbt
deps/seed/run/test -> data_quality_checks.

Manually triggered (schedule=None) — see docs/data_sources.md for the URL
templates and per-year overrides, and
C:\\Users\\João Victor\\.claude\\plans\\i-have-this-data-radiant-newell.md
for the full pipeline design.

Extract tasks cache their source to a scratch CSV under DATA_DIR/_xcom/ and
pass only the *path* (plus the year) through XCom — the census alone is
~215k rows, far too large to serialize into Airflow's XCom backend.
"""
from __future__ import annotations

import datetime as dt
import logging

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from sqlalchemy import create_engine, text

from inep_pipeline import config, extract
from inep_pipeline.load import load_dataframe_full_refresh, load_dataframe_partitioned, load_dataframe_to_raw

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = "/opt/airflow/dbt/inep_dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
XCOM_SCRATCH_DIR = config.DATA_DIR / "_xcom"

default_args = {
    "owner": "inep_uaua",
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}


def _cache_to_scratch(df, name: str) -> str:
    XCOM_SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    path = XCOM_SCRATCH_DIR / f"{name}.csv"
    df.to_csv(path, index=False)
    return str(path)


@dag(
    dag_id="inep_etl_dag",
    description="Extract, load and transform INEP/IBGE education data for Uauá-BA.",
    schedule=None,
    start_date=dt.datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    # A full backfill fans out to ~24 mapped extract tasks. Reading a 300 MB
    # census CSV or a 65k-row spreadsheet costs ~0.5-1 GB each; cap
    # concurrency so the whole set doesn't OOM the container (7.4 GB shared
    # with Postgres + the scheduler).
    max_active_tasks=3,
    tags=["inep", "uaua", "education"],
)
def inep_etl_dag():

    # ---- year lists (resolved fresh every run, see module docstring) ----

    @task
    def anos_ingestao() -> list[int]:
        return config.get_anos_ingestao()

    @task
    def anos_censo(anos: list[int]) -> list[int]:
        # Census only exists up to CENSO_ANO_MAXIMO (later years not
        # published yet) — don't map a task instance onto a year with no file.
        return [a for a in anos if a <= config.CENSO_ANO_MAXIMO]

    @task
    def anos_rendimento_municipios(anos: list[int]) -> list[int]:
        # Early years use an older spreadsheet template with no
        # machine-readable header row (see config.TAXAS_RENDIMENTO_*_ANO_MINIMO)
        # — excluded so the raw table is never created from that schema.
        return [a for a in anos if a >= config.TAXAS_RENDIMENTO_MUNICIPIOS_ANO_MINIMO]

    @task
    def anos_rendimento_uf(anos: list[int]) -> list[int]:
        return [a for a in anos if a >= config.TAXAS_RENDIMENTO_UF_ANO_MINIMO]

    # ---- extract (mapped per year) ----------------------------------

    @task
    def extract_censo(ano: int) -> dict:
        return {"ano": ano, "csv_path": _cache_to_scratch(extract.read_censo_escolar(ano), f"escolas_{ano}")}

    @task
    def extract_taxas_rendimento_uf(ano: int) -> dict:
        return {
            "ano": ano,
            "csv_path": _cache_to_scratch(
                extract.extract_taxas_rendimento_uf(ano), f"taxas_rendimento_uf_{ano}"
            ),
        }

    @task
    def extract_taxas_rendimento_municipios(ano: int) -> dict:
        return {
            "ano": ano,
            "csv_path": _cache_to_scratch(
                extract.extract_taxas_rendimento_municipios(ano),
                f"taxas_rendimento_municipios_{ano}",
            ),
        }

    @task
    def extract_distorcao_idade_serie(ano: int) -> dict:
        return {
            "ano": ano,
            "csv_path": _cache_to_scratch(
                extract.extract_distorcao_idade_serie(ano), f"distorcao_idade_serie_{ano}"
            ),
        }

    # IDEB files are edition-wide (2019/2021/2023/2025 in one file), split by
    # nível × modalidade, not by year — mapped over config.IDEB_FONTES, not
    # ANOS_INGESTAO.
    @task
    def extract_ideb(nivel: str, modalidade: str) -> str:
        return _cache_to_scratch(extract.extract_ideb(nivel, modalidade), f"ideb_{nivel}_{modalidade}")

    @task
    def extract_populacao_ibge(anos: list[int]) -> str:
        return _cache_to_scratch(extract.extract_populacao_ibge(anos), "populacao_municipios")

    # ---- load (mapped per year) ------------------------------------

    @task
    def load_raw_escolas(ano: int, csv_path: str):
        import pandas as pd

        df = pd.read_csv(csv_path, dtype=str)
        load_dataframe_to_raw(
            df, "raw", "escolas", "NU_ANO_CENSO", ano, config.get_postgres_uri()
        )

    @task
    def load_raw_taxas_rendimento_uf(ano: int, csv_path: str):
        import pandas as pd

        df = pd.read_csv(csv_path)
        # The pipeline controls the year-partition column itself (rather
        # than trusting the source file to carry one with this exact name)
        # so idempotent per-year delete+reload works regardless of how
        # INEP's real file is laid out — overwrites a same-named column if
        # the source happens to already have one.
        df["NU_ANO_CENSO"] = ano
        load_dataframe_to_raw(
            df, "raw", "taxas_rendimento_uf", "NU_ANO_CENSO", ano, config.get_postgres_uri()
        )

    @task
    def load_raw_taxas_rendimento_municipios(ano: int, csv_path: str):
        import pandas as pd

        df = pd.read_csv(csv_path)
        df["NU_ANO_CENSO"] = ano
        load_dataframe_to_raw(
            df, "raw", "taxas_rendimento_municipios", "NU_ANO_CENSO", ano,
            config.get_postgres_uri(),
        )

    @task
    def load_raw_distorcao(ano: int, csv_path: str):
        import pandas as pd

        df = pd.read_csv(csv_path)
        df["NU_ANO_CENSO"] = ano
        load_dataframe_to_raw(
            df, "raw", "distorcao_idade_serie_municipios", "NU_ANO_CENSO", ano,
            config.get_postgres_uri(),
        )

    @task
    def load_raw_ideb(csv_path: str, nivel: str, modalidade: str):
        import pandas as pd

        df = pd.read_csv(csv_path)
        df["NIVEL_AGREGACAO"] = nivel
        df["MODALIDADE"] = modalidade
        # Every (nível, modalidade) source gets its own dedicated raw table
        # (see config.ideb_raw_table — even "brasil" vs "ufs" turned out to
        # have different column counts, an extra 2017 edition in "ufs"), so
        # a full replace is always correct here; no partition key needed.
        table = config.ideb_raw_table(nivel, modalidade)
        load_dataframe_full_refresh(df, "raw", table, config.get_postgres_uri())

    @task
    def load_raw_populacao(csv_path: str):
        import pandas as pd

        df = pd.read_csv(csv_path)
        # Full refresh, not year-partitioned: the whole SIDRA series is
        # re-pulled in one call each run, and the table is small.
        load_dataframe_full_refresh(
            df, "raw", "populacao_municipios", config.get_postgres_uri(),
        )

    # ---- transform -----------------------------------------------------

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"dbt deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"dbt seed --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    )

    # ---- data quality --------------------------------------------------

    @task
    def data_quality_checks(anos_censo: list[int]):
        """Beyond the dbt tests: every ingested census year must have some
        schools for Uauá, and the reference year (config.NU_ANO_CENSO) must
        match the 38 schools verified in notebooks/inep_qedu_data_dev.ipynb."""
        engine = create_engine(config.get_postgres_uri())
        problems = []
        with engine.connect() as conn:
            # raw.escolas columns are TEXT (see inep_pipeline/load.py), so
            # bind params are compared as strings, not ints.
            rows = conn.execute(
                text(
                    'SELECT "NU_ANO_CENSO" AS ano, COUNT(*) AS n FROM raw.escolas '
                    'WHERE "CO_MUNICIPIO" = :municipio GROUP BY "NU_ANO_CENSO"'
                ),
                {"municipio": str(config.CO_MUNICIPIO_ALVO)},
            ).fetchall()
        por_ano = {int(r.ano): r.n for r in rows}
        logger.info("Uauá schools per census year: %s", por_ano)

        for ano in anos_censo:
            if por_ano.get(ano, 0) == 0:
                problems.append(f"no schools for Uauá in census {ano}")
        if config.NU_ANO_CENSO in anos_censo and por_ano.get(config.NU_ANO_CENSO) != 38:
            problems.append(
                f"expected 38 schools for Uauá/{config.NU_ANO_CENSO}, "
                f"found {por_ano.get(config.NU_ANO_CENSO)}"
            )
        if problems:
            raise AirflowException("Data quality checks failed: " + "; ".join(problems))
        logger.info("Data quality checks passed for years %s.", anos_censo)

    # ---- wiring ----------------------------------------------------------

    anos = anos_ingestao()
    anos_censo_ = anos_censo(anos)
    anos_rendimento_uf_ = anos_rendimento_uf(anos)
    anos_rendimento_municipios_ = anos_rendimento_municipios(anos)

    load_escolas = load_raw_escolas.expand_kwargs(extract_censo.expand(ano=anos_censo_))
    load_rendimento_uf = load_raw_taxas_rendimento_uf.expand_kwargs(
        extract_taxas_rendimento_uf.expand(ano=anos_rendimento_uf_)
    )
    load_rendimento_municipios = load_raw_taxas_rendimento_municipios.expand_kwargs(
        extract_taxas_rendimento_municipios.expand(ano=anos_rendimento_municipios_)
    )
    load_distorcao = load_raw_distorcao.expand_kwargs(
        extract_distorcao_idade_serie.expand(ano=anos)
    )
    load_populacao = load_raw_populacao(extract_populacao_ibge(anos))

    load_ideb_tasks = []
    for nivel, modalidade in config.IDEB_FONTES:
        suffix = f"{nivel}_{modalidade}"
        ideb_csv = extract_ideb.override(task_id=f"extract_ideb_{suffix}")(nivel, modalidade)
        load_ideb_tasks.append(
            load_raw_ideb.override(task_id=f"load_raw_ideb_{suffix}")(ideb_csv, nivel, modalidade)
        )

    quality = data_quality_checks(anos_censo_)

    [
        load_escolas, load_rendimento_uf, load_rendimento_municipios,
        load_distorcao, load_populacao, *load_ideb_tasks,
    ] >> dbt_deps >> dbt_seed >> dbt_run >> dbt_test >> quality


inep_etl_dag()
