"""INEP Uauá-BA education data pipeline — v1 (2024 census/rendimento/
distortion + IDEB 2019-2025).

extract_* (parallel) -> matching load_raw_* (parallel) -> dbt deps/seed/run/test
-> data_quality_checks.

Manually triggered (schedule=None) — see docs/data_sources.md for the
Airflow Variables this DAG depends on, and
C:\\Users\\João Victor\\.claude\\plans\\i-have-this-data-radiant-newell.md
for the full pipeline design.

Extract tasks download/read their source and cache it to a scratch CSV under
DATA_DIR/_xcom/, passing only the *path* through XCom rather than the whole
dataframe — the census alone is ~215k rows x 426 columns, far too large to
serialize into Airflow's XCom backend directly.
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
    tags=["inep", "uaua", "education"],
)
def inep_etl_dag():

    # ---- extract -----------------------------------------------------

    @task
    def extract_taxas_rendimento_uf() -> str:
        return _cache_to_scratch(extract.extract_taxas_rendimento_uf(), "taxas_rendimento_uf")

    @task
    def extract_taxas_rendimento_municipios() -> str:
        return _cache_to_scratch(
            extract.extract_taxas_rendimento_municipios(), "taxas_rendimento_municipios"
        )

    @task
    def extract_distorcao_idade_serie() -> str:
        return _cache_to_scratch(extract.extract_distorcao_idade_serie(), "distorcao_idade_serie")

    @task
    def extract_ideb(nivel: str, modalidade: str) -> str:
        return _cache_to_scratch(extract.extract_ideb(nivel, modalidade), f"ideb_{nivel}_{modalidade}")

    @task
    def extract_populacao_ibge() -> str:
        return _cache_to_scratch(extract.extract_populacao_ibge(), "populacao_municipios")

    # ---- load --------------------------------------------------------

    @task
    def load_raw_escolas():
        # No extract task: the census CSV is already local (see
        # notebooks/data/microdados_censo_escolar_2024_defeso/), so this
        # task reads it directly rather than round-tripping through XCom.
        df = extract.read_censo_escolar()
        load_dataframe_to_raw(
            df, "raw", "escolas", "NU_ANO_CENSO", config.NU_ANO_CENSO,
            config.get_postgres_uri(),
        )

    @task
    def load_raw_taxas_rendimento_uf(csv_path: str):
        import pandas as pd

        df = pd.read_csv(csv_path)
        # The pipeline controls the year-partition column itself (rather
        # than trusting the source file to carry one with this exact name)
        # so idempotent per-year delete+reload works regardless of how
        # INEP's real file is laid out — overwrites a same-named column if
        # the source happens to already have one.
        df["NU_ANO_CENSO"] = config.NU_ANO_CENSO
        load_dataframe_to_raw(
            df, "raw", "taxas_rendimento_uf", "NU_ANO_CENSO", config.NU_ANO_CENSO,
            config.get_postgres_uri(),
        )

    @task
    def load_raw_taxas_rendimento_municipios(csv_path: str):
        import pandas as pd

        df = pd.read_csv(csv_path)
        df["NU_ANO_CENSO"] = config.NU_ANO_CENSO
        load_dataframe_to_raw(
            df, "raw", "taxas_rendimento_municipios", "NU_ANO_CENSO", config.NU_ANO_CENSO,
            config.get_postgres_uri(),
        )

    @task
    def load_raw_distorcao(csv_path: str):
        import pandas as pd

        df = pd.read_csv(csv_path)
        df["NU_ANO_CENSO"] = config.NU_ANO_CENSO
        load_dataframe_to_raw(
            df, "raw", "distorcao_idade_serie_municipios", "NU_ANO_CENSO", config.NU_ANO_CENSO,
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
        # Full refresh, not year-partitioned delete: SIDRA's period-column
        # semantics (D2C vs D3C) are still pending confirmation — see
        # stg_populacao_municipios.sql — and the table is small enough that
        # a full replace is simplest and always correct.
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
    def data_quality_checks():
        """Extra sanity check beyond dbt tests: raw.escolas row count for
        Uauá/2024 must match the 38 schools already verified in
        notebooks/inep_qedu_data_dev.ipynb."""
        engine = create_engine(config.get_postgres_uri())
        with engine.connect() as conn:
            # raw.escolas columns are TEXT (see inep_pipeline/load.py), so
            # bind params are compared as strings, not ints.
            count = conn.execute(
                text(
                    'SELECT COUNT(*) FROM raw.escolas '
                    'WHERE "NU_ANO_CENSO" = :ano AND "CO_MUNICIPIO" = :municipio'
                ),
                {"ano": str(config.NU_ANO_CENSO), "municipio": str(config.CO_MUNICIPIO_ALVO)},
            ).scalar()
        if count != 38:
            raise AirflowException(
                f"Expected 38 schools for Uauá/{config.NU_ANO_CENSO} in "
                f"raw.escolas, found {count}."
            )
        logger.info("Data quality check passed: %s schools for Uauá/%s.", count, config.NU_ANO_CENSO)

    # ---- wiring ----------------------------------------------------------

    load_escolas = load_raw_escolas()
    load_rendimento_uf = load_raw_taxas_rendimento_uf(extract_taxas_rendimento_uf())
    load_rendimento_municipios = load_raw_taxas_rendimento_municipios(
        extract_taxas_rendimento_municipios()
    )
    load_distorcao = load_raw_distorcao(extract_distorcao_idade_serie())
    load_populacao = load_raw_populacao(extract_populacao_ibge())

    load_ideb_tasks = []
    for nivel, modalidade in config.IDEB_FONTES:
        suffix = f"{nivel}_{modalidade}"
        ideb_csv = extract_ideb.override(task_id=f"extract_ideb_{suffix}")(nivel, modalidade)
        load_ideb_tasks.append(
            load_raw_ideb.override(task_id=f"load_raw_ideb_{suffix}")(ideb_csv, nivel, modalidade)
        )

    quality = data_quality_checks()

    [
        load_escolas, load_rendimento_uf, load_rendimento_municipios,
        load_distorcao, load_populacao, *load_ideb_tasks,
    ] >> dbt_deps >> dbt_seed >> dbt_run >> dbt_test >> quality


inep_etl_dag()
