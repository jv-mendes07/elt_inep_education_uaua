"""Reusable idempotent loader for the `raw` schema.

Ports the pattern already proven in notebooks/inep_qedu_data_dev.ipynb:
create the schema/table from the dataframe's own columns if needed, delete
any existing rows matching the partition being (re)loaded, then bulk-load
via COPY FROM STDIN (much faster than row-by-row INSERT for the full
national census). Column names are preserved exactly as they appear in the
source dataframe — see the note in dbt/inep_dbt/models/staging/_sources.yml
about staging models needing to quote them.
"""
from __future__ import annotations

import io
import logging

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


def _table_exists(conn, schema: str, table: str) -> bool:
    result = conn.execute(
        text("SELECT to_regclass(:qualified)"),
        {"qualified": f"{schema}.{table}"},
    ).scalar()
    return result is not None


def _table_columns(conn, schema: str, table: str) -> list[str]:
    rows = conn.execute(
        text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = :s AND table_name = :t ORDER BY ordinal_position"
        ),
        {"s": schema, "t": table},
    ).fetchall()
    return [r[0] for r in rows]


def _align_to_table(df: pd.DataFrame, existing_cols: list[str], schema: str, table: str) -> pd.DataFrame:
    """Reindex `df` to the raw table's existing column set/order before COPY.

    Raw tables are appended to across years/partitions; a source column that
    drifted in or out of a later year's file (INEP does this — an extra IDEB
    edition, a new etapa column) would otherwise misalign the positional
    COPY and raise BadCopyFileFormat. Extra source columns are dropped
    (logged); columns absent from this partition are filled NULL.
    """
    df_cols = set(df.columns)
    dropped = [c for c in df.columns if c not in existing_cols]
    added = [c for c in existing_cols if c not in df_cols]
    if dropped:
        logger.warning("%s.%s: source columns not in table, dropped: %s", schema, table, dropped)
    if added:
        logger.warning("%s.%s: table columns absent from this load, filled NULL: %s", schema, table, added)
    return df.reindex(columns=existing_cols)


def _create_table_from_df(conn, df: pd.DataFrame, schema: str, table: str) -> None:
    """Create `{schema}.{table}` with every column typed TEXT, matching
    `df`'s columns exactly (case preserved).

    Deliberately NOT using pandas' `df.to_sql(...)` for this: pandas >=2.0
    only recognizes a SQLAlchemy engine/connection as "connectable" when
    SQLAlchemy itself is >=2.0 (see `pandas.compat._optional.VERSIONS`) —
    this project pins SQLAlchemy 1.4.x to match Airflow 2.9's own
    constraint, so `to_sql` silently falls back to a legacy DBAPI2 code
    path that assumes a raw `.cursor()`-bearing connection and raises
    `AttributeError: 'Connection'/'Engine' object has no attribute
    'cursor'`. Typing every raw column as TEXT sidesteps the conflict
    entirely and is a better fit anyway — every staging model already casts
    explicitly (see dbt/inep_dbt/models/staging/*.sql), so the raw layer
    never needed pandas' dtype inference.
    """
    columns_ddl = ", ".join(f'"{col}" text' for col in df.columns)
    conn.execute(text(f"CREATE TABLE {schema}.{table} ({columns_ddl})"))


def _copy_dataframe(df: pd.DataFrame, schema: str, table: str, pg_conn_uri: str) -> None:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
    buffer.seek(0)

    raw_conn = psycopg2.connect(pg_conn_uri.replace("postgresql+psycopg2://", "postgresql://"))
    cur = None
    try:
        cur = raw_conn.cursor()
        cur.copy_expert(
            f"COPY {schema}.{table} FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')",
            buffer,
        )
        raw_conn.commit()
        logger.info("Loaded %s rows into %s.%s via COPY.", len(df), schema, table)
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        if cur is not None:
            cur.close()
        raw_conn.close()


def load_dataframe_full_refresh(
    df: pd.DataFrame,
    schema: str,
    table: str,
    pg_conn_uri: str,
) -> int:
    """Replace `{schema}.{table}` wholesale with `df`.

    For small reference datasets where a partition-key-based idempotent
    delete (see load_dataframe_partitioned) doesn't apply cleanly — e.g. the
    IBGE population reference table, whose exact period-column semantics are
    still pending confirmation (see stg_populacao_municipios.sql). A full
    replace is always correct regardless of that.

    TRUNCATE (not DROP+CREATE) when the table already exists: once dbt has
    run once, staging views depend on these raw tables, and DROP TABLE
    fails with "cannot drop table because other objects depend on it" —
    confirmed by running the DAG a second time. TRUNCATE clears the data
    without touching the table object those views reference.
    """
    engine = create_engine(pg_conn_uri)
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        if _table_exists(conn, schema, table):
            df = _align_to_table(df, _table_columns(conn, schema, table), schema, table)
            conn.execute(text(f"TRUNCATE TABLE {schema}.{table}"))
        else:
            _create_table_from_df(conn, df, schema, table)

    _copy_dataframe(df, schema, table, pg_conn_uri)
    logger.info("Full-refreshed %s.%s with %s rows.", schema, table, len(df))
    return len(df)


def load_dataframe_partitioned(
    df: pd.DataFrame,
    schema: str,
    table: str,
    key_values: dict,
    pg_conn_uri: str,
) -> int:
    """Idempotently (re)load `df` into `{schema}.{table}` for the partition
    identified by `key_values` (e.g. {"NU_ANO_CENSO": 2024} or
    {"NIVEL_AGREGACAO": "BRASIL", "MODALIDADE": "ensino_medio_integrado"}).

    Returns the number of rows loaded. Safe to re-run: existing rows
    matching every key in `key_values` are deleted before the new rows are
    inserted, so a task retry or a manual re-trigger never duplicates data.
    Multiple partitions can share one table (e.g. raw.ideb across several
    IDEB modalidade/nível combos) without clobbering each other.
    """
    engine = create_engine(pg_conn_uri)

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

        if _table_exists(conn, schema, table):
            df = _align_to_table(df, _table_columns(conn, schema, table), schema, table)
            # Every raw.* column is TEXT (see _create_table_from_df), so
            # bind params are stringified here — an int/str bind parameter
            # compared against a TEXT column raises "operator does not
            # exist: text = integer" otherwise.
            where_clause = " AND ".join(f'"{col}" = :{col}' for col in key_values)
            conn.execute(
                text(f"DELETE FROM {schema}.{table} WHERE {where_clause}"),
                {col: str(val) for col, val in key_values.items()},
            )
            logger.info("Removed existing %s rows for %s before reload.", table, key_values)
        else:
            _create_table_from_df(conn, df, schema, table)
            logger.info("Created %s.%s from dataframe schema (first load).", schema, table)

    _copy_dataframe(df, schema, table, pg_conn_uri)
    return len(df)


def load_dataframe_to_raw(
    df: pd.DataFrame,
    schema: str,
    table: str,
    year_col: str,
    year_value,
    pg_conn_uri: str,
) -> int:
    """Backwards-compatible single-year-key wrapper around
    load_dataframe_partitioned — kept as the simple entry point for sources
    partitioned by a single year column (escolas, taxas_rendimento,
    distorcao_idade_serie)."""
    return load_dataframe_partitioned(df, schema, table, {year_col: year_value}, pg_conn_uri)
