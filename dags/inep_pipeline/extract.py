"""Extraction functions — one per source dataset.

Each function returns a pandas DataFrame ready to hand to
inep_pipeline.load.load_dataframe_to_raw. Network downloads are cached to
inep_pipeline.config.DATA_DIR so a DAG re-run doesn't re-fetch unnecessarily.
"""
from __future__ import annotations

import json
import logging
import zipfile
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)


def read_censo_escolar() -> pd.DataFrame:
    """Read the already-local 2024 school census CSV.

    Same read_csv parameters proven in notebooks/inep_qedu_data_dev.ipynb —
    INEP's census files use ';' separators, latin-1 encoding, and ','
    decimals.
    """
    return pd.read_csv(
        config.CENSO_CSV_PATH,
        sep=";",
        encoding="latin-1",
        decimal=",",
        low_memory=False,
    )


def _download_file(url: str, dest_dir: Path, filename: str) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / filename
    logger.info("Downloading %s -> %s", url, dest_path)
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    dest_path.write_bytes(response.content)
    return dest_path


def _read_tabular(path: Path, header_row: int = 0) -> pd.DataFrame:
    """`header_row` is 0-indexed (as in pandas' `header=` kwarg) — INEP's
    xlsx exports bury the machine-readable column-code row under several
    title/merged-header rows; see the *_HEADER_ROW constants in config.py.
    """
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, sep=";", encoding="latin-1", low_memory=False, header=header_row)
    if path.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(path, header=header_row)
    if path.suffix.lower() == ".ods":
        return pd.read_excel(path, engine="odf", header=header_row)
    raise ValueError(f"Unsupported tabular file extension: {path.suffix}")


def _download_and_read(
    url: str,
    dest_dir: Path,
    internal_path_var: str | None,
    header_row: int = 0,
) -> pd.DataFrame:
    """Download `url`, unzip if needed, and read the target file.

    If the download is a .zip and `internal_path_var` is set, the Airflow
    Variable of that name must hold the path *inside the zip* of the file to
    read (INEP zips commonly bundle Brasil/UF/Município/Escola files
    together — see docs/data_sources.md). Falls back to reading the first
    tabular file found in the zip if the Variable is unset, logging a
    warning so it's obvious in the task logs that the choice was implicit.
    """
    from airflow.models import Variable  # local import: keep module airflow-optional for tests

    filename = url.split("/")[-1] or "download"
    downloaded = _download_file(url, dest_dir, filename)

    if downloaded.suffix.lower() != ".zip":
        return _read_tabular(downloaded, header_row=header_row)

    with zipfile.ZipFile(downloaded) as zf:
        internal_path = (
            Variable.get(internal_path_var, default_var=None)
            if internal_path_var
            else None
        )
        if internal_path is None:
            candidates = [
                n
                for n in zf.namelist()
                if n.lower().endswith((".csv", ".xlsx", ".xls", ".ods"))
            ]
            if not candidates:
                raise ValueError(f"No tabular file found inside {downloaded}")
            internal_path = candidates[0]
            logger.warning(
                "No internal path Variable set for %s; defaulting to first "
                "tabular member found: %s. Set Airflow Variable '%s' to "
                "pin this explicitly.",
                downloaded, internal_path, internal_path_var,
            )
        extracted_dir = dest_dir / downloaded.stem
        zf.extract(internal_path, extracted_dir)
        return _read_tabular(extracted_dir / internal_path, header_row=header_row)


def _manual_override(dest_dir: Path) -> Path | None:
    """Look for a file the operator dropped by hand in `dest_dir` (any
    .csv/.xlsx/.xls/.ods) before attempting a network download — the
    workflow used to confirm the taxas de rendimento / distorção / IDEB
    schemas in the first place. See docs/data_sources.md."""
    if not dest_dir.exists():
        return None
    candidates = sorted(
        p for p in dest_dir.rglob("*") if p.suffix.lower() in (".csv", ".xlsx", ".xls", ".ods")
    )
    return candidates[0] if candidates else None


def _extract_with_fallback(
    dest_dir: Path,
    url: str,
    internal_path_var: str,
    header_row: int,
    label: str,
) -> pd.DataFrame:
    manual = _manual_override(dest_dir)
    if manual is not None:
        logger.info("Using manually-downloaded %s file: %s", label, manual)
        return _read_tabular(manual, header_row=header_row)

    return _download_and_read(url, dest_dir, internal_path_var, header_row=header_row)


def extract_taxas_rendimento_uf() -> pd.DataFrame:
    """Brasil/Região/UF grain — no CO_MUNICIPIO column; used as the
    BRASIL/REGIAO/UF benchmark rows (see stg_taxas_rendimento_uf.sql)."""
    return _extract_with_fallback(
        config.DATA_DIR / "taxas_rendimento_2024",
        config.get_variable_url(
            config.VAR_TAXAS_RENDIMENTO_UF_URL, config.DEFAULT_TAXAS_RENDIMENTO_UF_URL
        ),
        "inep_taxas_rendimento_uf_url_internal_path",
        config.TAXAS_RENDIMENTO_HEADER_ROW,
        "taxas de rendimento (Brasil/UF)",
    )


def extract_taxas_rendimento_municipios() -> pd.DataFrame:
    """Município grain — has CO_MUNICIPIO/NO_MUNICIPIO; this is what makes
    Uauá appear in the dashboard (see stg_taxas_rendimento_municipios.sql)."""
    return _extract_with_fallback(
        config.DATA_DIR / "taxas_rendimento_2024_municipios",
        config.get_variable_url(
            config.VAR_TAXAS_RENDIMENTO_MUNICIPIOS_URL,
            config.DEFAULT_TAXAS_RENDIMENTO_MUNICIPIOS_URL,
        ),
        "inep_taxas_rendimento_municipios_url_internal_path",
        config.TAXAS_RENDIMENTO_HEADER_ROW,
        "taxas de rendimento (Municípios)",
    )


def extract_distorcao_idade_serie() -> pd.DataFrame:
    """Município grain (the only one sourced so far — see docs/data_sources.md)."""
    return _extract_with_fallback(
        config.DATA_DIR / "distorcao_idade_serie_2024_municipios",
        config.get_variable_url(
            config.VAR_DISTORCAO_MUNICIPIOS_URL, config.DEFAULT_DISTORCAO_MUNICIPIOS_URL
        ),
        "inep_distorcao_municipios_url_internal_path",
        config.DISTORCAO_HEADER_ROW,
        "distorção idade-série (Municípios)",
    )


def extract_ideb(nivel: str, modalidade: str) -> pd.DataFrame:
    """Extract one IDEB (nível de agregação, modalidade) file — each file
    already spans every edition (2019/2021/2023/2025) as wide columns; see
    config.IDEB_FONTES.

    Falls back to a manually-downloaded local file if present — INEP's
    "Ideb por Escola" results now live behind an interactive BI panel
    rather than a guaranteed bulk-download file for every combination (see
    the IDEB note in docs/data_sources.md). Drop the file at
    notebooks/data/ideb_2019_2025/<nivel>_<modalidade>/ (any name) and this
    function picks it up instead of attempting a download.
    """
    dest_dir = config.DATA_DIR / "ideb_2019_2025"
    manual = _manual_override(dest_dir / f"{nivel}_{modalidade}") or _manual_override(dest_dir)
    if manual is not None:
        logger.info("Using manually-downloaded IDEB %s/%s file: %s", nivel, modalidade, manual)
        df = _read_tabular(manual, header_row=config.IDEB_HEADER_ROW)
    else:
        url = config.get_ideb_url(nivel, modalidade)
        df = _download_and_read(
            url, dest_dir, f"inep_ideb_url_{nivel}_{modalidade}_internal_path",
            header_row=config.IDEB_HEADER_ROW,
        )

    if nivel in ("brasil", "ufs"):
        # Only the brasil/ufs template's first two columns (unidade
        # geográfica, rede de ensino) lack a machine-readable code in the
        # source header row — pandas would otherwise name them
        # "Unnamed: 0"/"Unnamed: 1" (confirmed against
        # divulgacao_brasil_Ensino_Medio_integrado_ideb_2025.xlsx). The
        # município template already has real codes here (SG_UF,
        # CO_MUNICIPIO, NO_MUNICIPIO, REDE — confirmed against
        # divulgacao_anos_iniciais_municipios_2025.xlsx), so this rename
        # must NOT run for nivel="municipios"/"escolas" or it would clobber
        # those.
        df = df.rename(columns={df.columns[0]: "UNIDADE_GEOGRAFICA", df.columns[1]: "REDE"})
    return df


def extract_populacao_ibge() -> pd.DataFrame:
    """Pull município population estimates from the IBGE SIDRA API.

    The /values endpoint returns a JSON array whose first element is a
    header-labels row (not data) — skipped here. Column keys (D1C, D1N, V,
    ...) are the SIDRA API's own field names, preserved as-is; see the
    TODO(verificar) note in stg_populacao_municipios.sql.
    """
    url = config.get_ibge_populacao_url()
    logger.info("Fetching IBGE SIDRA population data: %s", url)
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    rows = json.loads(response.text)
    # SIDRA's /values endpoint always returns a row 0 holding the field
    # *labels* (e.g. {"D1C": "Município (Código)", ...}) rather than data —
    # drop it so raw.populacao_municipios only holds actual observations.
    if rows:
        rows = rows[1:]
    return pd.DataFrame(rows)
