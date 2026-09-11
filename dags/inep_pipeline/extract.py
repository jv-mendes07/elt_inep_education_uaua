"""Extraction functions — one per source dataset.

Each function returns a pandas DataFrame ready to hand to
inep_pipeline.load.load_dataframe_to_raw. Network downloads are cached to
inep_pipeline.config.DATA_DIR so a DAG re-run doesn't re-fetch unnecessarily.
"""
from __future__ import annotations

import functools
import json
import logging
import os
import re
import tempfile
import zipfile
from pathlib import Path

import certifi
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import config

logger = logging.getLogger(__name__)

_CERTS_DIR = Path(__file__).parent / "certs"

# download.inep.gov.br's WAF drops connections whose User-Agent is the
# default `python-requests/x` (seen as SSL UNEXPECTED_EOF during the
# handshake) — a browser-like UA gets through. Retries cover the occasional
# genuine connection blip on their side.
_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


@functools.lru_cache(maxsize=1)
def _http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(_HTTP_HEADERS)
    retry = Retry(
        total=4,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


@functools.lru_cache(maxsize=1)
def _ca_bundle() -> str:
    """Path to a CA bundle = certifi's roots + every extra PEM in certs/.

    download.inep.gov.br serves only its leaf certificate (no intermediate),
    so `requests` can't build a chain to a trusted root on its own — the
    missing RNP/GlobalSign intermediate is committed under certs/. Written
    to one temp file, passed as `verify=` to every download. Harmless for
    hosts whose chain is already complete (e.g. IBGE SIDRA)."""
    extra = "".join(
        p.read_text(encoding="utf-8") for p in sorted(_CERTS_DIR.glob("*.pem"))
    )
    if not extra:
        return certifi.where()
    combined = Path(tempfile.gettempdir()) / "inep_ca_bundle.pem"
    content = Path(certifi.where()).read_text(encoding="utf-8") + "\n" + extra
    # Atomic write: several mapped-task processes call this concurrently and
    # would otherwise race on a half-written file (SSLError "[X509] PEM lib").
    tmp = combined.with_suffix(f".pem.{os.getpid()}")
    tmp.write_text(content, encoding="utf-8")
    os.replace(tmp, combined)
    return str(combined)


def _read_censo_csv(path: Path) -> pd.DataFrame:
    """Read one year's census CSV, keeping only the columns dbt consumes.

    `usecols` with a callable tolerates a column being absent in a given
    year; `reindex` then forces the exact `config.CENSO_COLUNAS` set and
    order for every year, so multi-year loads into the same all-TEXT
    raw.escolas table stay positionally consistent for COPY (a column that
    drifted in/out of the source would otherwise misalign the copy). Same
    read_csv dialect proven in notebooks/inep_qedu_data_dev.ipynb: ';'
    separator, latin-1, ',' decimal.
    """
    wanted = set(config.CENSO_COLUNAS)
    df = pd.read_csv(
        path,
        sep=";",
        encoding="latin-1",
        decimal=",",
        low_memory=False,
        usecols=lambda c: c in wanted,
    )
    missing = [c for c in config.CENSO_COLUNAS if c not in df.columns]
    if missing:
        logger.warning("Census file %s is missing columns %s — loaded as NULL.", path, missing)
    return df.reindex(columns=config.CENSO_COLUNAS)


def read_censo_escolar(ano: int) -> pd.DataFrame:
    """Read one year's school census, column-reduced to `config.CENSO_COLUNAS`.

    Prefers a file already dropped by hand under
    notebooks/data/*censo*<ano>*/ (the convention used to bootstrap 2024);
    otherwise downloads INEP's ~30 MB microdata zip and globs out the main
    `microdados_ed_basica_<ano>.csv` member (its internal path is not
    consistent year to year — see docs/data_sources.md).
    """
    for local in sorted(config.DATA_DIR.glob(config.censo_local_glob(ano))):
        logger.info("Using manually-downloaded census file for %s: %s", ano, local)
        return _read_censo_csv(local)

    url = config.get_censo_url(ano)
    dest_dir = config.DATA_DIR / f"microdados_censo_escolar_{ano}"
    downloaded = _download_file(url, dest_dir, f"microdados_censo_escolar_{ano}.zip")
    with zipfile.ZipFile(downloaded) as zf:
        members = [n for n in zf.namelist() if re.search(config.CENSO_MEMBRO_REGEX, n, re.IGNORECASE)]
        if not members:
            raise ValueError(f"No 'microdados_ed_basica' CSV found inside {downloaded}")
        member = members[0]
        zf.extract(member, dest_dir)
        return _read_censo_csv(dest_dir / member)


def _download_file(url: str, dest_dir: Path, filename: str) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / filename
    logger.info("Downloading %s -> %s", url, dest_path)
    response = _http_session().get(url, timeout=120, verify=_ca_bundle())
    response.raise_for_status()
    dest_path.write_bytes(response.content)
    return dest_path


def _read_tabular(path: Path, header_row: int = 0) -> pd.DataFrame:
    """`header_row` is 0-indexed (as in pandas' `header=` kwarg) — INEP's
    xlsx exports bury the machine-readable column-code row under several
    title/merged-header rows; see the *_HEADER_ROW constants in config.py.

    Spreadsheets are read with the calamine engine (Rust): openpyxl/odfpy
    load the whole workbook DOM into Python and peaked at multiple GB on
    INEP's ~65k-row files — enough to get the task OOM-killed in the
    container. calamine reads the same file in ~5s / <1GB.
    """
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, sep=";", encoding="latin-1", low_memory=False, header=header_row)
    if suffix in (".xlsx", ".xls", ".ods"):
        return pd.read_excel(path, header=header_row, engine="calamine")
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
            # INEP zips from 2019 on ship the SAME sheet as BOTH .xlsx and
            # .ods (plus an md5 .txt). Prefer .xlsx — .ods via odfpy is
            # several times slower and much heavier on memory.
            _pref = {".xlsx": 0, ".xls": 1, ".csv": 2, ".ods": 3}
            candidates = sorted(
                (n for n in zf.namelist() if Path(n).suffix.lower() in _pref),
                key=lambda n: (_pref[Path(n).suffix.lower()], n),
            )
            if not candidates:
                raise ValueError(f"No tabular file found inside {downloaded}")
            internal_path = candidates[0]
            logger.warning(
                "No internal path Variable set for %s; using best tabular "
                "member found: %s. Set Airflow Variable '%s' to pin this.",
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


def extract_taxas_rendimento_uf(ano: int) -> pd.DataFrame:
    """Brasil/Região/UF grain — no CO_MUNICIPIO column; used as the
    BRASIL/REGIAO/UF benchmark rows (see stg_taxas_rendimento_uf.sql)."""
    return _extract_with_fallback(
        config.DATA_DIR / f"taxas_rendimento_{ano}",
        config.get_taxas_rendimento_uf_url(ano),
        "inep_taxas_rendimento_uf_url_internal_path",
        config.TAXAS_RENDIMENTO_HEADER_ROW,
        f"taxas de rendimento {ano} (Brasil/UF)",
    )


def extract_taxas_rendimento_municipios(ano: int) -> pd.DataFrame:
    """Município grain — has CO_MUNICIPIO/NO_MUNICIPIO; this is what makes
    Uauá appear in the dashboard (see stg_taxas_rendimento_municipios.sql)."""
    return _extract_with_fallback(
        config.DATA_DIR / f"taxas_rendimento_{ano}_municipios",
        config.get_taxas_rendimento_municipios_url(ano),
        "inep_taxas_rendimento_municipios_url_internal_path",
        config.TAXAS_RENDIMENTO_HEADER_ROW,
        f"taxas de rendimento {ano} (Municípios)",
    )


def extract_distorcao_idade_serie(ano: int) -> pd.DataFrame:
    """Município grain (the only one sourced so far — see docs/data_sources.md)."""
    return _extract_with_fallback(
        config.DATA_DIR / f"distorcao_idade_serie_{ano}_municipios",
        config.get_distorcao_municipios_url(ano),
        "inep_distorcao_municipios_url_internal_path",
        config.DISTORCAO_HEADER_ROW,
        f"distorção idade-série {ano} (Municípios)",
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


def extract_populacao_ibge(anos: list[int] | None = None) -> pd.DataFrame:
    """Pull município population estimates from the IBGE SIDRA API for the
    period covering `anos` (one call for the whole range).

    The /values endpoint returns a JSON array whose first element is a
    header-labels row (not data) — skipped here. Column keys (D1C, D1N, V,
    D3C, ...) are the SIDRA API's own field names, preserved as-is; D3C is
    the year (confirmed against a live call — see stg_populacao_municipios.sql).
    """
    url = config.get_ibge_populacao_url(anos)
    logger.info("Fetching IBGE SIDRA population data: %s", url)
    response = _http_session().get(url, timeout=60, verify=_ca_bundle())
    response.raise_for_status()
    rows = json.loads(response.text)
    # SIDRA's /values endpoint always returns a row 0 holding the field
    # *labels* (e.g. {"D1C": "Município (Código)", ...}) rather than data —
    # drop it so raw.populacao_municipios only holds actual observations.
    if rows:
        rows = rows[1:]
    return pd.DataFrame(rows)
