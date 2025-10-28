"""Downloader and parser for CVM InfDiario datasets."""
from __future__ import annotations

import logging
import zipfile
from datetime import date
from pathlib import Path
from typing import Iterable, List, Optional, Set

import pandas as pd

from ..common import download, normalization

LOGGER = logging.getLogger(__name__)

BASE_URL_FI = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS"
BASE_URL_FIM = "https://dados.cvm.gov.br/dados/FIM/DOC/INF_DIARIO/DADOS"


def build_monthly_urls(reference_months: Iterable[date]) -> List[str]:
    urls = []
    for month in reference_months:
        ym = month.strftime("%Y%m")
        urls.append(f"{BASE_URL_FI}/inf_diario_fi_{ym}.zip")
        urls.append(f"{BASE_URL_FIM}/inf_diario_fim_{ym}.zip")
    return urls


COLUMN_MAPPING = {
    "CNPJ_FUNDO": "cnpj",
    "CNPJ_FUNDO_CLASSE": "cnpj",
    "DT_COMPTC": "data_cotacao",
    "VL_TOTAL": "valor_total",
    "VL_QUOTA": "valor_cota",
    "VL_PATRIM_LIQ": "patrimonio_liquido",
    "CAPTC_DIA": "captacoes",
    "RESG_DIA": "resgates",
    "NR_COTST": "numero_cotistas",
}

DATE_COLUMNS = ["data_cotacao"]
CHUNKSIZE = 200_000


def _filter_chunk_by_cnpj(chunk: pd.DataFrame, *, cnpj_filter: Set[str]) -> pd.DataFrame:
    """Return only the rows whose CNPJ matches the monitored list."""
    cnpj_column = next(
        (col for col in ("CNPJ_FUNDO", "CNPJ_FUNDO_CLASSE") if col in chunk.columns),
        None,
    )
    if cnpj_column is None:
        return pd.DataFrame(columns=chunk.columns)

    normalized = chunk[cnpj_column].map(
        lambda value: normalization.normalize_cnpj(value) if isinstance(value, str) else value
    )
    mask = normalized.isin(cnpj_filter)
    if not mask.any():
        return pd.DataFrame(columns=chunk.columns)
    return chunk.loc[mask].copy()


def load_csv_from_archive(path: Path, *, cnpj_filter: Optional[Set[str]] = None) -> pd.DataFrame:
    # There is a single CSV inside each archive. We list files and pick the first.
    with zipfile.ZipFile(path) as zf:
        inner_files = [info for info in zf.infolist() if info.filename.endswith(".csv")]
        if not inner_files:
            raise ValueError(f"No CSV file found inside archive {path}")
        with zf.open(inner_files[0]) as fh:
            if not cnpj_filter:
                return pd.read_csv(fh, sep=";", decimal=",", dtype=str)

            filtered_frames: List[pd.DataFrame] = []
            first_columns: Optional[pd.Index] = None
            for chunk in pd.read_csv(
                fh,
                sep=";",
                decimal=",",
                dtype=str,
                chunksize=CHUNKSIZE,
            ):
                if first_columns is None:
                    first_columns = chunk.columns
                filtered = _filter_chunk_by_cnpj(chunk, cnpj_filter=cnpj_filter)
                if not filtered.empty:
                    filtered_frames.append(filtered)

        if not filtered_frames:
            if first_columns is None:
                return pd.DataFrame()
            return pd.DataFrame(columns=list(first_columns))
        return pd.concat(filtered_frames, ignore_index=True)


def parse_inf_diario(
    urls: Iterable[str],
    *,
    workdir: Path,
    cnpj_filter: Optional[Set[str]] = None,
) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    staging_dir = workdir / "cvm" / "inf_diario"
    staging_dir.mkdir(parents=True, exist_ok=True)

    for url in urls:
        try:
            zip_path = download.download_to_file(url, staging_dir / Path(url).name)
        except download.DownloadError as exc:
            LOGGER.error("Could not download %s: %s", url, exc)
            continue
        try:
            df = load_csv_from_archive(zip_path, cnpj_filter=cnpj_filter)
        except Exception as exc:  # pragma: no cover - network data dependent
            LOGGER.error("Failed to parse %s: %s", zip_path, exc)
            continue
        if not df.empty:
            frames.append(df)

    if not frames:
        raise RuntimeError("No InfDiario files were downloaded successfully")

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.rename(columns=COLUMN_MAPPING)
    merged = merged.loc[:, ~merged.columns.duplicated()]
    numeric_columns = [
        "valor_total",
        "valor_cota",
        "patrimonio_liquido",
        "captacoes",
        "resgates",
        "numero_cotistas",
    ]
    for column in numeric_columns:
        if column in merged.columns:
            merged[column] = pd.to_numeric(merged[column], errors="coerce")
    if "numero_cotistas" in merged.columns:
        merged["numero_cotistas"] = merged["numero_cotistas"].fillna(0).astype(int)
    merged["cnpj"] = merged["cnpj"].apply(normalization.normalize_cnpj)
    for column in DATE_COLUMNS:
        merged[column] = pd.to_datetime(merged[column], format="%Y-%m-%d", errors="coerce")
    merged["fonte"] = "CVM"
    return merged
