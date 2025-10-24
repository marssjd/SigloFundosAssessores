"""Downloader and parser for CVM InfMensal datasets."""
from __future__ import annotations

import csv
import io
import logging
import sys
import zipfile
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from ..common import download, normalization

LOGGER = logging.getLogger(__name__)

try:
    csv.field_size_limit(sys.maxsize)
except (OverflowError, ValueError):
    csv.field_size_limit(10**7)

BASE_URL = "https://dados.cvm.gov.br/dados/FI/DOC/INF_MENSAL/DADOS"
BASE_URL_CDA = "https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS"
BASE_URL_PERFIL = "https://dados.cvm.gov.br/dados/FI/DOC/PERFIL_MENSAL/DADOS"

HOLDINGS_USECOLS = {
    "CNPJ_FUNDO_CLASSE",
    "DT_COMPTC",
    "TP_ATIVO",
    "TP_APLIC",
    "EMISSOR",
    "EMISSOR_LIGADO",
    "DS_ATIVO",
    "DS_ATIVO_EXTERIOR",
    "NM_FUNDO_CLASSE_SUBCLASSE_COTA",
    "CD_ISIN",
    "CD_ATIVO",
    "CD_ATIVO_BV_MERC",
    "VL_MERC_POS_FINAL",
}

PL_USECOLS = {"CNPJ_FUNDO_CLASSE", "DT_COMPTC", "VL_PATRIM_LIQ"}

COTISTAS_COUNT_PREFIX = "NR_COTST_"

HOLDINGS_COLUMNS_ORDER = [
    "cnpj",
    "data_referencia",
    "tipo_ativo",
    "emissor",
    "isin",
    "valor_mercado",
]


def build_monthly_urls(reference_months: Iterable[date]) -> List[str]:
    urls = []
    for month in reference_months:
        ym = month.strftime("%Y%m")
        urls.append(f"{BASE_URL}/inf_mensal_fi_{ym}.zip")
    return urls


def load_csv_from_archive(path: Path, *, pattern: str) -> pd.DataFrame:
    with zipfile.ZipFile(path) as zf:
        inner_files = [info for info in zf.infolist() if pattern in info.filename]
        if not inner_files:
            raise ValueError(f"No {pattern} file found inside {path}")
        with zf.open(inner_files[0]) as fh:
            df = pd.read_csv(fh, sep=";", decimal=",", dtype=str)
    return df


def _safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _normalize_cnpj_series(series: pd.Series) -> pd.Series:
    return series.fillna("").apply(normalization.normalize_cnpj)


def _parse_decimal(value: str | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        if "," in cleaned:
            cleaned = cleaned.replace(".", "").replace(",", ".")
    else:
        cleaned = value
    try:
        return float(cleaned)
    except (TypeError, ValueError):
        return None


def _load_cda_zip(zip_path: Path, cnpj_filter: set[str] | None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    holdings_records: List[Dict[str, object]] = []
    pl_records: List[Dict[str, object]] = []
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            lower_name = name.lower()
            if not lower_name.endswith(".csv"):
                continue
            try:
                fh = zf.open(name)
            except KeyError:
                continue
            with io.TextIOWrapper(fh, encoding="latin1", newline="") as text_fh:
                reader = csv.DictReader(text_fh, delimiter=";", quotechar='"')
                for row in reader:
                    try:
                        raw_cnpj = row.get("CNPJ_FUNDO_CLASSE") or row.get("CNPJ_FUNDO")
                        cnpj = normalization.normalize_cnpj(raw_cnpj or "")
                        if not cnpj or (cnpj_filter and cnpj not in cnpj_filter):
                            continue
                        data_ref = row.get("DT_COMPTC")
                        if not data_ref:
                            continue
                        if "blc" in lower_name:
                            tipo_ativo = row.get("TP_ATIVO") or row.get("TP_APLIC")
                            emissor = next(
                                (
                                    row.get(col)
                                    for col in [
                                        "EMISSOR",
                                        "DS_ATIVO",
                                        "DS_ATIVO_EXTERIOR",
                                        "NM_FUNDO_CLASSE_SUBCLASSE_COTA",
                                        "EMISSOR_LIGADO",
                                    ]
                                    if row.get(col)
                                ),
                                None,
                            )
                            isin = next(
                                (
                                    row.get(col)
                                    for col in ["CD_ISIN", "CD_ATIVO", "CD_ATIVO_BV_MERC"]
                                    if row.get(col)
                                ),
                                None,
                            )
                            valor = _parse_decimal(row.get("VL_MERC_POS_FINAL"))
                            if valor is None or valor <= 0:
                                continue
                            holdings_records.append(
                                {
                                    "cnpj": cnpj,
                                    "data_referencia": data_ref,
                                    "tipo_ativo": tipo_ativo,
                                    "emissor": emissor,
                                    "isin": isin,
                                    "valor_mercado": valor,
                                }
                            )
                        elif "_pl_" in lower_name:
                            valor = _parse_decimal(row.get("VL_PATRIM_LIQ"))
                            if valor is None:
                                continue
                            pl_records.append(
                                {
                                    "cnpj": cnpj,
                                    "data_referencia": data_ref,
                                    "patrimonio_liquido": valor,
                                }
                            )
                    except csv.Error:
                        continue

    holdings = pd.DataFrame.from_records(holdings_records, columns=HOLDINGS_COLUMNS_ORDER)
    if not holdings.empty:
        holdings["data_referencia"] = pd.to_datetime(
            holdings["data_referencia"], errors="coerce"
        )
        holdings = holdings.dropna(subset=["cnpj", "data_referencia"])
        holdings["fonte"] = "CVM"
        holdings = (
            holdings.groupby(
                ["cnpj", "data_referencia", "tipo_ativo", "emissor", "isin", "fonte"],
                as_index=False,
            )["valor_mercado"]
            .sum()
        )

    pl = pd.DataFrame.from_records(pl_records, columns=["cnpj", "data_referencia", "patrimonio_liquido"])
    if not pl.empty:
        pl["data_referencia"] = pd.to_datetime(pl["data_referencia"], errors="coerce")
        pl = pl.dropna(subset=["cnpj", "data_referencia"])
        pl = (
            pl.groupby(["cnpj", "data_referencia"], as_index=False)["patrimonio_liquido"]
            .sum()
        )

    return holdings, pl


def _load_perfil_csv(path: Path, cnpj_filter: set[str] | None) -> pd.DataFrame:
    records: List[Dict[str, object]] = []
    with path.open("r", encoding="latin1", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=";", quotechar='"')
        for row in reader:
            try:
                cnpj = normalization.normalize_cnpj(row.get("CNPJ_FUNDO_CLASSE", ""))
                if not cnpj or (cnpj_filter and cnpj not in cnpj_filter):
                    continue
                data_ref = row.get("DT_COMPTC")
                if not data_ref:
                    continue
                total = 0.0
                for key, value in row.items():
                    if key and key.startswith(COTISTAS_COUNT_PREFIX):
                        parsed = _parse_decimal(value)
                        if parsed:
                            total += parsed
                records.append(
                    {
                        "cnpj": cnpj,
                        "data_referencia": data_ref,
                        "numero_cotistas": int(round(total)),
                    }
                )
            except csv.Error:
                continue

    df = pd.DataFrame.from_records(records, columns=["cnpj", "data_referencia", "numero_cotistas"])
    if df.empty:
        return df
    df["data_referencia"] = pd.to_datetime(df["data_referencia"], errors="coerce")
    df = df.dropna(subset=["cnpj", "data_referencia"])
    return df


def parse_inf_mensal_fallback(
    reference_months: Iterable[date], *, workdir: Path, cnpj_filter: Iterable[str] | None = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    holdings_frames: List[pd.DataFrame] = []
    pl_frames: List[pd.DataFrame] = []
    perfil_frames: List[pd.DataFrame] = []

    cda_dir = workdir / "cvm" / "cda"
    perfil_dir = workdir / "cvm" / "perfil_mensal"
    cda_dir.mkdir(parents=True, exist_ok=True)
    perfil_dir.mkdir(parents=True, exist_ok=True)

    normalized_filter = (
        {normalization.normalize_cnpj(cnpj) for cnpj in cnpj_filter} if cnpj_filter else None
    )

    for month in reference_months:
        ym = month.strftime("%Y%m")
        cda_url = f"{BASE_URL_CDA}/cda_fi_{ym}.zip"
        try:
            zip_path = download.download_to_file(cda_url, cda_dir / f"cda_fi_{ym}.zip")
            holdings_df, pl_df = _load_cda_zip(zip_path, normalized_filter)
            if not holdings_df.empty:
                holdings_frames.append(holdings_df)
            if not pl_df.empty:
                pl_frames.append(pl_df)
        except download.DownloadError as exc:
            LOGGER.error("Could not download %s: %s", cda_url, exc)

        perfil_url = f"{BASE_URL_PERFIL}/perfil_mensal_fi_{ym}.csv"
        try:
            perfil_path = download.download_to_file(
                perfil_url, perfil_dir / f"perfil_mensal_fi_{ym}.csv"
            )
            perfil_df = _load_perfil_csv(perfil_path, normalized_filter)
            if not perfil_df.empty:
                perfil_frames.append(perfil_df)
        except download.DownloadError as exc:
            LOGGER.error("Could not download %s: %s", perfil_url, exc)

    holdings = (
        pd.concat(holdings_frames, ignore_index=True) if holdings_frames else pd.DataFrame()
    )

    perfil = (
        pd.concat(perfil_frames, ignore_index=True) if perfil_frames else pd.DataFrame()
    )

    cotistas_columns = ["cnpj", "data_referencia", "numero_cotistas", "patrimonio_liquido"]
    if perfil.empty and not pl_frames:
        return holdings, pd.DataFrame(columns=cotistas_columns)

    pl = pd.concat(pl_frames, ignore_index=True) if pl_frames else pd.DataFrame()
    if not perfil.empty:
        perfil = (
            perfil.dropna(subset=["cnpj", "data_referencia"])
            .groupby(["cnpj", "data_referencia"], as_index=False)
            .agg({"numero_cotistas": "sum"})
        )
    if not pl.empty:
        pl = pl.dropna(subset=["cnpj", "data_referencia"])

    if perfil.empty:
        cotistas = pl.rename(columns={"patrimonio_liquido": "patrimonio_liquido"}).assign(
            numero_cotistas=pd.NA
        )
    else:
        cotistas = perfil.merge(pl, on=["cnpj", "data_referencia"], how="left")

    if "patrimonio_liquido" in cotistas.columns:
        cotistas["patrimonio_liquido"] = _safe_numeric(cotistas["patrimonio_liquido"])
    if "numero_cotistas" in cotistas.columns:
        cotistas["numero_cotistas"] = _safe_numeric(cotistas["numero_cotistas"]).fillna(0).round()
        cotistas["numero_cotistas"] = cotistas["numero_cotistas"].astype(int, errors="ignore")

    cotistas["fonte"] = "CVM"
    if not holdings.empty:
        holdings["fonte"] = "CVM"

    return holdings, cotistas.reindex(columns=cotistas_columns + ["fonte"])


def parse_inf_mensal(urls: Iterable[str], *, workdir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    carteira_frames: List[pd.DataFrame] = []
    cotistas_frames: List[pd.DataFrame] = []
    staging_dir = workdir / "cvm" / "inf_mensal"
    staging_dir.mkdir(parents=True, exist_ok=True)

    for url in urls:
        try:
            zip_path = download.download_to_file(url, staging_dir / Path(url).name)
        except download.DownloadError as exc:
            LOGGER.error("Could not download %s: %s", url, exc)
            continue
        try:
            carteira_frames.append(
                load_csv_from_archive(zip_path, pattern="carteira")
                .rename(columns={"CNPJ_FUNDO": "cnpj", "CNPJ_FUNDO_CLASSE": "cnpj"})
                .loc[:, lambda df: ~df.columns.duplicated()]
            )
        except Exception as exc:  # pragma: no cover - depends on remote file
            LOGGER.warning("Failed to load carteira data from %s: %s", zip_path, exc)
        try:
            cotistas_frames.append(
                load_csv_from_archive(zip_path, pattern="cotist")
                .rename(columns={"CNPJ_FUNDO": "cnpj", "CNPJ_FUNDO_CLASSE": "cnpj"})
                .loc[:, lambda df: ~df.columns.duplicated()]
            )
        except Exception as exc:  # pragma: no cover
            LOGGER.warning("Failed to load cotistas data from %s: %s", zip_path, exc)

    if not carteira_frames and not cotistas_frames:
        LOGGER.warning("No InfMensal files were downloaded successfully")
        empty_cols = ["cnpj", "data_referencia"]
        return (
            pd.DataFrame(columns=empty_cols),
            pd.DataFrame(columns=empty_cols),
        )

    carteira = (
        pd.concat(carteira_frames, ignore_index=True)
        if carteira_frames
        else pd.DataFrame()
    )
    cotistas = (
        pd.concat(cotistas_frames, ignore_index=True)
        if cotistas_frames
        else pd.DataFrame()
    )

    for df in (carteira, cotistas):
        if df.empty:
            continue
        if "cnpj" in df.columns:
            df["cnpj"] = df["cnpj"].apply(normalization.normalize_cnpj)
        if "data_referencia" in df.columns:
            df["data_referencia"] = pd.to_datetime(
                df["data_referencia"], format="%Y-%m-%d", errors="coerce"
            )
        df["fonte"] = "CVM"
        numeric_cols = [
            col
            for col in ["valor_mercado", "quantidade", "numero_cotistas", "patrimonio_liquido"]
            if col in df.columns
        ]
        for column in numeric_cols:
            df[column] = pd.to_numeric(df[column], errors="coerce")
        if "numero_cotistas" in df.columns:
            df["numero_cotistas"] = df["numero_cotistas"].fillna(0).astype(int)

    return carteira, cotistas
