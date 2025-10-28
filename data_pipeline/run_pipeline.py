"""Command line entry-point for running the Siglo Fundos data pipeline."""
from __future__ import annotations

import json
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import pandas as pd
import typer
from dotenv import load_dotenv

from .b3 import pipeline as b3_pipeline
from .common import bigquery, config, io, logging_utils, normalization, sheets
from .cvm.pipeline import CVMPipeline
from .mais_retorno import fallback as mais_retorno_fallback

APP = typer.Typer(help="Pipeline de ingestao de dados da CVM/B3 para BigQuery")
LOGGER = logging.getLogger(__name__)


def load_environment() -> None:
    load_dotenv()
    logging_utils.configure_logging()


def get_config(config_path: Path | None = None) -> config.PipelineConfig:
    if config_path is None:
        config_path = Path("config/pipeline.yaml")
    cfg = config.load_config(config_path)
    cfg.bigquery_project = os.getenv("BIGQUERY_PROJECT", cfg.bigquery_project)
    cfg.bigquery_dataset_staging = os.getenv(
        "BIGQUERY_DATASET_STAGING", cfg.bigquery_dataset_staging
    )
    cfg.bigquery_dataset_curated = os.getenv(
        "BIGQUERY_DATASET_CURATED", cfg.bigquery_dataset_curated
    )
    cfg.gcs_bucket = os.getenv("GCS_BUCKET", cfg.gcs_bucket)

    sheet_cnpjs = sheets.load_cnpjs_from_sheet()
    if sheet_cnpjs:
        normalized_sheet = {normalization.normalize_cnpj(cnpj) for cnpj in sheet_cnpjs}
        filtered_fundos = [
            fund
            for fund in cfg.fundos
            if normalization.normalize_cnpj(fund.cnpj) in normalized_sheet
        ]
        missing = sorted(
            normalized_sheet
            - {normalization.normalize_cnpj(fund.cnpj) for fund in cfg.fundos}
        )
        if missing:
            LOGGER.warning(
                "Os seguintes CNPJs estão na planilha, mas não no YAML: %s",
                ", ".join(missing),
            )
        if not filtered_fundos:
            raise RuntimeError(
                "Nenhum fundo do config/pipeline.yaml corresponde aos CNPJs listados na planilha."
            )
        cfg.fundos = filtered_fundos
        LOGGER.info("Lista de fundos filtrada para %s registros via Google Sheets.", len(cfg.fundos))
    elif os.getenv("SHEETS_SPREADSHEET_ID"):
        raise RuntimeError(
            "A planilha informada não retornou CNPJs válidos. Verifique a coluna configurada."
        )

    return cfg


def collect_all_data(cfg: config.PipelineConfig, workdir: Path) -> Dict[str, pd.DataFrame]:
    cvm_runner = CVMPipeline(cfg, workdir=workdir)
    tables = cvm_runner.run()

    if cfg.enable_b3_ingestion and cfg.b3_planilhas:
        typer.echo("Carregando dados complementares da B3...")
        b3_df = b3_pipeline.load_planilhas(cfg.b3_planilhas, workdir=workdir)
        mapped = b3_pipeline.map_to_fato_cota_diaria(b3_df)
        if not mapped.empty:
            tables["fato_cota_diaria"] = pd.concat(
                [tables["fato_cota_diaria"], mapped], ignore_index=True
            )

    if cfg.enable_mais_retorno_fallback:
        mais_retorno_fallback.check_terms_of_use()

    fato_diario = tables.get("fato_cota_diaria")
    if fato_diario is not None and not fato_diario.empty:
        fato_diario = fato_diario.dropna(subset=["data_cotacao"])
        if "valor_cota" in fato_diario.columns:
            fato_diario = fato_diario[fato_diario["valor_cota"].notna()]
            fato_diario = fato_diario[fato_diario["valor_cota"] > 0]
        if "patrimonio_liquido" in fato_diario.columns:
            fato_diario = fato_diario[fato_diario["patrimonio_liquido"].notna()]
        tables["fato_cota_diaria"] = fato_diario

    fato_carteira = tables.get("fato_carteira_mensal")
    if fato_carteira is not None and not fato_carteira.empty:
        if "valor_mercado" in fato_carteira.columns:
            fato_carteira = fato_carteira[fato_carteira["valor_mercado"].notna()]
            fato_carteira = fato_carteira[fato_carteira["valor_mercado"] > 0]
        tables["fato_carteira_mensal"] = fato_carteira

    fato_cotistas = tables.get("fato_cotistas_mensal")
    if fato_cotistas is not None and not fato_cotistas.empty:
        fato_cotistas = fato_cotistas.dropna(subset=["data_referencia"])
        tables["fato_cotistas_mensal"] = fato_cotistas

    return tables


def save_tables(tables: Dict[str, pd.DataFrame], destination: Path) -> Dict[str, Path]:
    destination = io.ensure_directory(destination)
    paths: Dict[str, Path] = {}
    for name, df in tables.items():
        path = destination / f"{name}.csv"
        io.write_dataframe_csv(df, path)
        paths[name] = path
    return paths


def build_curated_tables(tables: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    curated: Dict[str, pd.DataFrame] = {}
    fato = tables.get("fato_cota_diaria")
    dim_fundo = tables.get("dim_fundo")
    if fato is not None and dim_fundo is not None and not fato.empty:
        enriched = fato.merge(dim_fundo, on="cnpj", how="left")
        curated["curated_cotas_por_categoria"] = (
            enriched.groupby(["data_cotacao", "categoria_cvm"], dropna=False)
            .agg({"valor_cota": "mean", "patrimonio_liquido": "sum"})
            .reset_index()
        )
        curated["curated_cotas_por_gestora"] = (
            enriched.groupby(["data_cotacao", "gestora"], dropna=False)
            .agg({"valor_cota": "mean", "patrimonio_liquido": "sum"})
            .reset_index()
        )
        curated["curated_cotas_por_grupo_looker"] = (
            enriched.groupby(["data_cotacao", "grupo_looker"], dropna=False)
            .agg({"valor_cota": "mean", "patrimonio_liquido": "sum"})
            .reset_index()
        )
    return curated


def create_bigquery_uploader(cfg: config.PipelineConfig) -> bigquery.BigQueryUploader:
    if (
        not cfg.bigquery_project
        or not cfg.bigquery_dataset_staging
        or not cfg.bigquery_dataset_curated
    ):
        raise RuntimeError("Configuracao do BigQuery incompleta no config/pipeline.yaml")
    return bigquery.BigQueryUploader(
        project=cfg.bigquery_project,
        staging_dataset=cfg.bigquery_dataset_staging,
        curated_dataset=cfg.bigquery_dataset_curated,
    )


def upload_tables(
    uploader: bigquery.BigQueryUploader,
    csv_paths: Dict[str, Path],
    *,
    curated: bool = False,
) -> None:
    destination = "curated" if curated else "staging"
    for name, path in csv_paths.items():
        uploader.load_csv(path, table=name, destination=destination)


def _safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def export_frontend_payload(
    cfg: config.PipelineConfig,
    tables: Dict[str, pd.DataFrame],
    output_dir: Path,
) -> Dict[str, Path]:
    """Serializa tabelas em JSON para o front-end estatico."""

    api_dir = io.ensure_directory(output_dir)
    funds_dir = io.ensure_directory(api_dir / "funds")

    diario = tables.get("fato_cota_diaria", pd.DataFrame()).copy()
    cotistas = tables.get("fato_cotistas_mensal", pd.DataFrame()).copy()
    carteira = tables.get("fato_carteira_mensal", pd.DataFrame()).copy()

    # Ensure dataframes have the expected 'cnpj' column so downstream filtering
    # doesn't raise KeyError when a table is empty or missing columns.
    # This keeps behavior identical for non-empty tables but makes the export
    # robust to missing tables coming from the CVM fallback logic.
    if "cnpj" not in diario.columns:
        diario = diario.copy()
        diario["cnpj"] = pd.Series(dtype="object")
    if "cnpj" not in cotistas.columns:
        cotistas = cotistas.copy()
        cotistas["cnpj"] = pd.Series(dtype="object")
    if "cnpj" not in carteira.columns:
        carteira = carteira.copy()
        carteira["cnpj"] = pd.Series(dtype="object")

    # Ensure datetime reference columns exist so downstream dropna/subset
    # operations don't raise KeyError when a table is empty or missing
    # expected columns. Use empty datetime Series for consistency.
    if "data_cotacao" not in diario.columns:
        diario = diario.copy()
        diario["data_cotacao"] = pd.to_datetime(pd.Series(dtype="datetime64[ns]"))
    if "data_referencia" not in cotistas.columns:
        cotistas = cotistas.copy()
        cotistas["data_referencia"] = pd.to_datetime(pd.Series(dtype="datetime64[ns]"))
    if "data_referencia" not in carteira.columns:
        carteira = carteira.copy()
        carteira["data_referencia"] = pd.to_datetime(pd.Series(dtype="datetime64[ns]"))

    if not diario.empty:
        diario["data_cotacao"] = pd.to_datetime(diario["data_cotacao"], errors="coerce")
        for column in [
            "valor_cota",
            "patrimonio_liquido",
            "numero_cotistas",
            "valor_total",
            "captacoes",
            "resgates",
        ]:
            if column in diario.columns:
                diario[column] = _safe_numeric(diario[column])

    if not cotistas.empty:
        cotistas["data_referencia"] = pd.to_datetime(
            cotistas["data_referencia"], errors="coerce"
        )
        for column in ["numero_cotistas", "patrimonio_liquido"]:
            if column in cotistas.columns:
                cotistas[column] = _safe_numeric(cotistas[column])

    if not carteira.empty:
        carteira["data_referencia"] = pd.to_datetime(
            carteira["data_referencia"], errors="coerce"
        )
        for column in ["valor_mercado", "quantidade"]:
            if column in carteira.columns:
                carteira[column] = _safe_numeric(carteira[column])

    progress_log = output_dir / "progress.log"
    if progress_log.exists():
        progress_log.unlink()

    generated_paths: Dict[str, Path] = {}
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    index_payload = {
        "generated_at": generated_at,
        "funds": [],
    }

    total_funds = len(cfg.fundos)

    for position, fund in enumerate(cfg.fundos, start=1):
        fund_meta = {
            "cnpj": fund.cnpj,
            "nome": fund.nome,
            "categoria_cvm": fund.categoria_cvm,
            "gestora": fund.gestora,
            "classe_anbima": fund.classe_anbima,
            "grupo_looker": fund.grupo_looker,
        }

        fund_diario = (
            diario[diario["cnpj"] == fund.cnpj]
            .dropna(subset=["data_cotacao"])
            .sort_values("data_cotacao")
            .copy()
        )
        if "valor_cota" in fund_diario.columns:
            fund_diario = fund_diario[fund_diario["valor_cota"].notna()]
            fund_diario = fund_diario[fund_diario["valor_cota"] > 0]

        fund_cotistas = (
            cotistas[cotistas["cnpj"] == fund.cnpj]
            .dropna(subset=["data_referencia"])
            .sort_values("data_referencia")
            .copy()
        )
        fund_carteira = (
            carteira[carteira["cnpj"] == fund.cnpj]
            .dropna(subset=["data_referencia"])
            .sort_values("data_referencia")
            .copy()
        )

        daily_records = []
        latest_snapshot = None
        if not fund_diario.empty:
            fund_diario["data"] = fund_diario["data_cotacao"].dt.strftime("%Y-%m-%d")
            fund_diario["retorno_pct"] = (
                fund_diario["valor_cota"].pct_change().fillna(0) * 100
            )
            daily_records_df = fund_diario[
                [
                    "data",
                    "valor_cota",
                    "patrimonio_liquido",
                    "numero_cotistas",
                    "retorno_pct",
                ]
            ].copy()
            daily_records_df["numero_cotistas"] = (
                daily_records_df["numero_cotistas"].fillna(0).round(0)
            )
            daily_records_df["patrimonio_liquido"] = daily_records_df[
                "patrimonio_liquido"
            ].fillna(0).round(2)
            daily_records_df["valor_cota"] = daily_records_df["valor_cota"].round(6)
            daily_records_df["retorno_pct"] = daily_records_df["retorno_pct"].round(4)
            daily_records = daily_records_df.to_dict(orient="records")

            latest_row = fund_diario.iloc[-1]
            latest_snapshot = {
                "data": latest_row["data"],
                "valor_cota": round(float(latest_row["valor_cota"]), 6)
                if pd.notna(latest_row.get("valor_cota"))
                else None,
                "patrimonio_liquido": round(
                    float(latest_row.get("patrimonio_liquido", 0)), 2
                )
                if pd.notna(latest_row.get("patrimonio_liquido"))
                else None,
                "numero_cotistas": int(latest_row["numero_cotistas"])
                if pd.notna(latest_row.get("numero_cotistas"))
                else None,
            }

        cotistas_records = []
        latest_cotistas = None
        if not fund_cotistas.empty:
            fund_cotistas["data"] = fund_cotistas["data_referencia"].dt.strftime(
                "%Y-%m-%d"
            )
            cotistas_df = fund_cotistas[
                ["data", "numero_cotistas", "patrimonio_liquido"]
            ].copy()
            cotistas_df["numero_cotistas"] = cotistas_df["numero_cotistas"].fillna(0).round(0)
            cotistas_df["patrimonio_liquido"] = cotistas_df["patrimonio_liquido"].fillna(0).round(2)
            cotistas_records = cotistas_df.to_dict(orient="records")
            latest_cotistas_row = fund_cotistas.iloc[-1]
            latest_cotistas = {
                "data": latest_cotistas_row["data"],
                "numero_cotistas": int(latest_cotistas_row["numero_cotistas"])
                if pd.notna(latest_cotistas_row.get("numero_cotistas"))
                else None,
                "patrimonio_liquido": round(
                    float(latest_cotistas_row.get("patrimonio_liquido", 0)), 2
                )
                if pd.notna(latest_cotistas_row.get("patrimonio_liquido"))
                else None,
            }

        carteira_por_tipo = []
        carteira_por_ativo = []
        latest_holdings = {"data": None, "total": 0.0, "top": []}
        if not fund_carteira.empty:
            fund_carteira["data"] = fund_carteira["data_referencia"].dt.strftime(
                "%Y-%m-%d"
            )
            grouped = (
                fund_carteira.groupby(["data", "tipo_ativo"], dropna=False)[
                    "valor_mercado"
                ]
                .sum()
                .reset_index()
            )
            grouped = grouped[grouped["valor_mercado"] > 0]
            grouped["valor_mercado"] = grouped["valor_mercado"].round(2)
            totals = grouped.groupby("data")["valor_mercado"].transform(
                lambda values: values.sum()
            )
            grouped["percentual"] = grouped["valor_mercado"] / totals.replace(0, pd.NA)
            grouped["percentual"] = grouped["percentual"].fillna(0) * 100
            grouped["percentual"] = grouped["percentual"].round(2)
            carteira_por_tipo = grouped.to_dict(orient="records")

            ativos_df = fund_carteira[
                [
                    "data",
                    "tipo_ativo",
                    "emissor",
                    "isin",
                    "valor_mercado",
                ]
            ].copy()
            ativos_df = ativos_df[ativos_df["valor_mercado"].notna()]
            ativos_df = ativos_df[ativos_df["valor_mercado"] > 0]
            if not ativos_df.empty:
                totals_ativos = ativos_df.groupby("data")["valor_mercado"].transform(
                    lambda values: values.sum()
                )
                ativos_df["percentual"] = ativos_df["valor_mercado"] / totals_ativos.replace(
                    0, pd.NA
                )
                ativos_df["percentual"] = ativos_df["percentual"].fillna(0) * 100
                ativos_df["valor_mercado"] = ativos_df["valor_mercado"].round(2)
                ativos_df["percentual"] = ativos_df["percentual"].round(4)
                carteira_por_ativo = ativos_df.to_dict(orient="records")

            latest_date = fund_carteira["data_referencia"].max()
            latest_subset = fund_carteira[
                fund_carteira["data_referencia"] == latest_date
            ].copy()
            latest_total = latest_subset["valor_mercado"].sum()
            if latest_total:
                latest_subset["percentual"] = (
                    latest_subset["valor_mercado"] / latest_total
                ) * 100
            else:
                latest_subset["percentual"] = 0
            latest_subset = latest_subset.sort_values(
                "valor_mercado", ascending=False
            ).head(10)
            latest_holdings = {
                "data": latest_date.strftime("%Y-%m-%d") if pd.notna(latest_date) else None,
                "total": round(float(latest_total), 2)
                if pd.notna(latest_total)
                else 0.0,
                "top": [
                    {
                        "emissor": row.get("emissor"),
                        "isin": row.get("isin"),
                        "tipo_ativo": row.get("tipo_ativo"),
                        "valor_mercado": round(float(row.get("valor_mercado", 0)), 2)
                        if pd.notna(row.get("valor_mercado"))
                        else 0.0,
                        "percentual": round(float(row.get("percentual", 0)), 2)
                        if pd.notna(row.get("percentual"))
                        else 0.0,
                    }
                    for _, row in latest_subset.iterrows()
                ],
            }

        fund_payload = {
            "metadata": fund_meta,
            "series": {
                "daily": daily_records,
                "cotistas": cotistas_records,
                "carteira_por_tipo": carteira_por_tipo,
                "carteira_por_ativo": carteira_por_ativo,
            },
            "latest_snapshot": latest_snapshot,
            "latest_cotistas": latest_cotistas,
            "latest_holdings": latest_holdings,
        }

        fund_path = funds_dir / f"{fund.cnpj}.json"
        with fund_path.open("w", encoding="utf-8") as fh:
            json.dump(fund_payload, fh, ensure_ascii=False)
        generated_paths[fund.cnpj] = fund_path
        index_payload["funds"].append(
            {
                **fund_meta,
                "dataset_path": f"funds/{fund.cnpj}.json",
                "daily_records": len(daily_records),
                "cotistas_records": len(cotistas_records),
                "has_carteira": bool(carteira_por_tipo),
            }
        )

        progress_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "index": position,
            "total": total_funds,
            "cnpj": fund.cnpj,
            "nome": fund.nome,
        }
        with progress_log.open("a", encoding="utf-8") as log_file:
            log_file.write(json.dumps(progress_entry, ensure_ascii=False) + "\n")

        if position % 10 == 0 or position == total_funds:
            typer.echo(
                f"[frontend] Exportados {position}/{total_funds} fundos (até {fund.nome})"
            )

    index_path = api_dir / "index.json"
    with index_path.open("w", encoding="utf-8") as fh:
        json.dump(index_payload, fh, ensure_ascii=False)
    generated_paths["index"] = index_path
    return generated_paths


def build_static_site(api_dir: Path, site_source: Path, destination: Path) -> None:
    """Copia assets do diretorio web/ e anexa os JSONs."""

    if not site_source.exists():
        typer.echo("Diretorio 'web' nao encontrado. Site estatico nao sera gerado.")
        return

    io.ensure_directory(destination)
    shutil.copytree(site_source, destination, dirs_exist_ok=True)

    data_destination = destination / "data"
    if data_destination.exists():
        shutil.rmtree(data_destination)
    shutil.copytree(api_dir, data_destination)


@APP.command()
def ingest(
    config_path: Path = typer.Option(Path("config/pipeline.yaml"), help="Arquivo de configuracao YAML"),
    workdir: Path = typer.Option(Path(".tmp_pipeline"), help="Diretorio temporario para downloads"),
    output_dir: Path = typer.Option(Path("output"), help="Diretorio para salvar CSVs"),
    skip_bigquery: bool = typer.Option(False, help="Nao realiza upload para o BigQuery"),
) -> None:
    """Executa a ingestao completa: download, limpeza, consolidacao e upload."""

    load_environment()
    cfg = get_config(config_path)
    typer.echo("Iniciando ingestao completa...")
    tables = collect_all_data(cfg, workdir)

    staging_dir = output_dir / "staging"
    curated_dir = output_dir / "curated"

    staging_paths = save_tables(tables, staging_dir)
    curated_tables = build_curated_tables(tables)
    curated_paths = save_tables(curated_tables, curated_dir)

    api_dir = output_dir / "api"
    export_frontend_payload(cfg, tables, api_dir)
    build_static_site(api_dir, Path("web"), output_dir / "site")

    if not skip_bigquery:
        uploader = create_bigquery_uploader(cfg)
        upload_tables(uploader, staging_paths, curated=False)
        upload_tables(uploader, curated_paths, curated=True)

    typer.echo("Ingestao concluida.")


@APP.command("export-local")
def export_local(
    config_path: Path = typer.Option(Path("config/pipeline.yaml"), help="Arquivo de configuracao YAML"),
    workdir: Path = typer.Option(Path(".tmp_pipeline"), help="Diretorio temporario"),
    output_dir: Path = typer.Option(Path("output"), help="Diretorio de saida"),
) -> None:
    """Executa o pipeline e mantem os CSVs locais sem upload para o BigQuery."""

    load_environment()
    cfg = get_config(config_path)
    tables = collect_all_data(cfg, workdir)
    staging_paths = save_tables(tables, output_dir / "staging")
    curated_tables = build_curated_tables(tables)
    curated_paths = save_tables(curated_tables, output_dir / "curated")

    api_dir = output_dir / "api"
    export_frontend_payload(cfg, tables, api_dir)
    build_static_site(api_dir, Path("web"), output_dir / "site")

    typer.echo(
        json.dumps(
            {
                "staging": [str(p) for p in staging_paths.values()],
                "curated": [str(p) for p in curated_paths.values()],
                "api": str(api_dir),
                "site": str(output_dir / "site"),
            },
            indent=2,
        )
    )


@APP.command("upload-bigquery")
def upload_bigquery(
    config_path: Path = typer.Option(Path("config/pipeline.yaml"), help="Arquivo de configuracao YAML"),
    output_dir: Path = typer.Option(Path("output"), help="Diretorio com CSVs"),
) -> None:
    """Faz upload dos CSVs existentes no diretorio de saida para o BigQuery."""

    load_environment()
    cfg = get_config(config_path)
    uploader = create_bigquery_uploader(cfg)

    staging_dir = output_dir / "staging"
    curated_dir = output_dir / "curated"

    staging_paths = {path.stem: path for path in staging_dir.glob("*.csv")}
    curated_paths = {path.stem: path for path in curated_dir.glob("*.csv")}

    if not staging_paths and not curated_paths:
        raise RuntimeError(
            "Nenhum CSV encontrado no diretorio de saida. Execute 'export-local' ou 'ingest' antes."
        )

    upload_tables(uploader, staging_paths, curated=False)
    upload_tables(uploader, curated_paths, curated=True)

    typer.echo("Upload concluido com sucesso.")


if __name__ == "__main__":
    APP()
