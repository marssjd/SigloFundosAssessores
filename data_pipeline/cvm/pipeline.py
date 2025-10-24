"""High level orchestration for CVM data ingestion."""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Dict, Iterable

import pandas as pd

from ..common.config import PipelineConfig
from . import dimensions, inf_diario, inf_mensal

LOGGER = logging.getLogger(__name__)


def month_iterator(end: date, months: int, *, skip_recent: int = 0) -> Iterable[date]:
    """Yield datetime.date objects going backwards, skipping the most recent months."""
    year = end.year
    month = end.month
    skipped = 0
    generated = 0

    while generated < months:
        current = date(year, month, 1)
        if skipped < skip_recent:
            skipped += 1
        else:
            yield current
            generated += 1
        month -= 1
        if month == 0:
            month = 12
            year -= 1


class CVMPipeline:
    def __init__(self, config: PipelineConfig, *, workdir: Path) -> None:
        self.config = config
        self.workdir = workdir

    def run(self) -> Dict[str, pd.DataFrame]:
        today = date.today()

        def build_months(skip_recent: int) -> list[date]:
            skip_value = max(0, int(skip_recent))
            return list(
                month_iterator(
                    today,
                    self.config.meses_retroativos,
                    skip_recent=skip_value,
                )
            )

        primary_skip = (
            int(self.config.meses_ignorar_recente)
            if self.config.meses_ignorar_recente is not None
            else 0
        )
        months = build_months(primary_skip)
        diario_urls = inf_diario.build_monthly_urls(months)
        mensal_urls = inf_mensal.build_monthly_urls(months)

        LOGGER.info("Downloading CVM InfDiario datasets (%s months)", len(months))
        diario_df = inf_diario.parse_inf_diario(diario_urls, workdir=self.workdir)
        LOGGER.info("Downloading CVM InfMensal datasets (%s months)", len(months))
        carteira_df, cotistas_df = inf_mensal.parse_inf_mensal(mensal_urls, workdir=self.workdir)
        monitored_cnpjs = {fund.cnpj for fund in self.config.fundos}
        if carteira_df.empty and cotistas_df.empty:
            LOGGER.info("Falling back to CDA/Perfil Mensal datasets for carteiras e cotistas")
            fallback_skips = [3, 4, 5, 6]
            for skip in fallback_skips:
                LOGGER.info("Tentando fallback CDA/Perfil com skip_recent=%s", skip)
                tentativa_meses = build_months(skip)
                carteira_df, cotistas_df = inf_mensal.parse_inf_mensal_fallback(
                    tentativa_meses, workdir=self.workdir, cnpj_filter=monitored_cnpjs
                )
                if not carteira_df.empty or not cotistas_df.empty:
                    LOGGER.info("Fallback CDA/Perfil encontrou dados com skip_recent=%s", skip)
                    break
            else:
                LOGGER.warning(
                    "Fallback CDA/Perfil não encontrou dados mesmo após tentar skip_recent em %s",
                    fallback_skips,
                )


        LOGGER.info("Filtering datasets for monitored funds")
        cnpjs = monitored_cnpjs
        diario_df = diario_df[diario_df["cnpj"].isin(cnpjs)]
        if not carteira_df.empty:
            carteira_df = carteira_df[carteira_df["cnpj"].isin(cnpjs)]
        if not cotistas_df.empty:
            cotistas_df = cotistas_df[cotistas_df["cnpj"].isin(cnpjs)]

        dims = {
            "dim_fundo": dimensions.build_dim_fundo(self.config),
            "dim_gestora": dimensions.build_dim_gestora(self.config),
            "dim_categoria_cvm": dimensions.build_dim_categoria_cvm(self.config),
            "dim_classe_anbima": dimensions.build_dim_classe_anbima(self.config),
        }

        facts = {
            "fato_cota_diaria": diario_df,
            "fato_carteira_mensal": carteira_df,
            "fato_cotistas_mensal": cotistas_df,
        }

        return {**facts, **dims}
