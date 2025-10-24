import json
from pathlib import Path

import pandas as pd

from data_pipeline.common.config import FundConfig, PipelineConfig
from data_pipeline.run_pipeline import export_frontend_payload


def test_export_frontend_payload_creates_index_and_fund_files(tmp_path: Path) -> None:
    cfg = PipelineConfig(
        fundos=[
            FundConfig(
                cnpj="12345678000190",
                nome="Fundo Teste",
                categoria_cvm="Renda Fixa",
                gestora="Gestora X",
                classe_anbima="Soberano",
                grupo_looker="Institucional",
            )
        ]
    )

    tables = {
        "fato_cota_diaria": pd.DataFrame(
            [
                {
                    "cnpj": "12345678000190",
                    "data_cotacao": "2024-06-01",
                    "valor_cota": "1.00",
                    "patrimonio_liquido": "1000000",
                    "numero_cotistas": "100",
                },
                {
                    "cnpj": "12345678000190",
                    "data_cotacao": "2024-06-02",
                    "valor_cota": "1.05",
                    "patrimonio_liquido": "1050000",
                    "numero_cotistas": "102",
                },
            ]
        ),
        "fato_cotistas_mensal": pd.DataFrame(
            [
                {
                    "cnpj": "12345678000190",
                    "data_referencia": "2024-06-01",
                    "numero_cotistas": "100",
                    "patrimonio_liquido": "1000000",
                },
                {
                    "cnpj": "12345678000190",
                    "data_referencia": "2024-07-01",
                    "numero_cotistas": "110",
                    "patrimonio_liquido": "1200000",
                },
            ]
        ),
        "fato_carteira_mensal": pd.DataFrame(
            [
                {
                    "cnpj": "12345678000190",
                    "data_referencia": "2024-07-01",
                    "tipo_ativo": "Titulos Publicos",
                    "valor_mercado": "750000",
                    "emissor": "Tesouro Nacional",
                    "isin": "BRTEST100001",
                },
                {
                    "cnpj": "12345678000190",
                    "data_referencia": "2024-07-01",
                    "tipo_ativo": "Caixa",
                    "valor_mercado": "450000",
                    "emissor": "Banco XYZ",
                    "isin": "",
                },
            ]
        ),
    }

    export_frontend_payload(cfg, tables, tmp_path)

    index_path = tmp_path / "index.json"
    fund_path = tmp_path / "funds" / "12345678000190.json"

    assert index_path.exists()
    assert fund_path.exists()

    index_data = json.loads(index_path.read_text(encoding="utf-8"))
    assert index_data["funds"][0]["cnpj"] == "12345678000190"
    assert index_data["funds"][0]["dataset_path"] == "funds/12345678000190.json"

    fund_data = json.loads(fund_path.read_text(encoding="utf-8"))
    assert fund_data["metadata"]["nome"] == "Fundo Teste"
    assert fund_data["series"]["daily"]
    assert fund_data["series"]["cotistas"]
    assert fund_data["latest_holdings"]["top"]
