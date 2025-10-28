"""Utilities for reading the monitored funds list from Google Sheets."""
from __future__ import annotations

import json
import logging
import os
from typing import Iterable, List, Optional, Sequence, Set

import gspread

from . import normalization

LOGGER = logging.getLogger(__name__)


def _column_letter_to_index(column: str) -> int:
    """Translate spreadsheet column letters (e.g. 'A', 'AA') to a 1-based index."""
    if not column:
        raise ValueError("Column letter must not be empty")

    column = column.strip().upper()
    index = 0
    for char in column:
        if not char.isalpha():
            raise ValueError(f"Invalid column letter: {column}")
        index = index * 26 + (ord(char) - ord("A") + 1)
    return index


def _build_gspread_client() -> gspread.Client:
    """Create an authenticated gspread client using either a file or inline JSON."""
    credentials_path = os.getenv("SHEETS_CREDENTIALS_PATH")
    credentials_json = os.getenv("SHEETS_CREDENTIALS_JSON")

    if credentials_json:
        try:
            credentials_dict = json.loads(credentials_json)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Conteúdo inválido em SHEETS_CREDENTIALS_JSON") from exc
        return gspread.service_account_from_dict(credentials_dict)

    if credentials_path:
        return gspread.service_account(filename=credentials_path)

    raise RuntimeError(
        "Credenciais do Google Sheets não encontradas. Defina SHEETS_CREDENTIALS_PATH "
        "ou SHEETS_CREDENTIALS_JSON."
    )


def _select_worksheet(spreadsheet: gspread.Spreadsheet) -> gspread.Worksheet:
    worksheet_name = os.getenv("SHEETS_WORKSHEET_NAME")
    worksheet_gid = os.getenv("SHEETS_WORKSHEET_GID")

    if worksheet_name:
        return spreadsheet.worksheet(worksheet_name)

    if worksheet_gid:
        gid = int(worksheet_gid)
        for sheet in spreadsheet.worksheets():
            if sheet.id == gid:
                return sheet
        raise RuntimeError(
            f"Aba com GID {worksheet_gid} não encontrada na planilha {spreadsheet.id}"
        )

    return spreadsheet.sheet1


def load_cnpjs_from_sheet() -> List[str]:
    """Return the list of monitored CNPJs declared in the Google Sheet."""
    spreadsheet_id = os.getenv("SHEETS_SPREADSHEET_ID")
    if not spreadsheet_id:
        LOGGER.info("Variável SHEETS_SPREADSHEET_ID não definida; mantendo lista padrão do YAML.")
        return []

    client = _build_gspread_client()
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
    except Exception as exc:  # pragma: no cover - depends on external service
        raise RuntimeError(f"Não foi possível abrir a planilha {spreadsheet_id}: {exc}") from exc

    worksheet = _select_worksheet(spreadsheet)
    column_letter = os.getenv("SHEETS_CNPJ_COLUMN", "A")
    column_index = _column_letter_to_index(column_letter)
    values = worksheet.col_values(column_index)

    normalized: List[str] = []
    for value in values:
        normalized_cnpj = normalization.normalize_cnpj(value)
        if normalized_cnpj:
            normalized.append(normalized_cnpj)

    # Remove duplicidades preservando a ordem original da planilha
    unique_ordered = list(dict.fromkeys(normalized))
    LOGGER.info("Google Sheets retornou %s CNPJs monitorados", len(unique_ordered))
    return unique_ordered
