from __future__ import annotations

import math
from unittest.mock import MagicMock, patch

import gspread
import numpy as np
import pandas as pd
import pytest

from crosstab_tool.config.schema import OutputConfig, OutputDestination
from crosstab_tool.output.sheets import GoogleSheetsWriter, _cell


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "category": ["00 Topline", "01 Age"],
            "level": ["Topline", "18-29"],
            "count": [100, 40],
            "mean_p_support": [0.55, 0.61],
        }
    )


def _config() -> OutputConfig:
    return OutputConfig(
        destination=OutputDestination.GOOGLE_SHEETS,
        spreadsheet_id="sheet123",
        tab="results",
    )


def test_write_clears_and_updates_an_existing_worksheet():
    writer = GoogleSheetsWriter(_config())
    fake_ws = MagicMock()
    fake_sh = MagicMock(url="https://example.com/sheet123")
    fake_sh.worksheet.return_value = fake_ws
    fake_gc = MagicMock()
    fake_gc.open_by_key.return_value = fake_sh

    with patch.object(GoogleSheetsWriter, "_client", return_value=fake_gc):
        result = writer.write(_sample_df())

    fake_gc.open_by_key.assert_called_once_with("sheet123")
    fake_sh.worksheet.assert_called_once_with("results")
    fake_ws.clear.assert_called_once()
    fake_sh.add_worksheet.assert_not_called()

    fake_ws.update.assert_called_once()
    call_args, call_kwargs = fake_ws.update.call_args
    assert call_args[0] == [
        ["category", "level", "count", "mean_p_support"],
        ["00 Topline", "Topline", 100, 0.55],
        ["01 Age", "18-29", 40, 0.61],
    ]
    assert call_kwargs["value_input_option"] == "RAW"
    assert result == "https://example.com/sheet123"


def test_write_creates_worksheet_when_missing():
    writer = GoogleSheetsWriter(_config())
    fake_ws = MagicMock()
    fake_sh = MagicMock(url="https://example.com/sheet123")
    fake_sh.worksheet.side_effect = gspread.exceptions.WorksheetNotFound("results")
    fake_sh.add_worksheet.return_value = fake_ws
    fake_gc = MagicMock()
    fake_gc.open_by_key.return_value = fake_sh

    with patch.object(GoogleSheetsWriter, "_client", return_value=fake_gc):
        writer.write(_sample_df())

    df = _sample_df()
    fake_sh.add_worksheet.assert_called_once_with(
        "results", rows=len(df) + 10, cols=len(df.columns) + 5
    )
    fake_ws.update.assert_called_once()


def test_client_raises_clear_error_without_credentials(monkeypatch):
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    writer = GoogleSheetsWriter(_config())
    with pytest.raises(ValueError, match="GOOGLE_APPLICATION_CREDENTIALS"):
        writer._client()


def test_cell_converts_nan_and_none_to_empty_string():
    assert _cell(float("nan")) == ""
    assert _cell(None) == ""


def test_cell_converts_numpy_scalar_to_python_scalar():
    result = _cell(np.float64(0.5))
    assert result == 0.5
    assert type(result) is float


def test_cell_passes_through_plain_values():
    assert _cell("hello") == "hello"
    assert _cell(42) == 42
    assert not math.isnan(_cell(1.5))
