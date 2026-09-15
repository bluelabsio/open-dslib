"""Google Sheets output writer (default destination). Named `sheets.py` and
class `GoogleSheetsWriter` to match the import `cli.py` already has:
`from crosstab_tool.output.sheets import GoogleSheetsWriter`.

Ported from crosstab-tw's writers/sheets.py, with three things
deliberately NOT carried forward in this first pass, since OutputConfig
has no fields for them yet -- flagged as open questions rather than
silently added:

- `credentials_file` (per-job service-account key override) -- this pass
  authenticates via GOOGLE_APPLICATION_CREDENTIALS only.
- `share_with` (auto-sharing a newly created spreadsheet) -- moot for now
  since `open_by_key` requires the spreadsheet to already exist (see
  below); would matter if create-on-missing is added later.
- `number_format` (per-column float formatting).

Uses `output_config.spreadsheet_id` (must already exist) and
`output_config.tab` as the worksheet name -- there's no title-based
create-if-missing lookup the way crosstab-tw's `_open_or_create` had,
since retreat-la's schema only has a spreadsheet *id*, which implies an
existing sheet.
"""
from __future__ import annotations

import math
import os

import pandas as pd

from crosstab_tool.output.base import Writer, shaped


class GoogleSheetsWriter(Writer):
    def _client(self):
        import gspread
        from google.oauth2.service_account import Credentials

        keyfile = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if not keyfile or not os.path.exists(keyfile):
            raise ValueError(
                "Google Sheets output needs a service-account key: set the "
                "GOOGLE_APPLICATION_CREDENTIALS environment variable to the "
                "path of a service-account JSON file."
            )
        creds = Credentials.from_service_account_file(
            keyfile,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        return gspread.authorize(creds)

    def write(self, df: pd.DataFrame) -> str:
        import gspread

        gc = self._client()
        sh = gc.open_by_key(self.output_config.spreadsheet_id)
        out = shaped(df, self.output_config)

        tab_name = self.output_config.tab
        try:
            ws = sh.worksheet(tab_name)
            ws.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(tab_name, rows=len(out) + 10, cols=len(out.columns) + 5)

        values = [list(out.columns)] + [
            [_cell(v) for v in row] for row in out.itertuples(index=False)
        ]
        ws.update(values, value_input_option="RAW")

        return sh.url


def _cell(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    if hasattr(v, "item"):  # numpy scalar -> python scalar for JSON transport
        return v.item()
    return v
