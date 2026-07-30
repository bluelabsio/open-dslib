"""Google Sheets writer (primary output target).

Auth: a service-account JSON key, located via (in order) the job's
`output.credentials_file` or the GOOGLE_APPLICATION_CREDENTIALS environment
variable. Spreadsheets created by the service account are shared with the
emails in `output.share_with` (otherwise only the service account can see
them).
"""

from __future__ import annotations

import math
import os

import pandas as pd

from crosstab_tool.config import ConfigError, Job
from crosstab_tool.writers import Writer, shaped

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class SheetsWriter(Writer):
    def _client(self, job: Job):
        import gspread
        from google.oauth2.service_account import Credentials

        keyfile = job.output.credentials_file or os.environ.get(
            "GOOGLE_APPLICATION_CREDENTIALS"
        )
        if not keyfile or not os.path.exists(keyfile):
            raise ConfigError(
                "Google Sheets output needs a service-account key: set "
                "`output.credentials_file` in the job config or the "
                "GOOGLE_APPLICATION_CREDENTIALS environment variable to the "
                "path of a service-account JSON file."
            )
        creds = Credentials.from_service_account_file(keyfile, scopes=SCOPES)
        return gspread.authorize(creds)

    def _open_or_create(self, gc, job: Job):
        import gspread

        ref = job.output.spreadsheet
        # A bare 44-char token is treated as a spreadsheet key, else a title.
        if len(ref) == 44 and " " not in ref:
            try:
                return gc.open_by_key(ref), False
            except gspread.exceptions.APIError:
                pass
        try:
            return gc.open(ref), False
        except gspread.exceptions.SpreadsheetNotFound:
            sh = gc.create(ref)
            for email in job.output.share_with:
                sh.share(email, perm_type="user", role="writer", notify=False)
            return sh, True

    def write(self, df: pd.DataFrame, job: Job, metadata: dict) -> str:
        import gspread

        gc = self._client(job)
        sh, created = self._open_or_create(gc, job)
        out = shaped(df, job)

        for tab_name, frame in [
            (job.output.worksheet, out),
            ("run_metadata", pd.DataFrame(
                {"key": list(metadata), "value": [str(v) for v in metadata.values()]}
            )),
        ]:
            try:
                ws = sh.worksheet(tab_name)
                ws.clear()
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(
                    tab_name, rows=len(frame) + 10, cols=len(frame.columns) + 5
                )
            values = [list(frame.columns)] + [
                [_cell(v) for v in row] for row in frame.itertuples(index=False)
            ]
            ws.update(values, value_input_option="RAW")

        if job.output.number_format:
            ws = sh.worksheet(job.output.worksheet)
            float_cols = [
                i for i, c in enumerate(out.columns)
                if pd.api.types.is_float_dtype(out[c])
            ]
            for i in float_cols:
                col_letter = gspread.utils.rowcol_to_a1(1, i + 1).rstrip("1")
                ws.format(
                    f"{col_letter}2:{col_letter}{len(out) + 1}",
                    {"numberFormat": {"type": "NUMBER", "pattern": job.output.number_format}},
                )

        # Drop the default empty first sheet on newly created spreadsheets.
        if created:
            default = sh.sheet1
            if default.title not in (job.output.worksheet, "run_metadata"):
                sh.del_worksheet(default)

        return sh.url


def _cell(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    if hasattr(v, "item"):  # numpy scalar -> python scalar for JSON transport
        return v.item()
    return v
