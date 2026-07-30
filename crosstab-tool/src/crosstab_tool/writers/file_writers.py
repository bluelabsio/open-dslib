from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from crosstab_tool.config import Job
from crosstab_tool.writers import Writer, shaped


class CsvWriter(Writer):
    """CSV output plus a JSON metadata sidecar next to it."""

    def write(self, df: pd.DataFrame, job: Job, metadata: dict) -> str:
        path = Path(job.output.path)
        path.parent.mkdir(parents=True, exist_ok=True)
        shaped(df, job).to_csv(path, index=False)
        sidecar = path.with_suffix(path.suffix + ".meta.json")
        sidecar.write_text(json.dumps(metadata, indent=2))
        return str(path)


class ExcelWriter(Writer):
    """Excel output: results tab plus a run_metadata tab."""

    def write(self, df: pd.DataFrame, job: Job, metadata: dict) -> str:
        path = Path(job.output.path)
        path.parent.mkdir(parents=True, exist_ok=True)
        out = shaped(df, job)
        with pd.ExcelWriter(path, engine="openpyxl") as xl:
            out.to_excel(xl, sheet_name=job.output.worksheet[:31], index=False)
            meta_df = pd.DataFrame(
                {"key": list(metadata), "value": [str(v) for v in metadata.values()]}
            )
            meta_df.to_excel(xl, sheet_name="run_metadata", index=False)
            if job.output.number_format:
                ws = xl.book[job.output.worksheet[:31]]
                for col_idx, col in enumerate(out.columns, start=1):
                    if pd.api.types.is_float_dtype(out[col]):
                        for row in range(2, len(out) + 2):
                            ws.cell(row=row, column=col_idx).number_format = (
                                job.output.number_format
                            )
        return str(path)
