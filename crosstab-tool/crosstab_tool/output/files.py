"""CSV/Excel output writer (Req 4.5.2). Named `files.py` and class
`FileWriter` to match the import `cli.py` already has:
`from crosstab_tool.output.files import FileWriter`.

Branches on `output_config.destination` internally rather than having
separate CsvWriter/ExcelWriter classes (crosstab-tw's shape) -- kept as one
class since `cli.py` only ever constructs `FileWriter(config.output)` for
both non-Sheets destinations and never looks up CSV/Excel separately.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from crosstab_tool.config.schema import OutputDestination
from crosstab_tool.output.base import Writer, shaped


class FileWriter(Writer):
    def write(self, df: pd.DataFrame) -> str:
        if self.output_config.destination not in (OutputDestination.CSV, OutputDestination.EXCEL):
            raise ValueError(
                f"FileWriter can't handle destination={self.output_config.destination!r} "
                "(expected csv or excel; google_sheets uses GoogleSheetsWriter)"
            )

        path = Path(self.output_config.path)
        path.parent.mkdir(parents=True, exist_ok=True)
        out = shaped(df, self.output_config)

        if self.output_config.destination == OutputDestination.CSV:
            out.to_csv(path, index=False)
        else:
            with pd.ExcelWriter(path, engine="openpyxl") as xl:
                # Excel sheet names are capped at 31 chars; results is a
                # fixed, predictable name rather than deriving one from the
                # job (job name isn't available here -- see open questions
                # on whether Writer should also receive JobConfig, not just
                # OutputConfig).
                out.to_excel(xl, sheet_name="results", index=False)
        return str(path)


def _write_metadata_sidecar(path: Path, metadata: dict) -> None:
    """Not currently called -- cli.py writes run metadata separately via
    metadata/run_metadata.py, so there's no sidecar today. Left here as a
    documented extension point in case a per-output-file metadata sidecar
    (crosstab-tw had one for CSV) turns out to be wanted alongside the
    existing runs/ directory metadata."""
    sidecar = path.with_suffix(path.suffix + ".meta.json")
    sidecar.write_text(json.dumps(metadata, indent=2, default=str))
