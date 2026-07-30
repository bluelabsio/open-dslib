"""Output writers. All consume the same tidy result DataFrame; the
destination is a job-level config choice. New destinations implement
Writer and get registered in WRITERS."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from crosstab_tool.config import Job
from crosstab_tool.postagg import to_wide


class Writer(ABC):
    @abstractmethod
    def write(self, df: pd.DataFrame, job: Job, metadata: dict) -> str:
        """Write results; returns a human-readable location (path or URL)."""


def shaped(df: pd.DataFrame, job: Job) -> pd.DataFrame:
    if job.output.layout == "wide":
        return to_wide(df).reset_index()
    return df


def get_writer(job: Job) -> Writer:
    from crosstab_tool.writers.file_writers import CsvWriter, ExcelWriter
    from crosstab_tool.writers.sheets import SheetsWriter

    writers = {"csv": CsvWriter, "excel": ExcelWriter, "sheets": SheetsWriter}
    return writers[job.output.destination]()
