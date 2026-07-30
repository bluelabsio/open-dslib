"""Data source abstraction (Req 4.1, 5.3).

v1 ships only RedshiftSource. New backends (other relational DBs, Google
Sheets, CSV/Excel, S3 — all deferred per Req 4.1) implement this same
interface so the query builder and CLI never special-case the source type.
"""
from __future__ import annotations

import abc

import pandas as pd


class DataSource(abc.ABC):
    def __init__(self, name: str, connection: str):
        self.name = name
        self.connection = connection

    @abc.abstractmethod
    def execute(self, sql: str) -> pd.DataFrame:
        """Run a SQL string against this source and return a DataFrame.

        Only ever called with the already-aggregated query from
        query/builder.py (Req 5.1: push group-by/join work into the
        source; never pull the full 260M-row base table here).
        """

    @abc.abstractmethod
    def qualified_table(self, table: str) -> str:
        """Format `table` correctly for this source's SQL dialect (schema
        qualification, quoting, etc.)."""
