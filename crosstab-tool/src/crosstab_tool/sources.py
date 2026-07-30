"""Data sources: a minimal common interface so non-Redshift backends can be
added later (Section 4.1 of the requirements) without redesign. Only the
Redshift implementation is built in v1.

The connection pattern (env-var driven SQLAlchemy engine) is adapted from
open-dslib's EngineContext.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from urllib.parse import quote_plus as urlquote

import pandas as pd
import sqlalchemy
from sqlalchemy import text


class DataSource(ABC):
    """Anything that can run the generated crosstab SQL and return the
    (small, aggregated) result as a DataFrame."""

    @abstractmethod
    def run_query(self, sql: str) -> pd.DataFrame: ...


class RedshiftSource(DataSource):
    """Connects using environment variables prefixed by `name`, e.g. for
    name='REDSHIFT': REDSHIFT_USER, REDSHIFT_PW, REDSHIFT_HOST, REDSHIFT_PORT,
    REDSHIFT_DB (matching the existing dslib convention)."""

    def __init__(self, name: str = "REDSHIFT", driver: str = "redshift_connector"):
        missing = [
            f"{name}_{suffix}"
            for suffix in ("USER", "PW", "HOST", "PORT", "DB")
            if f"{name}_{suffix}" not in os.environ
        ]
        if missing:
            raise KeyError(
                f"Missing connection environment variable(s): {', '.join(missing)}. "
                f"Set them, or point the job's source.connection at a different prefix."
            )
        user = urlquote(os.environ[f"{name}_USER"])
        password = urlquote(os.environ[f"{name}_PW"])
        host = os.environ[f"{name}_HOST"]
        port = os.environ[f"{name}_PORT"]
        db = os.environ[f"{name}_DB"]
        self.engine = sqlalchemy.create_engine(
            f"redshift+{driver}://{user}:{password}@{host}:{port}/{db}"
        )

    def run_query(self, sql: str) -> pd.DataFrame:
        with self.engine.connect() as con:
            return pd.read_sql(text(sql), con)

    def columns_by_table(self, tables: list[str]) -> dict[str, set[str]]:
        """Lowercased column names present on each of the given (optionally
        schema-qualified) tables, via information_schema, keyed by the table
        string as passed in. Lets the runner drop a configured grouping/metric
        column that no longer exists, and detect one that exists on more than
        one joined table (and would be ambiguous when referenced unqualified),
        instead of failing the whole crosstab query."""
        conditions = []
        params: dict[str, str] = {}
        for i, t in enumerate(tables):
            parts = t.split(".")
            params[f"table{i}"] = parts[-1]
            if len(parts) > 1:
                params[f"schema{i}"] = parts[-2]
                conditions.append(f"(table_name = :table{i} AND table_schema = :schema{i})")
            else:
                conditions.append(f"table_name = :table{i}")
        sql = (
            "SELECT table_schema, table_name, column_name FROM information_schema.columns "
            "WHERE " + " OR ".join(conditions)
        )
        with self.engine.connect() as con:
            rows = con.execute(text(sql), params).fetchall()

        by_qualified_name: dict[str, set[str]] = {}
        for schema, table_name, column_name in rows:
            by_qualified_name.setdefault(f"{schema}.{table_name}", set()).add(
                column_name.lower()
            )

        result: dict[str, set[str]] = {}
        for t in tables:
            if "." in t:
                result[t] = by_qualified_name.get(t, set())
            else:
                # Unqualified table name: merge columns from any schema match.
                merged: set[str] = set()
                for qualified, cols in by_qualified_name.items():
                    if qualified.split(".")[-1] == t:
                        merged |= cols
                result[t] = merged
        return result

    def fetch_rows(self, sql: str, chunksize: int = 100_000):
        """Escape hatch for custom logic that truly needs row-level data.
        Yields DataFrame chunks; see README 'Custom functions and scale'
        before pointing this at a full 260M-row table."""
        with self.engine.connect() as con:
            yield from pd.read_sql(text(sql), con, chunksize=chunksize)
