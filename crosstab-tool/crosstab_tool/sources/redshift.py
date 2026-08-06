"""Redshift data source (Req 4.1 — v1's only in-scope backend).

Connection pattern intentionally mirrors open-dslib's `EngineContext`:
env-var-prefixed credentials over a SQLAlchemy engine. That pattern was
the one piece of open-dslib judged worth carrying forward — see
docs/architecture.md ("What we kept from the existing repos").
"""
from __future__ import annotations

import os

import certifi
import pandas as pd
from sqlalchemy import create_engine, text

from crosstab_tool.sources.base import DataSource


class RedshiftSource(DataSource):
    def _engine_url(self) -> str:
        prefix = self.connection
        user = os.environ[f"{prefix}_USER"]
        pw = os.environ[f"{prefix}_PW"]
        host = os.environ[f"{prefix}_HOST"]
        db = os.environ[f"{prefix}_DB"]
        port = os.environ.get(f"{prefix}_PORT", "5439")
        return f"redshift+psycopg2://{user}:{pw}@{host}:{port}/{db}"

    def _connect_args(self) -> dict:
        # sqlalchemy-redshift defaults sslrootcert to its own bundled AWS
        # Redshift CA file, which doesn't validate connections proxied
        # through BlueLabs' Satori layer (a different cert chain). Point
        # at certifi's public CA bundle instead — keeps sslmode=verify-full
        # (real verification), just against the right trust store. An env
        # var override is here in case a connection ever needs a private CA.
        prefix = self.connection
        sslrootcert = os.environ.get(f"{prefix}_SSLROOTCERT", certifi.where())
        return {"sslrootcert": sslrootcert}

    def execute(self, sql: str) -> pd.DataFrame:
        # `sql` may be multiple `;`-separated statements (e.g. a
        # CREATE TEMP TABLE materializing the join, followed by the SELECT
        # that reads from it) that must run on the same session, since a
        # temp table only exists for the connection that created it.
        # psycopg2 sends the whole string to Redshift in one round trip and
        # fetches results for the last statement only -- exactly what's
        # needed here, and why this isn't split into separate `execute()`
        # calls on `conn`.
        engine = create_engine(self._engine_url(), connect_args=self._connect_args())
        with engine.connect() as conn:
            return pd.read_sql(text(sql), conn)

    def qualified_table(self, table: str) -> str:
        return table  # config already supplies schema.table form
