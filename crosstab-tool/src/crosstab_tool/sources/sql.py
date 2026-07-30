from __future__ import annotations

import getpass
from urllib.parse import quote_plus

import polars as pl

from crosstab_tool.spec.source_spec import SQLSourceSpec

_DEFAULT_PORTS = {"postgresql": 5432, "redshift": 5439}


def _resolve_connection(spec: SQLSourceSpec) -> str:
    """Returns `spec.connection` as-is if set; otherwise prompts interactively for
    whatever's missing and builds a URI from it.

    Username/password are always prompted here -- there's no config field for them, so
    they never end up checked into a config file. `host`/`port`/`database` are prompted
    too if the spec didn't already supply them.
    """
    if spec.connection is not None:
        return spec.connection

    host = spec.host or input("Database host: ").strip()
    database = spec.database or input("Database name: ").strip()
    port = spec.port or _DEFAULT_PORTS.get(spec.dialect, 5432)
    user = input("Username: ").strip()
    password = getpass.getpass("Password: ")

    return f"{spec.dialect}://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database}"


class SQLSource:
    """Pulls `query`'s full result set via ConnectorX, then hands Polars an in-memory
    frame to aggregate locally.

    This is v1's documented scale-limited path (see docs/POLARS_SCALE.md): unlike the
    file sources, there is no lazy/pushdown execution here -- `GROUP BY`, filtering, and
    projection all need to happen in `query` itself if you want the warehouse's own
    engine to do that work, since Polars only ever sees the query's full result set.
    Practical at 100M+ row scale only when `query` already narrows the data down.

    The result is fetched once and cached on this instance (`describe_schema()` and
    `to_polars_lazyframe()` would otherwise each independently re-run `query`). Callers
    must still avoid building a *second* adapter for the same spec within one
    run_crosstab() call -- see core/runner.py's comment on why the adapter is built once
    and threaded through both validation and execution.

    If `spec.connection` isn't set, the connection URI is resolved (prompting
    interactively for whatever's missing, see `_resolve_connection`) once and cached
    alongside the fetched frame, so a user is only ever prompted once per instance.
    """

    def __init__(self, spec: SQLSourceSpec) -> None:
        self.spec = spec
        self._frame: pl.DataFrame | None = None
        self._connection: str | None = None

    def _fetch(self) -> pl.DataFrame:
        if self._frame is None:
            if self._connection is None:
                self._connection = _resolve_connection(self.spec)
            self._frame = pl.read_database_uri(self.spec.query, self._connection)
        return self._frame

    def to_polars_lazyframe(self) -> pl.LazyFrame:
        return self._fetch().lazy()

    def describe_schema(self) -> dict[str, str]:
        return {name: str(dtype) for name, dtype in self._fetch().schema.items()}

    def estimated_row_count(self) -> int | None:
        return self._fetch().height
