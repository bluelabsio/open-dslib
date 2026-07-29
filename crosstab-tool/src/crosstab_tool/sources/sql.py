from __future__ import annotations

import polars as pl

from crosstab_tool.spec.source_spec import SQLSourceSpec


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
    """

    def __init__(self, spec: SQLSourceSpec) -> None:
        self.spec = spec
        self._frame: pl.DataFrame | None = None

    def _fetch(self) -> pl.DataFrame:
        if self._frame is None:
            self._frame = pl.read_database_uri(self.spec.query, self.spec.connection)
        return self._frame

    def to_polars_lazyframe(self) -> pl.LazyFrame:
        return self._fetch().lazy()

    def describe_schema(self) -> dict[str, str]:
        return {name: str(dtype) for name, dtype in self._fetch().schema.items()}

    def estimated_row_count(self) -> int | None:
        return self._fetch().height
