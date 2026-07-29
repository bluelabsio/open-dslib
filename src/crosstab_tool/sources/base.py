from __future__ import annotations

from typing import Protocol

import polars as pl


class DataSourceAdapter(Protocol):
    """Narrow interface every current/future source must satisfy."""

    def to_polars_lazyframe(self) -> pl.LazyFrame: ...

    def describe_schema(self) -> dict[str, str]: ...

    def estimated_row_count(self) -> int | None: ...
