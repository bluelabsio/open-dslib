from __future__ import annotations

import polars as pl

from crosstab_tool.sources.base import DataSourceAdapter
from crosstab_tool.spec.source_spec import JoinSourceSpec


class JoinedSource:
    """Lazily joins two other sources -- e.g. a scores table and a separate covariates
    table -- without requiring the join to be written into a raw SQL query.

    Source-type-agnostic: `left`/`right` can be any `DataSourceAdapter` (SQL, file,
    DataFrame, even another `JoinedSource`), since this only composes each side's
    `to_polars_lazyframe()` -- it doesn't know or care what produced them.
    """

    def __init__(
        self, spec: JoinSourceSpec, left: DataSourceAdapter, right: DataSourceAdapter
    ) -> None:
        self.spec = spec
        self._left = left
        self._right = right

    def to_polars_lazyframe(self) -> pl.LazyFrame:
        left_lf = self._left.to_polars_lazyframe()
        right_lf = self._right.to_polars_lazyframe()
        if self.spec.join_keys is not None:
            return left_lf.join(right_lf, on=self.spec.join_keys, how=self.spec.how)
        return left_lf.join(
            right_lf, left_on=self.spec.left_on, right_on=self.spec.right_on, how=self.spec.how
        )

    def describe_schema(self) -> dict[str, str]:
        schema = self.to_polars_lazyframe().collect_schema()
        return {name: str(dtype) for name, dtype in schema.items()}

    def estimated_row_count(self) -> int | None:
        # Join cardinality isn't derivable from either side's row count alone (depends
        # on key overlap) -- unknown, same as ParquetSource/CSVSource.
        return None
