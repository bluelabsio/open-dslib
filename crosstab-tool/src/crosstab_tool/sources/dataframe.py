from __future__ import annotations

from typing import Any

import polars as pl


class PandasSource:
    def __init__(self, data: Any) -> None:
        self.data = data

    def to_polars_lazyframe(self) -> pl.LazyFrame:
        return pl.from_pandas(self.data).lazy()

    def describe_schema(self) -> dict[str, str]:
        schema = self.to_polars_lazyframe().collect_schema()
        return {name: str(dtype) for name, dtype in schema.items()}

    def estimated_row_count(self) -> int | None:
        return len(self.data)


class PolarsSource:
    def __init__(self, data: pl.DataFrame | pl.LazyFrame) -> None:
        self.data = data

    def to_polars_lazyframe(self) -> pl.LazyFrame:
        return self.data.lazy() if isinstance(self.data, pl.DataFrame) else self.data

    def describe_schema(self) -> dict[str, str]:
        schema = self.to_polars_lazyframe().collect_schema()
        return {name: str(dtype) for name, dtype in schema.items()}

    def estimated_row_count(self) -> int | None:
        return len(self.data) if isinstance(self.data, pl.DataFrame) else None


class ArrowSource:
    def __init__(self, data: Any) -> None:
        self.data = data

    def to_polars_lazyframe(self) -> pl.LazyFrame:
        frame = pl.from_arrow(self.data)
        assert isinstance(frame, pl.DataFrame)  # Table/RecordBatch always -> DataFrame
        return frame.lazy()

    def describe_schema(self) -> dict[str, str]:
        schema = self.to_polars_lazyframe().collect_schema()
        return {name: str(dtype) for name, dtype in schema.items()}

    def estimated_row_count(self) -> int | None:
        return self.data.num_rows
