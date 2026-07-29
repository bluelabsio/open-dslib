from __future__ import annotations

import polars as pl

from crosstab_tool.spec.source_spec import CSVSourceSpec, ParquetSourceSpec


class ParquetSource:
    def __init__(self, spec: ParquetSourceSpec) -> None:
        self.spec = spec

    def to_polars_lazyframe(self) -> pl.LazyFrame:
        return pl.scan_parquet(self.spec.path)

    def describe_schema(self) -> dict[str, str]:
        schema = self.to_polars_lazyframe().collect_schema()
        return {name: str(dtype) for name, dtype in schema.items()}

    def estimated_row_count(self) -> int | None:
        return None


class CSVSource:
    def __init__(self, spec: CSVSourceSpec) -> None:
        self.spec = spec

    def to_polars_lazyframe(self) -> pl.LazyFrame:
        return pl.scan_csv(self.spec.path)

    def describe_schema(self) -> dict[str, str]:
        schema = self.to_polars_lazyframe().collect_schema()
        return {name: str(dtype) for name, dtype in schema.items()}

    def estimated_row_count(self) -> int | None:
        return None
