from __future__ import annotations

import logging

import polars as pl

from crosstab_tool.spec.source_spec import CSVSourceSpec, ParquetSourceSpec

logger = logging.getLogger(__name__)


class ParquetSource:
    def __init__(self, spec: ParquetSourceSpec) -> None:
        self.spec = spec

    def to_polars_lazyframe(self) -> pl.LazyFrame:
        logger.debug("Scanning parquet source: %s", self.spec.path)
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
        logger.debug("Scanning CSV source: %s", self.spec.path)
        return pl.scan_csv(self.spec.path)

    def describe_schema(self) -> dict[str, str]:
        schema = self.to_polars_lazyframe().collect_schema()
        return {name: str(dtype) for name, dtype in schema.items()}

    def estimated_row_count(self) -> int | None:
        return None
