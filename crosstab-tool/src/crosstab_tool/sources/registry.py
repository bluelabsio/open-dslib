from typing import Any

import polars as pl

from crosstab_tool.sources.base import DataSourceAdapter
from crosstab_tool.sources.dataframe import ArrowSource, PandasSource, PolarsSource
from crosstab_tool.sources.files import CSVSource, ParquetSource
from crosstab_tool.sources.joined import JoinedSource
from crosstab_tool.sources.sql import SQLSource
from crosstab_tool.spec.source_spec import (
    CSVSourceSpec,
    DataFrameSourceSpec,
    DataSourceSpec,
    JoinSourceSpec,
    ParquetSourceSpec,
    SQLSourceSpec,
)


def build_source(spec: DataSourceSpec) -> DataSourceAdapter:
    if isinstance(spec, ParquetSourceSpec):
        return ParquetSource(spec)
    if isinstance(spec, CSVSourceSpec):
        return CSVSource(spec)
    if isinstance(spec, DataFrameSourceSpec):
        return _build_dataframe_source(spec.data)
    if isinstance(spec, SQLSourceSpec):
        return SQLSource(spec)
    if isinstance(spec, JoinSourceSpec):
        return JoinedSource(spec, build_source(spec.left), build_source(spec.right))
    raise ValueError(f"Unsupported source spec: {spec!r}")


def _build_dataframe_source(data: Any) -> DataSourceAdapter:
    if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
        return PolarsSource(data)

    try:
        import pandas as pd
    except ImportError:
        pass
    else:
        if isinstance(data, pd.DataFrame):
            return PandasSource(data)

    try:
        import pyarrow as pa
    except ImportError:
        pass
    else:
        if isinstance(data, (pa.Table, pa.RecordBatch)):
            return ArrowSource(data)

    raise TypeError(
        f"Unsupported in-memory data type: {type(data)!r} "
        "(expected a pandas/Polars DataFrame or a pyarrow Table/RecordBatch)"
    )
