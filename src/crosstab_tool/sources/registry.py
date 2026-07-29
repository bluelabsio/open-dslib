from crosstab_tool.sources.base import DataSourceAdapter
from crosstab_tool.sources.files import CSVSource, ParquetSource
from crosstab_tool.spec.source_spec import CSVSourceSpec, DataSourceSpec, ParquetSourceSpec


def build_source(spec: DataSourceSpec) -> DataSourceAdapter:
    if isinstance(spec, ParquetSourceSpec):
        return ParquetSource(spec)
    if isinstance(spec, CSVSourceSpec):
        return CSVSource(spec)
    raise ValueError(f"Unsupported source spec: {spec!r}")
