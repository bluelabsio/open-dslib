from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field


class ParquetSourceSpec(BaseModel):
    type: Literal["parquet"] = "parquet"
    path: str


class CSVSourceSpec(BaseModel):
    type: Literal["csv"] = "csv"
    path: str


class DataFrameSourceSpec(BaseModel):
    """An already-in-memory pandas/Polars/Arrow object. Python-API-only: there is no
    sensible YAML/JSON serialization for a live in-memory object, so this spec is only
    ever constructed programmatically, never parsed from a config file."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    type: Literal["dataframe"] = "dataframe"
    data: Any


# NB: kept as typing.Union (not `X | Y`) since this is a runtime expression, not an
# annotation -- `X | Y` for classes needs Python 3.10's PEP 604 support at the type level.
DataSourceSpec = Annotated[
    Union[ParquetSourceSpec, CSVSourceSpec, DataFrameSourceSpec],  # noqa: UP007
    Field(discriminator="type"),
]
