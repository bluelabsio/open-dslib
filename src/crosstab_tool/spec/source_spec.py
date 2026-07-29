from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field


class ParquetSourceSpec(BaseModel):
    type: Literal["parquet"] = "parquet"
    path: str


class CSVSourceSpec(BaseModel):
    type: Literal["csv"] = "csv"
    path: str


# NB: kept as typing.Union (not `X | Y`) since this is a runtime expression, not an
# annotation -- `X | Y` for classes needs Python 3.10's PEP 604 support at the type level.
DataSourceSpec = Annotated[
    Union[ParquetSourceSpec, CSVSourceSpec],  # noqa: UP007
    Field(discriminator="type"),
]
