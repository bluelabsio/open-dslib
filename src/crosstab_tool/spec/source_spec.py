from typing import Annotated, Any, Literal, Union

from pydantic import ConfigDict, Field

from crosstab_tool.spec._base import StrictModel


class ParquetSourceSpec(StrictModel):
    type: Literal["parquet"] = "parquet"
    path: str


class CSVSourceSpec(StrictModel):
    type: Literal["csv"] = "csv"
    path: str


class DataFrameSourceSpec(StrictModel):
    """An already-in-memory pandas/Polars/Arrow object. Python-API-only: there is no
    sensible YAML/JSON serialization for a live in-memory object, so this spec is only
    ever constructed programmatically, never parsed from a config file."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    type: Literal["dataframe"] = "dataframe"
    data: Any


class SQLSourceSpec(StrictModel):
    """A SQL warehouse source, read via ConnectorX (pl.read_database_uri).

    v1 scale-limited path: `query`'s full result set is pulled to the client before any
    Polars aggregation happens -- there is no GROUP BY pushdown into the warehouse. See
    docs/POLARS_SCALE.md for the tradeoffs and the stratified-sampling mitigation this
    documents but doesn't (yet) implement. Practical for v1 only when `query` already
    filters/pre-aggregates down to a manageable size.
    """

    type: Literal["sql"] = "sql"
    connection: str  # a ConnectorX-compatible URI, e.g. postgresql://user:pw@host/db
    query: str  # a full SQL query, e.g. "SELECT region, score FROM scored_population"


# NB: kept as typing.Union (not `X | Y`) since this is a runtime expression, not an
# annotation -- `X | Y` for classes needs Python 3.10's PEP 604 support at the type level.
DataSourceSpec = Annotated[
    Union[ParquetSourceSpec, CSVSourceSpec, DataFrameSourceSpec, SQLSourceSpec],  # noqa: UP007
    Field(discriminator="type"),
]
