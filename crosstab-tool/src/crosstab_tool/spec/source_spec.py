from typing import Annotated, Any, Literal, Optional, Union

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

    `connection` is optional precisely so credentials don't have to live in a config
    file: when it's omitted, `sources/sql.py` prompts interactively for username/
    password (and for `host`/`port`/`database` too, if those aren't given here either)
    rather than failing validation. `host`/`port`/`database` are safe to check into a
    config file since they aren't secrets; username/password never are, so there's no
    corresponding config field for them.
    """

    type: Literal["sql"] = "sql"
    # a ConnectorX-compatible URI, e.g. postgresql://user:pw@host/db (see class docstring
    # for why this -- and only this -- is optional; noqa: UP045 to match this module's
    # Optional[X] convention for pydantic fields, see comparison_spec.py's NB)
    connection: Optional[str] = None  # noqa: UP045
    query: str  # a full SQL query, e.g. "SELECT region, score FROM scored_population"
    dialect: str = "postgresql"  # used to build the URI when `connection` is omitted
    host: Optional[str] = None  # noqa: UP045
    port: Optional[int] = None  # noqa: UP045
    database: Optional[str] = None  # noqa: UP045


# NB: kept as typing.Union (not `X | Y`) since this is a runtime expression, not an
# annotation -- `X | Y` for classes needs Python 3.10's PEP 604 support at the type level.
DataSourceSpec = Annotated[
    Union[ParquetSourceSpec, CSVSourceSpec, DataFrameSourceSpec, SQLSourceSpec],  # noqa: UP007
    Field(discriminator="type"),
]
