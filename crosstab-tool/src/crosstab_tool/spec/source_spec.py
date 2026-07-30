from typing import Annotated, Any, Literal, Optional, Union

from pydantic import ConfigDict, Field, model_validator

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


class JoinSourceSpec(StrictModel):
    """Combines two other sources into one lazily-joined source -- e.g. a scores table
    and a separate covariates table -- without requiring the join to be written into a
    raw SQL query. Source-type-agnostic: `left`/`right` can be any `DataSourceSpec`
    (SQL, file, DataFrame, even another join), since `sources/joined.py`'s adapter only
    composes each side's `to_polars_lazyframe()` -- no engine changes needed for this.

    Join keys: either `join_keys` (same column name(s) on both sides) or `left_on` +
    `right_on` (different names per side) -- exactly one of these shapes must be given.
    `how` mirrors Polars' `join(how=...)` and has no default: an inner join silently
    drops unmatched rows, so the choice must be made explicitly rather than defaulted.
    """

    type: Literal["join"] = "join"
    left: "DataSourceSpec"
    right: "DataSourceSpec"
    how: Literal["inner", "left", "full"]
    join_keys: Optional[list[str]] = None  # noqa: UP045
    left_on: Optional[list[str]] = None  # noqa: UP045
    right_on: Optional[list[str]] = None  # noqa: UP045

    @model_validator(mode="after")
    def _validate_keys(self) -> "JoinSourceSpec":
        shared = self.join_keys is not None
        split = self.left_on is not None or self.right_on is not None

        if shared and split:
            raise ValueError(
                "specify either join_keys (same column name(s) on both sides) or "
                "left_on/right_on (different names per side), not both"
            )
        if not shared and not split:
            raise ValueError("join requires either join_keys or left_on+right_on to be set")
        if shared and not self.join_keys:
            raise ValueError("join_keys must contain at least one column")
        if split:
            if self.left_on is None or self.right_on is None:
                raise ValueError("left_on and right_on must both be set together")
            if not self.left_on or not self.right_on:
                raise ValueError("left_on/right_on must each contain at least one column")
            if len(self.left_on) != len(self.right_on):
                raise ValueError("left_on and right_on must have the same number of columns")
        return self


# NB: kept as typing.Union (not `X | Y`) since this is a runtime expression, not an
# annotation -- `X | Y` for classes needs Python 3.10's PEP 604 support at the type level.
DataSourceSpec = Annotated[
    Union[ParquetSourceSpec, CSVSourceSpec, DataFrameSourceSpec, SQLSourceSpec, JoinSourceSpec],  # noqa: UP007
    Field(discriminator="type"),
]

# JoinSourceSpec.left/right are forward references to DataSourceSpec (defined above,
# after JoinSourceSpec, since the union must include JoinSourceSpec itself to allow
# nested joins) -- this resolves them now rather than lazily at first validation.
JoinSourceSpec.model_rebuild()
