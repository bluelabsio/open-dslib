"""Job configuration schema for the crosstab tool.

Maps directly to Requirements Doc v0.2 Section 4.2 ("Job Configuration").
A job config (YAML/JSON) is the primary interface; this module is the
validated, typed representation the rest of the tool operates on. Fields
carry comments back to the requirement they satisfy.
"""
from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class AggFunction(str, Enum):
    """Built-in aggregation functions (Req 4.3). Not exhaustive by design —
    custom functions are referenced separately via a dotted import path
    (AggregationDefaults.custom_functions / ColumnRef.custom_aggregations),
    not added to this enum."""

    MEAN = "mean"
    COUNT = "count"
    FREQUENCY = "frequency"  # share of records per level (categorical counterfactuals)
    SUM = "sum"
    MIN = "min"
    MAX = "max"
    MEDIAN = "median"


class CrossColumnOp(str, Enum):
    DIFFERENCE = "difference"
    MULTIPLY = "multiply"
    CUSTOM = "custom"
    # Reserved, not implemented in v1 (Req 4.4 + Section 6/7): stubbed here
    # so the config schema doesn't need a breaking change when significance
    # testing is added later.
    TTEST = "ttest"
    CHI_SQUARE = "chi_square"


class SourceType(str, Enum):
    REDSHIFT = "redshift"
    # Deferred per Req 4.1 — reserved values so DataSourceConfig.type is
    # forward-compatible once these backends are built:
    POSTGRES = "postgres"
    GOOGLE_SHEETS = "google_sheets"
    CSV = "csv"
    EXCEL = "excel"
    S3 = "s3"


class OutputDestination(str, Enum):
    GOOGLE_SHEETS = "google_sheets"
    CSV = "csv"
    EXCEL = "excel"


class DataSourceConfig(BaseModel):
    """One named, queryable source (Req 4.1). `connection` is an env-var
    prefix — mirrors open-dslib's EngineContext pattern, e.g.
    `connection: REDSHIFT_MAIN` reads REDSHIFT_MAIN_USER/_PW/_HOST/_DB/_PORT.
    """

    name: str
    type: SourceType = SourceType.REDSHIFT
    connection: str
    table: str | None = None
    query: str | None = None  # raw SQL escape hatch instead of `table`

    @model_validator(mode="after")
    def _one_of_table_or_query(self) -> "DataSourceConfig":
        if bool(self.table) == bool(self.query):
            raise ValueError(
                f"source '{self.name}' must set exactly one of `table` or `query`"
            )
        return self


class JoinConfig(BaseModel):
    """A join from the job's base source onto another named source.
    Req 4.1: "joins ... must be configurable by key and join type".

    Field is named `key`, not `on` — PyYAML's default (YAML 1.1) loader
    parses a bare `on:` mapping key as the boolean `True`, not the string
    "on", which silently breaks a config that looks correct. `key` avoids
    the footgun entirely.
    """

    source: str  # name of a DataSourceConfig to join in
    key: str | list[str]  # join key(s), e.g. "voterbase_id"
    how: Literal["left", "inner", "right", "full"] = "left"


class BaseConfig(BaseModel):
    """The base CTE: one source plus zero or more joins — mirrors Appendix
    A's `base AS (SELECT ... LEFT JOIN ... USING(key))`."""

    from_: str = Field(alias="from")
    joins: list[JoinConfig] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


class ColumnRef(BaseModel):
    """A score or counterfactual column (Req 4.2). Counterfactuals use the
    same shape as scores per Req 2.1/3: "numeric ... or labeled categorical
    field[s] ... using the same grouping logic"."""

    name: str  # output name, e.g. "p_support"
    source: str  # which DataSourceConfig it lives in
    column: str  # actual column name in that source
    kind: Literal["numeric", "categorical"] = "numeric"
    aggregations: list[AggFunction] | None = None  # overrides job-level default
    custom_aggregations: list[str] = Field(default_factory=list)  # dotted paths


class GroupingVariable(BaseModel):
    """One row-block in the tidy output (Req 4.3) — a "category" in the
    Appendix A sense (e.g. "01 Age"). `label` controls output row order
    exactly like the reference SQL's numbered category convention
    (Req 4.5.1)."""

    label: str  # e.g. "01 Age" — numbering controls ORDER BY 1, 2
    column: str
    include_topline: bool | None = None  # None => inherit job-level default


class CrossColumnConfig(BaseModel):
    """A computed column across two or more already-aggregated columns
    (Req 4.4)."""

    name: str
    op: CrossColumnOp
    inputs: list[str]  # names of ColumnRefs (scores/counterfactuals)
    function: str | None = None  # dotted path, required when op == CUSTOM

    @model_validator(mode="after")
    def _custom_needs_function(self) -> "CrossColumnConfig":
        if self.op == CrossColumnOp.CUSTOM and not self.function:
            raise ValueError(f"cross_column '{self.name}' has op=custom but no `function`")
        return self


class AggregationDefaults(BaseModel):
    """Req 4.2: "aggregation function(s) to apply per column (default: mean
    for numeric scores, count/frequency for categorical), with support for
    passing a custom function"."""

    default: list[AggFunction] = Field(
        default_factory=lambda: [AggFunction.MEAN, AggFunction.COUNT]
    )
    custom_functions: dict[str, str] = Field(default_factory=dict)  # name -> "module:function"


class OutputConfig(BaseModel):
    """Req 4.5: "Output destination ... should be a job-level configuration
    choice, not hard-coded." Google Sheets is primary; CSV/Excel secondary
    (Req 4.5.2)."""

    destination: OutputDestination = OutputDestination.GOOGLE_SHEETS
    spreadsheet_id: str | None = None
    tab: str | None = None
    layout: Literal["long", "wide"] = "long"  # Req 4.5.1: long/tidy is the default shape
    path: str | None = None  # for csv/excel destinations

    @model_validator(mode="after")
    def _destination_has_target(self) -> "OutputConfig":
        if self.destination == OutputDestination.GOOGLE_SHEETS and not (
            self.spreadsheet_id and self.tab
        ):
            raise ValueError("google_sheets output requires spreadsheet_id and tab")
        if self.destination in (OutputDestination.CSV, OutputDestination.EXCEL) and not self.path:
            raise ValueError(f"{self.destination.value} output requires `path`")
        return self


class RunMetadataConfig(BaseModel):
    """Req 4.6: "enough to answer 'what produced this sheet, and can I
    regenerate it.'" Actual timestamp/config-hash/SQL capture happens at
    run time (see metadata/run_metadata.py); this just lets a job opt out,
    control whether the executed SQL itself gets saved, and attach
    free-text notes.

    `save_sql` matters because the generated SQL can drift from what
    `query/builder.py` would produce today if the tool's SQL-generation
    logic changes in a later version — saving the literal SQL that ran is
    stronger reproducibility evidence than the config hash alone."""

    capture: bool = True
    save_sql: bool = True  # write the exact executed SQL alongside the run's metadata
    artifacts_dir: str = "runs"  # where per-run metadata (+ saved SQL) is written
    notes: str | None = None


class JobMeta(BaseModel):
    name: str
    model_version: str  # e.g. "p_support_v3_20260416" (Req 4.6 identifier)
    notes: str | None = None


class JobConfig(BaseModel):
    """Top-level job spec — the object produced by config/loader.py from a
    YAML/JSON file, and the only input the rest of the tool needs."""

    job: JobMeta
    sources: list[DataSourceConfig]
    base: BaseConfig
    scores: list[ColumnRef]
    counterfactuals: list[ColumnRef] = Field(default_factory=list)
    grouping_variables: list[GroupingVariable]
    include_topline: bool = True  # job-level default for GroupingVariable.include_topline
    aggregations: AggregationDefaults = Field(default_factory=AggregationDefaults)
    cross_column: list[CrossColumnConfig] = Field(default_factory=list)
    output: OutputConfig
    run_metadata: RunMetadataConfig = Field(default_factory=RunMetadataConfig)

    @model_validator(mode="after")
    def _names_resolve(self) -> "JobConfig":
        source_names = {s.name for s in self.sources}
        if self.base.from_ not in source_names:
            raise ValueError(f"base.from '{self.base.from_}' is not a defined source")
        for j in self.base.joins:
            if j.source not in source_names:
                raise ValueError(f"join source '{j.source}' is not a defined source")

        all_columns = [*self.scores, *self.counterfactuals]
        for col in all_columns:
            if col.source not in source_names:
                raise ValueError(
                    f"column '{col.name}' references undefined source '{col.source}'"
                )

        col_names = {c.name for c in all_columns}
        for cc in self.cross_column:
            missing = [i for i in cc.inputs if i not in col_names]
            if missing:
                raise ValueError(
                    f"cross_column '{cc.name}' references unknown column(s): {missing}"
                )
        return self
