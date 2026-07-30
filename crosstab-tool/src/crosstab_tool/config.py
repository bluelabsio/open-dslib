"""Job configuration: YAML parsing, typed config objects, and validation.

The YAML config is the primary interface; the Python API is simply
constructing these dataclasses directly. Anything expressible declaratively
lives here; custom logic is registered in Python (see registry.py) and
referenced by name from the config.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

import yaml

from crosstab_tool.registry import AGGREGATIONS, CROSS_COLUMN_OPS


class ConfigError(ValueError):
    """Raised for invalid job configuration, with an actionable message."""


VALID_JOIN_TYPES = {"left", "inner", "right", "full"}
VALID_LAYOUTS = {"long", "wide"}
VALID_DESTINATIONS = {"csv", "excel", "sheets"}


@dataclass
class Join:
    table: str
    key: str  # join key, e.g. voterbase_id; USING(key) semantics
    how: str = "left"
    alias: str | None = None

    def __post_init__(self):
        if self.how not in VALID_JOIN_TYPES:
            raise ConfigError(
                f"Join to {self.table!r}: join type {self.how!r} is not one of "
                f"{sorted(VALID_JOIN_TYPES)}."
            )


@dataclass
class Source:
    """The base score table plus zero or more joined grouping-variable tables."""

    base_table: str
    connection: str = "REDSHIFT"  # env-var prefix, e.g. REDSHIFT_USER etc.
    joins: list[Join] = field(default_factory=list)


@dataclass
class Metric:
    """A score or counterfactual column to summarize.

    Numeric metrics get the aggregations in `aggregations` (default from the
    job). Categorical metrics get a frequency table: one share column per
    declared value.
    """

    column: str
    type: str = "numeric"  # numeric | categorical
    aggregations: list[str] | None = None  # None -> job default
    values: list[str] | None = None  # required for categorical
    label: str | None = None

    def __post_init__(self):
        if self.type not in ("numeric", "categorical"):
            raise ConfigError(
                f"Metric {self.column!r}: type must be 'numeric' or 'categorical', "
                f"got {self.type!r}."
            )
        if self.type == "categorical" and not self.values:
            raise ConfigError(
                f"Categorical metric {self.column!r} needs an explicit `values:` list "
                "so frequency columns can be generated in SQL (one share column per value)."
            )
        if self.type == "categorical" and self.aggregations:
            raise ConfigError(
                f"Categorical metric {self.column!r}: aggregations are not configurable "
                "for categorical columns; a frequency table is always produced."
            )


@dataclass
class Grouping:
    """A grouping variable. `order` controls the numbered-category prefix
    ('01 Age') that keeps output row order stable; it is assigned from list
    position when omitted."""

    column: str
    label: str | None = None
    order: int | None = None

    @property
    def display_label(self) -> str:
        return self.label or self.column

    def category(self) -> str:
        assert self.order is not None, "order is assigned during Job validation"
        return f"{self.order:02d} {self.display_label}"


@dataclass
class CrossColumn:
    """A computation across two already-aggregated result columns."""

    op: str  # difference | product | a registered custom name
    left: str
    right: str
    label: str | None = None

    @property
    def output_column(self) -> str:
        return self.label or f"{self.op}_{self.left}_{self.right}"


@dataclass
class Output:
    destination: str = "csv"  # csv | excel | sheets
    path: str | None = None  # csv/excel file path
    spreadsheet: str | None = None  # sheets: spreadsheet title or key
    worksheet: str = "crosstabs"
    layout: str = "long"
    credentials_file: str | None = None  # sheets service-account JSON
    share_with: list[str] = field(default_factory=list)  # emails granted access on create
    number_format: str | None = "0.0000"  # excel/sheets score formatting

    def __post_init__(self):
        if self.destination not in VALID_DESTINATIONS:
            raise ConfigError(
                f"Output destination {self.destination!r} is not one of "
                f"{sorted(VALID_DESTINATIONS)}."
            )
        if self.layout not in VALID_LAYOUTS:
            raise ConfigError(
                f"Output layout {self.layout!r} must be 'long' or 'wide'."
            )
        if self.destination in ("csv", "excel") and not self.path:
            raise ConfigError(
                f"Output destination {self.destination!r} requires a `path:`."
            )
        if self.destination == "sheets" and not self.spreadsheet:
            raise ConfigError(
                "Output destination 'sheets' requires a `spreadsheet:` (title or key)."
            )


@dataclass
class Job:
    name: str
    source: Source
    scores: list[Metric]
    groupings: list[Grouping]
    counterfactuals: list[Metric] = field(default_factory=list)
    aggregations: list[str] = field(default_factory=lambda: ["count", "mean"])
    cross_column: list[CrossColumn] = field(default_factory=list)
    output: Output = field(default_factory=lambda: Output(destination="csv", path="crosstab.csv"))
    score_version: str | None = None
    notes: str | None = None
    topline_label: str = "Topline"
    config_hash: str | None = None  # sha256 of source YAML, set by load_job

    def __post_init__(self):
        if not self.scores:
            raise ConfigError("A job needs at least one score column under `scores:`.")
        if not self.groupings:
            raise ConfigError(
                "A job needs at least one grouping variable under `groupings:` "
                "(a topline row is always added automatically)."
            )
        self._assign_grouping_order()
        self._validate_aggregations()
        self._validate_cross_column()

    @property
    def metrics(self) -> list[Metric]:
        return list(self.scores) + list(self.counterfactuals)

    def _assign_grouping_order(self):
        seen: dict[int, str] = {}
        next_auto = 1
        for g in self.groupings:
            if g.order is None:
                while next_auto in seen:
                    next_auto += 1
                g.order = next_auto
            if g.order in seen:
                raise ConfigError(
                    f"Groupings {seen[g.order]!r} and {g.column!r} both have order "
                    f"{g.order}; orders must be unique (they control row ordering)."
                )
            if g.order == 0:
                raise ConfigError(
                    f"Grouping {g.column!r}: order 0 is reserved for the Topline row."
                )
            seen[g.order] = g.column

    def _validate_aggregations(self):
        for agg_list, owner in [(self.aggregations, "job default")] + [
            (m.aggregations, f"metric {m.column!r}")
            for m in self.metrics
            if m.aggregations
        ]:
            for agg in agg_list:
                if agg not in AGGREGATIONS:
                    raise ConfigError(
                        f"Unknown aggregation {agg!r} ({owner}). Built-ins: "
                        f"{sorted(AGGREGATIONS)}. Custom aggregations must be "
                        "registered via crosstab_tool.registry.register_aggregation() "
                        "before the job is loaded."
                    )

    def _validate_cross_column(self):
        available = set(self.result_columns())
        for cc in self.cross_column:
            if cc.op not in CROSS_COLUMN_OPS:
                raise ConfigError(
                    f"Unknown cross-column op {cc.op!r}. Built-ins: "
                    f"{sorted(CROSS_COLUMN_OPS)}. Custom ops must be registered via "
                    "crosstab_tool.registry.register_cross_column() before the job is loaded."
                )
            for side, col in (("left", cc.left), ("right", cc.right)):
                if col not in available:
                    raise ConfigError(
                        f"Cross-column {cc.output_column!r}: {side} column {col!r} is not "
                        f"produced by this job. Available result columns: {sorted(available)}."
                    )
            available.add(cc.output_column)

    def result_columns(self) -> list[str]:
        """The statistic columns the aggregation step will produce, in order."""
        cols = ["count"]
        for m in self.metrics:
            if m.type == "categorical":
                cols += [f"freq_{m.column}_{_slug(v)}" for v in m.values]
            else:
                for agg in m.aggregations or self.aggregations:
                    if agg == "count":
                        continue  # a single shared row count, already included
                    cols.append(f"{AGGREGATIONS[agg].prefix}_{m.column}")
        return cols


def _slug(value: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in str(value)).strip("_").lower()


def _build(cls, data: dict, context: str):
    if not isinstance(data, dict):
        raise ConfigError(f"Expected a mapping for {context}, got {type(data).__name__}.")
    fields = {f for f in cls.__dataclass_fields__}
    unknown = set(data) - fields
    if unknown:
        raise ConfigError(
            f"Unknown key(s) {sorted(unknown)} in {context}. Valid keys: {sorted(fields)}."
        )
    return cls(**data)


def job_from_dict(raw: dict) -> Job:
    """Build a validated Job from a plain dict (parsed YAML)."""
    if not isinstance(raw, dict):
        raise ConfigError("Top-level config must be a mapping.")
    data = dict(raw)

    src = data.get("source")
    if src is None:
        raise ConfigError("Config is missing the required `source:` section.")
    joins = [_build(Join, j, "source.joins entry") for j in src.get("joins", [])]
    data["source"] = Source(
        base_table=src.get("base_table") or _missing("source.base_table"),
        connection=src.get("connection", "REDSHIFT"),
        joins=joins,
    )

    data["scores"] = [_build(Metric, m, "scores entry") for m in data.get("scores", [])]
    data["counterfactuals"] = [
        _build(Metric, m, "counterfactuals entry") for m in data.get("counterfactuals", [])
    ]
    data["groupings"] = [
        _build(Grouping, g, "groupings entry") if isinstance(g, dict) else Grouping(column=g)
        for g in data.get("groupings", [])
    ]
    data["cross_column"] = [
        _build(CrossColumn, c, "cross_column entry") for c in data.get("cross_column", [])
    ]
    if "output" in data:
        data["output"] = _build(Output, data["output"], "output section")
    return _build(Job, data, "job config")


def _missing(key: str):
    raise ConfigError(f"Config is missing required key `{key}`.")


def load_job(path: str) -> Job:
    """Load and validate a job config from a YAML file."""
    with open(path) as f:
        text = f.read()
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise ConfigError(f"Could not parse YAML in {path}: {e}") from e
    job = job_from_dict(raw)
    job.config_hash = hashlib.sha256(text.encode()).hexdigest()
    return job
