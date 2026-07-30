"""Pluggable aggregation and cross-column function registries.

Built-in aggregations compile to SQL and run inside Redshift (the default,
scale-safe path). Custom aggregations are registered as SQL templates so they
also push down — a deliberate constraint: anything registered here runs on the
full source table, so it must be expressible in Redshift SQL.

Cross-column functions run in Python on the already-aggregated (small) result
DataFrame, so they may be arbitrary callables. Statistical tests plug in
through the same interface later.

If a computation truly needs row-level data in Python, that is the documented
escape hatch — pull a partial aggregation or column subset explicitly (see
README "Custom functions and scale") rather than this registry silently
falling back to an in-memory group-by on 260M rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd


@dataclass(frozen=True)
class Aggregation:
    """A SQL-push-down aggregation. `template` is formatted with {col}."""

    name: str
    template: str
    prefix: str  # result column prefix, e.g. avg -> avg_p_support


AGGREGATIONS: dict[str, Aggregation] = {}


def register_aggregation(name: str, template: str, prefix: str | None = None) -> None:
    """Register a custom SQL aggregation, e.g.
    register_aggregation("stddev", "STDDEV({col})").
    """
    if "{col}" not in template and name != "count":
        raise ValueError(f"Aggregation {name!r}: template must contain '{{col}}'.")
    AGGREGATIONS[name] = Aggregation(name, template, prefix or name)


for _name, _template, _prefix in [
    ("mean", "AVG({col})", "avg"),
    ("count", "COUNT(*)", "count"),
    ("sum", "SUM({col})", "sum"),
    ("min", "MIN({col})", "min"),
    ("max", "MAX({col})", "max"),
    ("median", "MEDIAN({col})", "median"),
]:
    register_aggregation(_name, _template, _prefix)


# Cross-column ops: (left_series, right_series) -> series, applied per row of
# the aggregated result table.
CROSS_COLUMN_OPS: dict[str, Callable[[pd.Series, pd.Series], pd.Series]] = {}


def register_cross_column(name: str, fn: Callable[[pd.Series, pd.Series], pd.Series]) -> None:
    """Register a custom cross-column function (including, later, statistical
    tests — anything with the (left, right) -> result signature)."""
    CROSS_COLUMN_OPS[name] = fn


register_cross_column("difference", lambda a, b: a - b)
register_cross_column("product", lambda a, b: a * b)
register_cross_column("ratio", lambda a, b: a / b)
