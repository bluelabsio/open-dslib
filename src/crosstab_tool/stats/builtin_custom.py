"""Stats that don't reduce cleanly to a single native pl.Expr reduction, computed via
`map_batches` instead. This proves the registry's second stat-authoring path (needed
later for label-based metrics like AUC/calibration/lift) without any change to
engine/polars_engine.py or core/runner.py -- both paths return a plain pl.Expr."""

from __future__ import annotations

from typing import Any

import polars as pl
from scipy import stats as scipy_stats

from crosstab_tool.stats.registry import StatFunction, register_stat


def _skew_batch(values: pl.Series) -> float:
    values = values.drop_nulls()
    if len(values) < 3:
        return float("nan")
    return float(scipy_stats.skew(values.to_numpy()))


def _kurtosis_batch(values: pl.Series) -> float:
    values = values.drop_nulls()
    if len(values) < 4:
        return float("nan")
    return float(scipy_stats.kurtosis(values.to_numpy()))


def _map_batches_stat(name: str, batch_fn: Any) -> StatFunction:
    def build_expr(column: str, params: dict[str, Any]) -> pl.Expr:
        return (
            pl.col(column)
            .map_batches(batch_fn, return_dtype=pl.Float64, returns_scalar=True)
            .alias(f"{column}_{name}")
        )

    def output_columns(column: str, params: dict[str, Any]) -> list[str]:
        return [f"{column}_{name}"]

    return StatFunction(name=name, build_expr=build_expr, output_columns=output_columns)


register_stat(_map_batches_stat("skew", _skew_batch))
register_stat(_map_batches_stat("kurtosis", _kurtosis_batch))
