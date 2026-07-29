from __future__ import annotations

from typing import Any

import polars as pl

from crosstab_tool.stats.registry import StatFunction, register_stat


def _simple(name: str, expr_fn: Any, requires_numeric: bool = True) -> StatFunction:
    def build_expr(column: str, params: dict[str, Any]) -> pl.Expr:
        return expr_fn(pl.col(column)).alias(f"{column}_{name}")

    def output_columns(column: str, params: dict[str, Any]) -> list[str]:
        return [f"{column}_{name}"]

    return StatFunction(
        name=name,
        build_expr=build_expr,
        output_columns=output_columns,
        requires_numeric=requires_numeric,
    )


register_stat(_simple("count", lambda c: c.count(), requires_numeric=False))
register_stat(_simple("mean", lambda c: c.mean()))
register_stat(_simple("std", lambda c: c.std()))
register_stat(_simple("min", lambda c: c.min()))
register_stat(_simple("max", lambda c: c.max()))


def _percentile_output_name(column: str, params: dict[str, Any]) -> str:
    q = _require_quantile(params)
    return f"{column}_p{q * 100:g}"


def _require_quantile(params: dict[str, Any]) -> float:
    if "q" not in params:
        raise ValueError("percentile stat requires a 'q' param in [0, 1], e.g. {'q': 0.9}")
    q = float(params["q"])
    if not 0 <= q <= 1:
        raise ValueError(f"percentile 'q' must be within [0, 1], got {q!r}")
    return q


def _percentile_expr(column: str, params: dict[str, Any]) -> pl.Expr:
    q = _require_quantile(params)
    return pl.col(column).quantile(q).alias(_percentile_output_name(column, params))


register_stat(
    StatFunction(
        name="percentile",
        build_expr=_percentile_expr,
        output_columns=lambda column, params: [_percentile_output_name(column, params)],
    )
)
