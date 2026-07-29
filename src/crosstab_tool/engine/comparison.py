"""Paired and unpaired counterfactual/baseline comparison execution paths.

Both paths reuse the aggregate-first philosophy of engine/polars_engine.py: the
expensive part (scanning/joining/aggregating the raw data) stays in Polars, and only a
small (group-count-sized, not row-count-sized) table is ever handed to scipy/Python.
"""

from __future__ import annotations

from typing import Any

import polars as pl
from scipy import stats as scipy_stats

from crosstab_tool.sources.registry import build_source
from crosstab_tool.spec.comparison_spec import (
    ColumnBaselineSpec,
    ComparisonSpec,
    SourceBaselineSpec,
)
from crosstab_tool.spec.crosstab_spec import CrosstabSpec
from crosstab_tool.stats.comparison_registry import PAIRED_AGGREGATE_METRICS, PAIRED_TTEST_METRIC
from crosstab_tool.stats.registry import get_stat

_DIFF_COLUMN = "__diff"
_TTEST_MEAN = "__ttest_mean"
_TTEST_STD = "__ttest_std"
_TTEST_COUNT = "__ttest_count"
_WILCOXON_VALUES = "__wilcoxon_values"
_CURRENT_VALUES = "__current_values"
_BASELINE_VALUES = "__baseline_values"


def _groupset_key(groupset: list[str]) -> str:
    return "__".join(groupset) if groupset else "__overall__"


def _agg_plan(lf: pl.LazyFrame, groupset: list[str], exprs: list[pl.Expr]) -> pl.LazyFrame:
    if not groupset:
        return lf.select(exprs)
    return lf.group_by(groupset, maintain_order=True).agg(exprs)


def _sampled(expr: pl.Expr, max_sample_size: int | None) -> pl.Expr:
    return expr.head(max_sample_size) if max_sample_size is not None else expr


# --------------------------------------------------------------------------- paired ---


def _build_diff_lazyframe(comparison: ComparisonSpec, current_lf: pl.LazyFrame) -> pl.LazyFrame:
    baseline = comparison.baseline
    column = comparison.column

    if isinstance(baseline, ColumnBaselineSpec):
        return current_lf.with_columns(
            (pl.col(column) - pl.col(baseline.column)).alias(_DIFF_COLUMN)
        )

    assert isinstance(baseline, SourceBaselineSpec) and baseline.join_keys is not None
    baseline_score_col = f"__baseline_{column}"
    baseline_lf = (
        build_source(baseline.source)
        .to_polars_lazyframe()
        .select([*baseline.join_keys, pl.col(column).alias(baseline_score_col)])
    )
    joined = current_lf.join(baseline_lf, on=baseline.join_keys, how="inner")
    return joined.with_columns((pl.col(column) - pl.col(baseline_score_col)).alias(_DIFF_COLUMN))


def _paired_comparison_exprs(comparison: ComparisonSpec) -> list[pl.Expr]:
    exprs = []
    for metric in comparison.metrics:
        if metric in PAIRED_AGGREGATE_METRICS:
            stat_name, params = PAIRED_AGGREGATE_METRICS[metric]
            expr = get_stat(stat_name).build_expr(_DIFF_COLUMN, params)
            exprs.append(expr.alias(f"{comparison.column}_{metric}"))
        elif metric == "paired_ttest":
            exprs.append(pl.col(_DIFF_COLUMN).mean().alias(_TTEST_MEAN))
            exprs.append(pl.col(_DIFF_COLUMN).std().alias(_TTEST_STD))
            exprs.append(pl.col(_DIFF_COLUMN).count().alias(_TTEST_COUNT))
        elif metric == "wilcoxon":
            values = _sampled(pl.col(_DIFF_COLUMN), comparison.max_sample_size)
            exprs.append(values.implode().alias(_WILCOXON_VALUES))
    return exprs


def _paired_ttest_columns(frame: pl.DataFrame, column: str) -> pl.DataFrame:
    stats: list[float] = []
    pvalues: list[float] = []
    rows = zip(  # noqa: B905 (three columns of the same frame, always equal length)
        frame[_TTEST_MEAN].to_list(),
        frame[_TTEST_STD].to_list(),
        frame[_TTEST_COUNT].to_list(),
    )
    for mean_diff, std_diff, n in rows:
        if not n or n < 2 or not std_diff:
            stats.append(float("nan"))
            pvalues.append(float("nan"))
            continue
        standard_error = std_diff / (n**0.5)
        t_stat = mean_diff / standard_error
        stats.append(float(t_stat))
        pvalues.append(float(2 * scipy_stats.t.sf(abs(t_stat), df=n - 1)))

    return frame.with_columns(
        pl.Series(f"{column}_paired_ttest_stat", stats),
        pl.Series(f"{column}_paired_ttest_pvalue", pvalues),
    ).drop([_TTEST_MEAN, _TTEST_STD, _TTEST_COUNT])


def _wilcoxon_columns(frame: pl.DataFrame, column: str) -> pl.DataFrame:
    stats: list[float] = []
    pvalues: list[float] = []
    for values in frame[_WILCOXON_VALUES].to_list():
        cleaned = [v for v in (values or []) if v is not None]
        try:
            result = scipy_stats.wilcoxon(cleaned)
        except ValueError:
            stats.append(float("nan"))
            pvalues.append(float("nan"))
            continue
        stats.append(float(result.statistic))
        pvalues.append(float(result.pvalue))

    return frame.with_columns(
        pl.Series(f"{column}_wilcoxon_stat", stats),
        pl.Series(f"{column}_wilcoxon_pvalue", pvalues),
    ).drop([_WILCOXON_VALUES])


def run_paired(
    spec: CrosstabSpec,
    current_lf: pl.LazyFrame,
    base_exprs: list[pl.Expr],
    groupsets: list[list[str]],
) -> dict[str, pl.DataFrame]:
    comparison = spec.comparison
    assert comparison is not None

    diff_lf = _build_diff_lazyframe(comparison, current_lf)
    combined_exprs = [*base_exprs, *_paired_comparison_exprs(comparison)]

    plans = [_agg_plan(diff_lf, groupset, combined_exprs) for groupset in groupsets]
    keys = [_groupset_key(groupset) for groupset in groupsets]
    collected = pl.collect_all(plans)

    results = {}
    for key, frame in zip(keys, collected):  # noqa: B905 (built pairwise, always equal length)
        if PAIRED_TTEST_METRIC in comparison.metrics:
            frame = _paired_ttest_columns(frame, comparison.column)
        if "wilcoxon" in comparison.metrics:
            frame = _wilcoxon_columns(frame, comparison.column)
        results[key] = frame
    return results


# ------------------------------------------------------------------------- unpaired ---


def _distributional_columns(frame: pl.DataFrame, column: str, metrics: list[str]) -> pl.DataFrame:
    current_lists = frame[_CURRENT_VALUES].to_list()
    baseline_lists = frame[_BASELINE_VALUES].to_list()
    pairs = list(zip(current_lists, baseline_lists))  # noqa: B905 (same frame, equal length)

    new_series = []
    if "ks_test" in metrics:
        new_series.extend(_two_sample_columns(pairs, f"{column}_ks_test", scipy_stats.ks_2samp))
    if "mannwhitney" in metrics:
        new_series.extend(
            _two_sample_columns(pairs, f"{column}_mannwhitney", scipy_stats.mannwhitneyu)
        )

    return frame.with_columns(new_series)


def _two_sample_columns(pairs: list[tuple], prefix: str, test_fn: Any) -> list[pl.Series]:
    stats: list[float] = []
    pvalues: list[float] = []
    for current, baseline in pairs:
        current = [v for v in (current or []) if v is not None]
        baseline = [v for v in (baseline or []) if v is not None]
        if not current or not baseline:
            stats.append(float("nan"))
            pvalues.append(float("nan"))
            continue
        try:
            result = test_fn(current, baseline)
        except ValueError:
            stats.append(float("nan"))
            pvalues.append(float("nan"))
            continue
        stats.append(float(result.statistic))
        pvalues.append(float(result.pvalue))
    return [pl.Series(f"{prefix}_stat", stats), pl.Series(f"{prefix}_pvalue", pvalues)]


def run_unpaired(
    spec: CrosstabSpec,
    current_lf: pl.LazyFrame,
    base_exprs: list[pl.Expr],
    groupsets: list[list[str]],
) -> dict[str, pl.DataFrame]:
    comparison = spec.comparison
    assert comparison is not None
    baseline = comparison.baseline
    assert isinstance(baseline, SourceBaselineSpec) and baseline.join_keys is None
    column = comparison.column

    baseline_lf = build_source(baseline.source).to_polars_lazyframe()
    for filter_expr in spec.filters:
        baseline_lf = baseline_lf.filter(pl.sql_expr(filter_expr))

    current_values_expr = _sampled(pl.col(column), comparison.max_sample_size).implode().alias(
        _CURRENT_VALUES
    )
    baseline_values_expr = _sampled(pl.col(column), comparison.max_sample_size).implode().alias(
        _BASELINE_VALUES
    )

    current_plans = [
        _agg_plan(current_lf, g, [*base_exprs, current_values_expr]) for g in groupsets
    ]
    baseline_plans = [_agg_plan(baseline_lf, g, [baseline_values_expr]) for g in groupsets]
    keys = [_groupset_key(g) for g in groupsets]

    collected_current = pl.collect_all(current_plans)
    collected_baseline = pl.collect_all(baseline_plans)

    results = {}
    rows = zip(  # noqa: B905 (built pairwise, always equal length)
        keys, groupsets, collected_current, collected_baseline
    )
    for key, groupset, current_frame, baseline_frame in rows:
        if groupset:
            merged = current_frame.join(baseline_frame, on=groupset, how="inner")
        else:
            merged = pl.concat([current_frame, baseline_frame], how="horizontal")
        merged = _distributional_columns(merged, column, comparison.metrics)
        merged = merged.drop([_CURRENT_VALUES, _BASELINE_VALUES])
        results[key] = merged
    return results
