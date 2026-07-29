from __future__ import annotations

import polars as pl

from crosstab_tool.engine import comparison as comparison_engine
from crosstab_tool.sources.base import DataSourceAdapter
from crosstab_tool.spec.crosstab_spec import CrosstabSpec
from crosstab_tool.stats.registry import get_stat


def _groupset_key(groupset: list[str]) -> str:
    return "__".join(groupset) if groupset else "__overall__"


def _build_plan(lf: pl.LazyFrame, groupset: list[str], exprs: list[pl.Expr]) -> pl.LazyFrame:
    if not groupset:
        return lf.select(exprs)
    return lf.group_by(groupset, maintain_order=True).agg(exprs)


def run(spec: CrosstabSpec, source: DataSourceAdapter) -> dict[str, pl.DataFrame]:
    """Execute spec against its (already-built) source: filter -> group_by/agg per
    groupset -> collect.

    ``source`` must be the same adapter instance core/runner.py already built and
    validated against -- see the note on core/validation.py's ``validate_spec`` for why
    (a SQL source, M4, must not be queried twice for a single run_crosstab() call).

    All groupsets are collected together via ``pl.collect_all`` so Polars can overlap
    their execution instead of running them one full scan at a time.
    """
    lf = source.to_polars_lazyframe()

    for filter_expr in spec.filters:
        lf = lf.filter(pl.sql_expr(filter_expr))

    exprs = [get_stat(stat.name).build_expr(stat.column, stat.params) for stat in spec.stats]
    groupsets = spec.groupby.expand_to_groupsets()

    if spec.comparison is not None:
        if spec.comparison.is_paired:
            return comparison_engine.run_paired(spec, lf, exprs, groupsets)
        return comparison_engine.run_unpaired(spec, lf, exprs, groupsets)

    plans = [_build_plan(lf, groupset, exprs) for groupset in groupsets]
    keys = [_groupset_key(groupset) for groupset in groupsets]

    collected = pl.collect_all(plans)
    return dict(zip(keys, collected))  # noqa: B905 (keys/plans built pairwise, always equal length)
