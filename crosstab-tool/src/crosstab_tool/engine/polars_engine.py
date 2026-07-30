from __future__ import annotations

import logging
import time

import polars as pl

from crosstab_tool.engine import comparison as comparison_engine
from crosstab_tool.sources.base import DataSourceAdapter
from crosstab_tool.spec.crosstab_spec import CrosstabSpec
from crosstab_tool.stats.registry import get_stat

logger = logging.getLogger(__name__)


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

    if spec.filters:
        logger.info("Applying %d filter(s)", len(spec.filters))
        for filter_expr in spec.filters:
            logger.debug("Filter: %s", filter_expr)
            lf = lf.filter(pl.sql_expr(filter_expr))

    exprs = [get_stat(stat.name).build_expr(stat.column, stat.params) for stat in spec.stats]
    groupsets = spec.groupby.expand_to_groupsets()
    keys = [_groupset_key(groupset) for groupset in groupsets]
    logger.info("Computing %d groupset(s): %s", len(groupsets), ", ".join(keys))

    if spec.comparison is not None:
        mode = "paired" if spec.comparison.is_paired else "unpaired"
        logger.info(
            "Running %s comparison against baseline, metrics=%s", mode, spec.comparison.metrics
        )
        if spec.comparison.is_paired:
            return comparison_engine.run_paired(spec, lf, exprs, groupsets)
        return comparison_engine.run_unpaired(spec, lf, exprs, groupsets)

    plans = [_build_plan(lf, groupset, exprs) for groupset in groupsets]

    start = time.perf_counter()
    collected = pl.collect_all(plans)
    logger.info(
        "Collected %d groupset(s) in %.2fs (%s)",
        len(collected),
        time.perf_counter() - start,
        ", ".join(f"{key}={frame.height} row(s)" for key, frame in zip(keys, collected)),  # noqa: B905
    )
    return dict(zip(keys, collected))  # noqa: B905 (keys/plans built pairwise, always equal length)
