from __future__ import annotations

import logging

from crosstab_tool.sources.base import DataSourceAdapter
from crosstab_tool.sources.registry import build_source
from crosstab_tool.spec.comparison_spec import ColumnBaselineSpec, ComparisonSpec
from crosstab_tool.spec.crosstab_spec import CrosstabSpec
from crosstab_tool.spec.groupby_spec import CubeSpec
from crosstab_tool.stats.comparison_registry import validate_metrics_for_baseline
from crosstab_tool.stats.registry import get_stat, is_numeric_dtype

logger = logging.getLogger(__name__)


def validate_spec(spec: CrosstabSpec, source: DataSourceAdapter) -> None:
    """Spec/schema-level checks that don't require scanning any data rows.

    ``source`` must be the adapter already built from ``spec.source`` by the caller
    (see core/runner.py) -- for a file/in-memory source, building a second adapter
    would be free, but for a SQL source (M4) it would mean running the query twice.

    Column existence and dtype compatibility are checked against the source's schema.
    """
    logger.debug("Fetching source schema for validation")
    schema = source.describe_schema()
    logger.debug("Source schema has %d column(s): %s", len(schema), ", ".join(schema))

    groupsets = spec.groupby.expand_to_groupsets()
    logger.info(
        "Checking %d groupset(s) and %d stat(s) against the source schema",
        len(groupsets),
        len(spec.stats),
    )

    for groupset in groupsets:
        for column in groupset:
            if column not in schema:
                raise ValueError(f"groupby column {column!r} not found in source schema")

    for stat_spec in spec.stats:
        stat = get_stat(stat_spec.name)  # raises ValueError for unknown stat names

        if stat_spec.column not in schema:
            raise ValueError(f"stat column {stat_spec.column!r} not found in source schema")

        if stat.requires_numeric and not is_numeric_dtype(schema[stat_spec.column]):
            raise ValueError(
                f"stat {stat_spec.name!r} requires a numeric column, but "
                f"{stat_spec.column!r} has dtype {schema[stat_spec.column]!r}"
            )

    if isinstance(spec.groupby, CubeSpec):
        _validate_cube_cardinality(groupsets, spec.options, source)

    if spec.comparison is not None:
        logger.info("Validating comparison/baseline configuration")
        _validate_comparison(spec.comparison, schema)

    logger.info("Spec validation passed")


def _validate_cube_cardinality(
    groupsets: list[list[str]], options: dict, source: DataSourceAdapter
) -> None:
    """Guards against a cube's combinatorial groupset count blowing up on large data --
    e.g. a 4-column cube over 100M rows is up to 15 separate full-scan `group_by().agg()`
    passes (see docs/implementation-plan.md's Engine Strategy section on why Polars needs
    one pass per groupset, unlike SQL `GROUPING SETS`; that's slated to move into
    docs/architecture.md as part of M7).

    Opt-in via ``options.cardinality_guardrail.max_groupset_count_x_cardinality`` (see
    examples/configs/cube_groupbys.yaml) -- omitted by default so existing/simple cube
    configs aren't forced to tune a threshold they don't need. Also a no-op when the
    source can't estimate its row count (``estimated_row_count()`` returns ``None``),
    since there's nothing to guard against without a number.
    """
    guardrail = options.get("cardinality_guardrail")
    if not guardrail:
        return
    threshold = guardrail.get("max_groupset_count_x_cardinality")
    if threshold is None:
        return

    row_count = source.estimated_row_count()
    if row_count is None:
        logger.debug(
            "Cube cardinality guardrail configured, but source can't estimate row count "
            "-- skipping"
        )
        return

    estimate = len(groupsets) * row_count
    logger.info(
        "Cube cardinality guardrail: %d groupset(s) x ~%d row(s) = %d (threshold %d)",
        len(groupsets),
        row_count,
        estimate,
        threshold,
    )
    if estimate > threshold:
        raise ValueError(
            f"cube groupby would run {len(groupsets)} groupset passes over an estimated "
            f"{row_count} rows ({estimate} > options.cardinality_guardrail."
            f"max_groupset_count_x_cardinality={threshold}); narrow groupby.columns/"
            f"max_depth, or raise the threshold if this is intentional"
        )


def _validate_comparison(comparison: ComparisonSpec, schema: dict[str, str]) -> None:
    column = comparison.column
    if column not in schema:
        raise ValueError(f"comparison column {column!r} not found in source schema")
    if not is_numeric_dtype(schema[column]):
        raise ValueError(f"comparison column {column!r} must be numeric, has {schema[column]!r}")

    baseline = comparison.baseline
    if isinstance(baseline, ColumnBaselineSpec):
        if baseline.column not in schema:
            raise ValueError(f"baseline column {baseline.column!r} not found in source schema")
        if not is_numeric_dtype(schema[baseline.column]):
            raise ValueError(f"baseline column {baseline.column!r} must be numeric")
    else:
        # NB: unlike the main source, this builds a *second* baseline adapter separate
        # from the one engine/comparison.py builds during execution. Harmless for
        # file/in-memory baselines; for a SQL baseline (M4) it means the baseline query
        # runs twice per run_crosstab() call. Not fixed here -- narrower than the main
        # source's double-fetch (only affects comparison + SQL baseline together) and
        # would require threading the baseline adapter through engine/comparison.py's
        # paired/unpaired functions too. Documented as a known follow-up.
        baseline_schema = build_source(baseline.source).describe_schema()
        if column not in baseline_schema:
            raise ValueError(f"comparison column {column!r} not found in baseline source schema")
        if not is_numeric_dtype(baseline_schema[column]):
            raise ValueError(f"comparison column {column!r} must be numeric in baseline source")

        if baseline.join_keys is not None:
            for key in baseline.join_keys:
                if key not in schema:
                    raise ValueError(f"join key {key!r} not found in current source schema")
                if key not in baseline_schema:
                    raise ValueError(f"join key {key!r} not found in baseline source schema")

    validate_metrics_for_baseline(comparison.metrics, comparison.is_paired)
