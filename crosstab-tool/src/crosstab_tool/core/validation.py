from __future__ import annotations

from crosstab_tool.sources.base import DataSourceAdapter
from crosstab_tool.sources.registry import build_source
from crosstab_tool.spec.comparison_spec import ColumnBaselineSpec, ComparisonSpec
from crosstab_tool.spec.crosstab_spec import CrosstabSpec
from crosstab_tool.stats.comparison_registry import validate_metrics_for_baseline
from crosstab_tool.stats.registry import get_stat, is_numeric_dtype


def validate_spec(spec: CrosstabSpec, source: DataSourceAdapter) -> None:
    """Spec/schema-level checks that don't require scanning any data rows.

    ``source`` must be the adapter already built from ``spec.source`` by the caller
    (see core/runner.py) -- for a file/in-memory source, building a second adapter
    would be free, but for a SQL source (M4) it would mean running the query twice.

    Column existence and dtype compatibility are checked against the source's schema.
    Combinatorial groupset-explosion guardrails are out of scope here; they only apply
    to cube/auto-generated groupbys (M6).
    """
    schema = source.describe_schema()

    for groupset in spec.groupby.groups:
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

    if spec.comparison is not None:
        _validate_comparison(spec.comparison, schema)


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
