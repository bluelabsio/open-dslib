from __future__ import annotations

from crosstab_tool.sources.registry import build_source
from crosstab_tool.spec.comparison_spec import ColumnBaselineSpec, ComparisonSpec
from crosstab_tool.spec.crosstab_spec import CrosstabSpec
from crosstab_tool.stats.comparison_registry import validate_metrics_for_baseline
from crosstab_tool.stats.registry import get_stat, is_numeric_dtype


def validate_spec(spec: CrosstabSpec) -> None:
    """Spec/schema-level checks that don't require scanning any data rows.

    Column existence and dtype compatibility are checked against the source's lazy
    schema (metadata only, no data read). Combinatorial groupset-explosion guardrails
    are out of scope here; they only apply to cube/auto-generated groupbys (M6).
    """
    schema = build_source(spec.source).describe_schema()

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
