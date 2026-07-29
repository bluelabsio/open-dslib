from __future__ import annotations

from crosstab_tool.sources.registry import build_source
from crosstab_tool.spec.crosstab_spec import CrosstabSpec
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
