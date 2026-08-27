"""SQL builder — turns a JobConfig into the Appendix A-style query.

Generalizes the reference SQL (Requirements Doc Appendix A): one base
table (source + joins), one SELECT per grouping variable plus a Topline
row, UNION ALL'd together, GROUP BY 1, 2 (category, level), ORDER BY 1, 2.

The base source+joins are materialized once via `CREATE TEMP TABLE base
AS (...)` rather than a `WITH base AS (...)` CTE. Redshift inlines CTEs
at every reference instead of materializing them, so with a CTE each of
the N+1 UNION ALL blocks (topline + one per grouping variable) would
independently re-run the entire join chain -- join cost scales as
`joins * (groupings + 1)` instead of `joins` once. A temp table pays the
join cost a single time and every block below just scans it.
"""
from __future__ import annotations

from crosstab_tool.config.schema import ColumnRef, DataSourceConfig, JobConfig
from crosstab_tool.query.aggregations import agg_sql
from crosstab_tool.query.identifiers import SQLGenerationError, check_identifier, quote_literal


def _table_ref(source: DataSourceConfig) -> str:
    # `table` is a schema.table name usable directly in FROM/JOIN -- checked
    # as an identifier below. `query` is the raw-SQL escape hatch: it's
    # trusted, hand-written SQL wrapped as a derived table, and deliberately
    # NOT identifier-checked here, since it's not an identifier at all.
    if source.table:
        return check_identifier(source.table, f"source '{source.name}'.table")
    return f"({source.query})"


def _needed_columns(config: JobConfig) -> dict[str, str]:
    """Maps each column name `base` must expose to the source alias it
    should be read from -- scores/counterfactuals already declare `source`;
    grouping variables declare it via the optional `source` field, falling
    back to `base.from_` when omitted. Raises if two entries claim the same
    column name from different sources, since a single output column can't
    come from both."""
    columns: dict[str, str] = {}
    for col in [*config.scores, *config.counterfactuals]:
        name = check_identifier(col.column, f"column '{col.name}'.column")
        source = check_identifier(col.source, f"column '{col.name}'.source")
        if name in columns and columns[name] != source:
            raise SQLGenerationError(
                f"column '{col.column}' is claimed by both source '{columns[name]}' "
                f"and '{source}' -- give it a distinct name in one of the configs"
            )
        columns[name] = source
    for gv in config.grouping_variables:
        name = check_identifier(gv.column, f"grouping variable '{gv.label}'.column")
        source = check_identifier(
            gv.source or config.base.from_, f"grouping variable '{gv.label}'.source"
        )
        if name in columns and columns[name] != source:
            raise SQLGenerationError(
                f"grouping variable '{gv.label}' column '{gv.column}' is claimed by both "
                f"source '{columns[name]}' and '{source}' -- give it a distinct name or "
                "set an explicit `source` that matches"
            )
        columns[name] = source
    return columns


def build_base_select(config: JobConfig) -> str:
    sources_by_name = {s.name: s for s in config.sources}
    base_source = check_identifier(config.base.from_, "base.from")
    froms = [f"FROM {_table_ref(sources_by_name[base_source])} AS {base_source}"]
    for join in config.base.joins:
        keys = join.key if isinstance(join.key, list) else [join.key]
        keys = [check_identifier(k, f"join '{join.source}' key") for k in keys]
        using = ", ".join(keys)
        join_alias = check_identifier(join.source, f"join '{join.source}' alias")
        table_ref = _table_ref(sources_by_name[join.source])
        froms.append(f"{join.how.upper()} JOIN {table_ref} AS {join_alias} USING({using})")
    # Select only the columns scores/counterfactuals/grouping_variables
    # actually reference, each qualified by its declared source, instead of
    # a blind `*`/`alias.*` over the whole join chain. Two independently
    # built source tables can share an unrelated column name (e.g. an
    # `__updated_at` audit column) that has nothing to do with the join
    # key -- USING(key) can't merge those, and CREATE TEMP TABLE (a real
    # table, unlike a CTE) rejects the resulting duplicate column outright.
    # Selecting only what's needed sidesteps the whole class of collisions.
    needed = _needed_columns(config)
    select_clause = ", ".join(f"{source}.{name}" for name, source in needed.items())
    return f"SELECT {select_clause}\n" + "\n".join(froms)


def _agg_selects(columns: list[ColumnRef], defaults: list) -> list[str]:
    selects = []
    for col in columns:
        column = check_identifier(col.column, f"column '{col.name}'.column")
        alias_name = check_identifier(col.name, f"column '{col.name}' name (used as SQL alias)")
        aggs = col.aggregations or defaults
        for agg in aggs:
            selects.append(agg_sql(agg, column, alias=f"{agg.value}_{alias_name}"))
        # Custom aggregations are resolved and applied post-hoc on the
        # already-aggregated result, not inlined into this SQL — see
        # compute/ for the equivalent of cross_column's CUSTOM handling.
    return selects


def build_group_select(config: JobConfig, category_label: str, level_expr: str) -> str:
    columns = [*config.scores, *config.counterfactuals]
    agg_selects = _agg_selects(columns, config.aggregations.default)
    select_parts = [
        f"{quote_literal(category_label)} AS category",
        f"{level_expr} AS level",
        "COUNT(*) AS count",
        *agg_selects,
    ]
    select_clause = ",\n  ".join(select_parts)
    return f"SELECT\n  {select_clause}\nFROM base\nGROUP BY 1, 2"


def build_query(config: JobConfig) -> str:
    base_select = build_base_select(config)
    blocks = []

    if config.include_topline:
        blocks.append(build_group_select(config, "00 Topline", quote_literal("Topline")))

    for gv in config.grouping_variables:
        gv_column = check_identifier(gv.column, f"grouping variable '{gv.label}'.column")
        # Every block's `level` value is UNION ALL'd together, so it must be
        # a consistent type across blocks; the Topline row's is a string
        # literal, so non-text grouping columns (boolean/integer/etc.) need
        # an explicit cast or the UNION fails outright.
        blocks.append(build_group_select(config, gv.label, f"CAST({gv_column} AS VARCHAR)"))

    union = "\nUNION ALL\n".join(blocks)
    return (
        f"CREATE TEMP TABLE base AS (\n{base_select}\n);\n\n"
        f"{union}\nORDER BY 1, 2;"
    )


def column_output_names(col: ColumnRef, default_aggregations: list) -> list[str]:
    """The result-column name(s) a single score/counterfactual produces --
    one `f"{agg}_{col.name}"` per aggregation applied to it (its own
    `aggregations` override, or the job-level default). Mirrors
    `_agg_selects` exactly, without generating any SQL."""
    aggs = col.aggregations or default_aggregations
    return [f"{agg.value}_{col.name}" for agg in aggs]


def expected_result_columns(config: JobConfig) -> list[str]:
    """The column names `build_query(config)`'s SQL would actually produce:
    category, level, count, plus `column_output_names(...)` for each
    score/counterfactual.

    Used by `cli.py`'s `validate` command to check `cross_column.inputs`
    resolve against real output columns before a job ever runs against
    Redshift -- see `compute/cross_column.py`'s `resolve_column_name`.
    """
    columns = ["category", "level", "count"]
    for col in [*config.scores, *config.counterfactuals]:
        columns.extend(column_output_names(col, config.aggregations.default))
    return columns
