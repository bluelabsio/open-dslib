"""SQL builder — turns a JobConfig into the Appendix A-style query.

Generalizes the reference SQL (Requirements Doc Appendix A): one base CTE
(source + joins), one SELECT per grouping variable plus a Topline row,
UNION ALL'd together, GROUP BY 1, 2 (category, level), ORDER BY 1, 2 —
matching the existing hand-written pattern exactly so this is a drop-in
replacement for it.
"""
from __future__ import annotations

from crosstab_tool.config.schema import ColumnRef, DataSourceConfig, JobConfig
from crosstab_tool.query.aggregations import agg_sql
from crosstab_tool.query.identifiers import check_identifier, quote_literal


def _table_ref(source: DataSourceConfig) -> str:
    # `table` is a schema.table name usable directly in FROM/JOIN -- checked
    # as an identifier below. `query` is the raw-SQL escape hatch: it's
    # trusted, hand-written SQL wrapped as a derived table, and deliberately
    # NOT identifier-checked here, since it's not an identifier at all.
    if source.table:
        return check_identifier(source.table, f"source '{source.name}'.table")
    return f"({source.query})"


def build_base_cte(config: JobConfig) -> str:
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
    return f"SELECT {base_source}.*\n" + "\n".join(froms)


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
    base_cte = build_base_cte(config)
    blocks = []

    if config.include_topline:
        blocks.append(build_group_select(config, "00 Topline", quote_literal("Topline")))

    for gv in config.grouping_variables:
        gv_column = check_identifier(gv.column, f"grouping variable '{gv.label}'.column")
        blocks.append(build_group_select(config, gv.label, gv_column))

    union = "\nUNION ALL\n".join(blocks)
    return f"WITH base AS (\n{base_cte}\n)\n{union}\nORDER BY 1, 2"
