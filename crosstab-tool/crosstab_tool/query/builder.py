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


def _table_ref(source: DataSourceConfig) -> str:
    # `table` is a schema.table name usable directly in FROM/JOIN; `query`
    # is the raw-SQL escape hatch, which needs wrapping as a derived table.
    return source.table if source.table else f"({source.query})"


def build_base_cte(config: JobConfig) -> str:
    sources_by_name = {s.name: s for s in config.sources}
    base_source = config.base.from_
    froms = [f"FROM {_table_ref(sources_by_name[base_source])} AS {base_source}"]
    for join in config.base.joins:
        keys = join.key if isinstance(join.key, list) else [join.key]
        using = ", ".join(keys)
        table_ref = _table_ref(sources_by_name[join.source])
        froms.append(f"{join.how.upper()} JOIN {table_ref} AS {join.source} USING({using})")
    return f"SELECT {base_source}.*\n" + "\n".join(froms)


def _agg_selects(columns: list[ColumnRef], defaults: list) -> list[str]:
    selects = []
    for col in columns:
        aggs = col.aggregations or defaults
        for agg in aggs:
            selects.append(agg_sql(agg, col.column, alias=f"{agg.value}_{col.name}"))
        # Custom aggregations are resolved and applied post-hoc on the
        # already-aggregated result, not inlined into this SQL — see
        # compute/ for the equivalent of cross_column's CUSTOM handling.
    return selects


def build_group_select(config: JobConfig, category_label: str, level_expr: str) -> str:
    columns = [*config.scores, *config.counterfactuals]
    agg_selects = _agg_selects(columns, config.aggregations.default)
    select_parts = [
        f"'{category_label}' AS category",
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
        blocks.append(build_group_select(config, "00 Topline", "'Topline'"))

    for gv in config.grouping_variables:
        blocks.append(build_group_select(config, gv.label, gv.column))

    union = "\nUNION ALL\n".join(blocks)
    return f"WITH base AS (\n{base_cte}\n)\n{union}\nORDER BY 1, 2"
