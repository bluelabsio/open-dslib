"""SQL generation: compile a Job into the union-all-of-group-bys query that
runs entirely inside Redshift, generalizing the hand-written reference pattern:

    WITH base AS (<score table LEFT JOIN grouping tables USING(key)>)
    SELECT '00 Topline' AS category, 'Topline' AS level, <aggs> FROM base GROUP BY 1, 2
    UNION ALL
    SELECT '01 Age' AS category, age_bucket AS level, <aggs> FROM base GROUP BY 1, 2
    ...
    ORDER BY category, level
"""

from __future__ import annotations

import re

from crosstab_tool.config import ConfigError, Job, _slug
from crosstab_tool.registry import AGGREGATIONS

# Identifiers are interpolated into SQL, so restrict them to safe shapes
# (optionally schema-qualified names) instead of attempting to escape.
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*){0,2}$")


def _check_identifier(name: str, what: str) -> str:
    if not _IDENTIFIER.match(name):
        raise ConfigError(
            f"{what} {name!r} is not a valid SQL identifier "
            "(letters, digits, underscores, optional schema qualification)."
        )
    return name


def _quote_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def base_cte(job: Job, join_select_columns: dict[str, list[str]] | None = None) -> str:
    """`join_select_columns` (alias -> explicit column list) lets the runner
    pin a join table's contribution to specific columns, so that a column
    name shared with the base table can be excluded from the join's `alias.*`
    wildcard — the base table's version wins instead of both being selected
    under the same name (which Redshift would then treat as ambiguous)."""
    join_select_columns = join_select_columns or {}
    base = _check_identifier(job.source.base_table, "source.base_table")
    select_cols = ["s.*"]
    for i, join in enumerate(job.source.joins):
        alias = join.alias or f"t{i + 1}"
        cols = join_select_columns.get(alias)
        if cols is None:
            select_cols.append(f"{alias}.*")
        else:
            select_cols.append(
                ", ".join(
                    f"{alias}.{_check_identifier(c, 'join column')}" for c in cols
                )
            )
    sql = f"    SELECT {', '.join(select_cols)}\n    FROM {base} s"
    for i, join in enumerate(job.source.joins):
        alias = _check_identifier(join.alias or f"t{i + 1}", "join alias")
        table = _check_identifier(join.table, "join table")
        key = _check_identifier(join.key, "join key")
        sql += f"\n    {join.how.upper()} JOIN {table} {alias} USING ({key})"
    return sql


def metric_select_sql(job: Job) -> list[str]:
    """One SQL expression per statistic column, aligned with Job.result_columns()."""
    exprs = ["COUNT(*) AS count"]
    for m in job.metrics:
        col = _check_identifier(m.column, "metric column")
        if m.type == "categorical":
            for v in m.values:
                exprs.append(
                    f"AVG(CASE WHEN {col} = {_quote_literal(v)} THEN 1.0 ELSE 0.0 END) "
                    f"AS freq_{col}_{_slug(v)}"
                )
        else:
            for agg in m.aggregations or job.aggregations:
                if agg == "count":
                    continue
                a = AGGREGATIONS[agg]
                exprs.append(f"{a.template.format(col=col)} AS {a.prefix}_{col}")
    return exprs


def _tab_select(job: Job, category: str, level_expr: str) -> str:
    exprs = metric_select_sql(job)
    stat_lines = "\n    , ".join(exprs)
    return (
        f"SELECT {_quote_literal(category)} AS category\n"
        f"    , {level_expr} AS level\n"
        f"    , {stat_lines}\n"
        f"FROM base\nGROUP BY 1, 2"
    )


def build_query(job: Job, join_select_columns: dict[str, list[str]] | None = None) -> str:
    """The full crosstab query for a job. See `base_cte` for `join_select_columns`."""
    tabs = [
        _tab_select(job, f"00 {job.topline_label}", _quote_literal(job.topline_label))
    ]
    for g in sorted(job.groupings, key=lambda g: g.order):
        col = _check_identifier(g.column, "grouping column")
        tabs.append(_tab_select(job, g.category(), f"CAST({col} AS VARCHAR)"))
    body = "\n\nUNION ALL\n\n".join(tabs)
    cte = base_cte(job, join_select_columns)
    return f"WITH base AS (\n{cte}\n)\n\n{body}\n\nORDER BY category, level;"
