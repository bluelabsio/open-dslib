"""End-to-end job execution: config -> SQL -> Redshift -> post-agg -> writer."""

from __future__ import annotations

import logging

import pandas as pd

from crosstab_tool.config import ConfigError, Job
from crosstab_tool.metadata import run_metadata
from crosstab_tool.postagg import apply_cross_column
from crosstab_tool.sources import DataSource, RedshiftSource
from crosstab_tool.sqlgen import build_query
from crosstab_tool.writers import get_writer

logger = logging.getLogger("crosstab_tool")


def run_job(job: Job, source: DataSource | None = None) -> str:
    """Run a crosstab job; returns the output location (path or URL).

    `source` may be injected (tests, future non-Redshift backends);
    defaults to a RedshiftSource using the job's connection prefix.
    """
    source = source or RedshiftSource(job.source.connection)
    join_select_columns = _resolve_columns(job, source)
    sql = build_query(job, join_select_columns)
    logger.info("Running crosstab query for job %r ...", job.name)
    df = source.run_query(sql)
    df = _order_result(df)
    df = apply_cross_column(df, job)
    location = get_writer(job).write(df, job, run_metadata(job, sql))
    logger.info("Wrote %d rows to %s", len(df), location)
    return location


def _order_result(df: pd.DataFrame) -> pd.DataFrame:
    # The query already orders by category, level; re-assert here so writers
    # can rely on it regardless of driver behavior.
    return df.sort_values(["category", "level"], kind="stable").reset_index(drop=True)


def _resolve_columns(job: Job, source: DataSource) -> dict[str, list[str]] | None:
    """Best-effort: if a configured grouping or metric column doesn't exist
    on the source table(s) at all (renamed/dropped upstream), drop it and log
    a warning instead of failing the whole crosstab query.

    The base CTE selects `s.*, t1.*, ...` from every joined table, so a
    column name present on more than one of them would otherwise be
    ambiguous to reference unqualified. The base table always wins in that
    case: this returns a `join_select_columns` map (see `sqlgen.base_cte`)
    that excludes each such column from the join(s) that also have it, so
    only the base table's version survives under that name. A name shared
    between two *join* tables only (not the base table) still has no
    resolution rule, so it's dropped with a warning instead of guessed at.

    Only supported when the source can report its per-table schema
    (Redshift); returns None otherwise, and build_query falls back to
    unrestricted `alias.*` wildcards for every join.
    """
    get_columns_by_table = getattr(source, "columns_by_table", None)
    if get_columns_by_table is None:
        return None

    tables = [job.source.base_table] + [j.table for j in job.source.joins]
    columns_by_table = get_columns_by_table(tables)
    base_table = job.source.base_table

    def keep(column: str, what: str) -> bool:
        hits = [t for t in tables if column.lower() in columns_by_table[t]]
        if not hits:
            logger.warning("%s %r not found in %s; skipping.", what, column, tables)
            return False
        if len(hits) > 1 and base_table not in hits:
            logger.warning(
                "%s %r exists on more than one joined table (%s); skipping "
                "because referencing it unqualified would be ambiguous.",
                what, column, ", ".join(hits),
            )
            return False
        if len(hits) > 1:
            logger.info(
                "%s %r exists on both the base table and %s; using the base "
                "table's version.", what, column, ", ".join(h for h in hits if h != base_table),
            )
        return True

    job.groupings = [g for g in job.groupings if keep(g.column, "Grouping column")]
    if not job.groupings:
        raise ConfigError(f"None of the configured grouping columns exist in {tables}.")

    for attr in ("scores", "counterfactuals"):
        kept = [m for m in getattr(job, attr) if keep(m.column, "Metric column")]
        setattr(job, attr, kept)
    if not job.scores:
        raise ConfigError(f"None of the configured score columns exist in {tables}.")

    base_columns = columns_by_table[base_table]
    join_select_columns: dict[str, list[str]] = {}
    for i, j in enumerate(job.source.joins):
        alias = j.alias or f"t{i + 1}"
        conflicts = columns_by_table[j.table] & base_columns
        if conflicts:
            join_select_columns[alias] = sorted(columns_by_table[j.table] - conflicts)
    return join_select_columns
