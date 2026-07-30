"""End-to-end job execution: config -> SQL -> Redshift -> post-agg -> writer."""

from __future__ import annotations

import logging

import pandas as pd

from crosstab_tool.config import Job
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
    sql = build_query(job)
    source = source or RedshiftSource(job.source.connection)
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
