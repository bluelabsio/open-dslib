"""Post-aggregation work, all on the small (rows = grouping levels) result
table: cross-column computations and the optional wide/pivoted layout."""

from __future__ import annotations

import pandas as pd

from crosstab_tool.config import Job
from crosstab_tool.registry import CROSS_COLUMN_OPS


def apply_cross_column(df: pd.DataFrame, job: Job) -> pd.DataFrame:
    """Append each configured cross-column computation as a new column.
    Computations run in config order, so later ones may reference earlier
    outputs."""
    df = df.copy()
    for cc in job.cross_column:
        fn = CROSS_COLUMN_OPS[cc.op]
        df[cc.output_column] = fn(df[cc.left], df[cc.right])
    return df


def to_wide(df: pd.DataFrame) -> pd.DataFrame:
    """Alternative pivoted layout: one row per statistic, one column per
    category/level. The long layout remains the default output shape."""
    long = df.melt(id_vars=["category", "level"], var_name="statistic")
    wide = long.pivot_table(
        index="statistic", columns=["category", "level"], values="value", sort=False
    )
    # Preserve the original statistic (column) order rather than pivot's sort.
    stat_order = [c for c in df.columns if c not in ("category", "level")]
    return wide.reindex(stat_order)
