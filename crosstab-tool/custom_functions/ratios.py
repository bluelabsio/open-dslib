"""Ratio-shaped custom cross_column functions -- for computations that
combine more than one operation, so a single built-in op
(`add`/`difference`/`multiply`/`divide`) can't express them. A plain ratio
doesn't belong here anymore: use `op: divide` directly in the config
instead of writing a custom function for it.
"""
from __future__ import annotations

import pandas as pd


def percent_change(new: pd.Series, old: pd.Series) -> pd.Series:
    """Percent change of `new` relative to `old`, e.g. lift of a treated
    group over a control group. Combines a difference and a divide, which
    is why this needs `op: custom` rather than a single built-in op."""
    return (new - old) / old
