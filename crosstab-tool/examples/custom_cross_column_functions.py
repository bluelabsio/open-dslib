"""Example custom cross_column function, referenced by
custom_cross_column_example.yaml's `op: custom` entry via the dotted path
'custom_cross_column_functions:weighted_average_pct'.

This file lives in examples/ purely so the example is self-contained and
runnable with:

    PYTHONPATH=examples crosstab validate examples/custom_cross_column_example.yaml

For a REAL BlueLabs job, prefer this repo's own `custom_functions/`
package instead (check there first for an existing function before
writing a new one) -- it's on `sys.path` automatically, no PYTHONPATH
needed. This file lives in examples/ instead purely to keep this
specific example self-contained and runnable in isolation. See
crosstab-config-wizard/SKILL.md's "Writing a custom cross-column
function" section for the full requirements either way (function
signature, why `inputs` has to reference bare score/counterfactual
names).
"""
from __future__ import annotations

import pandas as pd


def weighted_average_pct(product_sum: pd.Series, weight_sum: pd.Series) -> pd.Series:
    """A weighted average, expressed as a percentage. This needs
    `op: custom` rather than the built-in `op: divide`, because it
    chains two operations: a divide (to get the weighted average) and a
    multiply-by-100 (to express it as a percentage instead of a decimal
    proportion) -- a single built-in op can only apply one operation."""
    return (product_sum / weight_sum) * 100
