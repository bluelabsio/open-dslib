"""Cross-column computation over already-aggregated columns (Req 4.4).

Operates on the tidy result DataFrame (category, level, count, agg_*
columns) that query/builder.py's SQL produces via a DataSource — never on
raw rows, keeping this step cheap regardless of source table size.

`cross_column.inputs` in the config refers to score/counterfactual *names*
(e.g. "p_support"), but query/builder.py never emits a bare column with
that name — it emits one column per requested aggregation, named
`f"{agg}_{name}"` (e.g. "mean_p_support", "count_p_support"). `_resolve`
bridges that gap: it defaults to the `mean_` aggregation, since that's the
natural "value" of a numeric score/counterfactual for a difference or
product. This was a real bug until an eval run caught it — a config with a
cross_column entry validated fine but raised a KeyError the moment it
actually ran, because nothing checked that the referenced name resolved to
an executable column.
"""
from __future__ import annotations

import pandas as pd

from crosstab_tool.config.schema import CrossColumnConfig, CrossColumnOp
from crosstab_tool.query.registry import resolve

_BUILTIN_OPS = {
    CrossColumnOp.DIFFERENCE: lambda a, b: a - b,
    CrossColumnOp.MULTIPLY: lambda a, b: a * b,
}


def _resolve_column(df: pd.DataFrame, name: str) -> pd.Series:
    """Resolve a cross_column `inputs` entry (a score/counterfactual name)
    against the actual aggregated result DataFrame."""
    if name in df.columns:
        return df[name]
    mean_col = f"mean_{name}"
    if mean_col in df.columns:
        return df[mean_col]
    raise KeyError(
        f"cross_column input '{name}' doesn't match any column in the result "
        f"set (tried '{name}' and '{mean_col}'). Available columns: "
        f"{list(df.columns)}. If this score/counterfactual doesn't use the "
        f"`mean` aggregation, reference its aggregated column name directly "
        f"(e.g. 'median_{name}') instead of the bare score name."
    )


def apply_cross_column(df: pd.DataFrame, cc: CrossColumnConfig) -> pd.Series:
    if cc.op == CrossColumnOp.CUSTOM:
        func = resolve(cc.function)
        return func(*[_resolve_column(df, c) for c in cc.inputs])

    if cc.op in (CrossColumnOp.TTEST, CrossColumnOp.CHI_SQUARE):
        raise NotImplementedError(
            f"'{cc.op.value}' is reserved for a future statistical-testing phase "
            "(Requirements Doc Section 6/7) and is not implemented in v1."
        )

    op = _BUILTIN_OPS[cc.op]
    cols = [_resolve_column(df, c) for c in cc.inputs]
    result = cols[0]
    for c in cols[1:]:
        result = op(result, c)
    return result
