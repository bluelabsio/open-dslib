"""Built-in aggregation functions -> SQL fragments (Req 4.3).

Each function takes a column expression and returns a full `expr AS alias`
fragment, pushed down to Redshift per Req 5.1.1. Custom aggregations
bypass this module entirely — see query/registry.py.
"""
from __future__ import annotations

from crosstab_tool.config.schema import AggFunction

_SQL_TEMPLATES = {
    AggFunction.MEAN: "AVG(CAST({col} AS DOUBLE PRECISION))",
    AggFunction.COUNT: "COUNT({col})",
    AggFunction.FREQUENCY: "COUNT({col})::FLOAT / NULLIF(COUNT(*) OVER (), 0)",
    AggFunction.SUM: "SUM({col})",
    AggFunction.MIN: "MIN({col})",
    AggFunction.MAX: "MAX({col})",
    AggFunction.MEDIAN: "MEDIAN({col})",  # Redshift-native MEDIAN()
}


def agg_sql(agg: AggFunction, column: str, alias: str) -> str:
    template = _SQL_TEMPLATES[agg]
    return f"{template.format(col=column)} AS {alias}"
