from __future__ import annotations

import sqlite3

import pytest

from crosstab_tool.config.schema import AggFunction
from crosstab_tool.query.aggregations import agg_sql


def test_frequency_sql_shape():
    sql = agg_sql(AggFunction.FREQUENCY, "val", "frequency_val")
    assert sql == "COUNT(*)::FLOAT / SUM(COUNT(*)) OVER () AS frequency_val"


def test_frequency_computes_population_share_of_each_level():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE base (age_bucket TEXT)")
    conn.executemany(
        "INSERT INTO base VALUES (?)",
        [("18-29",)] * 4 + [("30-44",)] * 2 + [("45-64",)] * 3,
    )

    raw_expr = agg_sql(AggFunction.FREQUENCY, "val", "frequency").split(" AS ")[0]
    # sqlite doesn't understand Postgres/Redshift's `::FLOAT` cast syntax --
    # translate to sqlite's CAST(...) for this test only. The expression
    # under test is otherwise byte-for-byte what `agg_sql` produces.
    prefix, _, rest = raw_expr.partition("::FLOAT")
    frequency_expr = f"CAST({prefix} AS FLOAT){rest}"
    sql = f"""
        SELECT age_bucket AS level, COUNT(*) AS count, {frequency_expr} AS frequency
        FROM base
        GROUP BY age_bucket
        ORDER BY age_bucket
    """
    rows = {level: (count, freq) for level, count, freq in conn.execute(sql)}

    assert rows["18-29"] == (4, 4 / 9)
    assert rows["30-44"] == (2, 2 / 9)
    assert rows["45-64"] == (3, 3 / 9)
    assert sum(freq for _, freq in rows.values()) == pytest.approx(1.0)
