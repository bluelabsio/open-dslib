import re

import pytest

from crosstab_tool.config import ConfigError, job_from_dict
from crosstab_tool.sqlgen import build_query


def reference_job_dict():
    """Config equivalent of the Appendix A hand-written reference SQL."""
    return {
        "name": "m3_universe_tabs",
        "source": {
            "base_table": "c_tfp.plain_language_support_score_20260416",
            "joins": [
                {
                    "table": "c_tfp.co_modeling_basetable_20260622",
                    "key": "voterbase_id",
                    "how": "left",
                }
            ],
        },
        "scores": [{"column": "p_support"}, {"column": "p_support_standardized"}],
        "groupings": [{"column": "age_bucket_full", "label": "Age"}],
        "aggregations": ["count", "mean"],
        "output": {"destination": "csv", "path": "out.csv"},
    }


def test_reproduces_reference_pattern():
    sql = build_query(job_from_dict(reference_job_dict()))
    # (1) base CTE joining score table to basetable on the key
    assert "WITH base AS (" in sql
    assert (
        "LEFT JOIN c_tfp.co_modeling_basetable_20260622 t1 USING (voterbase_id)" in sql
    )
    # (2) numbered category labels, (3) explicit Topline row
    assert "'00 Topline' AS category" in sql
    assert "'Topline' AS level" in sql
    assert "'01 Age' AS category" in sql
    # (4) one avg_<column> per score
    assert "AVG(p_support) AS avg_p_support" in sql
    assert "AVG(p_support_standardized) AS avg_p_support_standardized" in sql
    # (5) UNION ALL of all grouping variables, (6) final ordering
    assert sql.count("UNION ALL") == 1
    assert sql.rstrip().endswith("ORDER BY category, level;")


def test_one_tab_per_grouping_plus_topline():
    d = reference_job_dict()
    d["groupings"] = [{"column": c} for c in ["a", "b", "c"]]
    sql = build_query(job_from_dict(d))
    assert sql.count("GROUP BY 1, 2") == 4
    assert sql.count("UNION ALL") == 3


def test_grouping_order_controls_category_numbers():
    d = reference_job_dict()
    d["groupings"] = [
        {"column": "party", "label": "Party", "order": 9},
        {"column": "age", "label": "Age", "order": 1},
    ]
    sql = build_query(job_from_dict(d))
    assert sql.index("'01 Age'") < sql.index("'09 Party'")


def test_categorical_counterfactual_frequency_columns():
    d = reference_job_dict()
    d["counterfactuals"] = [
        {"column": "flag", "type": "categorical", "values": ["Support", "Don't Know"]}
    ]
    sql = build_query(job_from_dict(d))
    assert "AVG(CASE WHEN flag = 'Support' THEN 1.0 ELSE 0.0 END) AS freq_flag_support" in sql
    # literal is escaped, column slug is sanitized
    assert "flag = 'Don''t Know'" in sql
    assert "freq_flag_don_t_know" in sql


def test_median_and_custom_aggregations():
    d = reference_job_dict()
    d["scores"] = [{"column": "p", "aggregations": ["mean", "median", "min", "max", "sum"]}]
    sql = build_query(job_from_dict(d))
    for expr in ["MEDIAN(p) AS median_p", "MIN(p) AS min_p", "MAX(p) AS max_p", "SUM(p) AS sum_p"]:
        assert expr in sql


def test_registered_custom_aggregation_pushes_down():
    from crosstab_tool.registry import AGGREGATIONS, register_aggregation

    register_aggregation("stddev", "STDDEV({col})")
    try:
        d = reference_job_dict()
        d["scores"] = [{"column": "p", "aggregations": ["stddev"]}]
        sql = build_query(job_from_dict(d))
        assert "STDDEV(p) AS stddev_p" in sql
    finally:
        del AGGREGATIONS["stddev"]


def test_malicious_identifier_rejected():
    d = reference_job_dict()
    d["groupings"] = [{"column": "age; DROP TABLE x --"}]
    with pytest.raises(ConfigError, match="not a valid SQL identifier"):
        build_query(job_from_dict(d))


def test_statistic_columns_consistent_across_tabs():
    """Every UNION ALL branch must select the same columns in the same order."""
    d = reference_job_dict()
    d["counterfactuals"] = [
        {"column": "flag", "type": "categorical", "values": ["A", "B"]}
    ]
    sql = build_query(job_from_dict(d))
    selects = re.findall(r"SELECT .*?GROUP BY 1, 2", sql, flags=re.S)
    assert len(selects) == 2
    cols = [[c for c in re.findall(r"AS (\w+)", s) if c != "VARCHAR"] for s in selects]
    assert cols[0] == cols[1]
