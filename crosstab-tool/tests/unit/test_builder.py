from pathlib import Path

import pytest

from crosstab_tool.config.loader import load_job_config
from crosstab_tool.query.builder import build_query

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"


def test_model3_universe_tabs_loads_and_validates():
    config = load_job_config(EXAMPLES / "model3_universe_tabs.yaml")
    assert config.job.name == "model3_universe_tabs"
    assert len(config.grouping_variables) == 13
    assert config.grouping_variables[0].label == "01 Age"
    assert config.include_topline is True


def test_model3_universe_tabs_query_shape():
    config = load_job_config(EXAMPLES / "model3_universe_tabs.yaml")
    sql = build_query(config)

    # base table joins scores to basetable (aliased to their config names) on
    # the reference key, resolved to each source's actual physical table,
    # materialized once via CREATE TEMP TABLE rather than a WITH CTE (a CTE
    # would be re-inlined -- and its joins re-run -- for every UNION ALL block)
    assert "CREATE TEMP TABLE base AS" in sql
    # basetable is the base FROM (defines the population), scores is left-joined in --
    # the recommended convention per config_reference.md's note on `base.from`
    assert "FROM modeling.tfp_modeling_basetable_20260622 AS basetable" in sql
    assert (
        "LEFT JOIN modeling.tfp_plain_language_support_score_20260416 AS scores "
        "USING(voterbase_id)" in sql
    )
    # base table selects only the columns scores/counterfactuals/grouping
    # variables actually reference, each qualified by its declared source --
    # not a blind `*`/`alias.*` over the whole join chain, which would
    # surface duplicate column names for any two joined tables that happen
    # to share an unrelated column name (CREATE TEMP TABLE, unlike a CTE,
    # is a real table and rejects those outright)
    assert "scores.p_support" in sql
    assert "basetable.age_bucket_full" in sql

    # topline row present, matching Appendix A's '00 Topline' / GROUP BY 1,2
    assert "'00 Topline' AS category" in sql
    assert "'Topline' AS level" in sql

    # one block per grouping variable, each grouping on its own column
    assert sql.count("GROUP BY 1, 2") == 1 + len(config.grouping_variables)
    # cast to a consistent type across UNION ALL branches (Topline's level
    # is a string literal, so non-text grouping columns need this or the
    # UNION fails with a type-mismatch error at execution time)
    assert "CAST(age_bucket_full AS VARCHAR) AS level" in sql
    assert "'09 Party' AS category" in sql

    # one avg_<score> per score, matching avg_p_support / avg_p_support_standardized
    assert "AVG(CAST(p_support AS DOUBLE PRECISION)) AS mean_p_support" in sql
    assert (
        "AVG(CAST(p_support_standardized AS DOUBLE PRECISION)) AS mean_p_support_standardized"
        in sql
    )

    # unioned and ordered exactly like the reference SQL
    assert sql.count("UNION ALL") == len(config.grouping_variables)  # topline + N-1 unions = N joins
    assert sql.strip().endswith("ORDER BY 1, 2;")


def test_counterfactual_cross_column_example_loads():
    config = load_job_config(EXAMPLES / "counterfactual_example.yaml")
    assert [c.name for c in config.counterfactuals] == ["p_support_prior"]
    assert [cc.name for cc in config.cross_column] == ["p_support_delta"]
    assert config.cross_column[0].inputs == ["p_support", "p_support_prior"]

    sql = build_query(config)
    # the `query` source is wrapped as a derived table, not identifier-checked
    assert "(SELECT voterbase_id, p_support AS p_support_prior" in sql
    assert "AVG(CAST(p_support_prior AS DOUBLE PRECISION)) AS mean_p_support_prior" in sql


def test_custom_cross_column_example_loads_and_resolves():
    import sys

    config = load_job_config(EXAMPLES / "custom_cross_column_example.yaml")
    assert config.scores == []
    assert [c.name for c in config.counterfactuals] == ["p_support_x_weight", "weight"]
    assert all(c.hidden for c in config.counterfactuals)
    cc = config.cross_column[0]
    assert cc.name == "weighted_avg_p_support_pct"
    assert cc.function == "custom_cross_column_functions:weighted_average_pct"

    sql = build_query(config)
    assert "AVG(CAST(p_support_x_weight AS DOUBLE PRECISION)) AS mean_p_support_x_weight" in sql
    assert "AVG(CAST(weight AS DOUBLE PRECISION)) AS mean_weight" in sql

    # the custom function itself must actually import and compute correctly --
    # `crosstab validate` only checks that `inputs` resolve to real column
    # names, never that the function imports or runs (see the postmortem in
    # compute/cross_column.py's docstring), so exercise it directly here.
    sys.path.insert(0, str(EXAMPLES))
    try:
        import pandas as pd

        from crosstab_tool.compute.cross_column import apply_cross_column
        from crosstab_tool.output.base import drop_hidden

        df = pd.DataFrame({"mean_p_support_x_weight": [0.42], "mean_weight": [0.6]})
        result = apply_cross_column(df, cc)
        assert result.iloc[0] == pytest.approx(70.0)

        df["weighted_avg_p_support_pct"] = result
        out = drop_hidden(df, config)
        assert list(out.columns) == ["weighted_avg_p_support_pct"]
    finally:
        sys.path.remove(str(EXAMPLES))


def test_bad_config_rejected(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        """
job: {name: bad, model_version: v1}
connection: REDSHIFT_MAIN
sources:
  - {name: scores, table: foo.bar}
base: {from: scores}
scores:
  - {name: p_support, source: DOES_NOT_EXIST, column: p_support}
grouping_variables: []
output: {destination: google_sheets, spreadsheet_id: x, tab: y}
"""
    )
    with pytest.raises(Exception):
        load_job_config(bad)
