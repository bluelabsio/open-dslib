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
    assert "FROM modeling.tfp_plain_language_support_score_20260416 AS scores" in sql
    assert (
        "LEFT JOIN modeling.tfp_modeling_basetable_20260622 AS basetable "
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
    # counterfactuals/cross_column are commented out in this fixture: its
    # `p_support_v3` placeholder column doesn't exist on the real table, and
    # nothing here currently exercises the counterfactual/cross_column
    # feature end-to-end -- see git history for the previously-asserted shape
    # once a real "prior model" column is identified.
    config = load_job_config(EXAMPLES / "counterfactual_example.yaml")
    assert config.counterfactuals == []
    assert config.cross_column == []

    sql = build_query(config)
    assert "mean_p_support_prior" not in sql


def test_bad_config_rejected(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        """
job: {name: bad, model_version: v1}
sources:
  - {name: scores, connection: REDSHIFT_MAIN, table: foo.bar}
base: {from: scores}
scores:
  - {name: p_support, source: DOES_NOT_EXIST, column: p_support}
grouping_variables: []
output: {destination: google_sheets, spreadsheet_id: x, tab: y}
"""
    )
    with pytest.raises(Exception):
        load_job_config(bad)
