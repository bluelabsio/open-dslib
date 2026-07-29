import statistics

import polars as pl
import pytest
from scipy import stats as scipy_stats

from crosstab_tool import CrosstabSpec, run_crosstab

# ------------------------------------------------------------------- paired: column ---

CURRENT = [10.0, 20.0, 30.0, 100.0, 200.0, 300.0]
PRIOR = [8.0, 18.0, 25.0, 90.0, 210.0, 280.0]
REGIONS = ["A", "A", "A", "B", "B", "B"]
DIFFS = [c - p for c, p in zip(CURRENT, PRIOR)]  # noqa: B905  [2, 2, 5, 10, -10, 20]


@pytest.fixture
def paired_column_df():
    return pl.DataFrame({"region": REGIONS, "current_score": CURRENT, "prior_score": PRIOR})


def _group_diffs(region: str) -> list[float]:
    return [d for r, d in zip(REGIONS, DIFFS) if r == region]  # noqa: B905


def _paired_column_spec(metrics):
    return CrosstabSpec.model_validate(
        {
            "source": {"type": "dataframe", "data": None},  # patched per-test
            "score_columns": ["current_score"],
            "groupby": {"type": "explicit", "groups": [["region"]]},
            "stats": [{"name": "count", "column": "current_score"}],
            "comparison": {
                "column": "current_score",
                "baseline": {"type": "column", "column": "prior_score"},
                "metrics": metrics,
            },
        }
    )


def test_paired_column_mean_median_std_diff(paired_column_df):
    spec = _paired_column_spec(["mean_diff", "median_diff", "std_diff"])
    spec.source.data = paired_column_df

    result = run_crosstab(spec).to_pandas()["region"].sort_values("region")

    for _, row in result.iterrows():
        expected = _group_diffs(row["region"])
        assert row["current_score_mean_diff"] == pytest.approx(statistics.mean(expected))
        assert row["current_score_median_diff"] == pytest.approx(statistics.median(expected))
        assert row["current_score_std_diff"] == pytest.approx(statistics.stdev(expected))


def test_paired_ttest_matches_scipy(paired_column_df):
    spec = _paired_column_spec(["paired_ttest"])
    spec.source.data = paired_column_df

    result = run_crosstab(spec).to_pandas()["region"]

    for _, row in result.iterrows():
        expected = scipy_stats.ttest_1samp(_group_diffs(row["region"]), popmean=0)
        assert row["current_score_paired_ttest_stat"] == pytest.approx(expected.statistic)
        assert row["current_score_paired_ttest_pvalue"] == pytest.approx(expected.pvalue)


def test_wilcoxon_matches_scipy(paired_column_df):
    spec = _paired_column_spec(["wilcoxon"])
    spec.source.data = paired_column_df

    result = run_crosstab(spec).to_pandas()["region"]

    for _, row in result.iterrows():
        expected_stat, expected_p = scipy_stats.wilcoxon(_group_diffs(row["region"]))
        assert row["current_score_wilcoxon_stat"] == pytest.approx(expected_stat)
        assert row["current_score_wilcoxon_pvalue"] == pytest.approx(expected_p)


def test_base_stats_and_comparison_coexist(paired_column_df):
    spec = _paired_column_spec(["mean_diff"])
    spec.source.data = paired_column_df

    result = run_crosstab(spec).to_pandas()["region"].sort_values("region")
    assert result["current_score_count"].to_list() == [3, 3]
    assert "current_score_mean_diff" in result.columns


# ------------------------------------------------------------- paired: source+join ---


def test_paired_source_join_keys():
    current_df = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "region": ["X", "X", "Y", "Y", "X"],
            "score": [5.0, 7.0, 20.0, 30.0, 100.0],  # id=5 has no baseline match
        }
    )
    baseline_df = pl.DataFrame(
        {"id": [1, 2, 3, 4], "score": [4.0, 8.0, 15.0, 25.0]}
    )

    spec = CrosstabSpec.model_validate(
        {
            "source": {"type": "dataframe", "data": current_df},
            "score_columns": ["score"],
            "groupby": {"type": "explicit", "groups": [["region"]]},
            "stats": [{"name": "count", "column": "score"}],
            "comparison": {
                "column": "score",
                "baseline": {
                    "type": "source",
                    "source": {"type": "dataframe", "data": baseline_df},
                    "join_keys": ["id"],
                },
                "metrics": ["mean_diff"],
            },
        }
    )

    result = run_crosstab(spec).to_pandas()["region"].sort_values("region")

    # id=5 (region X) has no baseline match -> dropped by the inner join.
    x_row = result[result["region"] == "X"].iloc[0]
    y_row = result[result["region"] == "Y"].iloc[0]
    assert x_row["score_count"] == 2
    assert x_row["score_mean_diff"] == pytest.approx(statistics.mean([5.0 - 4.0, 7.0 - 8.0]))
    assert y_row["score_count"] == 2
    assert y_row["score_mean_diff"] == pytest.approx(statistics.mean([20.0 - 15.0, 30.0 - 25.0]))


# ------------------------------------------------------------- unpaired: source ---

CURRENT_A = [1.0, 2.0, 3.0, 4.0, 5.0]
CURRENT_B = [10.0, 11.0, 12.0, 13.0, 14.0]
BASELINE_A = [1.0, 1.0, 2.0, 2.0, 3.0]
BASELINE_B = [9.0, 10.0, 11.0, 12.0, 50.0]


@pytest.fixture
def unpaired_dfs():
    current_df = pl.DataFrame(
        {
            "region": ["A"] * len(CURRENT_A) + ["B"] * len(CURRENT_B),
            "score": CURRENT_A + CURRENT_B,
        }
    )
    baseline_df = pl.DataFrame(
        {
            "region": ["A"] * len(BASELINE_A) + ["B"] * len(BASELINE_B),
            "score": BASELINE_A + BASELINE_B,
        }
    )
    return current_df, baseline_df


def _unpaired_spec(current_df, baseline_df, metrics, groups):
    return CrosstabSpec.model_validate(
        {
            "source": {"type": "dataframe", "data": current_df},
            "score_columns": ["score"],
            "groupby": {"type": "explicit", "groups": groups},
            "stats": [{"name": "count", "column": "score"}],
            "comparison": {
                "column": "score",
                "baseline": {
                    "type": "source",
                    "source": {"type": "dataframe", "data": baseline_df},
                },
                "metrics": metrics,
            },
        }
    )


def test_unpaired_ks_and_mannwhitney_match_scipy(unpaired_dfs):
    current_df, baseline_df = unpaired_dfs
    spec = _unpaired_spec(current_df, baseline_df, ["ks_test", "mannwhitney"], [["region"]])

    result = run_crosstab(spec).to_pandas()["region"].sort_values("region")

    expected = {
        "A": (CURRENT_A, BASELINE_A),
        "B": (CURRENT_B, BASELINE_B),
    }
    for _, row in result.iterrows():
        cur, base = expected[row["region"]]
        ks = scipy_stats.ks_2samp(cur, base)
        mwu = scipy_stats.mannwhitneyu(cur, base)
        assert row["score_ks_test_stat"] == pytest.approx(ks.statistic)
        assert row["score_ks_test_pvalue"] == pytest.approx(ks.pvalue)
        assert row["score_mannwhitney_stat"] == pytest.approx(mwu.statistic)
        assert row["score_mannwhitney_pvalue"] == pytest.approx(mwu.pvalue)


def test_unpaired_overall_groupset(unpaired_dfs):
    current_df, baseline_df = unpaired_dfs
    spec = _unpaired_spec(current_df, baseline_df, ["ks_test"], [[]])

    result = run_crosstab(spec).to_pandas()["__overall__"]

    expected = scipy_stats.ks_2samp(CURRENT_A + CURRENT_B, BASELINE_A + BASELINE_B)
    assert result["score_ks_test_stat"].iloc[0] == pytest.approx(expected.statistic)
    assert result["score_ks_test_pvalue"].iloc[0] == pytest.approx(expected.pvalue)


def test_max_sample_size_truncates_before_test(unpaired_dfs):
    current_df, baseline_df = unpaired_dfs
    spec = _unpaired_spec(current_df, baseline_df, ["ks_test"], [["region"]])
    spec.comparison.max_sample_size = 2

    result = run_crosstab(spec).to_pandas()["region"].sort_values("region")

    expected = {
        "A": (CURRENT_A[:2], BASELINE_A[:2]),
        "B": (CURRENT_B[:2], BASELINE_B[:2]),
    }
    for _, row in result.iterrows():
        cur, base = expected[row["region"]]
        ks = scipy_stats.ks_2samp(cur, base)
        assert row["score_ks_test_stat"] == pytest.approx(ks.statistic)


# ------------------------------------------------------------------- validation ---


def test_unpaired_baseline_rejects_paired_metric(unpaired_dfs):
    current_df, baseline_df = unpaired_dfs
    spec = _unpaired_spec(current_df, baseline_df, ["mean_diff"], [["region"]])

    with pytest.raises(ValueError, match="Unsupported comparison metric"):
        run_crosstab(spec)


def test_paired_baseline_rejects_distributional_metric(paired_column_df):
    spec = _paired_column_spec(["ks_test"])
    spec.source.data = paired_column_df

    with pytest.raises(ValueError, match="Unsupported comparison metric"):
        run_crosstab(spec)


def test_comparison_column_must_be_numeric(paired_column_df):
    df = paired_column_df.with_columns(pl.col("region").alias("not_numeric"))
    spec = CrosstabSpec.model_validate(
        {
            "source": {"type": "dataframe", "data": df},
            "score_columns": ["current_score"],
            "groupby": {"type": "explicit", "groups": [["region"]]},
            "stats": [{"name": "count", "column": "current_score"}],
            "comparison": {
                "column": "not_numeric",
                "baseline": {"type": "column", "column": "prior_score"},
                "metrics": ["mean_diff"],
            },
        }
    )

    with pytest.raises(ValueError, match="must be numeric"):
        run_crosstab(spec)


def test_missing_baseline_column_fails_validation(paired_column_df):
    spec = _paired_column_spec(["mean_diff"])
    spec.source.data = paired_column_df
    spec.comparison.baseline.column = "does_not_exist"

    with pytest.raises(ValueError, match="does_not_exist"):
        run_crosstab(spec)
