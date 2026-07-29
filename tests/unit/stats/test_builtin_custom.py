import polars as pl
import pytest
from scipy import stats as scipy_stats

from crosstab_tool.stats import builtin_custom  # noqa: F401  (registers skew/kurtosis)
from crosstab_tool.stats.registry import get_stat

VALUES = [1.0, 2.0, 2.0, 3.0, 3.0, 3.0, 10.0]


@pytest.mark.parametrize("name", ["skew", "kurtosis"])
def test_output_column_naming(name):
    stat = get_stat(name)
    assert stat.output_columns("model_score", {}) == [f"model_score_{name}"]


def test_skew_matches_scipy():
    stat = get_stat("skew")
    df = pl.DataFrame({"model_score": VALUES})
    result = df.select(stat.build_expr("model_score", {}))
    assert result["model_score_skew"][0] == pytest.approx(scipy_stats.skew(VALUES))


def test_kurtosis_matches_scipy():
    stat = get_stat("kurtosis")
    df = pl.DataFrame({"model_score": VALUES})
    result = df.select(stat.build_expr("model_score", {}))
    assert result["model_score_kurtosis"][0] == pytest.approx(scipy_stats.kurtosis(VALUES))


def test_skew_computed_per_group_in_group_by():
    df = pl.DataFrame(
        {
            "grp": ["a", "a", "a", "b", "b", "b", "b"],
            "model_score": VALUES,
        }
    )
    stat = get_stat("skew")
    result = df.group_by("grp", maintain_order=True).agg(
        stat.build_expr("model_score", {})
    )

    group_a = [1.0, 2.0, 2.0]
    group_b = [3.0, 3.0, 3.0, 10.0]
    expected = {
        "a": scipy_stats.skew(group_a),
        "b": scipy_stats.skew(group_b),
    }
    for row in result.iter_rows(named=True):
        assert row["model_score_skew"] == pytest.approx(expected[row["grp"]])


def test_skew_returns_nan_for_tiny_groups():
    stat = get_stat("skew")
    df = pl.DataFrame({"model_score": [1.0, 2.0]})
    result = df.select(stat.build_expr("model_score", {}))
    assert result["model_score_skew"][0] != result["model_score_skew"][0]  # NaN != NaN
