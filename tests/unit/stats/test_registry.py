import polars as pl
import pytest

from crosstab_tool.stats import builtin  # noqa: F401  (registers built-in stats)
from crosstab_tool.stats.registry import get_stat, is_numeric_dtype


def test_unknown_stat_raises():
    with pytest.raises(ValueError, match="Unknown stat"):
        get_stat("not_a_real_stat")


@pytest.mark.parametrize("name", ["count", "mean", "std", "min", "max"])
def test_simple_stats_build_named_expr(name):
    stat = get_stat(name)
    expr = stat.build_expr("model_score", {})
    assert stat.output_columns("model_score", {}) == [f"model_score_{name}"]

    df = pl.DataFrame({"model_score": [1.0, 2.0, 3.0]})
    result = df.select(expr)
    assert result.columns == [f"model_score_{name}"]


def test_percentile_stat_requires_q_param():
    stat = get_stat("percentile")
    with pytest.raises(ValueError, match="requires a 'q' param"):
        stat.build_expr("model_score", {})


def test_percentile_stat_output_column_name():
    stat = get_stat("percentile")
    assert stat.output_columns("model_score", {"q": 0.9}) == ["model_score_p90"]

    df = pl.DataFrame({"model_score": [1.0, 2.0, 3.0, 4.0]})
    result = df.select(stat.build_expr("model_score", {"q": 0.5}))
    assert result.columns == ["model_score_p50"]


def test_count_does_not_require_numeric():
    assert get_stat("count").requires_numeric is False


def test_is_numeric_dtype():
    assert is_numeric_dtype("Float64")
    assert is_numeric_dtype("Int64")
    assert not is_numeric_dtype("Utf8")
    assert not is_numeric_dtype("String")
