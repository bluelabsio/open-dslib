import pandas as pd
import pytest

from crosstab_tool.compute.cross_column import apply_cross_column
from crosstab_tool.config.schema import CrossColumnConfig, CrossColumnOp


def _sample_df():
    # Shaped like the real output of query/builder.py + a DataSource: no
    # bare "p_support_v5" column, only the aggregated mean_/count_ ones.
    return pd.DataFrame(
        {
            "category": ["00 Topline"],
            "level": ["Topline"],
            "count": [100],
            "mean_p_support_v5": [0.62],
            "count_p_support_v5": [100],
            "mean_p_support_v4": [0.55],
            "count_p_support_v4": [100],
        }
    )


def test_difference_resolves_against_mean_columns():
    # Regression test: this exact shape used to raise KeyError('p_support_v5')
    # because apply_cross_column looked up the bare score name instead of
    # the aggregated `mean_<name>` column the SQL builder actually produces.
    df = _sample_df()
    cc = CrossColumnConfig(
        name="diff_v5_vs_v4", op=CrossColumnOp.DIFFERENCE, inputs=["p_support_v5", "p_support_v4"]
    )
    result = apply_cross_column(df, cc)
    assert result.iloc[0] == pytest.approx(0.62 - 0.55)


def test_multiply_resolves_against_mean_columns():
    df = _sample_df()
    cc = CrossColumnConfig(
        name="product", op=CrossColumnOp.MULTIPLY, inputs=["p_support_v5", "p_support_v4"]
    )
    result = apply_cross_column(df, cc)
    assert result.iloc[0] == pytest.approx(0.62 * 0.55)


def test_add_resolves_against_mean_columns():
    df = _sample_df()
    cc = CrossColumnConfig(
        name="total", op=CrossColumnOp.ADD, inputs=["p_support_v5", "p_support_v4"]
    )
    result = apply_cross_column(df, cc)
    assert result.iloc[0] == pytest.approx(0.62 + 0.55)


def test_divide_resolves_against_mean_columns():
    df = _sample_df()
    cc = CrossColumnConfig(
        name="ratio", op=CrossColumnOp.DIVIDE, inputs=["p_support_v5", "p_support_v4"]
    )
    result = apply_cross_column(df, cc)
    assert result.iloc[0] == pytest.approx(0.62 / 0.55)


def test_bare_column_still_preferred_when_present():
    # If a bare column genuinely exists (e.g. a custom aggregation wrote one
    # directly), it should be used as-is rather than forcing mean_ lookup.
    df = _sample_df()
    df["p_support_v5"] = [0.9]
    cc = CrossColumnConfig(
        name="diff", op=CrossColumnOp.DIFFERENCE, inputs=["p_support_v5", "p_support_v4"]
    )
    result = apply_cross_column(df, cc)
    assert result.iloc[0] == pytest.approx(0.9 - 0.55)


def test_unresolvable_column_raises_clear_error():
    df = _sample_df()
    cc = CrossColumnConfig(
        name="diff", op=CrossColumnOp.DIFFERENCE, inputs=["p_support_v5", "totally_missing"]
    )
    with pytest.raises(KeyError, match="totally_missing"):
        apply_cross_column(df, cc)
