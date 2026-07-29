import pytest

from crosstab_tool.stats.comparison_registry import validate_metrics_for_baseline


def test_paired_allows_diff_and_ttest_and_wilcoxon():
    validate_metrics_for_baseline(["mean_diff", "median_diff", "std_diff"], is_paired=True)
    validate_metrics_for_baseline(["paired_ttest"], is_paired=True)
    validate_metrics_for_baseline(["wilcoxon"], is_paired=True)


def test_paired_rejects_distributional_metrics():
    with pytest.raises(ValueError, match="Unsupported comparison metric"):
        validate_metrics_for_baseline(["ks_test"], is_paired=True)


def test_unpaired_allows_distributional_metrics():
    validate_metrics_for_baseline(["ks_test", "mannwhitney"], is_paired=False)


def test_unpaired_rejects_paired_metrics():
    with pytest.raises(ValueError, match="Unsupported comparison metric"):
        validate_metrics_for_baseline(["mean_diff"], is_paired=False)

    with pytest.raises(ValueError, match="Unsupported comparison metric"):
        validate_metrics_for_baseline(["wilcoxon"], is_paired=False)
