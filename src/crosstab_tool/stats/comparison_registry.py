from __future__ import annotations

from typing import Any

# Simple diff stats reuse the existing stat registry (stats/registry.py) applied to a
# constructed "__diff" column -- no new execution machinery needed for these.
PAIRED_AGGREGATE_METRICS: dict[str, tuple[str, dict[str, Any]]] = {
    "mean_diff": ("mean", {}),
    "median_diff": ("percentile", {"q": 0.5}),
    "std_diff": ("std", {}),
}

# paired_ttest only needs aggregate exprs (mean/std/count of __diff); the t-stat/p-value
# is a lightweight scipy post-processing step on the small aggregated table.
PAIRED_TTEST_METRIC = "paired_ttest"

# wilcoxon is the exception requiring the raw per-group __diff array (rank-based test).
PAIRED_RAW_METRICS = frozenset({"wilcoxon"})

PAIRED_METRICS = frozenset(PAIRED_AGGREGATE_METRICS) | PAIRED_RAW_METRICS | {PAIRED_TTEST_METRIC}

# Unpaired/distributional metrics compare two independently-aggregated raw score arrays
# (current vs. baseline), not row-matched diffs.
UNPAIRED_METRICS = frozenset({"ks_test", "mannwhitney"})


def validate_metrics_for_baseline(metrics: list[str], is_paired: bool) -> None:
    allowed = PAIRED_METRICS if is_paired else UNPAIRED_METRICS
    unknown = sorted(set(metrics) - allowed)
    if unknown:
        kind = "paired" if is_paired else "unpaired/distributional"
        raise ValueError(
            f"Unsupported comparison metric(s) for a {kind} baseline: {unknown}; "
            f"allowed: {sorted(allowed)}"
        )
