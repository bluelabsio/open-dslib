"""Descriptive-statistics crosstabs for ML model scores."""

from crosstab_tool.core.result import CrosstabResult
from crosstab_tool.core.runner import run_crosstab
from crosstab_tool.spec.crosstab_spec import CrosstabSpec
from crosstab_tool.stats import builtin as _builtin  # noqa: F401  (registers built-in stats)

__version__ = "0.1.0"
__all__ = ["run_crosstab", "CrosstabResult", "CrosstabSpec"]
