"""Descriptive-statistics crosstabs for ML model scores."""

from crosstab_tool import stats as _stats  # noqa: F401  (import registers built-in stats)
from crosstab_tool.core.result import CrosstabResult
from crosstab_tool.core.runner import run_crosstab
from crosstab_tool.spec.crosstab_spec import CrosstabSpec

__version__ = "0.1.0"
__all__ = ["run_crosstab", "CrosstabResult", "CrosstabSpec"]
