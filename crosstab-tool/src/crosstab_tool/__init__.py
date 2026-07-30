"""Descriptive-statistics crosstabs for ML model scores."""

import logging as _logging

from crosstab_tool import stats as _stats  # noqa: F401  (import registers built-in stats)
from crosstab_tool.core.result import CrosstabResult
from crosstab_tool.core.runner import run_crosstab
from crosstab_tool.spec.crosstab_spec import CrosstabSpec

__version__ = "0.1.0"
__all__ = ["run_crosstab", "CrosstabResult", "CrosstabSpec"]

# Library convention: don't configure handlers here, just make sure logging calls from
# any crosstab_tool module have somewhere to go if the application never configures its
# own handler. cli/app.py (via logging_config.configure_logging) is the one place that
# actually attaches a handler for `xtab`.
_logging.getLogger(__name__).addHandler(_logging.NullHandler())
