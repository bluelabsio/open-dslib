"""crosstab-tool: configurable crosstab / counterfactual reporting.

Python API surface (the escape hatch alongside YAML configs):

    from crosstab_tool import (
        Job, Source, Join, Metric, Grouping, CrossColumn, Output,
        load_job, run_job, build_query,
        register_aggregation, register_cross_column,
    )
"""

from crosstab_tool.config import (
    ConfigError,
    CrossColumn,
    Grouping,
    Job,
    Join,
    Metric,
    Output,
    Source,
    job_from_dict,
    load_job,
)
from crosstab_tool.registry import register_aggregation, register_cross_column
from crosstab_tool.runner import run_job
from crosstab_tool.sqlgen import build_query

__all__ = [
    "ConfigError",
    "CrossColumn",
    "Grouping",
    "Job",
    "Join",
    "Metric",
    "Output",
    "Source",
    "build_query",
    "job_from_dict",
    "load_job",
    "register_aggregation",
    "register_cross_column",
    "run_job",
]
