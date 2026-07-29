from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

from crosstab_tool.spec.comparison_spec import ComparisonSpec
from crosstab_tool.spec.groupby_spec import GroupBySpec
from crosstab_tool.spec.source_spec import DataSourceSpec
from crosstab_tool.spec.stat_spec import StatSpec


class CrosstabSpec(BaseModel):
    """The central, engine-agnostic contract: what to compute and against what data.

    Config files (YAML/JSON) are a direct serialization of this model; the Python API
    constructs the same object directly. Both feed the same execution path.
    """

    source: DataSourceSpec
    score_columns: list[str]
    groupby: GroupBySpec
    stats: list[StatSpec]
    filters: list[str] = Field(default_factory=list)
    options: dict[str, Any] = Field(default_factory=dict)
    comparison: Optional[ComparisonSpec] = None  # noqa: UP045 (see spec/comparison_spec.py)

    @field_validator("score_columns")
    @classmethod
    def _validate_score_columns(cls, score_columns: list[str]) -> list[str]:
        if not score_columns:
            raise ValueError("score_columns must contain at least one column")
        return score_columns

    @field_validator("stats")
    @classmethod
    def _validate_stats(cls, stats: list[StatSpec]) -> list[StatSpec]:
        if not stats:
            raise ValueError("stats must contain at least one StatSpec")
        return stats
