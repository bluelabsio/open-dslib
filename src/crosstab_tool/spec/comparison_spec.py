from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Field, field_validator

from crosstab_tool.spec.source_spec import DataSourceSpec

# NB: Optional[X] / Union[X, Y] (not `X | Y`) throughout this module for pydantic fields
# -- `X | Y` needs Python 3.10's PEP 604 support at the *type* level, which pydantic's
# runtime annotation resolution requires even with `from __future__ import annotations`.


class ColumnBaselineSpec(BaseModel):
    """The counterfactual score lives in another column of the SAME source.

    Inherently row-aligned -> paired comparison.
    """

    type: Literal["column"] = "column"
    column: str


class SourceBaselineSpec(BaseModel):
    """The counterfactual score lives in a separate dataset.

    join_keys set -> paired comparison after an inner join on those keys.
    join_keys omitted -> unpaired/distributional comparison: each groupset is
    aggregated independently in both datasets and compared at the
    distribution level, not row-matched.
    """

    type: Literal["source"] = "source"
    source: DataSourceSpec
    join_keys: Optional[list[str]] = None  # noqa: UP045


BaselineSpec = Annotated[
    Union[ColumnBaselineSpec, SourceBaselineSpec],  # noqa: UP007
    Field(discriminator="type"),
]


class ComparisonSpec(BaseModel):
    """Optional counterfactual/baseline comparison for a single score column."""

    column: str
    baseline: BaselineSpec
    metrics: list[str]
    max_sample_size: Optional[int] = None  # noqa: UP045

    @field_validator("metrics")
    @classmethod
    def _validate_metrics(cls, metrics: list[str]) -> list[str]:
        if not metrics:
            raise ValueError("comparison.metrics must contain at least one metric")
        return metrics

    @property
    def is_paired(self) -> bool:
        if isinstance(self.baseline, ColumnBaselineSpec):
            return True
        return self.baseline.join_keys is not None
