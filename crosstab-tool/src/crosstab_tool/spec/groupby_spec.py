from itertools import combinations
from typing import Annotated, Literal, Optional, Union

from pydantic import Field, field_validator, model_validator

from crosstab_tool.spec._base import StrictModel


class GroupBySpec(StrictModel):
    """An explicit, user-defined list of groupsets, each reported as its own crosstab."""

    type: Literal["explicit"] = "explicit"
    groups: list[list[str]]

    @field_validator("groups")
    @classmethod
    def _validate_groups(cls, groups: list[list[str]]) -> list[list[str]]:
        if not groups:
            raise ValueError("groupby.groups must contain at least one groupset")
        for group in groups:
            if len(group) != len(set(group)):
                raise ValueError(f"duplicate column in groupset: {group!r}")
        return groups

    def expand_to_groupsets(self) -> list[list[str]]:
        return self.groups


class CubeSpec(StrictModel):
    """Auto-generated groupset combinations (cube/rollup) over a covariate set, instead
    of an explicit list -- e.g. `columns=[region, product]` expands to `[[region],
    [product], [region, product]]` (plus `[]` when `include_empty`).

    `max_depth` caps how many columns combine per groupset (omit for the full 2^k power
    set); `include_empty` additionally computes the ungrouped ("overall") crosstab.
    Both this and `GroupBySpec` normalize via `expand_to_groupsets()` into the same flat
    `list[list[str]]` shape -- `engine/polars_engine.py` consumes either identically and
    doesn't know or care which mode produced them.
    """

    type: Literal["cube"] = "cube"
    columns: list[str]
    max_depth: Optional[int] = None  # noqa: UP045 -- None means the full power set
    include_empty: bool = True

    @field_validator("columns")
    @classmethod
    def _validate_columns(cls, columns: list[str]) -> list[str]:
        if not columns:
            raise ValueError("groupby.columns must contain at least one column")
        if len(columns) != len(set(columns)):
            raise ValueError(f"duplicate column in groupby.columns: {columns!r}")
        return columns

    @model_validator(mode="after")
    def _validate_max_depth(self) -> "CubeSpec":
        if self.max_depth is not None and not (1 <= self.max_depth <= len(self.columns)):
            raise ValueError(
                f"groupby.max_depth must be between 1 and len(columns)={len(self.columns)}, "
                f"got {self.max_depth}"
            )
        return self

    def expand_to_groupsets(self) -> list[list[str]]:
        depth_ceiling = self.max_depth if self.max_depth is not None else len(self.columns)
        groupsets = [
            list(combo)
            for depth in range(1, depth_ceiling + 1)
            for combo in combinations(self.columns, depth)
        ]
        if self.include_empty:
            groupsets.insert(0, [])
        return groupsets


# NB: kept as typing.Union (not `X | Y`) -- same pydantic-runtime-resolution reason as
# spec/comparison_spec.py's NB and spec/source_spec.py's DataSourceSpec.
GroupBySpecUnion = Annotated[
    Union[GroupBySpec, CubeSpec],  # noqa: UP007
    Field(discriminator="type"),
]
