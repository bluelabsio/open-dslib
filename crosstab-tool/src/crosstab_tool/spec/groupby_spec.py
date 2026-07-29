from typing import Literal

from pydantic import field_validator

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
