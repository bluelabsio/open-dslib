from typing import Any

from pydantic import Field

from crosstab_tool.spec._base import StrictModel


class StatSpec(StrictModel):
    name: str
    column: str
    params: dict[str, Any] = Field(default_factory=dict)
