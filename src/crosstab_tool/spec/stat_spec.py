from typing import Any

from pydantic import BaseModel, Field


class StatSpec(BaseModel):
    name: str
    column: str
    params: dict[str, Any] = Field(default_factory=dict)
