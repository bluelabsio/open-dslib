from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl

from crosstab_tool.spec.crosstab_spec import CrosstabSpec


@dataclass
class CrosstabResult:
    """One Polars DataFrame per groupset, keyed by groupset identifier (e.g. "region",
    "region__product", "__overall__")."""

    frames: dict[str, pl.DataFrame]
    spec: CrosstabSpec

    def to_pandas(self) -> dict[str, Any]:
        return {key: frame.to_pandas() for key, frame in self.frames.items()}
