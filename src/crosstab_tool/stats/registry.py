from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import polars as pl

NUMERIC_DTYPE_PREFIXES = ("Int", "UInt", "Float", "Decimal")

ExprBuilder = Callable[[str, dict[str, Any]], pl.Expr]
OutputColumnsFn = Callable[[str, dict[str, Any]], list[str]]


@dataclass(frozen=True)
class StatFunction:
    name: str
    build_expr: ExprBuilder
    output_columns: OutputColumnsFn
    requires_numeric: bool = True


_REGISTRY: dict[str, StatFunction] = {}


def register_stat(stat: StatFunction) -> None:
    _REGISTRY[stat.name] = stat


def get_stat(name: str) -> StatFunction:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise ValueError(
            f"Unknown stat {name!r}; registered stats: {sorted(_REGISTRY)}"
        ) from None


def is_numeric_dtype(dtype_name: str) -> bool:
    return dtype_name.startswith(NUMERIC_DTYPE_PREFIXES)
