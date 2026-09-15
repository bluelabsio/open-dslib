"""Pluggable custom function registry (Req 4.2, 4.3, 4.4, 5.3).

Custom aggregation and cross-column functions are referenced in config as
dotted import paths (e.g. "myproject.custom_aggs:trimmed_mean") rather
than hard-coded into the tool's core. This module resolves those paths at
run time.

Per Req 5.1.1: a custom function that needs row-level access should
operate on a small, already-partially-aggregated result (or use a
vectorized engine like DuckDB/Polars — see the [vectorized] extra in
pyproject.toml) rather than a naive pandas group-by over the full
260M-row base table. That guidance isn't enforced by this module; flag it
in custom-function docs (see docs/architecture.md open question).
"""
from __future__ import annotations

import importlib
from typing import Callable


def resolve(dotted_path: str) -> Callable:
    module_path, _, func_name = dotted_path.rpartition(":")
    if not module_path:
        raise ValueError(
            f"custom function path '{dotted_path}' must be 'module.path:function_name'"
        )
    module = importlib.import_module(module_path)
    return getattr(module, func_name)
