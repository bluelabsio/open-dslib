"""File I/O + spec parsing, thin enough that cli/app.py has zero business logic of its
own -- everything here just delegates to spec/serde.py and core/runner.py."""

from __future__ import annotations

from pathlib import Path

from crosstab_tool.core.result import CrosstabResult
from crosstab_tool.core.runner import run_crosstab
from crosstab_tool.spec.crosstab_spec import CrosstabSpec
from crosstab_tool.spec.serde import spec_from_file


def load_spec(config_path: Path) -> CrosstabSpec:
    return spec_from_file(config_path)


def load_and_run(config_path: Path) -> CrosstabResult:
    return run_crosstab(load_spec(config_path))
