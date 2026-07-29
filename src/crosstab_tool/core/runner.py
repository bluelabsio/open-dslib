from __future__ import annotations

from crosstab_tool.core.result import CrosstabResult
from crosstab_tool.core.validation import validate_spec
from crosstab_tool.engine.polars_engine import run as engine_run
from crosstab_tool.spec.crosstab_spec import CrosstabSpec


class CrosstabRunner:
    """Single orchestration path shared by the Python API and the CLI."""

    def run(self, spec: CrosstabSpec) -> CrosstabResult:
        validate_spec(spec)
        frames = engine_run(spec)
        return CrosstabResult(frames=frames, spec=spec)


def run_crosstab(spec: CrosstabSpec) -> CrosstabResult:
    return CrosstabRunner().run(spec)
