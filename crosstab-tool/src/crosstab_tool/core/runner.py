from __future__ import annotations

import logging

from crosstab_tool.core.result import CrosstabResult
from crosstab_tool.core.validation import validate_spec
from crosstab_tool.engine.polars_engine import run as engine_run
from crosstab_tool.sources.registry import build_source
from crosstab_tool.spec.crosstab_spec import CrosstabSpec

logger = logging.getLogger(__name__)


class CrosstabRunner:
    """Single orchestration path shared by the Python API and the CLI."""

    def run(self, spec: CrosstabSpec) -> CrosstabResult:
        # Built once and threaded through both validation and execution: for a file or
        # in-memory source this is free either way, but for a SQL source (M4)
        # `describe_schema()` and `to_polars_lazyframe()` both require actually running
        # the query -- building the adapter twice would mean running it twice.
        logger.info("Building source adapter (type=%s)", spec.source.type)
        source = build_source(spec.source)

        logger.info("Validating spec against source schema")
        validate_spec(spec, source)

        logger.info("Running crosstab engine")
        frames = engine_run(spec, source)
        logger.info("Done: computed %d groupset(s): %s", len(frames), ", ".join(frames))

        return CrosstabResult(frames=frames, spec=spec)


def run_crosstab(spec: CrosstabSpec) -> CrosstabResult:
    return CrosstabRunner().run(spec)
