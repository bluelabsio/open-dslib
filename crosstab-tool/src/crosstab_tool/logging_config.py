"""Logging setup for the `xtab` CLI.

Library modules only ever do ``logging.getLogger(__name__)`` and log at INFO/DEBUG --
they never configure handlers themselves (standard library convention, see
`crosstab_tool/__init__.py`'s NullHandler). This module is the one place that actually
attaches a handler, and it's only ever called from `cli/app.py`; a Python-API caller
that wants log output configures the "crosstab_tool" logger however suits their own
application instead.
"""

from __future__ import annotations

import logging

from rich.logging import RichHandler

_PACKAGE_LOGGER_NAME = "crosstab_tool"


def configure_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO

    handler = RichHandler(show_path=False, markup=False, rich_tracebacks=True)
    handler.setLevel(level)

    logger = logging.getLogger(_PACKAGE_LOGGER_NAME)
    logger.setLevel(level)
    logger.handlers = [handler]
    logger.propagate = False
