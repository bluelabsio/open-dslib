"""Output writer interface (Req 4.5, 5.3).

`cli.py` (already committed) constructs a writer as `Writer(config.output)`
and calls `writer.write(df)` -- no metadata argument, since run-metadata
capture is handled separately by `metadata/run_metadata.py` and never
passed through the writer. This module matches that existing contract
rather than crosstab-tw's `write(df, job, metadata)` shape, which bundled a
run_metadata tab into the same call.

Ported/adapted from crosstab-tw's `writers/` module. New destinations
implement `Writer`; per the "skill functionality" registry work (Phase 3 of
the unification plan), a longer-term home for writer dispatch is
`query/registry.py`'s dotted-path resolution rather than the destination
enum switch `cli.py` currently does inline -- flagged as an open question,
not done in this pass.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from crosstab_tool.config.schema import OutputConfig


class Writer(ABC):
    def __init__(self, output_config: OutputConfig):
        self.output_config = output_config

    @abstractmethod
    def write(self, df: pd.DataFrame) -> str:
        """Write `df` per `self.output_config`; returns a human-readable
        location (file path or URL) for the CLI to report back."""


def to_wide(df: pd.DataFrame) -> pd.DataFrame:
    """Index by (category, level) rather than a further pivot -- the tidy
    shape from query/builder.py is already one row per category/level.
    Kept as its own function (mirroring crosstab-tw's postagg.to_wide) so a
    real pivot (e.g. grouping-variable values as columns) has one place to
    live if that's wanted later."""
    return df.set_index(["category", "level"])


def shaped(df: pd.DataFrame, output_config: OutputConfig) -> pd.DataFrame:
    if output_config.layout == "wide":
        return to_wide(df).reset_index()
    return df
