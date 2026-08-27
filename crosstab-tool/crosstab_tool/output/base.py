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

from crosstab_tool.config.schema import JobConfig, OutputConfig
from crosstab_tool.query.builder import column_output_names


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


def drop_hidden(df: pd.DataFrame, config: JobConfig) -> pd.DataFrame:
    """Drop columns belonging to scores/counterfactuals marked `hidden:
    true` before writing output. They're still fully computed in SQL and
    remain usable as `cross_column` inputs -- `hidden` only controls
    whether they show up in what a Writer actually writes, for
    intermediate columns (e.g. a row-level product needed only to feed a
    weighted-average cross_column) that aren't meant to be read directly.
    Call this after cross_column computation (a hidden column can still
    be a cross_column input) and before the config's own Writer runs."""
    hidden_cols = [
        name
        for col in [*config.scores, *config.counterfactuals]
        if col.hidden
        for name in column_output_names(col, config.aggregations.default)
    ]
    return df.drop(columns=[c for c in hidden_cols if c in df.columns])
