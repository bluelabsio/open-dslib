#!/usr/bin/env python3
"""Validate a crosstab-tool job config against the tool's real schema, and
print a preview of the SQL it would generate.

Deliberately imports the actual crosstab_tool package rather than keeping a
second copy of the schema here — that second copy would drift out of sync
with schema.py as the tool evolves, and a "validator" that's checking
against a stale schema is worse than no validator.

Usage:
    python3 validate_config.py <path-to-crosstab-tool-repo> <path-to-config.yaml>

Exit code 0 + SQL preview on success. Exit code 1 + the validation error
message on failure (pydantic's error messages already say which field and
why, so they're passed through as-is rather than re-wrapped).
"""
from __future__ import annotations

import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 2

    repo_path, config_path = sys.argv[1], sys.argv[2]
    sys.path.insert(0, str(Path(repo_path).resolve()))

    try:
        from crosstab_tool.config.loader import load_job_config
        from crosstab_tool.query.builder import build_query
    except ImportError as e:
        print(
            f"Could not import crosstab_tool from '{repo_path}': {e}\n"
            "Check that this path points at the crosstab-tool repo root "
            "(it should contain a crosstab_tool/ package), and that "
            "pydantic and pyyaml are installed."
        )
        return 1

    try:
        config = load_job_config(config_path)
    except Exception as e:
        print(f"Config is INVALID: {e}")
        return 1

    print(f"Config is valid: job '{config.job.name}' ({config.job.model_version})")
    print(f"  {len(config.scores)} score(s), {len(config.counterfactuals)} counterfactual(s), "
          f"{len(config.grouping_variables)} grouping variable(s)")
    print(f"  output -> {config.output.destination.value}"
          + (f" ({config.output.spreadsheet_id}, tab '{config.output.tab}')"
             if config.output.spreadsheet_id else f" ({config.output.path})"))

    sql = build_query(config)
    print("\n--- Generated SQL preview ---\n")
    print(sql)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
