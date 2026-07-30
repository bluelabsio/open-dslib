"""CLI entry point.

    crosstab run job.yaml          # execute the job end to end
    crosstab sql job.yaml          # print the generated SQL (dry run)
    crosstab validate job.yaml     # parse + validate the config only
"""

from __future__ import annotations

import argparse
import logging
import sys

from crosstab_tool.config import ConfigError, load_job


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="crosstab",
        description="Configurable crosstab / counterfactual reporting tool.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    for name, help_text in [
        ("run", "run a job end to end"),
        ("sql", "print the generated SQL without connecting to the database"),
        ("validate", "validate a job config and exit"),
    ]:
        p = sub.add_parser(name, help=help_text)
        p.add_argument("config", help="path to the job YAML config")
    parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    try:
        job = load_job(args.config)
    except (ConfigError, FileNotFoundError) as e:
        print(f"Config error: {e}", file=sys.stderr)
        return 2

    if args.command == "validate":
        print(f"OK: job {job.name!r} is valid. Result columns: {job.result_columns()}")
        return 0

    from crosstab_tool.sqlgen import build_query

    if args.command == "sql":
        print(build_query(job))
        return 0

    from crosstab_tool.runner import run_job

    try:
        location = run_job(job)
    except ConfigError as e:
        print(f"Config error: {e}", file=sys.stderr)
        return 2
    print(f"Done: {location}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
