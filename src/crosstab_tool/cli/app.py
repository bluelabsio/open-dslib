"""Typer app: `xtab run/validate/schema`, a thin wrapper over core.runner. No crosstab
business logic lives here -- only argument parsing, calling the existing spec-loading /
validation / execution functions, and formatting output."""

from __future__ import annotations

# NB: Optional[X] (not `X | Y`) below -- Typer resolves these annotations at runtime via
# typing.get_type_hints(), which needs Python 3.10 for `X | Y` to work on concrete
# classes. Same caveat as spec/comparison_spec.py, different library hitting it.
from collections.abc import Callable
from pathlib import Path
from typing import NoReturn, Optional, TypeVar

import polars.exceptions as pl_exceptions
import typer
from pydantic import ValidationError
from rich.console import Console

from crosstab_tool.cli.config_loader import load_spec
from crosstab_tool.core.result import CrosstabResult
from crosstab_tool.core.runner import run_crosstab
from crosstab_tool.core.validation import validate_spec
from crosstab_tool.sources.registry import build_source
from crosstab_tool.spec.serde import spec_json_schema

app = typer.Typer(
    add_completion=False,
    help="crosstab-tool: descriptive-statistics crosstabs for ML model scores.",
)
console = Console()
err_console = Console(stderr=True)

_SUPPORTED_FORMATS = ("parquet", "csv")
# Everything expected from a bad-but-plausible user input: an invalid/incomplete spec
# (ValidationError/ValueError), a source that can't actually be reached (OSError covers
# a missing local file; ConnectorX/database errors surface as generic RuntimeErrors),
# or a source Polars itself couldn't read (bad/unreachable cloud path, malformed file,
# wrong credentials -- all subclasses of polars.exceptions.PolarsError).
_OPERATIONAL_ERRORS = (
    ValidationError,
    ValueError,
    OSError,
    RuntimeError,
    pl_exceptions.PolarsError,
)

T = TypeVar("T")

CONFIG_OPTION = typer.Option(
    ...,
    "--config",
    "-c",
    exists=True,
    dir_okay=False,
    readable=True,
    help="Path to a YAML/JSON CrosstabSpec config file.",
)


def _fail(message: str) -> NoReturn:
    err_console.print(f"[bold red]Error:[/bold red] {message}")
    raise typer.Exit(code=1)


def _or_fail(fn: Callable[..., T], *args: object) -> T:
    try:
        return fn(*args)
    except _OPERATIONAL_ERRORS as exc:
        _fail(str(exc))


@app.command()
def run(
    config: Path = CONFIG_OPTION,
    out: Optional[Path] = typer.Option(  # noqa: B008, UP045
        None,
        "--out",
        "-o",
        help="Directory to write one output file per groupset into (prints to the "
        "terminal instead if omitted).",
    ),
    format: str = typer.Option(  # noqa: B008 (typer's standard default pattern)
        "parquet",
        "--format",
        "-f",
        help="Output file format when --out is given: parquet or csv.",
    ),
) -> None:
    """Run a crosstab spec and print the results, or write them to --out."""
    if out is not None and format not in _SUPPORTED_FORMATS:
        _fail(f"unsupported --format {format!r} (expected one of {_SUPPORTED_FORMATS})")

    spec = _or_fail(load_spec, config)
    result: CrosstabResult = _or_fail(run_crosstab, spec)

    if out is None:
        for key, frame in result.frames.items():
            console.print(f"[bold]{key}[/bold]")
            console.print(frame)
        return

    out.mkdir(parents=True, exist_ok=True)
    for key, frame in result.frames.items():
        path = out / f"{key}.{format}"
        if format == "parquet":
            frame.write_parquet(path)
        else:
            frame.write_csv(path)
    console.print(f"Wrote {len(result.frames)} groupset(s) to {out} ({format})")


def _validate(config: Path) -> None:
    spec = load_spec(config)
    source = build_source(spec.source)
    validate_spec(spec, source)


@app.command()
def validate(config: Path = CONFIG_OPTION) -> None:
    """Validate a config file (schema, columns, dtypes) without running it."""
    _or_fail(_validate, config)
    console.print(f"[green]OK[/green] -- {config} is valid.")


@app.command()
def schema() -> None:
    """Print the CrosstabSpec JSON Schema (editor autocompletion / doc generation)."""
    console.print_json(data=spec_json_schema())


def main() -> None:
    app()


if __name__ == "__main__":
    main()
