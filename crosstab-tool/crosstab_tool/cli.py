"""Entry point: `crosstab run path/to/job.yaml`"""
from __future__ import annotations

import sys
from pathlib import Path

import click
from dotenv import find_dotenv, load_dotenv
from pydantic import ValidationError

from crosstab_tool.compute.cross_column import apply_cross_column, resolve_column_name
from crosstab_tool.config.loader import load_job_config
from crosstab_tool.config.schema import OutputDestination
from crosstab_tool.metadata.run_metadata import build_run_metadata, write_run_artifacts
from crosstab_tool.output.base import drop_hidden
from crosstab_tool.output.files import FileWriter
from crosstab_tool.output.sheets import GoogleSheetsWriter
from crosstab_tool.query.builder import build_query, expected_result_columns
from crosstab_tool.query.identifiers import SQLGenerationError
from crosstab_tool.sources.redshift import RedshiftSource

# crosstab-tool/ repo root (parent of this crosstab_tool/ package directory) --
# added to sys.path below so `custom_functions/` (the shared, git-tracked
# library of custom cross_column functions) is importable via a dotted path
# like "custom_functions.ratios:divide" without the user having to set
# PYTHONPATH themselves. A private, one-off function can still live outside
# this repo and be reached via PYTHONPATH the usual way.
REPO_ROOT = Path(__file__).resolve().parents[1]


def _find_env_file() -> str | None:
    """Locate a `.env` to load: search the cwd and its parent directories
    first (`find_dotenv(usecwd=True)` walks upward until it finds one),
    then fall back to `~/.env`. Lets credentials live at a repo root or in
    the user's home directory instead of requiring `.env` to sit in
    whatever directory `crosstab` happens to be invoked from."""
    env_path = find_dotenv(usecwd=True)
    if env_path:
        return env_path
    home_env = Path.home() / ".env"
    return str(home_env) if home_env.exists() else None


@click.group()
def cli():
    env_path = _find_env_file()
    if env_path:
        load_dotenv(env_path)
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))


@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option(
    "-o", "--output", "output_path", type=click.Path(),
    help="Where to write the .sql file. Defaults to <job_name>.sql in the current directory.",
)
def sql(config_path: str, output_path: str | None):
    """Build the SQL for a config and save it to a .sql file.

    Does not connect to Redshift or write any output — just the config ->
    SQL step, for running the query yourself (e.g. pasted into DBeaver) or
    reviewing it before wiring up real credentials.
    """
    config = load_job_config(config_path)
    generated_sql = build_query(config)
    out_path = Path(output_path) if output_path else Path(f"{config.job.name}.sql")
    out_path.write_text(generated_sql + "\n")
    click.echo(f"SQL written to {out_path}")


@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
def validate(config_path: str):
    """Validate a config against the schema, preview the generated SQL,
    and confirm any `cross_column` entries resolve against the columns
    that SQL would actually produce -- all without a live Redshift
    connection.

    This is the check to script against (e.g. from the
    crosstab-config-wizard skill, or CI) instead of re-implementing
    schema/resolution knowledge separately -- schema validation
    (load_job_config), SQL shape (build_query), and cross_column
    resolution (resolve_column_name) all live here as the single source
    of truth, so a standalone validator can't silently drift out of sync
    as the schema evolves.

    Exits non-zero with a clear message on any failure: schema errors
    from load_job_config, SQL-generation errors from a bad identifier
    (query.identifiers.SQLGenerationError), or a cross_column input that
    doesn't resolve to a real output column.
    """
    try:
        config = load_job_config(config_path)
    except ValidationError as e:
        raise click.ClickException(str(e)) from e
    try:
        generated_sql = build_query(config)
    except SQLGenerationError as e:
        raise click.ClickException(str(e)) from e
    expected_columns = expected_result_columns(config)

    for cc in config.cross_column:
        for name in cc.inputs:
            try:
                resolve_column_name(expected_columns, name)
            except KeyError as e:
                raise click.ClickException(str(e)) from e

    click.echo(f"Config valid: {config.job.name} ({config.job.model_version})")
    click.echo(
        f"{len(config.grouping_variables)} grouping variable(s), "
        f"{len(config.scores)} score(s), {len(config.counterfactuals)} counterfactual(s), "
        f"{len(config.cross_column)} cross_column entry(ies) -- all resolved OK"
    )
    click.echo("")
    click.echo("--- Generated SQL preview ---")
    click.echo(generated_sql)


@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
def run(config_path: str):
    config = load_job_config(config_path)
    sql = build_query(config)

    source = RedshiftSource(name=config.job.name, connection=config.connection)
    df = source.execute(sql)

    for cc in config.cross_column:
        df[cc.name] = apply_cross_column(df, cc)

    df = drop_hidden(df, config)

    writer = (
        GoogleSheetsWriter(config.output)
        if config.output.destination == OutputDestination.GOOGLE_SHEETS
        else FileWriter(config.output)
    )
    writer.write(df)

    meta = build_run_metadata(config, Path(config_path).read_text())
    if config.run_metadata.capture:
        sql_to_save = sql if config.run_metadata.save_sql else None
        metadata_path = write_run_artifacts(meta, config.run_metadata.artifacts_dir, sql=sql_to_save)
        click.echo(f"Run complete: {meta.to_dict()} (metadata written to {metadata_path})")
    else:
        click.echo(f"Run complete: {meta.to_dict()} (capture disabled, nothing written to disk)")


if __name__ == "__main__":
    cli()
