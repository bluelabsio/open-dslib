"""Entry point: `crosstab run path/to/job.yaml`"""
from __future__ import annotations

from pathlib import Path

import click
from dotenv import load_dotenv

from crosstab_tool.compute.cross_column import apply_cross_column
from crosstab_tool.config.loader import load_job_config
from crosstab_tool.config.schema import OutputDestination
from crosstab_tool.metadata.run_metadata import build_run_metadata, write_run_artifacts
from crosstab_tool.output.files import FileWriter
from crosstab_tool.output.sheets import GoogleSheetsWriter
from crosstab_tool.query.builder import build_query
from crosstab_tool.sources.redshift import RedshiftSource


@click.group()
def cli():
    load_dotenv()


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
def run(config_path: str):
    config = load_job_config(config_path)
    sql = build_query(config)

    source_cfg = next(s for s in config.sources if s.name == config.base.from_)
    source = RedshiftSource(name=source_cfg.name, connection=source_cfg.connection)
    df = source.execute(sql)

    for cc in config.cross_column:
        df[cc.name] = apply_cross_column(df, cc)

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
