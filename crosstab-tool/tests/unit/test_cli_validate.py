from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from crosstab_tool.cli import cli

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"

_BASE_YAML = """
job:
  name: {name}
  model_version: v1
connection: REDSHIFT_MAIN
sources:
  - name: base
    table: schema.tbl
base:
  from: base
scores:
{scores}
grouping_variables:
  - label: "01 Age"
    column: age_bucket
cross_column:
{cross_column}
output:
  destination: csv
  path: /tmp/out.csv
"""


def test_validate_passes_on_real_example_config():
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", str(EXAMPLES / "model3_universe_tabs.yaml")])
    assert result.exit_code == 0
    assert "Config valid" in result.output
    assert "Generated SQL preview" in result.output


def test_validate_fails_clean_on_unresolvable_cross_column(tmp_path):
    """The exact bug crosstab-config-wizard's postmortem describes: a score
    that doesn't use `mean` referenced by bare name in cross_column."""
    config_path = tmp_path / "bad.yaml"
    config_path.write_text(
        _BASE_YAML.format(
            name="bad",
            scores=(
                "  - name: p_support\n"
                "    source: base\n"
                "    column: p_support\n"
                "    aggregations: [count]\n"
                "  - name: p_support_v3\n"
                "    source: base\n"
                "    column: p_support_v3\n"
            ),
            cross_column=(
                "  - name: diff\n"
                "    op: difference\n"
                "    inputs: [p_support, p_support_v3]\n"
            ),
        )
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", str(config_path)])

    assert result.exit_code != 0
    assert "doesn't match any column in the result set" in result.output


def test_validate_fails_clean_on_schema_error(tmp_path):
    config_path = tmp_path / "bad_schema.yaml"
    config_path.write_text(
        _BASE_YAML.format(
            name="bad_schema",
            scores="  - name: p_support\n    source: base\n    column: p_support\n",
            cross_column=(
                "  - name: diff\n"
                "    op: difference\n"
                "    inputs: [p_support, does_not_exist]\n"
            ),
        )
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", str(config_path)])

    assert result.exit_code != 0
    assert "unknown column" in result.output.lower() or "does_not_exist" in result.output
