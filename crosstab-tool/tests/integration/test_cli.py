import polars as pl
import pytest
from typer.testing import CliRunner

from crosstab_tool.cli.app import app

runner = CliRunner()


@pytest.fixture
def scores_parquet(tmp_path):
    df = pl.DataFrame(
        {
            "region": ["A", "A", "B"],
            "model_score": [10.0, 20.0, 30.0],
        }
    )
    path = tmp_path / "scores.parquet"
    df.write_parquet(path)
    return path


@pytest.fixture
def config_path(tmp_path, scores_parquet):
    path = tmp_path / "config.yaml"
    path.write_text(
        f"""
source:
  type: parquet
  path: {scores_parquet}
score_columns: [model_score]
groupby:
  type: explicit
  groups: [[region]]
stats:
  - name: count
    column: model_score
  - name: mean
    column: model_score
"""
    )
    return path


def test_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "run" in result.output
    assert "validate" in result.output
    assert "schema" in result.output


def test_schema_command_prints_valid_json():
    import json

    result = runner.invoke(app, ["schema"])
    assert result.exit_code == 0
    schema = json.loads(result.output)
    assert schema["title"] == "CrosstabSpec"


def test_validate_valid_config(config_path):
    result = runner.invoke(app, ["validate", "--config", str(config_path)])
    assert result.exit_code == 0
    assert "OK" in result.output


def test_validate_bad_column_fails_cleanly(tmp_path, scores_parquet):
    bad_config = tmp_path / "bad.yaml"
    bad_config.write_text(
        f"""
source:
  type: parquet
  path: {scores_parquet}
score_columns: [model_score]
groupby:
  type: explicit
  groups: [[not_a_real_column]]
stats:
  - name: count
    column: model_score
"""
    )
    result = runner.invoke(app, ["validate", "--config", str(bad_config)])
    assert result.exit_code == 1
    assert "not_a_real_column" in result.output


def test_validate_missing_data_file_fails_cleanly(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text(
        """
source:
  type: parquet
  path: /this/path/does/not/exist.parquet
score_columns: [model_score]
groupby:
  type: explicit
  groups: [[region]]
stats:
  - name: count
    column: model_score
"""
    )
    result = runner.invoke(app, ["validate", "--config", str(config)])
    assert result.exit_code == 1
    assert "Error" in result.output


def test_validate_malformed_data_file_fails_cleanly_not_a_traceback(tmp_path):
    # Regression test: polars raises its own exception hierarchy (ComputeError etc,
    # under polars.exceptions.PolarsError) for a source it can't actually read --
    # e.g. an unreachable cloud path or, as reproduced here without any network
    # dependency, a file that exists but isn't valid parquet. This must be caught and
    # formatted like any other operational error, not leak a raw Python traceback.
    bad_data = tmp_path / "not_really.parquet"
    bad_data.write_text("this is not parquet data\n")

    config = tmp_path / "config.yaml"
    config.write_text(
        f"""
source:
  type: parquet
  path: {bad_data}
score_columns: [model_score]
groupby:
  type: explicit
  groups: [[region]]
stats:
  - name: count
    column: model_score
"""
    )
    result = runner.invoke(app, ["validate", "--config", str(config)])
    assert result.exit_code == 1
    assert "Traceback" not in result.output
    assert "Error" in result.output


def test_run_missing_config_file_exits_nonzero(tmp_path):
    result = runner.invoke(app, ["run", "--config", str(tmp_path / "nope.yaml")])
    assert result.exit_code != 0


def test_run_invalid_config_content_fails_cleanly(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text("source: {type: bogus}\n")
    result = runner.invoke(app, ["run", "--config", str(config)])
    assert result.exit_code == 1
    assert "Error" in result.output


def test_run_prints_results_to_terminal_with_stdout_flag(config_path):
    result = runner.invoke(app, ["run", "--config", str(config_path), "--stdout"])
    assert result.exit_code == 0
    assert "region" in result.output
    assert "model_score_count" in result.output


def test_run_defaults_to_a_directory_named_after_the_config_file(config_path):
    # config_path is <tmp_path>/config.yaml -> default output dir <tmp_path>/config/
    result = runner.invoke(app, ["run", "--config", str(config_path)])
    assert result.exit_code == 0

    default_out_dir = config_path.with_suffix("")
    written = default_out_dir / "region.parquet"
    assert written.exists()

    frame = pl.read_parquet(written).sort("region")
    assert frame["region"].to_list() == ["A", "B"]
    assert frame["model_score_count"].to_list() == [2, 1]


def test_run_writes_parquet_files_with_out(tmp_path, config_path):
    out_dir = tmp_path / "out"
    result = runner.invoke(
        app, ["run", "--config", str(config_path), "--out", str(out_dir)]
    )
    assert result.exit_code == 0
    written = out_dir / "region.parquet"
    assert written.exists()

    frame = pl.read_parquet(written).sort("region")
    assert frame["region"].to_list() == ["A", "B"]
    assert frame["model_score_count"].to_list() == [2, 1]


def test_run_writes_csv_files_with_out_and_format(tmp_path, config_path):
    out_dir = tmp_path / "out"
    result = runner.invoke(
        app,
        ["run", "--config", str(config_path), "--out", str(out_dir), "--format", "csv"],
    )
    assert result.exit_code == 0
    written = out_dir / "region.csv"
    assert written.exists()

    frame = pl.read_csv(written).sort("region")
    assert frame["region"].to_list() == ["A", "B"]


def test_run_rejects_unsupported_format(tmp_path, config_path):
    out_dir = tmp_path / "out"
    result = runner.invoke(
        app,
        ["run", "--config", str(config_path), "--out", str(out_dir), "--format", "xlsx"],
    )
    assert result.exit_code == 1
    assert "unsupported --format" in result.output


def test_run_overall_groupset_writes_as_overall_file(tmp_path, scores_parquet):
    config = tmp_path / "config.yaml"
    config.write_text(
        f"""
source:
  type: parquet
  path: {scores_parquet}
score_columns: [model_score]
groupby:
  type: explicit
  groups: [[region], []]
stats:
  - name: count
    column: model_score
"""
    )
    out_dir = tmp_path / "out"
    result = runner.invoke(app, ["run", "--config", str(config), "--out", str(out_dir)])
    assert result.exit_code == 0
    assert sorted(p.name for p in out_dir.iterdir()) == ["__overall__.parquet", "region.parquet"]
