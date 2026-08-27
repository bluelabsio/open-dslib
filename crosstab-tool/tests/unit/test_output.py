from __future__ import annotations

import pandas as pd
import pytest

from crosstab_tool.config.schema import (
    BaseConfig,
    ColumnRef,
    DataSourceConfig,
    GroupingVariable,
    JobConfig,
    JobMeta,
    OutputConfig,
    OutputDestination,
)
from crosstab_tool.output.base import drop_hidden, shaped, to_wide
from crosstab_tool.output.files import FileWriter


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "category": ["00 Topline", "01 Age"],
            "level": ["Topline", "18-29"],
            "count": [100, 40],
            "mean_p_support": [0.55, 0.61],
        }
    )


def test_to_wide_indexes_by_category_level():
    wide = to_wide(_sample_df())
    assert wide.index.names == ["category", "level"]
    assert "mean_p_support" in wide.columns


def test_shaped_long_is_passthrough():
    df = _sample_df()
    config = OutputConfig(destination=OutputDestination.CSV, path="/tmp/x.csv", layout="long")
    assert shaped(df, config) is df


def test_shaped_wide_reindexes():
    df = _sample_df()
    config = OutputConfig(destination=OutputDestination.CSV, path="/tmp/x.csv", layout="wide")
    out = shaped(df, config)
    assert list(out.columns[:2]) == ["category", "level"]


def test_file_writer_csv(tmp_path):
    out_path = tmp_path / "results.csv"
    config = OutputConfig(destination=OutputDestination.CSV, path=str(out_path))
    writer = FileWriter(config)
    result_path = writer.write(_sample_df())

    assert result_path == str(out_path)
    assert out_path.exists()
    written = pd.read_csv(out_path)
    assert list(written.columns) == ["category", "level", "count", "mean_p_support"]


def test_file_writer_excel(tmp_path):
    out_path = tmp_path / "results.xlsx"
    config = OutputConfig(destination=OutputDestination.EXCEL, path=str(out_path))
    writer = FileWriter(config)
    writer.write(_sample_df())

    assert out_path.exists()
    written = pd.read_excel(out_path, sheet_name="results")
    assert len(written) == 2


def _config_with_hidden(**score_overrides) -> JobConfig:
    return JobConfig(
        job=JobMeta(name="t", model_version="v1"),
        connection="REDSHIFT_MAIN",
        sources=[DataSourceConfig(name="base", table="schema.tbl")],
        base=BaseConfig(**{"from": "base"}),
        scores=[
            ColumnRef(name="p_support", source="base", column="p_support"),
            ColumnRef(
                name="p_support_x_weight",
                source="base",
                column="p_support_x_weight",
                **score_overrides,
            ),
        ],
        grouping_variables=[GroupingVariable(label="01 Age", column="age_bucket")],
        aggregations={"default": ["mean"]},
        output=OutputConfig(destination=OutputDestination.CSV, path="/tmp/x.csv"),
    )


def test_drop_hidden_removes_only_flagged_columns():
    config = _config_with_hidden(hidden=True)
    df = pd.DataFrame(
        {
            "category": ["00 Topline"],
            "level": ["Topline"],
            "count": [100],
            "mean_p_support": [0.55],
            "mean_p_support_x_weight": [0.21],
        }
    )
    out = drop_hidden(df, config)
    assert list(out.columns) == ["category", "level", "count", "mean_p_support"]


def test_drop_hidden_is_a_no_op_when_nothing_hidden():
    config = _config_with_hidden(hidden=False)
    df = pd.DataFrame(
        {
            "category": ["00 Topline"],
            "level": ["Topline"],
            "count": [100],
            "mean_p_support": [0.55],
            "mean_p_support_x_weight": [0.21],
        }
    )
    out = drop_hidden(df, config)
    assert list(out.columns) == list(df.columns)


def test_file_writer_rejects_sheets_destination(tmp_path):
    config = OutputConfig(
        destination=OutputDestination.GOOGLE_SHEETS,
        spreadsheet_id="x" * 44,
        tab="results",
    )
    writer = FileWriter(config)
    with pytest.raises(ValueError, match="GoogleSheetsWriter"):
        writer.write(_sample_df())
