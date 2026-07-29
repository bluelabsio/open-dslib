import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from crosstab_tool import CrosstabSpec, run_crosstab

_DATA = {
    "region": ["A", "A", "B", "B", "B"],
    "model_score": [10.0, 20.0, 100.0, 200.0, 300.0],
}


def _spec_for(data):
    return CrosstabSpec.model_validate(
        {
            "source": {"type": "dataframe", "data": data},
            "score_columns": ["model_score"],
            "groupby": {"type": "explicit", "groups": [["region"]]},
            "stats": [
                {"name": "count", "column": "model_score"},
                {"name": "mean", "column": "model_score"},
            ],
        }
    )


def _assert_expected_result(result):
    frame = result.frames["region"].sort("region")
    assert frame["region"].to_list() == ["A", "B"]
    assert frame["model_score_count"].to_list() == [2, 3]
    assert frame["model_score_mean"].to_list() == pytest.approx([15.0, 200.0])


def test_pandas_dataframe_source():
    _assert_expected_result(run_crosstab(_spec_for(pd.DataFrame(_DATA))))


def test_polars_dataframe_source():
    _assert_expected_result(run_crosstab(_spec_for(pl.DataFrame(_DATA))))


def test_polars_lazyframe_source():
    _assert_expected_result(run_crosstab(_spec_for(pl.DataFrame(_DATA).lazy())))


def test_arrow_table_source():
    _assert_expected_result(run_crosstab(_spec_for(pa.table(_DATA))))


def test_unsupported_in_memory_type_raises():
    with pytest.raises(TypeError, match="Unsupported in-memory data type"):
        run_crosstab(_spec_for({"not": "a dataframe"}))
