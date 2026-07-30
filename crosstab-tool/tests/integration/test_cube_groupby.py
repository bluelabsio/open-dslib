"""Integration tests for cube/auto-groupby (M6). The cube-expansion logic itself lives
in spec/groupby_spec.py and is unit-tested there; this exercises the full run_crosstab()
path (against the file source) plus the cardinality guardrail in core/validation.py
(against the in-memory DataFrame source, since ParquetSource.estimated_row_count() is
always None -- see the guardrail tests below for why that source switch matters).
"""

import polars as pl
import pytest

from crosstab_tool import CrosstabSpec, run_crosstab


@pytest.fixture
def scores_df():
    return pl.DataFrame(
        {
            "region": ["A", "A", "B", "B", "B"],
            "product": ["x", "y", "x", "x", "y"],
            "model_score": [10.0, 20.0, 100.0, 200.0, 300.0],
        }
    )


@pytest.fixture
def scores_parquet(tmp_path, scores_df):
    path = tmp_path / "scores.parquet"
    scores_df.write_parquet(path)
    return path


def _spec(path, groupby, options=None):
    return CrosstabSpec.model_validate(
        {
            "source": {"type": "parquet", "path": str(path)},
            "score_columns": ["model_score"],
            "groupby": groupby,
            "stats": [{"name": "count", "column": "model_score"}],
            "options": options or {},
        }
    )


def _dataframe_spec(df, groupby, options=None):
    # ParquetSource.estimated_row_count() always returns None (see sources/files.py),
    # so the cardinality guardrail -- which is a no-op without a row-count estimate --
    # needs a source that actually reports one; PolarsSource (DataFrameSourceSpec over
    # a pl.DataFrame) does.
    return CrosstabSpec.model_validate(
        {
            "source": {"type": "dataframe", "data": df},
            "score_columns": ["model_score"],
            "groupby": groupby,
            "stats": [{"name": "count", "column": "model_score"}],
            "options": options or {},
        }
    )


def test_cube_expands_to_full_power_set(scores_parquet):
    spec = _spec(
        scores_parquet,
        groupby={"type": "cube", "columns": ["region", "product"]},
    )
    result = run_crosstab(spec)

    assert set(result.frames) == {"region", "product", "region__product", "__overall__"}
    assert result.frames["__overall__"]["model_score_count"].to_list() == [5]
    assert result.frames["region"].sort("region")["model_score_count"].to_list() == [2, 3]


def test_cube_include_empty_false_omits_overall(scores_parquet):
    spec = _spec(
        scores_parquet,
        groupby={"type": "cube", "columns": ["region", "product"], "include_empty": False},
    )
    result = run_crosstab(spec)

    assert set(result.frames) == {"region", "product", "region__product"}


def test_cube_max_depth_limits_groupsets(scores_parquet):
    spec = _spec(
        scores_parquet,
        groupby={
            "type": "cube",
            "columns": ["region", "product"],
            "max_depth": 1,
            "include_empty": False,
        },
    )
    result = run_crosstab(spec)

    assert set(result.frames) == {"region", "product"}


def test_cube_unknown_column_fails_validation(scores_parquet):
    spec = _spec(
        scores_parquet,
        groupby={"type": "cube", "columns": ["not_a_column"]},
    )
    with pytest.raises(ValueError, match="not_a_column"):
        run_crosstab(spec)


def test_cardinality_guardrail_is_a_noop_without_a_row_count_estimate(scores_parquet):
    # ParquetSource.estimated_row_count() always returns None, so even a threshold of 1
    # must not block this -- there's nothing to estimate against.
    spec = _spec(
        scores_parquet,
        groupby={"type": "cube", "columns": ["region", "product"]},
        options={"cardinality_guardrail": {"max_groupset_count_x_cardinality": 1}},
    )
    result = run_crosstab(spec)

    assert set(result.frames) == {"region", "product", "region__product", "__overall__"}


def test_cardinality_guardrail_blocks_oversized_cube(scores_df):
    spec = _dataframe_spec(
        scores_df,
        groupby={"type": "cube", "columns": ["region", "product"]},
        options={"cardinality_guardrail": {"max_groupset_count_x_cardinality": 1}},
    )
    with pytest.raises(ValueError, match="cardinality_guardrail"):
        run_crosstab(spec)


def test_cardinality_guardrail_allows_cube_under_threshold(scores_df):
    spec = _dataframe_spec(
        scores_df,
        groupby={"type": "cube", "columns": ["region", "product"]},
        options={"cardinality_guardrail": {"max_groupset_count_x_cardinality": 1_000_000}},
    )
    result = run_crosstab(spec)

    assert set(result.frames) == {"region", "product", "region__product", "__overall__"}


def test_cardinality_guardrail_is_a_noop_for_explicit_groupby(scores_df):
    # Guardrail is cube-specific (per docs/implementation-plan.md); an explicit groupby
    # with a similarly low threshold must not be blocked by it.
    spec = _dataframe_spec(
        scores_df,
        groupby={"type": "explicit", "groups": [["region"]]},
        options={"cardinality_guardrail": {"max_groupset_count_x_cardinality": 1}},
    )
    result = run_crosstab(spec)

    assert set(result.frames) == {"region"}
