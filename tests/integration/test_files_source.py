import polars as pl
import pytest

from crosstab_tool import CrosstabSpec, run_crosstab


@pytest.fixture
def scores_parquet(tmp_path):
    df = pl.DataFrame(
        {
            "region": ["A", "A", "B", "B", "B"],
            "product": ["x", "y", "x", "x", "y"],
            "model_score": [10.0, 20.0, 100.0, 200.0, 300.0],
        }
    )
    path = tmp_path / "scores.parquet"
    df.write_parquet(path)
    return path


def _spec(path, groups, stats, filters=None):
    return CrosstabSpec.model_validate(
        {
            "source": {"type": "parquet", "path": str(path)},
            "score_columns": ["model_score"],
            "groupby": {"type": "explicit", "groups": groups},
            "stats": stats,
            "filters": filters or [],
        }
    )


def test_single_groupby_count_and_mean(scores_parquet):
    spec = _spec(
        scores_parquet,
        groups=[["region"]],
        stats=[
            {"name": "count", "column": "model_score"},
            {"name": "mean", "column": "model_score"},
        ],
    )
    result = run_crosstab(spec)

    frame = result.frames["region"].sort("region")
    assert frame["region"].to_list() == ["A", "B"]
    assert frame["model_score_count"].to_list() == [2, 3]
    assert frame["model_score_mean"].to_list() == pytest.approx([15.0, 200.0])


def test_multiple_groupsets_reported_independently(scores_parquet):
    spec = _spec(
        scores_parquet,
        groups=[["region"], ["region", "product"], []],
        stats=[{"name": "count", "column": "model_score"}],
    )
    result = run_crosstab(spec)

    assert set(result.frames) == {"region", "region__product", "__overall__"}
    assert result.frames["__overall__"]["model_score_count"].to_list() == [5]
    assert result.frames["region__product"].sort(["region", "product"])[
        "model_score_count"
    ].to_list() == [1, 1, 2, 1]


def test_filters_are_applied_before_aggregation(scores_parquet):
    spec = _spec(
        scores_parquet,
        groups=[["region"]],
        stats=[{"name": "count", "column": "model_score"}],
        filters=["model_score > 15"],
    )
    result = run_crosstab(spec)

    frame = result.frames["region"].sort("region")
    assert frame["region"].to_list() == ["A", "B"]
    assert frame["model_score_count"].to_list() == [1, 3]


def test_percentile_stat_end_to_end(scores_parquet):
    spec = _spec(
        scores_parquet,
        groups=[[]],
        stats=[{"name": "percentile", "column": "model_score", "params": {"q": 0.5}}],
    )
    result = run_crosstab(spec)

    assert result.frames["__overall__"]["model_score_p50"].to_list() == [100.0]


def test_unknown_groupby_column_fails_validation(scores_parquet):
    spec = _spec(
        scores_parquet,
        groups=[["not_a_column"]],
        stats=[{"name": "count", "column": "model_score"}],
    )
    with pytest.raises(ValueError, match="not_a_column"):
        run_crosstab(spec)


def test_stat_on_non_numeric_column_fails_validation(scores_parquet):
    spec = _spec(
        scores_parquet,
        groups=[["region"]],
        stats=[{"name": "mean", "column": "region"}],
    )
    with pytest.raises(ValueError, match="numeric"):
        run_crosstab(spec)
