"""Integration tests for JoinSourceSpec/JoinedSource -- combining a scores source and a
separate covariates source without writing the join into a raw query. Exercised here
over in-memory DataFrame sources (source-type-agnostic: the same JoinedSource adapter
composes any two DataSourceAdapters, regardless of what produced them).
"""

import polars as pl
import pytest

from crosstab_tool import CrosstabSpec, run_crosstab


@pytest.fixture
def scores_df():
    return pl.DataFrame({"entity_id": [1, 2, 3], "model_score": [10.0, 20.0, 30.0]})


@pytest.fixture
def covariates_df():
    return pl.DataFrame({"entity_id": [1, 2, 4], "region": ["A", "B", "C"]})


def _spec(scores_df, covariates_df, how, join_keys=None, left_on=None, right_on=None):
    source = {
        "type": "join",
        "left": {"type": "dataframe", "data": scores_df},
        "right": {"type": "dataframe", "data": covariates_df},
        "how": how,
    }
    if join_keys is not None:
        source["join_keys"] = join_keys
    if left_on is not None:
        source["left_on"] = left_on
        source["right_on"] = right_on

    return CrosstabSpec.model_validate(
        {
            "source": source,
            "score_columns": ["model_score"],
            "groupby": {"type": "explicit", "groups": [["region"]]},
            "stats": [{"name": "count", "column": "model_score"}],
        }
    )


def test_inner_join_drops_unmatched_rows(scores_df, covariates_df):
    spec = _spec(scores_df, covariates_df, how="inner", join_keys=["entity_id"])
    result = run_crosstab(spec)

    frame = result.frames["region"].sort("region")
    assert frame["region"].to_list() == ["A", "B"]
    assert frame["model_score_count"].to_list() == [1, 1]


def test_left_join_keeps_unmatched_left_rows(scores_df, covariates_df):
    spec = _spec(scores_df, covariates_df, how="left", join_keys=["entity_id"])
    result = run_crosstab(spec)

    # entity_id=3 has no covariate match -> region is null, still counted
    frame = result.frames["region"].sort("region")
    assert frame["model_score_count"].sum() == 3


def test_left_on_right_on_with_differing_key_names(scores_df):
    covariates_df = pl.DataFrame({"account_id": [1, 2, 4], "region": ["A", "B", "C"]})
    spec = _spec(
        scores_df,
        covariates_df,
        how="inner",
        left_on=["entity_id"],
        right_on=["account_id"],
    )
    result = run_crosstab(spec)

    frame = result.frames["region"].sort("region")
    assert frame["region"].to_list() == ["A", "B"]
    assert frame["model_score_count"].to_list() == [1, 1]


def test_join_source_validates_covariate_column_exists(scores_df, covariates_df):
    source = {
        "type": "join",
        "left": {"type": "dataframe", "data": scores_df},
        "right": {"type": "dataframe", "data": covariates_df},
        "how": "inner",
        "join_keys": ["entity_id"],
    }
    spec = CrosstabSpec.model_validate(
        {
            "source": source,
            "score_columns": ["model_score"],
            "groupby": {"type": "explicit", "groups": [["not_a_real_column"]]},
            "stats": [{"name": "count", "column": "model_score"}],
        }
    )
    with pytest.raises(ValueError, match="not_a_real_column"):
        run_crosstab(spec)
