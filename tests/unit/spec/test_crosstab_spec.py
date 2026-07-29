import pytest
from pydantic import ValidationError

from crosstab_tool.spec.crosstab_spec import CrosstabSpec


def _base_spec_dict(**overrides):
    spec = {
        "source": {"type": "parquet", "path": "/data/scores.parquet"},
        "score_columns": ["model_score"],
        "groupby": {"type": "explicit", "groups": [["region"]]},
        "stats": [{"name": "count", "column": "model_score"}],
    }
    spec.update(overrides)
    return spec


def test_parses_minimal_valid_spec():
    spec = CrosstabSpec.model_validate(_base_spec_dict())
    assert spec.source.path == "/data/scores.parquet"
    assert spec.groupby.groups == [["region"]]
    assert spec.filters == []
    assert spec.options == {}


def test_discriminates_source_type():
    spec = CrosstabSpec.model_validate(
        _base_spec_dict(source={"type": "csv", "path": "/data/scores.csv"})
    )
    assert spec.source.type == "csv"


def test_rejects_unknown_source_type():
    with pytest.raises(ValidationError):
        CrosstabSpec.model_validate(_base_spec_dict(source={"type": "bogus", "path": "x"}))


def test_rejects_empty_score_columns():
    with pytest.raises(ValidationError):
        CrosstabSpec.model_validate(_base_spec_dict(score_columns=[]))


def test_rejects_empty_stats():
    with pytest.raises(ValidationError):
        CrosstabSpec.model_validate(_base_spec_dict(stats=[]))


def test_rejects_empty_groupby_groups():
    with pytest.raises(ValidationError):
        CrosstabSpec.model_validate(
            _base_spec_dict(groupby={"type": "explicit", "groups": []})
        )


def test_rejects_duplicate_column_within_a_groupset():
    with pytest.raises(ValidationError):
        CrosstabSpec.model_validate(
            _base_spec_dict(groupby={"type": "explicit", "groups": [["region", "region"]]})
        )
