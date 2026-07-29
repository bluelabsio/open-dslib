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


def test_rejects_unknown_top_level_field():
    with pytest.raises(ValidationError, match="extra_forbidden|Extra inputs"):
        CrosstabSpec.model_validate(_base_spec_dict(not_a_real_field=True))


def test_rejects_unknown_field_on_nested_source_spec():
    # Regression test: pydantic silently drops unrecognized fields by default, which
    # would let e.g. a typo'd or not-yet-implemented config key (like the `sampling`
    # block sketched in docs/POLARS_SCALE.md) do nothing instead of erroring.
    with pytest.raises(ValidationError, match="extra_forbidden|Extra inputs"):
        CrosstabSpec.model_validate(
            _base_spec_dict(
                source={
                    "type": "sql",
                    "connection": "sqlite:///x.db",
                    "query": "SELECT 1",
                    "sampling": {"strategy": "stratified"},
                }
            )
        )
