import pytest
from pydantic import ValidationError

from crosstab_tool.spec.comparison_spec import (
    ColumnBaselineSpec,
    ComparisonSpec,
    SourceBaselineSpec,
)


def test_column_baseline_is_paired():
    spec = ComparisonSpec.model_validate(
        {
            "column": "model_score",
            "baseline": {"type": "column", "column": "prior_score"},
            "metrics": ["mean_diff"],
        }
    )
    assert isinstance(spec.baseline, ColumnBaselineSpec)
    assert spec.is_paired is True


def test_source_baseline_with_join_keys_is_paired():
    spec = ComparisonSpec.model_validate(
        {
            "column": "model_score",
            "baseline": {
                "type": "source",
                "source": {"type": "parquet", "path": "/data/prior.parquet"},
                "join_keys": ["entity_id"],
            },
            "metrics": ["mean_diff"],
        }
    )
    assert isinstance(spec.baseline, SourceBaselineSpec)
    assert spec.is_paired is True


def test_source_baseline_without_join_keys_is_unpaired():
    spec = ComparisonSpec.model_validate(
        {
            "column": "model_score",
            "baseline": {
                "type": "source",
                "source": {"type": "parquet", "path": "/data/prior.parquet"},
            },
            "metrics": ["ks_test"],
        }
    )
    assert spec.baseline.join_keys is None
    assert spec.is_paired is False


def test_rejects_empty_metrics():
    with pytest.raises(ValidationError):
        ComparisonSpec.model_validate(
            {
                "column": "model_score",
                "baseline": {"type": "column", "column": "prior_score"},
                "metrics": [],
            }
        )


def test_rejects_unknown_baseline_type():
    with pytest.raises(ValidationError):
        ComparisonSpec.model_validate(
            {
                "column": "model_score",
                "baseline": {"type": "bogus"},
                "metrics": ["mean_diff"],
            }
        )


def test_crosstab_spec_comparison_defaults_to_none():
    from crosstab_tool.spec.crosstab_spec import CrosstabSpec

    spec = CrosstabSpec.model_validate(
        {
            "source": {"type": "parquet", "path": "/data/scores.parquet"},
            "score_columns": ["model_score"],
            "groupby": {"type": "explicit", "groups": [["region"]]},
            "stats": [{"name": "count", "column": "model_score"}],
        }
    )
    assert spec.comparison is None
