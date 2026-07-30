import pytest
from pydantic import ValidationError

from crosstab_tool.spec.source_spec import JoinSourceSpec

_LEFT = {"type": "parquet", "path": "/data/scores.parquet"}
_RIGHT = {"type": "parquet", "path": "/data/covariates.parquet"}


def test_join_keys_shared_names():
    spec = JoinSourceSpec(left=_LEFT, right=_RIGHT, how="left", join_keys=["entity_id"])
    assert spec.join_keys == ["entity_id"]


def test_left_on_right_on_differing_names():
    spec = JoinSourceSpec(
        left=_LEFT, right=_RIGHT, how="inner", left_on=["entity_id"], right_on=["account_id"]
    )
    assert spec.left_on == ["entity_id"]
    assert spec.right_on == ["account_id"]


def test_rejects_no_keys_at_all():
    with pytest.raises(ValidationError, match="join_keys or left_on"):
        JoinSourceSpec(left=_LEFT, right=_RIGHT, how="inner")


def test_rejects_both_join_keys_and_left_on():
    with pytest.raises(ValidationError, match="not both"):
        JoinSourceSpec(
            left=_LEFT,
            right=_RIGHT,
            how="inner",
            join_keys=["entity_id"],
            left_on=["entity_id"],
            right_on=["entity_id"],
        )


def test_rejects_left_on_without_right_on():
    with pytest.raises(ValidationError, match="must both be set together"):
        JoinSourceSpec(left=_LEFT, right=_RIGHT, how="inner", left_on=["entity_id"])


def test_rejects_mismatched_left_on_right_on_lengths():
    with pytest.raises(ValidationError, match="same number of columns"):
        JoinSourceSpec(
            left=_LEFT,
            right=_RIGHT,
            how="inner",
            left_on=["a", "b"],
            right_on=["a"],
        )


def test_rejects_empty_join_keys():
    with pytest.raises(ValidationError):
        JoinSourceSpec(left=_LEFT, right=_RIGHT, how="inner", join_keys=[])


def test_how_is_required():
    with pytest.raises(ValidationError):
        JoinSourceSpec(left=_LEFT, right=_RIGHT, join_keys=["entity_id"])


def test_supports_nested_joins():
    nested = JoinSourceSpec(left=_LEFT, right=_RIGHT, how="left", join_keys=["entity_id"])
    spec = JoinSourceSpec(
        left={"type": "join", **nested.model_dump(mode="json")},
        right=_RIGHT,
        how="left",
        join_keys=["entity_id"],
    )
    assert spec.left.type == "join"
