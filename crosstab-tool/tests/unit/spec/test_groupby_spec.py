import pytest
from pydantic import ValidationError

from crosstab_tool.spec.crosstab_spec import CrosstabSpec
from crosstab_tool.spec.groupby_spec import CubeSpec


def test_full_power_set_includes_empty_by_default():
    cube = CubeSpec(columns=["region", "product"])
    assert cube.expand_to_groupsets() == [
        [],
        ["region"],
        ["product"],
        ["region", "product"],
    ]


def test_include_empty_false_omits_overall_groupset():
    cube = CubeSpec(columns=["region", "product"], include_empty=False)
    assert cube.expand_to_groupsets() == [["region"], ["product"], ["region", "product"]]


def test_max_depth_caps_combination_size():
    cube = CubeSpec(columns=["region", "product", "channel"], max_depth=2, include_empty=False)
    groupsets = cube.expand_to_groupsets()
    assert groupsets == [
        ["region"],
        ["product"],
        ["channel"],
        ["region", "product"],
        ["region", "channel"],
        ["product", "channel"],
    ]
    assert all(len(g) <= 2 for g in groupsets)


def test_max_depth_equal_to_column_count_is_full_power_set():
    cube = CubeSpec(columns=["region", "product"], max_depth=2, include_empty=False)
    assert cube.expand_to_groupsets() == [["region"], ["product"], ["region", "product"]]


def test_rejects_empty_columns():
    with pytest.raises(ValidationError):
        CubeSpec(columns=[])


def test_rejects_duplicate_columns():
    with pytest.raises(ValidationError):
        CubeSpec(columns=["region", "region"])


def test_rejects_max_depth_below_one():
    with pytest.raises(ValidationError):
        CubeSpec(columns=["region", "product"], max_depth=0)


def test_rejects_max_depth_above_column_count():
    with pytest.raises(ValidationError):
        CubeSpec(columns=["region", "product"], max_depth=3)


def test_crosstab_spec_discriminates_cube_groupby():
    spec = CrosstabSpec.model_validate(
        {
            "source": {"type": "parquet", "path": "/data/scores.parquet"},
            "score_columns": ["model_score"],
            "groupby": {"type": "cube", "columns": ["region", "product"]},
            "stats": [{"name": "count", "column": "model_score"}],
        }
    )
    assert isinstance(spec.groupby, CubeSpec)
    assert spec.groupby.expand_to_groupsets() == [
        [],
        ["region"],
        ["product"],
        ["region", "product"],
    ]
