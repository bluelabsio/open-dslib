import json

import pytest
from pydantic import ValidationError

from crosstab_tool.spec.serde import (
    spec_from_dict,
    spec_from_file,
    spec_from_json,
    spec_from_yaml,
    spec_json_schema,
    spec_to_dict,
)

_SPEC_DICT = {
    "source": {"type": "parquet", "path": "/data/scores.parquet"},
    "score_columns": ["model_score"],
    "groupby": {"type": "explicit", "groups": [["region"]]},
    "stats": [{"name": "count", "column": "model_score"}],
}

_SPEC_YAML = """
source:
  type: parquet
  path: /data/scores.parquet
score_columns: [model_score]
groupby:
  type: explicit
  groups: [[region]]
stats:
  - name: count
    column: model_score
"""


def test_spec_from_dict():
    spec = spec_from_dict(_SPEC_DICT)
    assert spec.source.path == "/data/scores.parquet"


def test_spec_from_yaml():
    spec = spec_from_yaml(_SPEC_YAML)
    assert spec.source.path == "/data/scores.parquet"
    assert spec.groupby.groups == [["region"]]


def test_spec_from_json():
    spec = spec_from_json(json.dumps(_SPEC_DICT))
    assert spec.source.path == "/data/scores.parquet"


def test_spec_from_file_yaml(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(_SPEC_YAML)
    spec = spec_from_file(path)
    assert spec.source.path == "/data/scores.parquet"


def test_spec_from_file_yml_suffix(tmp_path):
    path = tmp_path / "config.yml"
    path.write_text(_SPEC_YAML)
    spec = spec_from_file(path)
    assert spec.source.path == "/data/scores.parquet"


def test_spec_from_file_json(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps(_SPEC_DICT))
    spec = spec_from_file(path)
    assert spec.source.path == "/data/scores.parquet"


def test_spec_from_file_rejects_unknown_extension(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text("not a real config")
    with pytest.raises(ValueError, match="Unsupported config file extension"):
        spec_from_file(path)


def test_spec_from_file_still_raises_validation_error_for_bad_content(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("source: {type: bogus}\n")
    with pytest.raises(ValidationError):
        spec_from_file(path)


def test_spec_to_dict_round_trips_through_from_dict():
    spec = spec_from_dict(_SPEC_DICT)
    dumped = spec_to_dict(spec)
    reloaded = spec_from_dict(dumped)
    assert reloaded == spec


def test_spec_json_schema_is_a_valid_json_schema_dict():
    schema = spec_json_schema()
    assert schema["title"] == "CrosstabSpec"
    assert "properties" in schema
    # must itself be JSON-serializable
    json.dumps(schema)
