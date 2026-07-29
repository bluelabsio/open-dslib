"""YAML/JSON <-> CrosstabSpec round-trip.

Config files are a direct serialization of CrosstabSpec (requirement 7): the CLI
(cli/config_loader.py) is the main consumer of the from_* functions here, but nothing
below is CLI-specific -- any caller (e.g. a notebook loading a saved config) can use
these directly.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from crosstab_tool.spec.crosstab_spec import CrosstabSpec

_YAML_SUFFIXES = {".yaml", ".yml"}
_JSON_SUFFIXES = {".json"}


def spec_from_dict(data: dict[str, Any]) -> CrosstabSpec:
    return CrosstabSpec.model_validate(data)


def spec_from_yaml(text: str) -> CrosstabSpec:
    return spec_from_dict(yaml.safe_load(text))


def spec_from_json(text: str) -> CrosstabSpec:
    return spec_from_dict(json.loads(text))


def spec_from_file(path: str | Path) -> CrosstabSpec:
    path = Path(path)
    text = path.read_text()

    if path.suffix in _YAML_SUFFIXES:
        return spec_from_yaml(text)
    if path.suffix in _JSON_SUFFIXES:
        return spec_from_json(text)

    raise ValueError(
        f"Unsupported config file extension {path.suffix!r} for {path} "
        f"(expected one of {sorted(_YAML_SUFFIXES | _JSON_SUFFIXES)})"
    )


def spec_to_dict(spec: CrosstabSpec) -> dict[str, Any]:
    """JSON-safe dict form of an already-constructed spec.

    Only meaningful for specs built from a file-backed source (parquet/csv/sql): a
    DataFrameSourceSpec holds a live in-memory object with no serialization, by design
    (see spec/source_spec.py) -- dumping one of those isn't a supported round-trip.
    """
    return spec.model_dump(mode="json")


def spec_json_schema() -> dict[str, Any]:
    return CrosstabSpec.model_json_schema()
