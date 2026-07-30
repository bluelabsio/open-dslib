"""Loads and validates a job config from YAML (or JSON) into a JobConfig."""
from __future__ import annotations

import json
from pathlib import Path

import yaml

from crosstab_tool.config.schema import JobConfig


def load_job_config(path: str | Path) -> JobConfig:
    path = Path(path)
    raw = path.read_text()
    if path.suffix in (".yaml", ".yml"):
        data = yaml.safe_load(raw)
    elif path.suffix == ".json":
        data = json.loads(raw)
    else:
        raise ValueError(f"unsupported config extension: {path.suffix}")
    return JobConfig.model_validate(data)
