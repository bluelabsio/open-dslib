"""Run metadata capture (Req 4.6 — reproducibility and versioning)."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from crosstab_tool.config.schema import JobConfig


@dataclass
class RunMetadata:
    job_name: str
    model_version: str
    run_timestamp: str
    config_hash: str
    notes: str | None = None
    sql_path: str | None = None  # set by write_run_artifacts if SQL was saved

    def to_dict(self) -> dict:
        return asdict(self)


def build_run_metadata(config: JobConfig, config_raw_text: str) -> RunMetadata:
    config_hash = hashlib.sha256(config_raw_text.encode("utf-8")).hexdigest()[:12]
    return RunMetadata(
        job_name=config.job.name,
        model_version=config.job.model_version,
        run_timestamp=datetime.now(timezone.utc).isoformat(),
        config_hash=config_hash,
        notes=config.run_metadata.notes,
    )


def write_run_artifacts(
    meta: RunMetadata, artifacts_dir: str | Path, sql: str | None = None
) -> Path:
    """Write this run's metadata (and, if provided, the exact SQL that was
    executed) to `artifacts_dir`. Controlled by `RunMetadataConfig.capture`
    (whether to write anything) and `.save_sql` (whether `sql` is included).

    Files are named `<timestamp>_<job_name>.{json,sql}` so a run can be
    found and reproduced later without re-deriving SQL from the config —
    see the docstring on RunMetadataConfig.save_sql for why that matters.

    Returns the path to the metadata JSON file.
    """
    out_dir = Path(artifacts_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    stamp = meta.run_timestamp.replace(":", "-")
    base_name = f"{stamp}_{meta.job_name}"

    if sql is not None:
        sql_path = out_dir / f"{base_name}.sql"
        sql_path.write_text(sql)
        meta.sql_path = str(sql_path)

    metadata_path = out_dir / f"{base_name}.json"
    metadata_path.write_text(json.dumps(meta.to_dict(), indent=2))
    return metadata_path
