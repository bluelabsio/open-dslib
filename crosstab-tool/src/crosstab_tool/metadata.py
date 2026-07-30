"""Run metadata for reproducibility: enough to answer 'what produced this
sheet, and can I regenerate it.'"""

from __future__ import annotations

from datetime import datetime, timezone

from crosstab_tool.config import Job


def run_metadata(job: Job, sql: str) -> dict:
    return {
        "job_name": job.name,
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "score_version": job.score_version or "(not set)",
        "config_sha256": job.config_hash or "(constructed via Python API)",
        "source_base_table": job.source.base_table,
        "joined_tables": ", ".join(j.table for j in job.source.joins) or "(none)",
        "notes": job.notes or "",
        "generated_sql": sql,
    }
