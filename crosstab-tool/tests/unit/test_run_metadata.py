import json
from pathlib import Path

from crosstab_tool.config.loader import load_job_config
from crosstab_tool.metadata.run_metadata import build_run_metadata, write_run_artifacts
from crosstab_tool.query.builder import build_query

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"


def test_write_run_artifacts_saves_sql_and_metadata(tmp_path):
    config = load_job_config(EXAMPLES / "model3_universe_tabs.yaml")
    sql = build_query(config)
    meta = build_run_metadata(config, (EXAMPLES / "model3_universe_tabs.yaml").read_text())

    metadata_path = write_run_artifacts(meta, tmp_path, sql=sql)

    written = json.loads(metadata_path.read_text())
    assert written["job_name"] == "model3_universe_tabs"
    assert written["sql_path"] is not None

    sql_path = Path(written["sql_path"])
    assert sql_path.exists()
    assert sql_path.read_text() == sql
    assert "CREATE TEMP TABLE base AS" in sql_path.read_text()


def test_write_run_artifacts_without_sql(tmp_path):
    config = load_job_config(EXAMPLES / "model3_universe_tabs.yaml")
    meta = build_run_metadata(config, "irrelevant raw text")

    metadata_path = write_run_artifacts(meta, tmp_path, sql=None)

    written = json.loads(metadata_path.read_text())
    assert written["sql_path"] is None
    assert list(tmp_path.glob("*.sql")) == []
