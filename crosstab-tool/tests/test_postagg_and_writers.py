import json

import pandas as pd
import pytest

from crosstab_tool.config import job_from_dict
from crosstab_tool.postagg import apply_cross_column, to_wide
from crosstab_tool.runner import run_job
from crosstab_tool.sources import DataSource


def make_job(tmp_path, **overrides):
    d = {
        "name": "t",
        "source": {"base_table": "s.scores"},
        "scores": [{"column": "a"}, {"column": "b"}],
        "groupings": [{"column": "party", "label": "Party"}],
        "cross_column": [
            {"op": "difference", "left": "avg_a", "right": "avg_b", "label": "a_minus_b"}
        ],
        "output": {"destination": "csv", "path": str(tmp_path / "out.csv")},
        "score_version": "v3",
    }
    d.update(overrides)
    return job_from_dict(d)


RESULT = pd.DataFrame(
    {
        "category": ["00 Topline", "01 Party", "01 Party"],
        "level": ["Topline", "D", "R"],
        "count": [100, 60, 40],
        "avg_a": [0.5, 0.7, 0.2],
        "avg_b": [0.4, 0.5, 0.25],
    }
)


class FakeSource(DataSource):
    def __init__(self):
        self.sql = None

    def run_query(self, sql):
        self.sql = sql
        return RESULT.copy()


def test_apply_cross_column(tmp_path):
    job = make_job(tmp_path)
    out = apply_cross_column(RESULT, job)
    assert out["a_minus_b"].tolist() == pytest.approx([0.1, 0.2, -0.05])


def test_to_wide_shape():
    wide = to_wide(RESULT)
    assert list(wide.index) == ["count", "avg_a", "avg_b"]
    assert wide.loc["avg_a", ("01 Party", "D")] == 0.7


def test_run_job_end_to_end_csv(tmp_path):
    job = make_job(tmp_path)
    src = FakeSource()
    location = run_job(job, source=src)

    assert "WITH base AS (" in src.sql
    df = pd.read_csv(location)
    assert list(df.columns) == ["category", "level", "count", "avg_a", "avg_b", "a_minus_b"]
    assert df.iloc[0]["category"] == "00 Topline"

    meta = json.loads((tmp_path / "out.csv.meta.json").read_text())
    assert meta["score_version"] == "v3"
    assert "WITH base AS (" in meta["generated_sql"]


def test_run_job_excel(tmp_path):
    job = make_job(
        tmp_path,
        output={"destination": "excel", "path": str(tmp_path / "out.xlsx")},
    )
    location = run_job(job, source=FakeSource())
    sheets = pd.read_excel(location, sheet_name=None)
    assert set(sheets) == {"crosstabs", "run_metadata"}
    assert "a_minus_b" in sheets["crosstabs"].columns


def test_run_job_wide_layout(tmp_path):
    job = make_job(
        tmp_path,
        output={"destination": "csv", "path": str(tmp_path / "w.csv"), "layout": "wide"},
    )
    run_job(job, source=FakeSource())
    df = pd.read_csv(tmp_path / "w.csv", header=[0, 1])
    assert len(df) == 4  # count, avg_a, avg_b, a_minus_b
