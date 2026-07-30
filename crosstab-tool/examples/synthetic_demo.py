"""Smoke-test the full pipeline against generated data — no Redshift needed.

Builds a synthetic score table + demographic basetable in local DuckDB,
then runs the exact same code path as production (build_query -> execute ->
cross-column -> writer), swapping only the DataSource.

    .venv/bin/python examples/synthetic_demo.py
"""

import numpy as np
import pandas as pd
import duckdb

from crosstab_tool import job_from_dict, run_job
from crosstab_tool.sources import DataSource

N = 200_000
rng = np.random.default_rng(42)


class DuckDBSource(DataSource):
    """Runs the generated SQL in an in-memory DuckDB instead of Redshift."""

    def __init__(self, con):
        self.con = con

    def run_query(self, sql: str) -> pd.DataFrame:
        return self.con.execute(sql).df()


con = duckdb.connect()
con.execute("CREATE SCHEMA c_tfp")

# Score table: support probability correlated with party, plus an older
# model version to act as the counterfactual.
party = rng.choice(["Democrat", "Republican", "Unaffiliated"], N, p=[0.35, 0.3, 0.35])
base_rate = np.select(
    [party == "Democrat", party == "Republican"], [0.78, 0.22], default=0.5
)
p_support = np.clip(base_rate + rng.normal(0, 0.12, N), 0, 1)

scores = pd.DataFrame(
    {
        "voterbase_id": np.arange(N),
        "p_support": p_support,
        "p_support_v2": np.clip(p_support + rng.normal(0.03, 0.05, N), 0, 1),
    }
)
basetable = pd.DataFrame(
    {
        "voterbase_id": np.arange(N),
        "party": party,
        "age_bucket_full": rng.choice(
            ["18-29", "30-44", "45-64", "65+"], N, p=[0.2, 0.25, 0.35, 0.2]
        ),
        "urbanicity": rng.choice(["Urban", "Suburban", "Rural"], N),
        "vote_method_2024": rng.choice(["Mail", "Early", "Election Day", "Did not vote"], N),
    }
)
con.register("scores_df", scores)
con.register("basetable_df", basetable)
con.execute("CREATE TABLE c_tfp.synthetic_scores AS SELECT * FROM scores_df")
con.execute("CREATE TABLE c_tfp.synthetic_basetable AS SELECT * FROM basetable_df")

job = job_from_dict(
    {
        "name": "synthetic_demo",
        "score_version": "synthetic_seed42",
        "source": {
            "base_table": "c_tfp.synthetic_scores",
            "joins": [
                {"table": "c_tfp.synthetic_basetable", "key": "voterbase_id", "how": "left"}
            ],
        },
        "scores": [{"column": "p_support"}, {"column": "p_support_v2"}],
        "counterfactuals": [
            {
                "column": "vote_method_2024",
                "type": "categorical",
                "values": ["Mail", "Early", "Election Day", "Did not vote"],
            }
        ],
        "groupings": [
            {"column": "age_bucket_full", "label": "Age"},
            {"column": "party", "label": "Party"},
            {"column": "urbanicity", "label": "Urbanicity"},
        ],
        "aggregations": ["count", "mean"],
        "cross_column": [
            {
                "op": "difference",
                "left": "avg_p_support_v2",
                "right": "avg_p_support",
                "label": "v2_minus_v1",
            }
        ],
        "output": {
            "destination": "excel",
            "path": "examples/synthetic_demo_output.xlsx",
        },
    }
)

location = run_job(job, source=DuckDBSource(con))
print(f"\nWrote {location}\n")
result = pd.read_excel(location)
with pd.option_context("display.width", 160, "display.max_columns", 20):
    print(result.round(4).to_string(index=False))

# Sanity checks: counts add up, party effect visible, frequencies sum to ~1.
topline = result[result.category == "00 Topline"].iloc[0]
assert topline["count"] == N
per_group = result[result.category == "02 Party"]
assert per_group["count"].sum() == N
dem = per_group[per_group.level == "Democrat"].iloc[0]
rep = per_group[per_group.level == "Republican"].iloc[0]
assert dem.avg_p_support > 0.7 > 0.3 > rep.avg_p_support
freq_cols = [c for c in result.columns if c.startswith("freq_")]
assert abs(result[freq_cols].sum(axis=1) - 1.0).max() < 1e-9
print("\nSanity checks passed: counts, group means, frequency shares.")
