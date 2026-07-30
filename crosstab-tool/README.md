# crosstab-tool

Configurable crosstab / counterfactual reporting: joins model scores to
grouping variables in Redshift, computes group-level summary statistics via
SQL push-down, and writes analyst-readable results to Google Sheets, Excel,
or CSV. Replaces the hand-written per-model "universe tabs" SQL.

## Quick start

```bash
pip install -e .
crosstab validate examples/m3_universe_tabs.yaml   # check a config
crosstab sql examples/m3_universe_tabs.yaml        # inspect generated SQL (no DB needed)
crosstab run examples/m3_universe_tabs.yaml        # run end to end
```

A minimal job config:

```yaml
name: my_model_tabs
score_version: my_score_20260701
source:
  base_table: schema.my_score_table
  connection: REDSHIFT            # env-var prefix, see Connections below
  joins:
    - {table: schema.basetable, key: voterbase_id, how: left}
scores:
  - {column: p_support}
counterfactuals:                  # optional comparison columns
  - {column: p_support_v2}
  - {column: support_id_2024, type: categorical, values: [Support, Oppose]}
groupings:                        # order controls the numbered category labels
  - {column: age_bucket, label: Age}          # -> "01 Age"
  - {column: party, label: Party}             # -> "02 Party"
aggregations: [count, mean]       # per-metric override: aggregations: [mean, median]
cross_column:                     # computed post-aggregation, in Python
  - {op: difference, left: avg_p_support, right: avg_p_support_v2, label: v1_v2_gap}
output:
  destination: sheets             # sheets | excel | csv
  spreadsheet: My Model Tabs
  share_with: [you@bluelabs.com]
```

Output is a long/tidy table — one row per grouping-variable level (plus a
`00 Topline` row), one column per statistic — ordered by category, then
level. Set `output.layout: wide` for a pivoted alternative. Every run writes
a `run_metadata` tab (or `.meta.json` sidecar for CSV) with the timestamp,
config hash, score version, and the exact SQL executed, so any sheet can be
traced back to what produced it.

## Connections

Redshift credentials come from environment variables under the prefix named
by `source.connection` (default `REDSHIFT`): `REDSHIFT_USER`, `REDSHIFT_PW`,
`REDSHIFT_HOST`, `REDSHIFT_PORT`, `REDSHIFT_DB` — the same convention as
open-dslib.

Google Sheets uses a service-account JSON key, via `output.credentials_file`
or the `GOOGLE_APPLICATION_CREDENTIALS` env var. Spreadsheets the tool
creates are only visible to the service account unless you list emails in
`output.share_with`.

## Python API

Everything in the YAML maps 1:1 to dataclasses; use them directly when a job
needs logic the config can't express:

```python
from crosstab_tool import Job, Source, Metric, Grouping, run_job, register_cross_column
import scipy.stats

register_cross_column("welch_t", my_welch_t_fn)   # then usable as op: welch_t

job = Job(
    name="api_job",
    source=Source(base_table="s.scores"),
    scores=[Metric(column="p_support")],
    groupings=[Grouping(column="party")],
)
run_job(job)
```

## Custom functions and scale

Standard aggregations always run inside Redshift — Python only ever touches
the aggregated result (one row per grouping level). Custom **aggregations**
are registered as SQL templates (`register_aggregation("stddev",
"STDDEV({col})")`) so they push down too. Custom **cross-column** functions
(and, later, statistical tests) are Python callables, safe because they run
on the small result table.

If a computation truly needs row-level data in Python, that is a deliberate
escape hatch, not a default: use `RedshiftSource.fetch_rows()` to stream a
column subset or a partial aggregation in chunks. Do **not** pull a full
260M-row basetable into a pandas DataFrame; push a partial aggregation into
Redshift first, or use a columnar engine (DuckDB/Polars) on the streamed
chunks.

## Design notes / provenance

Built against the Crosstab/Counterfactual Reporting Tool requirements doc
(v0.2). Per its Appendix B, both internal repos were treated as reference:

- **open-dslib**: the SQL-generation-and-push-down architecture and the
  env-var connection convention are carried forward (rewritten, not
  imported — the original had no join configuration, identifier validation,
  or tests).
- **surveytools-crosstab**: the config-validation approach (typed config
  objects, actionable error messages) and the registry pattern for pluggable
  calculations are modeled on it; its in-memory pandas engine was not
  reused because it doesn't fit the 260M-row scale requirement.

Statistical tests (t-test, chi-square) are not shipped in v1; the
`register_cross_column` interface is the intended plug-in point.

## Development

```bash
pip install -e ".[dev]"
pytest
```
