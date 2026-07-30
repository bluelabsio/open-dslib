# Architecture

Sketch of a config-driven crosstab/counterfactual reporting tool, per
`Crosstab_Tool_Requirements_v0_2`. Built fresh rather than on top of
`open-dslib` or `surveytools-crosstab` — see "What we kept from the
existing repos" below for the two exceptions.

## Project layout

```
crosstab-tool/
  pyproject.toml
  README.md
  crosstab_tool/
    __init__.py
    cli.py                    # `crosstab run job.yaml`
    config/
      schema.py                # pydantic models — the config contract (Req 4.2)
      loader.py                 # YAML/JSON -> validated JobConfig
    sources/
      base.py                   # DataSource interface (Req 4.1, 5.3)
      redshift.py                # v1's only backend
      # future: postgres.py, sheets.py, csv_excel.py, s3.py — same interface
    query/
      builder.py                 # JobConfig -> Appendix-A-style SQL (base CTE + UNION ALL)
      aggregations.py             # AggFunction -> SQL fragment (Req 4.3)
      registry.py                  # dotted-path custom function resolution (Req 4.2/4.3/4.4)
    compute/
      cross_column.py              # difference/multiply/custom over the aggregated result (Req 4.4)
    output/
      base.py                       # OutputWriter interface (Req 4.5, 5.3)
      sheets.py                      # Google Sheets (primary destination)
      files.py                        # CSV/Excel (secondary, Req 4.5.2)
    metadata/
      run_metadata.py                 # timestamp + config hash + model version (Req 4.6)
  examples/
    model3_universe_tabs.yaml           # Appendix A reference job, config-driven
    counterfactual_example.yaml          # counterfactual + cross-column example
  tests/
    unit/
    fixtures/
```

Each module maps to one requirements section so a reviewer can trace
"where does the doc's requirement X get satisfied" directly to a file.

## Data flow

1. `config/loader.py` reads YAML/JSON, validates it into a `JobConfig`
   (`config/schema.py`). Bad configs fail here with a specific error
   (unknown source, dangling cross-column reference, missing output
   target) — satisfies Req 5.2's "clear, actionable error messages."
2. `query/builder.py` turns the `JobConfig` into one SQL string: a `base`
   CTE (source + joins) and one `SELECT ... GROUP BY 1, 2` per grouping
   variable (plus Topline), `UNION ALL`'d and ordered — a direct, literal
   generalization of Appendix A's reference SQL. All aggregation work
   happens in this SQL, executed by Redshift (Req 5.1: push group-by/join
   to the database, not pandas).
3. `sources/redshift.py` executes that SQL and returns the small,
   already-aggregated result as a DataFrame — never the full base table.
4. `compute/cross_column.py` applies any configured difference/
   multiply/custom operations to that small result.
5. `output/{sheets,files}.py` writes it to Google Sheets (default) or
   CSV/Excel.
6. `metadata/run_metadata.py` stamps the run with a timestamp, a hash of
   the config file, and the model version, and — if `run_metadata.capture`
   is on — writes that metadata as JSON to `run_metadata.artifacts_dir`
   (default `runs/`). If `run_metadata.save_sql` is also on (the default),
   the exact SQL string executed in step 2 is written alongside it as a
   `.sql` file. Saving the literal executed SQL, not just a config hash,
   means a run stays reproducible even if `query/builder.py`'s
   SQL-generation logic changes in a later version of the tool — Req 4.6.

## Config schema summary

Top-level `JobConfig` fields (see `config/schema.py` for the full pydantic
definitions and validation rules):

| Field | Purpose |
|---|---|
| `job` | name, `model_version`, free-text notes — identifies the run (Req 4.6) |
| `sources` | named Redshift tables/queries |
| `base` | which source is the base, and its joins (`key` + join type) — Req 4.1. Field is `key`, not `on`: PyYAML parses a bare `on:` key as boolean `True` (YAML 1.1 legacy), which silently breaks the config. |
| `scores` / `counterfactuals` | numeric or categorical columns to summarize — Req 2.1, 4.2 |
| `grouping_variables` | numbered `label` (controls row order) + `column`; topline is automatic |
| `aggregations.default` | mean/count/frequency/sum/min/max/median; per-column override; `custom_functions` for dotted-path callables — Req 4.3 |
| `cross_column` | difference/multiply/custom (+ reserved `ttest`/`chi_square` for later) — Req 4.4 |
| `output` | destination (`google_sheets`/`csv`/`excel`), target, `layout: long\|wide` — Req 4.5 |
| `run_metadata` | opt in/out of metadata capture (`capture`), whether to save the exact executed SQL (`save_sql`), where run artifacts land (`artifacts_dir`, default `"runs"`), free-text notes — Req 4.6 |

Validation (`JobConfig._names_resolve`) catches dangling references before
any SQL is generated: an unknown `base.from`, a join to an undefined
source, a score/counterfactual pointing at a source that doesn't exist, or
a `cross_column` referencing a column name that isn't a defined
score/counterfactual.

## What we kept from the existing repos

Per the recommendation to build fresh: two small, specific patterns were
worth carrying forward rather than reinventing —

- **open-dslib's `EngineContext`** connection pattern (env-var-prefixed
  Redshift credentials over a SQLAlchemy engine) — reused directly in
  `sources/redshift.py`.
- **open-dslib's union-of-group-bys + topline SQL shape** — the
  `CrossTabs`/`Tab` classes already do roughly what `query/builder.py`
  does here; this sketch is a cleaner, config-schema-driven rewrite of
  the same idea, generalized to support counterfactuals and custom
  aggregations that the original doesn't have.

Everything else (Sheets output, join-key abstraction beyond raw SQL,
pluggable aggregation/cross-column registry, run metadata, the pydantic
config schema itself) is new — neither repo had a version of it to reuse,
per the earlier analysis.

## Open design questions carried over from Section 6

These are intentionally left as stubs/extension points rather than
resolved, matching the requirements doc's own "Open Design Questions":

- **Row-level custom-function engine** (DuckDB vs. Polars vs. other): not
  chosen. `query/registry.py` resolves any dotted-path callable, and
  `pyproject.toml` has a `[project.optional-dependencies].vectorized`
  extra (duckdb, polars) ready to add once a real custom function needs
  it — no core code changes required either way.
- **Config vs. Python API boundary**: sketched as "config covers
  everything expressible declaratively; `custom_aggregations` /
  `cross_column.function` dotted paths are the escape hatch into Python."
  Where exactly that line sits in practice is still open, per the doc.
- **Run metadata field list**: `RunMetadata` currently captures
  `job_name`, `model_version`, `run_timestamp`, `config_hash`, `notes` —
  a minimal set; extend `metadata/run_metadata.py` if more is needed.

## Explicitly not built yet (matches v1 non-goals)

Other data sources (Postgres, Sheets-as-input, CSV/Excel-as-input, S3),
statistical significance testing (`ttest`/`chi_square` raise
`NotImplementedError` today), plotting, and scheduling are all reserved as
enum values or interface seams but have no implementation — consistent
with Section 2.2 and Section 7 of the requirements doc.
