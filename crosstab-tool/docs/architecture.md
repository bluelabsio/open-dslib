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
    cli.py                    # `crosstab sql|validate|run job.yaml`
    config/
      schema.py                # pydantic models — the config contract (Req 4.2)
      loader.py                 # YAML/JSON -> validated JobConfig
    sources/
      base.py                   # DataSource interface (Req 4.1, 5.3)
      redshift.py                # v1's only backend
      # future: postgres.py, sheets.py, csv_excel.py, s3.py — same interface
    query/
      builder.py                 # JobConfig -> Appendix-A-style SQL (base CTE + UNION ALL)
      identifiers.py               # check_identifier/quote_literal — SQL-injection-shaped safety for generated SQL
      aggregations.py             # AggFunction -> SQL fragment (Req 4.3)
      registry.py                  # dotted-path custom function resolution (Req 4.2/4.3/4.4)
    compute/
      cross_column.py              # add/difference/multiply/divide/custom over the aggregated result (Req 4.4)
    output/
      base.py                       # Writer interface (Req 4.5, 5.3)
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
   to the database, not pandas). Every config value it interpolates into
   that SQL (table names, join keys, column names, aliases) is checked
   against `query/identifiers.py`'s `check_identifier` first; free-text
   labels (category/grouping-variable names) go through `quote_literal`
   instead, since they're values, not identifiers — see "Identifier and
   literal safety" below.
3. `sources/redshift.py` executes that SQL and returns the small,
   already-aggregated result as a DataFrame — never the full base table.
4. `compute/cross_column.py` applies any configured difference/
   add/multiply/divide/custom operations to that small result.
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
| `cross_column` | add/difference/multiply/divide/custom (+ reserved `ttest`/`chi_square` for later) — Req 4.4 |
| `output` | destination (`google_sheets`/`csv`/`excel`), target, `layout: long\|wide` — Req 4.5 |
| `run_metadata` | opt in/out of metadata capture (`capture`), whether to save the exact executed SQL (`save_sql`), where run artifacts land (`artifacts_dir`, default `"runs"`), free-text notes — Req 4.6 |

Validation (`JobConfig._names_resolve`) catches dangling references before
any SQL is generated: an unknown `base.from`, a join to an undefined
source, a score/counterfactual pointing at a source that doesn't exist, or
a `cross_column` referencing a column name that isn't a defined
score/counterfactual.

## Identifier and literal safety

`query/builder.py` interpolates config-supplied values directly into
generated SQL — there's no parameterized-query placeholder for "the name
of a column to group by." Since config files don't necessarily go through
the same review as code, this is a SQL-injection-shaped risk, not just a
correctness one, and it's handled explicitly rather than left implicit:

- **Identifiers** (table names, join keys/aliases, score/counterfactual
  columns and names, grouping-variable columns) go through
  `query/identifiers.py`'s `check_identifier`, which validates against a
  strict allow-list pattern (letters/digits/underscores, optionally
  schema-qualified) and raises `SQLGenerationError` — not a best-effort
  escape — if a value doesn't look like a real identifier.
- **Free-text values** (category and grouping-variable labels, which
  appear as SQL string literals rather than identifiers) go through
  `quote_literal`, which escapes embedded single quotes the standard SQL
  way. These are deliberately *not* run through `check_identifier`, since
  labels like `"Voter's Age"` are legitimate text, not malformed
  identifiers.
- `DataSourceConfig.query` (the raw-SQL escape hatch, used instead of
  `table`) is intentionally **not** identifier-checked — it's trusted,
  hand-written SQL wrapped as a derived table, not an identifier.

`cli.py`'s `validate` command surfaces `SQLGenerationError` as a clean
CLI error rather than a traceback.

## Validating a config without a live Redshift connection

`crosstab validate <config.yaml>` does three things, in order, none of
which touch Redshift:

1. Schema-validates the config (`config/loader.py`).
2. Builds the SQL (`query/builder.py`), which is also where identifier
   safety (above) gets exercised.
3. Confirms every `cross_column.inputs` entry resolves against the column
   names that SQL would actually produce — using
   `compute/cross_column.py`'s `resolve_column_name`, the same
   `mean_<name>`-fallback logic the runtime path (`apply_cross_column`)
   uses. This closes a real gap: a config can reference a valid
   score/counterfactual `name` that schema validation happily accepts,
   but that doesn't resolve to an actual output column (e.g. because that
   score's `aggregations` don't include `mean` and the `cross_column`
   entry references it by bare name expecting the `mean_` fallback) —
   previously this only surfaced as a `KeyError` at run time, against a
   live connection.

This is the command any external tooling (e.g. a config-generation
assistant) should call into rather than re-implementing schema or
resolution knowledge separately, since it stays in sync with
`config/schema.py` by construction.

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

**Added later, during cross-branch unification** (this repo also has
sibling branches, `crosstab-refactor-crm` and `crosstab-tw`, that
independently rebuilt the same tool — see the unification plan doc for
the full comparison): `query/identifiers.py`'s `check_identifier`/
`quote_literal` pair is adapted from `crosstab-tw`'s `sqlgen.py`
(`_check_identifier`/`_quote_literal`), which was the one piece worth
carrying forward from that branch specifically — this codebase's own SQL
generation had the same unvalidated-interpolation gap crosstab-tw's did.

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
- **Writer dispatch**: `cli.py` currently picks `FileWriter` vs.
  `GoogleSheetsWriter` with a plain `if`/`else` on
  `output.destination`. Decided (unification pass) to leave this as-is
  for now and revisit folding it into `query/registry.py`'s dotted-path
  pattern the next time a new writer or source type is actually added,
  rather than migrating it as its own isolated piece of work.

## Explicitly not built yet (matches v1 non-goals)

Other data sources (Postgres, Sheets-as-input, CSV/Excel-as-input, S3),
statistical significance testing (`ttest`/`chi_square` raise
`NotImplementedError` today), plotting, and scheduling are all reserved as
enum values or interface seams but have no implementation — consistent
with Section 2.2 and Section 7 of the requirements doc.

`crosstab-refactor-crm` (a sibling branch) separately built cube/
auto-combination groupbys and a counterfactual/comparison engine on top
of a different, Polars-based architecture. Decided (unification pass,
see the unification plan doc) not to port either: this repo's existing
`grouping_variables` + `cross_column` model covers the same ground and
stays the one going forward. Multi-source support (CSV/Parquet/DataFrame,
which CRM's branch also has) was similarly decided out of scope for now —
Redshift-only stands, consistent with this section's original v1
non-goals.
