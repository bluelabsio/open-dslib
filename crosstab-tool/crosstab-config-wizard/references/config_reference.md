# crosstab-tool config schema reference

Source of truth is `crosstab_tool/config/schema.py` in the crosstab-tool
repo (pydantic models). This file is a human-readable summary of that
schema plus the YAML shape, kept in sync with it — if the two ever
disagree, trust `schema.py` and update this file.

## Top-level shape

```yaml
job:
  name: <string>                # identifier for this job
  model_version: <string>       # e.g. "m3", "v2" — what score this run is against
  notes: <string, optional>     # written into the run's JSON metadata as `job_notes`,
                                 # distinct from run_metadata.notes below (written as `notes`)

connection: <string>          # env-var prefix, e.g. "REDSHIFT_MAIN" -> reads
                                # REDSHIFT_MAIN_{USER,PW,HOST,DB,PORT}. One job = one connection
                                # (see note below) — every source shares this one field.

sources:
  - name: <string>               # local name you reference elsewhere in this file
    type: redshift                # only "redshift" is implemented today
    table: <schema.table>           # exactly one of `table` or `query`
    # query: <raw SQL>                # escape hatch instead of `table` — see the worked
                                       # example below (examples/counterfactual_example.yaml)
                                       # for a full one; used for a filter, a derived/renamed
                                       # column, or anything else `table` can't express

base:
  from: <source name>            # which of the `sources` above is the base — default to the
                                   # demographics/basetable source, not the scores table (see note below)
  joins:
    - source: <source name>        # another defined source to join in
      key: <column name>             # join key(s) — a string, or a list for a composite key
      how: left                       # left | inner | right | full (default: left)

scores:
  - name: <string>                # output name, e.g. "p_support" — used as a SQL alias, so it
                                    # must be a valid identifier: letters/digits/underscores,
                                    # no spaces (e.g. "avg_score", not "Avg Score")
    source: <source name>            # which source this column lives in
    column: <string>                  # actual column name in that source
    kind: numeric                      # numeric | categorical (default: numeric) — NOT YET
                                        # IMPLEMENTED, see note below; setting it changes nothing
    aggregations: [mean, count]          # optional — overrides the job-level default for this column
    custom_aggregations: []               # NOT YET IMPLEMENTED — accepted but silently ignored,
                                           # see note below; don't rely on this field
    hidden: false                          # compute (and keep usable as a cross_column input) but
                                            # drop from the written output — for intermediate columns
                                            # not meant to be read directly. See
                                            # examples/custom_cross_column_example.yaml for a worked
                                            # example (its two counterfactuals are both hidden).

counterfactuals: []               # same shape as `scores` — comparison columns (prior model, benchmark, etc.)

include_topline: true             # job-level default: add an automatic "Topline" (whole-population) row

grouping_variables:
  - label: "01 Age"                # numbered category label — controls output ROW ORDER
    column: <string>                 # actual column name defining the group levels
    source: <source name>            # optional — which source the column lives in; omit for the base source

aggregations:
  default: [mean, count]           # applied to every score/counterfactual unless overridden per-column
                                    # — note a row COUNT(*) is *always* included automatically
                                    # regardless of this list; `count` here adds a second, per-column
                                    # count of non-null values, which most jobs don't need
  custom_functions: {}              # NOT YET IMPLEMENTED — accepted but silently ignored, see note below

cross_column: []
  # - name: <string>
  #   op: difference               # add | difference | multiply | divide | custom | (reserved: ttest, chi_square)
  #   inputs: [<score/counterfactual name>, <score/counterfactual name>]
  #   function: <dotted path>        # required only when op == custom — e.g. "custom_functions.ratios:percent_change"
                                      # for a function in this repo's shared custom_functions/ package (on
                                      # sys.path automatically, no PYTHONPATH needed), or "some_module:fn"
                                      # for a private one reached via PYTHONPATH. See the
                                      # crosstab-config-wizard skill's "Writing a custom cross-column
                                      # function" section for the full requirements (function signature,
                                      # what `inputs` must reference); this DOES work, unlike
                                      # custom_aggregations/custom_functions above

output:
  destination: google_sheets       # google_sheets | csv | excel
  spreadsheet_id: <string>          # required if destination == google_sheets
  tab: <string>                      # required if destination == google_sheets
  layout: long                       # long (tidy, default) | wide — NOTE: `wide` is currently a
                                      # placeholder (see note below) and produces identical output
                                      # to `long`; no real pivot is implemented yet
  path: <string>                     # required if destination == csv or excel

run_metadata:
  capture: true                    # whether to write any run metadata at all
  save_sql: true                    # also save the exact executed SQL (recommended — see below)
  artifacts_dir: "runs"              # where metadata JSON (+ saved SQL) is written;
                                       # relative to the cwd `crosstab` runs from —
                                       # absolute paths are allowed (see note below)
  notes: <string, optional>          # written into the run's JSON metadata as `notes`,
                                      # alongside job.notes above (written as `job_notes`)
```

## Field notes worth knowing before you interview someone

- **`grouping_variables[].label` controls row order**, not just naming. The
  numbered convention ("01 Age", "02 Female", ...) mirrors the existing
  hand-written SQL this tool replaces — it's not cosmetic, it's how
  stakeholders expect to scroll through the output. Ask the user for their
  intended order rather than assuming alphabetical or "however they
  mentioned it."
- **`base.from` should default to the demographics/basetable source.** The
  schema requires it explicitly, but when writing a config, make the
  basetable (the table holding the grouping/demographic columns) the base
  and left-join the score table(s) in — with left joins the base defines
  the population, and the intended population is usually the full
  basetable, not just scored records. Only put the score table as the base
  if the user explicitly wants the population restricted to scored people.
- **`base.joins[].key`, not `on`.** PyYAML's default loader parses a bare
  `on:` mapping key as the boolean `True` (a YAML 1.1 legacy quirk), which
  silently corrupts a config that reads correctly to a human. The schema
  uses `key` specifically to avoid this. If you ever see `on:` in a draft,
  it's wrong — fix it.
- **`grouping_variables[].source` defaults to the base table.** When unset,
  the column is read from the base source (`base.from`) — tell the user
  they don't need to provide a table for base-table grouping columns, only
  for ones living in a joined source. If set, it must name a defined source
  (an entry in `sources`) — the top-level validator rejects the config
  otherwise.
- **`counterfactuals` and `scores` are the same shape** — a counterfactual
  is just a comparison column analyzed with the same grouping logic, so
  don't treat it as a fundamentally different concept when interviewing.
- **`cross_column[].inputs` must reference `name`s already defined** in
  `scores` or `counterfactuals` — not column names, not source names. At
  run time, that bare `name` is resolved against the aggregated result
  columns with a `mean_` fallback only — there is **no fallback for
  `sum`, `median`, or any other aggregation**. A score/counterfactual
  using anything other than `mean` cannot be referenced from
  `cross_column` at all (fails clearly at `crosstab validate`, not
  silently). See the crosstab-config-wizard skill's "Writing a custom
  cross-column function" section for the workaround (using `mean`
  instead of `sum` on a ratio's numerator/denominator gives the same
  result, since the row count cancels).
- **`connection` is a single job-level field, not per-source.** At run
  time `cli.py` opens exactly one database engine — built from the job's
  `connection` — and runs the entire generated SQL (base plus every join)
  as one string over that single connection (`cli.py`'s `run` command).
  There's nowhere a per-source connection could actually be used, so
  every source in a job necessarily shares one Redshift cluster; this
  isn't a way to combine data from two different clusters/databases in
  one job.
- **`save_sql: true` (the default) is usually worth keeping.** It writes
  the literal SQL that ran, not just a hash of the config — stronger
  reproducibility evidence, since the config alone can't prove what SQL a
  given tool version actually generated.
- **`output.path` and `run_metadata.artifacts_dir` accept absolute paths**,
  and that's how you control where a run's files land. Both are written via
  `Path(...)` + `mkdir(parents=True, exist_ok=True)`
  (`output/files.py`, `metadata/run_metadata.py`), so a bare `runs` or
  `output/x.csv` resolves against whatever directory `crosstab` was invoked
  from — the repo, per the README — while an absolute path goes exactly
  where it says regardless of invocation directory. Prefer absolute paths
  when the user wants artifacts in their own working directory; do **not**
  suggest they `cd` there and run `crosstab` instead — `cli.py` now
  searches the cwd and its parents for `.env`, then falls back to
  `~/.env`, but the crosstab-tool repo's own `.env` (with the real
  Redshift credentials) is still the one that matters, and running from
  elsewhere risks picking up the wrong `.env` if the user happens to have
  one nearby. Note `output.path` is unused when `destination:
  google_sheets`.
- **A row `COUNT(*)` is always included automatically**, regardless of
  what's in `aggregations`. Adding `count` to `aggregations.default` (or
  a column's own `aggregations` override) adds a *second*, per-column
  count of non-null values for that score/counterfactual specifically —
  it does not affect whether the base row count appears. If a user just
  wants "how many records in each group," they don't need `count` in
  `aggregations` at all.
- **`kind: numeric | categorical` on a score/counterfactual is not yet
  implemented.** It's accepted and validated, but nothing in SQL
  generation reads it — the same `aggregations` list is applied
  regardless of `kind`. Don't tell a user it changes how a column is
  aggregated.
- **`custom_aggregations` (on a score/counterfactual) and
  `aggregations.custom_functions` (job-level) are not yet implemented.**
  Both are accepted and validated but never consumed anywhere — setting
  either one is silently a no-op. This is different from `cross_column`'s
  `op: custom` + `function`, which *does* work — see the
  crosstab-config-wizard skill's "Writing a custom cross-column function"
  section if a genuinely custom computation is needed.
- **`output.layout: wide` is currently a placeholder** — it does not pivot
  grouping-variable levels into columns the way "wide" usually implies in
  a crosstab. `output/base.py`'s `to_wide()` sets `category`/`level` as
  the index and immediately resets it, so `wide` output today has
  identical columns/rows to `long`. Tell the user this plainly rather
  than letting them assume a real pivot happened.
- **Placeholder/unconfirmed columns**: if a column name is a guess, mark it
  clearly with a trailing YAML comment like `# placeholder — confirm
  against <table>`. Don't leave a guess unmarked.

## Worked example

See `examples/model3_universe_tabs.yaml` in the crosstab-tool repo for a
full, validated example (a config-driven reproduction of the team's
existing hand-written "Model 3 Universe Tabs" SQL), and
`examples/counterfactual_example.yaml` for one that uses a counterfactual
and a built-in cross-column `difference`. For a `cross_column` that needs
`op: custom` (a computation combining more than one operation, like a
percent-change — a plain ratio should use the built-in `op: divide`
instead), see `examples/custom_cross_column_example.yaml` and its
accompanying `examples/custom_cross_column_functions.py` — together they
show the full working pattern, runnable as-is with
`PYTHONPATH=examples crosstab validate examples/custom_cross_column_example.yaml`.
