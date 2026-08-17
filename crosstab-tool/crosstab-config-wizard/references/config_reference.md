# crosstab-tool config schema reference

Source of truth is `crosstab_tool/config/schema.py` in the crosstab-tool
repo (pydantic models). This file is a human-readable summary of that
schema plus the YAML shape, kept in sync with it — if the two ever
disagree, trust `schema.py` and update this file.

## Top-level shape

```yaml
job:
  name: <string>                # identifier for this job
  model_version: <string>       # e.g. "p_support_v3_20260416" — what score this run is against
  notes: <string, optional>

sources:
  - name: <string>               # local name you reference elsewhere in this file
    type: redshift                # only "redshift" is implemented today
    connection: <string>           # env-var prefix, e.g. "REDSHIFT_MAIN" -> reads
                                     # REDSHIFT_MAIN_{USER,PW,HOST,DB,PORT}
    table: <schema.table>           # exactly one of `table` or `query`
    # query: <raw SQL>                # escape hatch instead of `table`

base:
  from: <source name>            # which of the `sources` above is the base — default to the
                                   # demographics/basetable source, not the scores table (see note below)
  joins:
    - source: <source name>        # another defined source to join in
      key: <column name>             # join key(s) — a string, or a list for a composite key
      how: left                       # left | inner | right | full (default: left)

scores:
  - name: <string>                # output name, e.g. "p_support"
    source: <source name>            # which source this column lives in
    column: <string>                  # actual column name in that source
    kind: numeric                      # numeric | categorical (default: numeric)
    aggregations: [mean, count]          # optional — overrides the job-level default for this column
    custom_aggregations: []               # optional — dotted paths, e.g. "myproj.aggs:trimmed_mean"

counterfactuals: []               # same shape as `scores` — comparison columns (prior model, benchmark, etc.)

include_topline: true             # job-level default: add an automatic "Topline" (whole-population) row

grouping_variables:
  - label: "01 Age"                # numbered category label — controls output ROW ORDER
    column: <string>                 # actual column name defining the group levels
    source: <source name>            # optional — which source the column lives in; omit for the base source
    include_topline: null              # optional override of the job-level default; usually leave unset

aggregations:
  default: [mean, count]           # applied to every score/counterfactual unless overridden per-column
  custom_functions: {}              # name -> "module.path:function" dotted-path registry

cross_column: []
  # - name: <string>
  #   op: difference               # difference | multiply | custom | (reserved: ttest, chi_square)
  #   inputs: [<score/counterfactual name>, <score/counterfactual name>]
  #   function: <dotted path>        # required only when op == custom

output:
  destination: google_sheets       # google_sheets | csv | excel
  spreadsheet_id: <string>          # required if destination == google_sheets
  tab: <string>                      # required if destination == google_sheets
  layout: long                       # long (tidy, default) | wide (pivoted)
  path: <string>                     # required if destination == csv or excel

run_metadata:
  capture: true                    # whether to write any run metadata at all
  save_sql: true                    # also save the exact executed SQL (recommended — see below)
  artifacts_dir: "runs"              # where metadata JSON (+ saved SQL) is written;
                                       # relative to the cwd `crosstab` runs from —
                                       # absolute paths are allowed (see note below)
  notes: <string, optional>
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
  `scores` or `counterfactuals` — not column names, not source names.
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
  suggest they `cd` there and run `crosstab` instead, because `cli.py`'s
  bare `load_dotenv()` searches upward from the cwd and won't find the
  repo's `.env` (and so won't find the Redshift credentials). Note
  `output.path` is unused when `destination: google_sheets`.
- **Placeholder/unconfirmed columns**: if a column name is a guess, mark it
  clearly with a trailing YAML comment like `# placeholder — confirm
  against <table>`, matching the convention in
  `examples/model3_universe_tabs.yaml`. Don't leave a guess unmarked.

## Worked example

See `examples/model3_universe_tabs.yaml` in the crosstab-tool repo for a
full, validated example (a config-driven reproduction of the team's
existing hand-written "Model 3 Universe Tabs" SQL), and
`examples/counterfactual_example.yaml` for one that uses a counterfactual
and a cross-column difference.
