# crosstab-tool

## What this is for

Crosstab/counterfactual reporting for model scores: joins a score table to
one or more grouping-variable tables in Redshift, computes group-level
summary statistics via SQL push-down (nothing but the small aggregated
result ever leaves the database), and writes an analyst-readable report to
Google Sheets, Excel, or CSV. It replaces hand-written per-model "universe
tabs" SQL with a single declarative YAML config.

Every run is traceable: alongside the report, the tool writes a
`run_metadata` tab (or `.meta.json` sidecar for CSV) recording the
timestamp, a hash of the config used, the score version label, and the
exact SQL that was executed — so any report can be traced back to what
produced it.

## Setup

```bash
pip install -e .
```

This installs the `crosstab` command. If it's not on your `PATH` after
install, run it as a module instead: `python3 -m crosstab_tool.cli ...`.

### Required: Redshift connection

The tool reads Redshift credentials from environment variables, prefixed by
whatever `source.connection` names in your job config (default `REDSHIFT`).
Set these before running a job (e.g. in a `.env` file you `source`, or your
shell profile):

```
REDSHIFT_USER=...
REDSHIFT_PW=...
REDSHIFT_HOST=...
REDSHIFT_PORT=...
REDSHIFT_DB=...
```

A config can point at a different set of credentials by setting
`source.connection: SOME_OTHER_PREFIX`, which then reads
`SOME_OTHER_PREFIX_USER`, etc. — useful if you connect to more than one
Redshift cluster.

### Optional: Google Sheets output

Only needed if a job's `output.destination` is `sheets`. Authentication is
via a Google **service account** — not a personal Google login and not a
plain API key (a bare API key can't authorize write access to Sheets/Drive).

One-time setup, in Google Cloud Console:

1. In a GCP project (create one, or use an existing one),
   enable the **Google Sheets API** and **Google Drive API**
   (APIs & Services → Library).
2. Create a service account (IAM & Admin → Service Accounts → Create).
3. Generate a key for it: open the service account → Keys tab → Add Key →
   Create new key → JSON. This downloads a `.json` file — treat it like a
   password. Store it somewhere on your machine outside this repo (it must
   never be committed to git).

Per-job setup, once the key file exists:

4. In your `.env` file, set `GOOGLE_APPLICATION_CREDENTIALS` to the **file
   path** of that downloaded JSON key — not the key's contents, just where
   it lives on disk:

   ```
   GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service-account.json
   ```

   (Alternatively, set `output.credentials_file` to that same path directly
   in the job YAML instead of using the env var — useful if different jobs
   need different service accounts.)
5. To have the tool write into a spreadsheet that already exists (instead
   of creating a new one), share that spreadsheet manually with the service
   account's own email — found in the key file's `client_email` field,
   looks like `xxx@your-project.iam.gserviceaccount.com` — as an **Editor**,
   before running the job. You will need to update the sheet name in the .yaml file.

Neither of these need to be exported globally — a `.env` file in this
directory that you `source` (or a tool like `direnv`) works fine. `.env` is
already covered by `.gitignore`; never commit real credentials.

## Configuring a job

Every job is one YAML file. Copy the template and fill in the placeholders:

```bash
cp examples/template.yaml my_job.yaml
```

Key sections:

- **`source`** — the table with the score(s) (`base_table`), which
  Redshift connection to use (`connection`), and any tables to join in for
  grouping/counterfactual columns (`joins`, each needs a `table` and a
  shared `key`).
- **`scores`** — the numeric column(s) being reported on.
- **`counterfactuals`** *(optional)* — comparison columns, same shape as
  `scores`. A counterfactual can live on its own table — just add another
  entry under `source.joins` for it and reference its column name directly,
  the same as any grouping column below.
- **`groupings`** — the demographic/segment columns to break results out
  by. List order controls the numbered category labels in the output
  (`01 Age`, `02 Party`, ...). A `00 Topline` row (the whole population,
  ungrouped) is always included automatically.
- **`aggregations`** — which statistics to compute per score (`count`,
  `mean`, etc.), overridable per metric.
- **`cross_column`** *(optional)* — a computation across two already-
  aggregated result columns (e.g. the difference between two scores),
  computed in Python after the SQL query returns.
- **`output`** — where results go: `csv`, `excel`, or `sheets`, plus
  `layout: long` (default, one row per category/level) or `wide` (pivoted).

Any column that doesn't actually exist on the configured table(s) is
dropped automatically at run time (with a warning) rather than failing the
whole query — useful when a column gets renamed or dropped upstream. If the
same column name exists on more than one joined table, the base table's
version is used; if it exists on two *different* joined tables with no
version on the base table, it's dropped (there's no way to guess which one
was intended).

Validate a config before spending a query on it:

```bash
crosstab validate my_job.yaml   # checks the config, no DB connection needed
crosstab sql my_job.yaml        # prints the exact SQL that would run, no DB connection needed
```

## Running

```bash
crosstab run my_job.yaml
```

This connects to Redshift, runs the generated query, and writes the result
to whichever `output.destination` the config specifies. The command prints
the final location (a file path, or the spreadsheet URL) when it finishes.

## Output shape

The default (`layout: long`) output is a tidy table: one row per
grouping-variable level (plus the `00 Topline` row), one column per
statistic, ordered by category then level. `layout: wide` pivots this into
one column per category/level combination instead, useful for side-by-side
comparison in a spreadsheet.

## Python API

Everything in the YAML maps 1:1 to dataclasses; use them directly when a
job needs logic the config can't express:

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
multi-hundred-million-row basetable into a pandas DataFrame; push a partial
aggregation into Redshift first, or use a columnar engine (DuckDB/Polars) on
the streamed chunks.

## Development

```bash
pip install -e ".[dev]"
pytest
```
