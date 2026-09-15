# crosstab-tool

A config-driven tool for **universe crosstabbing**: it takes a model score
(and optionally a counterfactual comparison score) in Redshift, breaks it
out by whatever demographic/behavioral grouping variables you choose, and
writes the resulting crosstab report to Google Sheets, Excel, or CSV.

The whole job — which tables, which columns, which breakdowns, where the
output goes — is described in one YAML config file. Nothing about a
specific job is hard-coded in the tool itself. Under the hood:

- **SQL does the heavy lifting.** The config compiles to a single SQL query
  (one join, one `SELECT ... GROUP BY` per grouping variable, `UNION ALL`'d
  together) that runs entirely in Redshift — the tool never pulls raw,
  row-level data into Python.
- **Python handles the small stuff afterward**: any cross-column
  computation (e.g. the difference between two scores) on the already-
  aggregated result, and writing that result out to Sheets/Excel/CSV.

You can write a job config by hand, or have the `crosstab-config-wizard`
Claude Code skill interview you and generate + validate one for you (see
below) — either way, the result is the same kind of YAML file. The skill
is especially worth reaching for over hand-writing a config once a job
gets more involved than a single score against a few breakdowns —
particularly if you need a counterfactual comparison or any computation
between columns (a difference, a ratio, a custom function), since those
parts of the schema have real, non-obvious requirements (see "Writing a
custom cross-column function" in `crosstab-config-wizard/SKILL.md`) that
are easy to get wrong writing YAML by hand.

## Repository layout

```
crosstab-tool/
├── crosstab_tool/              # the installed Python package
├── crosstab-config-wizard/     # the Claude Code skill (see "Set up the Claude skill" below)
├── custom_functions/           # shared, git-tracked custom cross_column functions
├── examples/                   # reference/teaching configs, safe to copy from
├── configs/                    # your own job configs go here (gitignored except .gitkeep)
├── docs/                       # architecture.md
└── tests/
```

- **`crosstab_tool/`** — the installed Python package; everything the
  `crosstab` CLI actually runs.
  - `cli.py` — the `crosstab` command group (`validate` / `sql` / `run`);
    also where `.env` discovery happens.
  - `config/` — `schema.py` (the pydantic models that define what a valid
    job config looks like) and `loader.py` (YAML/JSON -> validated
    `JobConfig`).
  - `query/` — turns a validated config into SQL: `builder.py` (the main
    `config -> SQL` translation), `aggregations.py` (the built-in
    `mean`/`count`/`sum`/etc. SQL fragments), `identifiers.py`
    (SQL-injection-safe identifier/literal checking), `registry.py`
    (resolves the dotted-path custom cross-column functions).
  - `compute/` — `cross_column.py`, the one step that runs in Python
    rather than SQL: computing an `add`/`difference`/`multiply`/`divide`/custom
    value across already-aggregated columns.
  - `sources/` — `DataSource` implementations; `redshift.py` is the only
    one implemented today.
  - `output/` — `Writer` implementations: `files.py` (CSV/Excel),
    `sheets.py` (Google Sheets), `base.py` (shared shaping logic,
    including the `long`/`wide` layout switch).
  - `metadata/` — `run_metadata.py`, which writes each run's JSON
    metadata (and, optionally, the exact SQL executed) to `artifacts_dir`.
- **`crosstab-config-wizard/`** — the Claude Code skill itself, kept in
  this repo and symlinked into `~/.claude/skills` (see "Set up the Claude
  skill" below) so it stays in sync with the tool it configures.
  - `SKILL.md` — the actual interview script Claude follows: what order
    to ask things in, field-level gotchas, and how to write/validate a
    custom cross-column function.
  - `references/` — `config_reference.md` (full field-by-field schema
    reference in prose) and `common_grouping_variables.md` (BlueLabs's
    common demographic column names by modeling frame).
  - `scripts/` — `validate_config.py`, the same validation logic the
    skill runs after writing a config; usable by hand too.
- **`custom_functions/`** — a shared, git-tracked package of custom
  `cross_column` functions (for computations beyond the built-in
  `add`/`difference`/`multiply`/`divide`, e.g. a percent-change). `cli.py` puts
  the repo root on `sys.path` automatically, so anything here is
  importable as `custom_functions.<module>:<function>` with no
  `PYTHONPATH` needed.
  Check here for an existing function before writing a new one — that's
  the point of this being shared rather than scattered across individual
  projects' own directories.
- **`examples/`** — YAML configs meant to be read and copied from, not
  necessarily run as-is; some use fictional table names purely to
  demonstrate a schema feature (see each file's header comment for
  which). `config_reference.yaml` is a field-by-field annotated template
  of the whole schema.
- **`configs/`** — where your own real job configs go; gitignored except
  for a `.gitkeep` placeholder, so this is meant as local scratch space,
  not a shared/committed catalog of jobs.
- **`docs/`** — `architecture.md` (design rationale).
- **`tests/`** — the test suite; `unit/` covers schema validation, SQL
  generation, cross-column resolution, and the CLI's `validate` command.

## (Optional) Set up a Google Service Account

Only needed if a job's output goes to Google Sheets — skip this if you're
only using CSV/Excel.

A [service account](https://docs.cloud.google.com/iam/docs/service-account-overview)
is a special Google account operated by a computer or application and
attached to a Google Cloud project; this tool uses the `gspread` Python
package to access the Google API as that service account. Steps, in the
order you actually need to do them:

1. [Create a project on Google Cloud](https://console.cloud.google.com/welcome)
   to link the service account to. Even though the sheet you create won't
   be part of this project, service accounts must be created this way.
2. Enable the Google Drive and Sheets APIs **on that project**: from the
   sidebar, APIs & Services -> Library, search for each, and enable it —
   or go directly to
   `https://console.cloud.google.com/apis/library/drive.googleapis.com?project=your-project-name`
   and
   `https://console.cloud.google.com/apis/library/sheets.googleapis.com?project=your-project-name`.
3. [Create the service account](https://docs.cloud.google.com/iam/docs/service-accounts-create)
   under IAM & Admin -> Service Accounts
   (`https://console.cloud.google.com/iam-admin/serviceaccounts?project=your-project-name`).
   It won't need any extra IAM permissions beyond the project API access
   enabled in step 2. Creating it generates an email address based on the
   name you gave it and your project ID — **this is the email you'll
   share any sheet (or folder of sheets) with**, as an Editor, before a
   job can write to it.
4. Generate a key for it: Keys -> Add Key -> Create new key -> JSON. Set
   `GOOGLE_APPLICATION_CREDENTIALS` in your `.env` to that file's path
   (see "Set up your environment" below). Keep the JSON file somewhere
   safe and don't share or commit it.

When you actually run a job with `destination: google_sheets`, you'll
need the target spreadsheet's Sheets ID (shared with the service account
per step 3) and the name of the tab to write to — those go in the job
config's `output.spreadsheet_id` / `output.tab`, not in `.env`.

## 1. Set up your environment

```bash
cd crosstab-tool
pip install -e ".[dev]"
cp .env.example .env
```

Then fill in `.env`:

- **Redshift credentials**, prefixed to match whatever `connection:` name
  your job config uses (`REDSHIFT_MAIN` in the example configs):
  ```
  REDSHIFT_MAIN_HOST=...
  REDSHIFT_MAIN_DB=...
  REDSHIFT_MAIN_USER=...
  REDSHIFT_MAIN_PW=...
  REDSHIFT_MAIN_PORT=5439        # optional, defaults to 5439
  ```
- **Google auth**, only needed if a job's output goes to Sheets:
  ```
  GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service-account.json
  ```
  This must be a service account (not a personal login or a plain API
  key), with the Sheets and Drive APIs enabled on its GCP project, and the
  target spreadsheet must already exist and already be shared with the
  service account's email as an Editor — this branch's Sheets writer opens
  an existing spreadsheet by ID; it doesn't create or auto-share one for
  you. If you don't already have a service account set up, see
  "(Optional) Set up a Google Service Account" above before filling this
  in — skip it entirely if you're only using CSV/Excel output.

You do not need to run `source .env` — `crosstab` loads it automatically
(via `python-dotenv`) the moment you run any subcommand. It searches your
current directory and its parent directories for `.env`, then falls back
to `~/.env` if none is found — so `.env` doesn't have to sit in whatever
directory you happen to run `crosstab` from, though keeping it at the
`crosstab-tool` repo root (as set up above) is still the simplest choice.
`.env` is already `.gitignore`d; never commit real credentials.

## 2. Set up the Claude skill

This step requires [Claude Code](https://docs.claude.com/en/docs/claude-code/overview)
set up in your terminal already — install and authenticate it first if
you haven't (see Anthropic's docs linked above), since the skill only
exists as something Claude Code can read.

When you pull the open-dslib repository, the Claude skill is contained within
the crosstab-config-wizard folder. However, when Claude reads a skill, it reads
from your personal .claude/skills folder. In order to redirect Claude to this
repository, regardless of what working directory you are using, use a symlink.

First, make sure the skills directory exists (it may not, on a fresh
Claude Code install):

```bash
mkdir -p ~/.claude/skills
```

Then symlink the skill in:

```
ln -s ~/open-dslib/crosstab-tool/crosstab-config-wizard ~/.claude/skills/crosstab-config-wizard 
```

If for some reason you already have access to the crosstab-config-wizard skill externally
and it is already in your .claude/skills folder, you can rename it to preserve that version
or delete it and rerun the symlink. Otherwise, a symlink will be created at .claude/skills/crosstab-config-wizard/crosstab-config-wizard.

```
rm -rf ~/.claude/skills/crosstab-config-wizard # to delete the existing folder
```

Then, when you pull from GitHub, any changes to the skill will also be reflected in your
personal .claude folder, and you can push changes as normal.

**To invoke it**, start Claude Code (`claude`, from anywhere — you don't
need to be in the `open-dslib` checkout) and either type `/crosstab-config-wizard`,
or just describe what you want in your own words (e.g. "set up a crosstab
job for the X model") — the skill's description is written to match
naturally on requests like that, so naming it explicitly isn't required.

## 3. Get a job config

A job config is one YAML file describing the tables, scores, groupings,
and output for a single crosstab run. Two ways to get one:

### Option A — the config wizard skill (recommended if you haven't written a crosstab config by hand before)

Invoke the `crosstab-config-wizard` skill and it'll interview you, then
write and validate a config for you. Make sure to run the skill 
with Claude Code or Cowork, so that it can locate the open-dslib 
repo on your computer. However, you don't have to change your 
working directory. It asks about these in order (skipping
whatever obviously doesn't apply to your job):

1. **Job identity** — what's this job/report called?
2. **Data** — which Redshift table has the score(s), and what model or
   score version is it? Is there a separate table with the
   demographic/grouping columns, and if so, what's the join key and join
   type (left join, etc.)? Also whether any table needs a **filter**
   first (e.g. the modeling frame commonly needs `deceased = 0`, or a job
   might need a geographic/registration-status restriction) — the skill
   handles this via a raw-SQL `query` source rather than a plain table
   reference.
3. **Scores** — which score column(s) do you want summarized, with a
   human-readable name for each?
4. **Counterfactuals** *(optional)* — anything to compare the score
   against (a prior model version, a benchmark, a labeled category)? Numeric
   or categorical?
5. **Grouping variables** — first, if the base table is BlueLabs's
   political, commercial, or L2-commercial modeling frame, the skill can
   offer that frame's common breakdown column names (age, ethnicity,
   education, income, urbanicity, region, party, and more) instead of
   making you type them out (see
   `crosstab-config-wizard/references/common_grouping_variables.md`).
   Then, what breakdowns do you want (age, party, region, etc.), and in
   what order? Order controls the numbered category labels in the
   output. Also whether you want the automatic "Topline"
   (whole-population) row — default yes.
6. **Aggregations** — mean is the default; only comes up if you want
   something else (median, sum, frequency). Note a row count is *always*
   included automatically — you only need to add `count` if you want a
   second, per-column count of non-null values, which most jobs don't
   need.
7. **Cross-column computation** *(optional)* — a computed sum,
   difference, ratio, or product between two of the scores/counterfactuals
   (`add` / `difference` / `multiply` / `divide` are all built in).
   Anything more involved — combining more than one operation, like a
   percent-change `(new - old) / old` — needs a custom Python function
   wired up via `op: custom` — see `crosstab-config-wizard/SKILL.md`'s
   "Writing a custom cross-column function" section for exactly how that
   has to work. Prefer this repo's shared `custom_functions/` package
   (it's on `sys.path` automatically, no `PYTHONPATH` needed, and is
   checked for an existing function before a new one gets written) over
   a private, project-specific function file.

   If a computation needs intermediate scores/counterfactuals that only
   exist to feed it (not something you want to actually read in the
   report), set `hidden: true` on those — they're still fully computed
   and usable as `cross_column` inputs, just left out of the written
   output.
8. **Output** — Google Sheets (default, needs a spreadsheet ID + tab name)
   or CSV/Excel (needs a file path). Long (tidy) layout is the default
   shape — note `wide` is currently a placeholder that produces identical
   output to `long`; no real pivot is implemented yet.
9. **Run metadata** — whether to capture run metadata and save the exact
   SQL executed; the defaults (yes to both, written to a `runs/` folder)
   are almost always fine.

The skill saves the resulting `<job_name>.yaml` wherever you ask — your
current working directory, this repo's `configs/` directory
(`open-dslib/crosstab-tool/configs/`), or both — and validates it against
the tool's real schema before handing it back, so you're not left guessing
whether it'll actually run.

### Option B — write it yourself

Copy an existing config close to what you want from `examples/` or
`configs/` and edit it — usually faster than starting from a blank file,
especially for the grouping-variables list. `examples/config_reference.yaml`
is a field-by-field annotated template of the whole schema; for full
prose explanations of the trickier fields (the ones the Claude skill
itself reads from), see `crosstab-config-wizard/references/config_reference.md`.
Note: some example configs still have placeholder column names (flagged
in comments) that haven't been confirmed against the real table schema
yet.

## 4. Validate before running

Check a config is well-formed and preview the SQL it would generate,
without touching Redshift:

```bash
crosstab validate configs/your_job.yaml   # schema + SQL + cross_column resolution check; prints an SQL preview
crosstab sql configs/your_job.yaml        # writes the generated SQL to <job_name>.sql (or -o <path>)
```

## 5. Run it

```bash
crosstab run configs/your_job.yaml
```

Do this from wherever you already have legitimate Redshift access set up
(your own machine, Positron, a shared analytics box) — not from an
environment with no route to your Redshift network. Additionally, you can
instruct Claude to run this after creating the config file, regardless of
your current working directory.

This connects to Redshift, runs the generated query, applies any
cross-column computation, and writes the result to whichever
`output.destination` the config specifies:

- **`google_sheets`** — writes into the configured spreadsheet/tab (must
  already exist and be shared with the service account, per the "(Optional)
  Set up a Google Service Account" section above).
- **`csv`** / **`excel`** — writes to the configured `path`; no Google
  setup needed for these.

If `run_metadata.capture` is on (the default), each run also writes a
metadata JSON file — and, if `save_sql` is on (also the default), the exact
SQL that was executed — to the job's `artifacts_dir` (default `runs/`,
created relative to wherever you run `crosstab` from). This is what makes
a report traceable back to what produced it later.

## No Python setup yet? Use DBeaver for the SQL part

You can still get value out of this tool before setting up a Python
environment:

1. Use the crosstab-config-wizard skill (or `crosstab validate`/`crosstab
   sql`, once you do have the tool installed) to get a validated config and
   its generated SQL.
2. Paste that SQL into a DBeaver SQL editor connected to Redshift and run
   it there.
3. Anything the tool would normally do in Python afterward — cross-column
   differences, writing to Sheets, run-metadata capture — becomes a manual
   step: export DBeaver's result grid and compute/paste it yourself.

This loses the automation but is a reasonable way to sanity-check a new
config against real data before investing in the full setup.
