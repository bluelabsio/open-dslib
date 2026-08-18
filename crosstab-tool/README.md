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
below) — either way, the result is the same kind of YAML file.

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
  key), with the Sheets and Drive APIs enabled on its GCP project. The
  target spreadsheet must already exist and already be shared with the
  service account's email as an Editor — this branch's Sheets writer opens
  an existing spreadsheet by ID; it doesn't create or auto-share one for
  you.

  **Service Account Setup/Information**

  A [service account](https://docs.cloud.google.com/iam/docs/service-account-overview) is a special Google account
  operated by a computer or application and attached to a Google Cloud project.
  In this case, we use the gspread Python package to access the Google API as the service
  account.

  First, enable the Google Drive and Google Sheets API within your project. Navigate from the sidebar to
  APIs & Services -> Library and search for the Drive and Sheets APIs. They can also be found and activated at

  https://console.cloud.google.com/apis/library/drive.googleapis.com?project=your-project-name
  https://console.cloud.google.com/apis/library/sheets.googleapis.com?project=your-project-name

  In order to set up a Google service account, you must first [create a project
  on Google Cloud](https://console.cloud.google.com/welcome) to link the service account to. Even though the sheet you create won't
  be a part of this project, service accounts must be created in this way. Then, navigate to
  IAM & Admin -> Service Accounts from the sidebar. This should be found at
  https://console.cloud.google.com/iam-admin/serviceaccounts?project=your-project-name

  When you are [creating the service account,](https://docs.cloud.google.com/iam/docs/service-accounts-create)
  it will record an email based on the name you give it and the project ID of your project. **This is the
  email that you will share on any sheet (or any folder containing sheets) that you would like it to make edits
  on.** Your service account won't need any extra permissions, just the project API access you
  configured earlier.

  Then, when you have created your service account, go to Keys -> Add Key -> Create new key 
  -> JSON. This is the file that you will set a path to for
  ```GOOGLE_APPLICATION_CREDENTIALS``` in your environment file. Please keep that
  JSON file somewhere safe and don't share it.

  When you upload a crosstab job to Google Sheets, you will need the Sheets ID of something
  already shared to your service account, and the name of the tab the script
  should write your output to.
  

You do not need to run `source .env` — `crosstab` loads it
automatically (via `python-dotenv`) the moment you run any subcommand, as
long as `.env` is in your current directory. `.env` is already
`.gitignore`d; never commit real credentials.

## 2. Set up the Claude skill

When you pull the open-dslib repository, the Claude skill is contained within
the crosstab-config-wizard folder. However, when Claude reads a skill, it reads
from your personal .claude/skills folder. In order to redirect Claude to this
repository, regardless of what working directory you are using, use a symlink:

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

## 3. Get a job config

A job config is one YAML file describing the tables, scores, groupings,
and output for a single crosstab run. Two ways to get one:

### Option A — the config wizard skill (recommended if you're not sure of the schema)

Invoke the `crosstab-config-wizard` skill and it'll interview you, then
write and validate a config for you. Make sure to run the skill 
with Claude Code or Cowork, so that it can locate the open-dslib 
repo on your computer. However, you don't have to change your 
working directory. It asks about these in order (skipping
whatever obviously doesn't apply to your job):

1. **Job identity** — what's this job/report called, and what model or
   score version is it for?
2. **Data** — which Redshift table has the score(s)? Is there a separate
   table with the demographic/grouping columns, and if so, what's the join
   key and join type (left join, etc.)?
3. **Scores** — which score column(s) do you want summarized, with a
   human-readable name for each?
4. **Counterfactuals** *(optional)* — anything to compare the score
   against (a prior model version, a benchmark, a labeled category)? Numeric
   or categorical?
5. **Grouping variables** — what breakdowns do you want (age, party,
   region, etc.), and in what order? Order controls the numbered category
   labels in the output. Also whether you want the automatic "Topline"
   (whole-population) row — default yes.
6. **Aggregations** — mean + count is the default; only comes up if you
   want something else (median, sum, frequency, a custom function).
7. **Cross-column computation** *(optional)* — a computed difference or
   ratio between two of the scores/counterfactuals.
8. **Output** — Google Sheets (default, needs a spreadsheet ID + tab name)
   or CSV/Excel (needs a file path). Long (tidy) layout is the default
   shape.
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
especially for the grouping-variables list. Alternatively, you can
build off of config_reference.yaml, which is the file that the Claude skill 
reads to bulid a config. Note: some example configs still have placeholder column
names (flagged in comments) that haven't been confirmed against the real
table schema yet.

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
  already exist and be shared with the service account, per step 1).
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
