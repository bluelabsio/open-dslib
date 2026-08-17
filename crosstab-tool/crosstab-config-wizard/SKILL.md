---
name: crosstab-config-wizard
description: >-
  Interview the user about their Redshift tables, model scores, counterfactual
  comparison columns, and demographic/behavioral grouping variables, then
  write and validate a job config YAML file for BlueLabs's crosstab-tool (the
  config-driven crosstab / counterfactual reporting tool that generates
  "universe tabs" style reports from Redshift model scores). Use this
  whenever the user wants to set up, create, build, configure, or generate a
  new crosstab job or "universe tabs" run, wants to crosstab a model score
  against demographics or a prior model version, mentions wiring up
  scores/counterfactuals/grouping variables for crosstab-tool, or describes
  their data (tables, join keys, columns) and wants it turned into a config
  — even if they never say the words "config," "YAML," or name the tool
  explicitly. Also use this to edit or extend an existing crosstab-tool job
  config (e.g. adding a grouping variable, a counterfactual, or a
  cross-column comparison) rather than hand-editing the YAML yourself.
---


# Crosstab config wizard

You're helping someone who knows their data (which Redshift tables, which
columns, what they want compared) but doesn't want to hand-write YAML against
crosstab-tool's config schema, or isn't sure exactly what the schema needs.
Your job is to have that conversation, then produce a config file that's
already been validated against the tool's actual schema — not just something
that looks plausible.

Read `references/config_reference.md` now if you haven't already in this
session — it has the full field-by-field schema, the exact YAML shape, and
known gotchas (like a YAML parsing trap that silently breaks configs). Don't
try to reconstruct the schema from memory; the reference is the source of
truth and the schema does change as the tool evolves.

## Before you start asking questions

Find the crosstab-tool repo (look for a directory containing
`crosstab_tool/config/schema.py`, usually named `crosstab-tool/`). If you
can't find it, ask the user where it lives rather than guessing — the
validation step later depends on it. Also check whether an existing config
close to what they want already exists in the repo's `configs/` or
`examples/` directories; starting from a close example and editing it is
often faster and less error-prone than building from a blank slate,
especially for the grouping-variables list.

## The interview

Ask about these in roughly this order. Don't front-load all of this as one
giant question — it's a lot to answer at once. Go section by section, and
skip sections that obviously don't apply (e.g. don't ask about
counterfactuals if the user makes clear this is a single-score job). Where
the user doesn't have a strong opinion, use the schema's defaults and say
so, rather than silently picking something.

1. **Job identity.** What's this job/report called, and what model or score
   version is it for? (`job.name`, `job.model_version`). A short note about
   purpose is nice to have but not essential.

2. **Data.** Which Redshift table holds the score(s)? Is there a separate
   table with the demographic/behavioral grouping columns, or is everything
   in one table? If separate, what's the join key (e.g. a person/voter ID),
   and is it a left join or something else? Don't assume `voterbase_id` or
   any other specific key — ask, even though it's common in BlueLabs work.

3. **Scores.** What score column(s) do they want summarized? Get a
   human-readable name and the actual column name for each.

4. **Counterfactuals (optional).** Is there anything they want to compare
   the score against — a prior model version, a benchmark score, or a
   labeled category? If yes, is it numeric or categorical?

5. **Grouping variables.** What breakdowns do they want (age, party,
   region, etc.)? For each, get the actual column name. Tell the user that
   grouping columns are assumed to live in the base table unless they say
   otherwise — they only need to name a table for columns that live in a
   joined source (the `source` field; omit it for base-table columns). These become the
   numbered categories in the output (01, 02, ...) in the order given —
   confirm the order matters to them, or just ask if they want a specific
   order vs. whatever's convenient. Ask whether they want the automatic
   "Topline" (whole-population) row — default yes.

6. **Aggregations.** Mean + count is the default and covers most cases;
   only dig deeper if they mention wanting something else (median, sum,
   frequency, or a custom function).

7. **Cross-column computation (optional).** Do they want a computed
   difference or ratio between any of the scores/counterfactuals?

8. **Output.** Google Sheets is the default destination — do they have a
   spreadsheet ID and tab name? If not, CSV/Excel to a file path is the
   fallback. Long (tidy) layout is the default shape.

   Then, in the same breath, ask **where the run artifacts should land** —
   one question covering both folders, since nobody wants them split:
   the output file (`output.path`, only applicable for CSV/Excel) and the
   run-metadata + saved-SQL folder (`run_metadata.artifacts_dir`). Both
   default to bare relative paths (`output/`, `runs/`), which means they
   land wherever `crosstab` is invoked from — in practice inside the
   crosstab-tool repo, which is usually not where the user wants their own
   work. Offer three choices, and suggest the second:

   - the crosstab-tool repo (leave the defaults `output/` and `runs/` as-is)
   - **their current working directory (suggest this)** — offer a concrete
     path built from the actual cwd rather than making them invent one,
     e.g. for a cwd of `/Users/toby/amazon`, propose
     `/Users/toby/amazon/crosstab_output/<job_name>.csv` and
     `/Users/toby/amazon/crosstab_runs/`
   - a path they specify themselves — always leave this option open, and
     if they give a bare relative path, resolve it against their cwd and
     write the absolute form

   **Write these as absolute paths in the config, and keep running
   `crosstab` from the repo directory.** Do not achieve this by telling the
   user to run `crosstab` from their own directory: `cli.py` calls
   `load_dotenv()` with no argument, which searches upward from the current
   directory, so invoking from elsewhere silently fails to find the repo's
   `.env` and the Redshift credentials with it. Absolute paths in the config
   are what make the destination independent of where the tool is invoked.
   Both writers call `mkdir(parents=True, exist_ok=True)`, so the
   directories don't need to exist beforehand.

   If the destination is Google Sheets, say plainly that `output.path` is
   unused for that destination — the only folder the run writes locally is
   `artifacts_dir` — so the question is just about where the metadata and
   saved SQL go.

9. **Run metadata.** Defaults (capture run metadata, save the executed SQL)
   are almost always fine — just confirm, don't belabor this section. Its
   `artifacts_dir` was already settled in step 8; don't ask about the
   location a second time here.

Throughout: if the user gives you a column name you're not confident
exists (e.g. they're guessing, or say "something like..."), write it in but
flag it clearly as unconfirmed in a YAML comment, the same way the existing
`examples/model3_universe_tabs.yaml` flags placeholder columns. Never
silently invent a column name to fill a gap — an unconfirmed guess that
looks confident is worse than an honest placeholder, because it fails
loudly instead of quietly at query time.

## Writing the config

Follow the exact structure in `references/config_reference.md` — field
names, nesting, and enum values matter for validation. Two things people
(and models) get wrong often enough to call out:

- The join-key field is `key`, not `on`. If you see `on:` in older
  drafts or in something the user pastes in, that's a bug, not a
  variant spelling — YAML parses a bare `on:` as the boolean `True`, so
  `on: voterbase_id` silently fails validation with a confusing "field
  required" error.
- Grouping variable order in the file is the output row order. If the user
  cares about output order (most people do, since it's what a stakeholder
  scrolls through), it needs to match what they actually asked for, not
  alphabetical or whatever order they happened to mention things in
  conversation.

Before writing the file, tell the user the default save location is
their current working directory, and that saving into the crosstab-tool
repo's `configs/` directory (e.g. `open-dslib/crosstab-tool/configs/`)
is also possible. Ask whether they want the `.yaml` saved in the
working directory, in the repo's `configs/`, or both. If the working
directory is chosen (alone or as part of "both"), ask where within it
the file should go — e.g. `./crosstab_configs/<job_name>.yaml` or just
`./<job_name>.yaml`. Save as `<job_name>.yaml` in the chosen
location(s), creating directories as needed. If both, the two files are
identical copies — validate against the repo copy.

## Do not modify the crosstab-tool repository

The crosstab-tool repository (e.g. `open-dslib/crosstab-tool`, or
wherever the tool lives on this machine) is read-only for this skill,
with one exception: saving the generated config YAML into its
`configs/` directory when the user has explicitly chosen that location
in the save prompt above. Everything else in the repo — the
`crosstab_tool/` package source, `examples/`, existing configs, tests,
anything — must not be created, edited, or deleted by this skill. In
particular: if you find what looks like a bug in the tool while
validating or tracing a config, report it to the user; do not patch it.
Any scratch scripts or fake DataFrames you build (e.g. to exercise
`compute/cross_column.py`) go in a temp/scratchpad directory, never
inside the repo.

If the user explicitly asks you to modify the crosstab-tool repo
(beyond saving a config to `configs/`), don't just do it — restate
exactly what would change and require an additional explicit
confirmation from them before making the edit.

## Validate before you consider it done

Run the bundled validator against the real schema — don't eyeball the YAML
and call it good, since a config that looks right can still fail validation
in ways that aren't obvious from reading it (wrong nesting, an enum typo, a
dangling reference to an undefined source):

```bash
python3 scripts/validate_config.py <path-to-crosstab-tool-repo> <path-to-config.yaml>
```

This prints either a clear validation error (fix and re-run) or a preview
of the generated SQL. Read that SQL preview yourself before telling the
user it's done — check that the joins, grouping columns, and aggregations
look like what they described. If the config has no crosstab-tool repo
findable to validate against, say so explicitly rather than silently
skipping validation; a config nobody has checked isn't finished.

**If the config has a `cross_column` entry, don't stop at "the schema
accepted it."** A first version of this skill did exactly that on a real
test case — it saw `cross_column.inputs` referencing valid score/
counterfactual names, confirmed the schema validator was happy, and told
the user it was fine. It wasn't: `query/builder.py` never emits a bare
column named after the score (e.g. `p_support`) — it emits one column per
aggregation (`mean_p_support`, `count_p_support`), and at the time
`compute/cross_column.py` looked up the bare name directly, so the job
would have raised a `KeyError` the moment it actually ran. (That specific
bug is now fixed — cross-column resolution falls back to `mean_<name>` —
but treat this as a reminder that config-schema validity and runtime
executability are two different questions, and this tool's boundary
between "generate SQL" and "compute in pandas afterward" is exactly the
kind of seam where they can silently diverge.) If a config has any
`cross_column` entries, trace them one level deeper: check that the
`inputs` names actually resolve against columns the generated SQL would
produce, and — if you're able to set up a small fake result DataFrame or
otherwise exercise `compute/cross_column.py` directly — confirm the
computation executes without error before calling the config done.

Once it validates, tell the user where the file is and show them the
grouping variables / scores / output destination as a quick summary so they
can catch anything that drifted during the back-and-forth — don't just
paste the whole YAML at them.
