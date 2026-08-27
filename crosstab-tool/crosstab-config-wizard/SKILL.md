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

1. **Job identity.** What's this job/report called? (`job.name`). A short
   note about purpose is nice to have but not essential.

2. **Data.** Ask which Redshift table holds the score(s) *first* — don't
   ask for `job.model_version` in the same breath, or before this. Only
   once the user has named that table, ask what model or score version it
   is (`job.model_version`) — now it's grounded in something concrete
   instead of an abstract "what version" question. These are two
   sequential questions, not one compound one, even though they're both
   part of this step. Also ask: is there a separate table with the
   demographic/behavioral grouping columns, or is everything in one
   table? If separate, what's the join key (e.g. a person/voter ID), and
   is it a left join or something else? Don't assume `voterbase_id` or
   any other specific key — ask, even though it's common in BlueLabs work.

   Also ask whether any of the tables need to be **filtered** before
   anything else happens — e.g. the modeling frame commonly needs
   `deceased = 0`, and some jobs need a geographic restriction or a
   registration-status filter (`is_registered = 1`). A filter like this
   can't be expressed in `grouping_variables` or `scores` — it has to be
   baked into the relevant source's SQL. If a source needs a filter, define
   it with a `query` (raw SQL) instead of a plain `table` reference, e.g.
   `SELECT * FROM schema.table WHERE deceased = 0` — see
   `references/config_reference.md` for the exact shape and a worked
   example. Don't assume no filter is needed; asking costs one question and
   a wrong assumption produces a technically-valid config with the wrong
   population.

3. **Scores.** What score column(s) do they want summarized? Get a
   human-readable name and the actual column name for each.

4. **Counterfactuals (optional).** Is there anything they want to compare
   the score against — a prior model version, a benchmark score, or a
   labeled category? If yes, is it numeric or categorical?

5. **Grouping variables.** First ask which modeling frame the base table
   is — political, commercial, L2 commercial, or none of these/something
   bespoke. If it's one of the first three, check
   `references/common_grouping_variables.md` — it has BlueLabs's common
   column-name mappings for the usual breakdowns (age, female, education
   modeled) for that frame, so the user doesn't have to type out every
   column name themselves. Offer these as defaults and let the user
   override or add to them; don't assume the list is exhaustive or skip
   confirming it with the user.

   Then ask what breakdowns they want (age, party, region, etc.). For
   each, get the actual column name — filling in from the frame's
   defaults above where they apply. Tell the user that grouping columns
   are assumed to live in the base table unless they say otherwise — they
   only need to name a table for columns that live in a joined source
   (the `source` field; omit it for base-table columns). These become the
   numbered categories in the output (01, 02, ...) in the order given —
   confirm the order matters to them, or just ask if they want a specific
   order vs. whatever's convenient. Ask whether they want the automatic
   "Topline" (whole-population) row — default yes.

6. **Aggregations.** Mean is the default and covers most cases; only dig
   deeper if they mention wanting something else (median, sum, frequency,
   or a custom function). Tell the user explicitly that a row count is
   *always* included automatically (`COUNT(*)`, one per category/level,
   regardless of `aggregations`) — they only need to add `count` here if
   they want a *second*, per-column count (e.g. how many non-null values a
   specific score has), which is a different, less commonly needed number.
   Most jobs never need to add `count` at all.

   If they want a genuinely custom aggregation (anything not in the
   built-in list), be direct that this isn't implemented yet: the schema
   accepts `scores[].custom_aggregations` and
   `aggregations.custom_functions`, but nothing in the tool's SQL
   generation actually reads either of them — setting them validates fine
   and is silently ignored. Don't write a config that relies on either
   field. If they need a genuinely custom *cross-column* computation
   instead (a function of two or more already-aggregated columns), that
   one does work — see the next step.

7. **Cross-column computation (optional).** Do they want a computed
   sum, difference, ratio, or product between any of the scores/
   counterfactuals? `add`, `difference`, `multiply`, and `divide` are all
   built in — use these whenever the computation is genuinely just one
   of those four applied across the inputs. If it's more complex than
   that basic arithmetic — combining more than one operation (a
   percent-change `(new - old) / old`), mixing more than two inputs
   asymmetrically, guarding against a zero denominator, or anything
   statistical (standard error, a confidence interval) — it needs
   `op: custom` with a `function`. See "Writing a custom cross-column
   function" below for exactly how that has to work; don't attempt it
   without reading that section, since the requirements (import path,
   function signature, what `inputs` must reference) are easy to get
   subtly wrong.

   If the computation needs intermediate scores/counterfactuals that only
   exist to feed it (e.g. a row-level product for a weighted average) —
   not something the user wants to read directly in the report — mention
   `hidden: true` as an option for those specific columns rather than
   defaulting to showing every computed column in the output. Confirm
   which columns (if any) the user actually wants hidden; don't guess.

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
   `crosstab` from the repo directory** — that's still the simplest,
   most reliable choice even though `cli.py` now also searches parent
   directories and falls back to `~/.env` (see `_find_env_file` in
   `cli.py`), since a `.env` living somewhere else entirely (a different
   repo, a stray home-directory copy) could shadow the one the user
   actually means. Absolute paths in the config are what make the output
   destination independent of where the tool happens to be invoked from.
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

## Writing a custom cross-column function

Only needed when the user wants a `cross_column` computation that isn't
`add`, `difference`, `multiply`, or `divide` (e.g. a percent-change, or
something combining more than one operation). See
`examples/custom_cross_column_example.yaml` and its
accompanying `examples/custom_cross_column_functions.py` for a complete,
runnable worked example of everything below. This is real, working
functionality — unlike
`scores[].custom_aggregations` / `aggregations.custom_functions`, which
are dead — but it has several requirements that are easy to get wrong if
you haven't read this section first:

1. **The function operates on already-aggregated pandas Series, not raw
   rows.** `compute/cross_column.py` calls it as `func(*input_series)`
   against the tidy result DataFrame (one row per category/level) —
   never against row-level data. Write it accordingly, e.g.:
   ```python
   def percent_change(new: pd.Series, old: pd.Series) -> pd.Series:
       return (new - old) / old
   ```
2. **`inputs` must reference the bare score/counterfactual `name`, not an
   aggregated column name** (the schema validates this — see
   `references/config_reference.md`'s note on `cross_column[].inputs`).
   At resolution time, `compute/cross_column.py` only has a `mean_`
   fallback: it looks for the bare name in the result columns, and if
   that's not found, for `mean_<name>`. There is **no equivalent
   fallback for `sum`, `median`, or any other aggregation** — a
   score/counterfactual using anything other than `mean` cannot be
   referenced from `cross_column` at all (it'll fail at `crosstab
   validate` with a clear "unknown column" error, not silently). If you
   need something that's naturally expressed as a ratio of sums (e.g. a
   weighted average, `SUM(a)/SUM(b)`), use `mean` instead of `sum` on
   the intermediate columns — `MEAN(a)/MEAN(b) == SUM(a)/SUM(b)` whenever
   both are aggregated over the same rows, since the row count cancels.
   This sidesteps the missing `sum_` fallback entirely and needs no
   schema changes.
3. **The function must be importable via a dotted path
   (`module:function_name`) at run time**, resolved by
   `crosstab_tool/query/registry.py`'s `resolve()` via
   `importlib.import_module`. There are two places it can live:

   - **Preferred: this repo's shared `custom_functions/` package**
     (`open-dslib/crosstab-tool/custom_functions/`). `cli.py` puts the
     crosstab-tool repo root on `sys.path` automatically, so anything
     here is importable as `custom_functions.<module>:<function_name>`
     with no `PYTHONPATH` needed. This is git-tracked and shared across
     everyone using the tool, so **before writing a new function, check
     the existing modules in `custom_functions/` for one that already
     does what's needed** (e.g. `custom_functions/ratios.py` has
     `percent_change`, a common need for a treatment-vs-control lift
     metric) — reuse it rather than writing a near-duplicate. Remember
     `add`, `difference`, `multiply`, and `divide` are all built-in ops now — a
     custom function is only needed for something combining more than
     one operation. If nothing existing fits, add a new function to an
     existing module if it's a good fit there, or a new module
     otherwise; this is the one exception to "Do not modify the
     crosstab-tool repository" below.
   - **A private, one-off function in the user's own project
     directory** (alongside their config, not inside crosstab-tool) —
     only when the computation is genuinely project-specific and
     wouldn't make sense to share. This still needs `PYTHONPATH` set to
     that directory when running `crosstab`, e.g.:
     ```bash
     PYTHONPATH=/path/to/users/project crosstab run configs/their_job.yaml
     ```

   Either way, still run `crosstab` itself from the crosstab-tool repo
   directory, for the same `.env`/credentials reason as everywhere else
   in this file.
4. **Verify it actually works before calling the config done** — don't
   just check that `crosstab validate` passes (that only checks the
   `inputs` names resolve to real columns, not that the function itself
   imports or computes correctly). Test both:
   ```bash
   # 1. the function actually imports and runs (add PYTHONPATH=... first
   #    if it's a private function, not needed for custom_functions/)
   python3 -c "
   from crosstab_tool.query.registry import resolve
   import pandas as pd
   f = resolve('custom_functions.module_name:function_name')
   print(f(pd.Series([10.0]), pd.Series([2.0])))
   "
   # 2. the whole cross_column entry resolves against a fake result frame
   #    (see 'Validate before you consider it done' below for the full pattern)
   ```

If the computation only exists to feed a `cross_column` entry and isn't
meant to be read directly (e.g. a row-level product needed only for a
weighted-average ratio), set `hidden: true` on that score/counterfactual
— it stays fully computed and usable as a `cross_column` input, just
dropped from the written output. See
`examples/custom_cross_column_example.yaml`, which uses exactly this
pattern.

## Do not modify the crosstab-tool repository

The crosstab-tool repository (e.g. `open-dslib/crosstab-tool`, or
wherever the tool lives on this machine) is read-only for this skill,
with two exceptions:

1. Saving the generated config YAML into its `configs/` directory when
   the user has explicitly chosen that location in the save prompt above.
2. Reading and adding to `custom_functions/` — this shared package is
   meant to be extended over time (see "Writing a custom cross-column
   function" above), so writing a new function there (or adding one to
   an existing module) is expected, not a repo modification to avoid.

Everything else in the repo — the `crosstab_tool/` package source,
`examples/`, existing configs, tests, anything — must not be created,
edited, or deleted by this skill. In particular: if you find what looks
like a bug in the tool while validating or tracing a config, report it to
the user; do not patch it. Any scratch scripts or fake DataFrames you
build (e.g. to exercise `compute/cross_column.py`) go in a
temp/scratchpad directory, never inside the repo — `custom_functions/` is
for real, kept functions only, not throwaway test code.

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
