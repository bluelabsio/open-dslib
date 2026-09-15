"""Shared, git-tracked library of custom `cross_column` functions for
crosstab-tool jobs.

`cli.py` puts the crosstab-tool repo root on `sys.path` automatically, so
any module here is importable via a dotted path like
`custom_functions.ratios:percent_change` in a job config's
`cross_column[].function` — no `PYTHONPATH` setup needed.

Before writing a new function here, check whether an existing one
already does what you need (read through the modules in this package) —
the whole point of this being a shared, repo-tracked location (rather
than a one-off script in an individual project directory) is so the same
computation isn't reimplemented per job. See
`crosstab-config-wizard/SKILL.md`'s "Writing a custom cross-column
function" section for the full requirements a function here has to meet
(signature, what `inputs` must reference, how to verify it actually
works) before adding one.
"""
