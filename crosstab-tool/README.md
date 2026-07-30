# crosstab-tool (sketch)

Configuration-driven crosstab / counterfactual reporting tool for
Redshift model scores. This is an early project skeleton, not a finished
build — see `docs/architecture.md` for the design and its mapping back to
`Crosstab_Tool_Requirements_v0_2`.

## Layout

- `crosstab_tool/` — the package (config, sources, query, compute, output, metadata, cli)
- `examples/` — sample job configs, including a config-driven version of the
  existing "m3 Universe Tabs" reference SQL (Appendix A)
- `tests/` — unit tests
- `docs/architecture.md` — module map, data flow, config schema summary, open questions

## Try it

```bash
pip install -e ".[dev]"
pytest
```

`examples/model3_universe_tabs.yaml` validates against the schema and
`query.builder.build_query()` will render it as SQL — see
`tests/unit/test_builder.py`. Or from the command line:

```bash
crosstab sql examples/model3_universe_tabs.yaml       # SQL preview only, no validation summary
crosstab validate examples/model3_universe_tabs.yaml  # schema + SQL + cross_column resolution check
```

Not yet wired up: real basetable column names in the example configs
(several are placeholders — search for `# placeholder`). Redshift and
Google Sheets connections work once you provide credentials — see below.

## Running a job for real

The config wizard's validation step (and CI, if this repo gets it) should
call `crosstab validate <config.yaml>` — it never touches Redshift, it
only validates a config, previews the SQL it would generate, and confirms
any `cross_column` entries actually resolve against real output columns.
Actually running a job (`crosstab run`) needs a live Redshift connection
and, for the default output, Google Sheets credentials. Two ways to do
that:

### Option A — run the tool itself (gets you the full feature set)

This gets you everything: automatic Sheets writing, cross-column
computation, run metadata capture. Do this from wherever you already have
legitimate Redshift access set up (your own machine, Positron, a shared
analytics box) — not from a Claude sandbox, which has no route to
BlueLabs' Redshift network and shouldn't be handed live credentials.

1. **Install** (Python 3.10+):
   ```bash
   cd crosstab-tool
   pip install -e ".[dev]"
   ```
   In Positron: open this folder, make sure its Python interpreter is
   selected (bottom-right interpreter picker, or `Cmd+Shift+P` → "Python:
   Select Interpreter"), then run the command above in Positron's
   integrated terminal.

2. **Set Redshift credentials**, prefixed with whatever `connection:` name
   your config uses (e.g. `REDSHIFT_MAIN` in the example configs). Copy
   `.env.example` to `.env` and fill in real values:
   ```bash
   cp .env.example .env
   # then edit .env with your actual host/db/user/password
   ```
   `crosstab run` loads `.env` automatically (via `python-dotenv`) — no
   `export`/`source` needed. `.env` is already `.gitignore`d so real
   credentials never get committed. Avoid pasting credentials inline in
   the terminal where they'd land in shell history.

3. **Set up Google Sheets credentials** (only needed if `output.destination:
   google_sheets`, the default). `gspread.service_account()` looks for a
   service account key at `~/.config/gspread/service_account.json` by
   default — ask whoever manages BlueLabs' Google Cloud service accounts
   for one scoped to Sheets, or point `GOOGLE_APPLICATION_CREDENTIALS` at
   an existing key. Skip this entirely if you set `destination: csv` or
   `excel` in the config instead.

4. **Run it:**
   ```bash
   crosstab run configs/your_job.yaml
   ```
   Errors here are usually a missing/misnamed basetable column (check the
   config's `# placeholder` comments first) or a credentials/permission
   issue — bring the error back and I can help debug the config or the
   tool's logic, without ever needing the credentials myself.

### Option B — DBeaver, no Python setup

If you'd rather not set up a Python environment yet, you can still use
everything upstream of the Python execution:

1. Use the crosstab-config-wizard skill to get a validated config, or run
   `crosstab validate configs/your_job.yaml` yourself, to get the
   generated SQL preview.
2. Paste that SQL into a DBeaver SQL editor connected to Redshift and run
   it there.
3. Anything the tool would normally do in Python afterward — cross-column
   differences, writing to Sheets, run-metadata capture — becomes a manual
   step: export DBeaver's result grid and compute/paste it yourself.

This loses the automation but is a reasonable way to sanity-check a new
config against real data before investing in the full setup.
