# crosstab-tool User Guide

`crosstab-tool` (CLI: `xtab`) computes descriptive-statistics crosstabs of ML model
scores against covariate groupings — count/mean/std/percentiles/etc. per group,
auto-generated combinations of covariates (cube/rollup), and optional comparison
against a counterfactual/baseline (e.g. a prior model vintage). It works as both a
config-driven CLI and a Python library, built natively on [Polars](https://pola.rs).

See `README.md` for current milestone status and `docs/implementation-plan.md` /
`docs/POLARS_SCALE.md` for the underlying design decisions and scale tradeoffs this
guide references throughout.

## Installation

Requires Python 3.10+.

```bash
git clone <this repo>
cd crosstab-tool
python3.12 -m venv .venv          # any 3.10+ interpreter works
source .venv/bin/activate
pip install -e ".[dev,sql]"       # "sql" extra pulls in ConnectorX, needed for `sql`/`join` sources
```

This installs the `xtab` console script. A `run_crosstab.sh` convenience wrapper also
lives at the repo root — see [Running it](#running-it) below.

## Quick start

Every run is driven by a single YAML (or JSON) config file that round-trips 1:1 with
the `CrosstabSpec` Python model — the CLI and the Python API share exactly one spec
shape and one execution path (`core/runner.py`).

```yaml
source:
  type: parquet
  path: /data/scores/current.parquet

score_columns:
  - model_score

groupby:
  type: explicit
  groups:
    - [region]
    - [region, product]

stats:
  - name: count
    column: model_score
  - name: mean
    column: model_score
```

```bash
xtab validate --config my_config.yaml   # schema/column/dtype checks, no execution
xtab run      --config my_config.yaml   # prints one table per groupset to the terminal
```

More worked examples live in `examples/configs/` — each has a comment explaining what
it demonstrates. The rest of this guide walks through every part of the config.

## Config anatomy

A config is a direct serialization of `CrosstabSpec` (`src/crosstab_tool/spec/crosstab_spec.py`):

| Field | Required | Purpose |
|---|---|---|
| `source` | yes | Where the data comes from |
| `score_columns` | yes | Numeric column(s) stats run against |
| `groupby` | yes | Explicit groupsets, or auto-generated (cube) |
| `stats` | yes | Which statistics to compute per group |
| `filters` | no | Row filters applied before aggregation |
| `options` | no | Free-form dict; currently only `cardinality_guardrail` is read |
| `comparison` | no | Optional counterfactual/baseline comparison |

All spec models reject unrecognized fields (a typo, or a field that documents a
not-yet-implemented idea, fails loudly rather than being silently ignored) — so if
`xtab validate` rejects something with `extra_forbidden`/`Extra inputs`, check the
field name and nesting against this guide.

### `source`

Five source types, discriminated by `type`. Any of them can be used as `comparison.baseline.source` too (see [Comparison](#comparison-counterfactualbaseline)).

**`parquet` / `csv`** — local or cloud (via `fsspec`, e.g. `s3://...`) flat files, read
lazily (`pl.scan_parquet`/`scan_csv`) with predicate/projection pushdown.

```yaml
source:
  type: parquet   # or csv
  path: /data/scores/current.parquet   # or s3://bucket/path/*.parquet
```

**`dataframe`** — an already-in-memory pandas/Polars/Arrow object. **Python-API only**
— there's no sensible YAML/JSON serialization for a live object, so this is only ever
constructed programmatically, never parsed from a config file:

```python
from crosstab_tool import CrosstabSpec, run_crosstab
import polars as pl

spec = CrosstabSpec.model_validate({
    "source": {"type": "dataframe", "data": my_polars_or_pandas_df},
    ...
})
```

**`sql`** — a SQL warehouse, read via ConnectorX (`pl.read_database_uri`).

```yaml
source:
  type: sql
  dialect: redshift     # or "postgresql" (default); see the Redshift note below
  # host: warehouse.internal      # optional
  # port: 5439                    # optional -- defaults per dialect (postgresql=5432, redshift=5439)
  # database: scores_db           # optional
  # connection: "postgresql://user:pw@host:5432/db"   # full URI -- overrides host/port/database
  query: >
    SELECT region, product, model_score
    FROM scored_population
    WHERE score_date = CURRENT_DATE
```

Two important things about `sql` sources:

1. **Scale-limited path (v1, documented, not a bug).** `query`'s *entire* result set is
   pulled client-side before Polars aggregates it — there's no `GROUP BY` pushdown into
   the warehouse (see `docs/POLARS_SCALE.md`). Narrow `query` itself (date filters,
   column projection) for anything at meaningful scale; don't rely on the top-level
   `filters` field to do that narrowing for you, since it only runs after the full
   result is already in memory.
2. **Credentials are never required in the config.** Leave `connection` (and
   optionally `host`/`database`) unset and `xtab` prompts interactively each run —
   `host`/`database` only if not already set here, `username`/`password` always
   (password hidden via `getpass`, never echoed, never logged). Set `connection`
   directly instead if you'd rather manage the credential yourself (e.g. via your own
   env-var substitution before invoking `xtab`).

**Redshift gotcha:** set `dialect: redshift`, not `postgresql`, even though Redshift is
wire-compatible with Postgres. ConnectorX only switches off its `COPY (query) TO
STDOUT`-based fast path — which Redshift's SQL parser rejects with `syntax error at or
near "("` — when the connection URI's scheme literally contains `"redshift"`. This is
purely about the URI scheme string, not an actual different driver.

**`join`** — combines two other sources (any type, including another `join`) without
writing the join into a raw SQL query, e.g. a scores table and a separate covariates
table:

```yaml
source:
  type: join
  how: left        # inner | left | full -- no default; an inner join silently drops
                    # unmatched rows, so pick deliberately
  join_keys: [entity_id]        # same column name on both sides...
  # left_on: [entity_id]        # ...or, if the key is named differently per side:
  # right_on: [account_id]
  left:
    type: sql
    dialect: redshift
    query: "SELECT entity_id, model_score FROM scores_table"
  right:
    type: sql
    dialect: redshift
    query: "SELECT entity_id, region, product FROM covariates_table"
```

Each `sql` sub-source prompts for its own credentials independently (twice, if scores
and covariates live on different warehouses).

### `score_columns`

Just the numeric column(s) stats will run against — at least one required.

### `groupby`

**`explicit`** — you list exactly which column combinations to compute, each reported
as its own crosstab:

```yaml
groupby:
  type: explicit
  groups:
    - [region]
    - [region, product]
    - []              # empty list -> one ungrouped/"overall" crosstab too
```

**`cube`** — auto-generates combinations of a covariate set (power set, optionally
depth-capped) instead of listing them by hand:

```yaml
groupby:
  type: cube
  columns: [region, product, age_bucket]
  max_depth: 2          # cap combination size (2 of the 3 columns at a time); omit for the full power set
  include_empty: true   # also compute the overall/ungrouped crosstab (default true)
```

`max_depth: 2` over 3 columns produces `[region]`, `[product]`, `[age_bucket]`,
`[region, product]`, `[region, age_bucket]`, `[product, age_bucket]` (plus `[]` if
`include_empty`). Both `explicit` and `cube` normalize to the same flat
`list[list[str]]` shape internally, so everything downstream (validation, the engine,
comparison) treats them identically.

**Cardinality guardrail** (cube only): a k-column cube means up to `2^k` separate
full-scan `group_by().agg()` passes — Polars has no single-query `GROUPING
SETS`/`CUBE` equivalent the way SQL engines do. Opt in to a fail-fast check via:

```yaml
options:
  cardinality_guardrail:
    max_groupset_count_x_cardinality: 5_000_000
```

This raises before running anything if `(number of groupsets) × (estimated row count)`
exceeds the threshold. It's a no-op if omitted, and a no-op for sources that can't
estimate their row count (`ParquetSource`/`CSVSource` always return `None` — an
in-memory DataFrame source does report one).

### `stats`

Each entry names a stat from the built-in registry (`src/crosstab_tool/stats/`),
targets one column, and optionally takes params:

| `name` | Params | Notes |
|---|---|---|
| `count` | — | Works on non-numeric columns too |
| `mean` | — | |
| `std` | — | |
| `min` | — | |
| `max` | — | |
| `percentile` | `q` (0–1, required) | Output column named `..._p<q*100>`, e.g. `_p90` |
| `skew` | — | Via `scipy.stats.skew`; needs ≥3 non-null values per group |
| `kurtosis` | — | Via `scipy.stats.kurtosis`; needs ≥4 non-null values per group |

```yaml
stats:
  - name: count
    column: model_score
  - name: percentile
    column: model_score
    params:
      q: 0.9
```

Adding a new stat is a one-function, one-`@register_stat`-call extension
(`stats/registry.py`) — no engine changes required, which is why the built-in list
above may grow without this guide's structure changing.

### `filters`

A list of Polars boolean expression strings (parsed via `pl.sql_expr`), ANDed together
and applied before aggregation:

```yaml
filters:
  - "model_score is not null"
  - "region != 'UNKNOWN'"
```

For a `sql` source, remember these run *after* the full query result is already
client-side — narrow expensive conditions into `query` itself instead.

### Comparison (counterfactual/baseline)

Optional. Compares a score column against a baseline and reports both difference
metrics and statistical tests, per group. Three shapes, all under `comparison`:

**1. Paired, same source** — baseline is another column in the same data (e.g. a prior
model vintage scored the same rows), inherently row-aligned:

```yaml
comparison:
  column: model_score
  baseline:
    type: column
    column: prior_model_score
  metrics: [mean_diff, median_diff, paired_ttest]
  max_sample_size: 200_000   # only relevant for raw-array metrics, see below
```

**2. Paired via join** — baseline lives in a separate dataset but shares an entity key,
so it's paired after an inner join on `join_keys`:

```yaml
comparison:
  column: model_score
  baseline:
    type: source
    source: {type: parquet, path: /data/scores/2026-q2.parquet}
    join_keys: [entity_id]
  metrics: [mean_diff, median_diff, paired_ttest]
```

**3. Unpaired/distributional** — baseline is a separate dataset with no shared key;
each groupset is aggregated independently in both datasets and compared as two
distributions rather than row-matched:

```yaml
comparison:
  column: model_score
  baseline:
    type: source
    source: {type: parquet, path: /data/scores/2026-q2.parquet}
    # no join_keys
  metrics: [ks_test, mannwhitney]
  max_sample_size: 200_000
```

**Metrics by mode:**

| Mode | Allowed metrics |
|---|---|
| Paired (`column`, or `source`+`join_keys`) | `mean_diff`, `median_diff`, `std_diff`, `paired_ttest`, `wilcoxon` |
| Unpaired (`source`, no `join_keys`) | `ks_test`, `mannwhitney` |

`mean_diff`/`median_diff`/`std_diff`/`paired_ttest` only ever need small aggregate
expressions (mean/std/count of the per-row diff), computed entirely in Polars — cheap
even at scale. `wilcoxon`, `ks_test`, and `mannwhitney` are the exception: they need
the *raw* per-group array materialized in memory (`scipy.stats` doesn't reduce to an
aggregate). `max_sample_size` caps how many rows per group get pulled into that array
before the test runs, guarding against a huge group blowing up memory — set it whenever
you use one of those three metrics on data that might have large groups.

A baseline built from a `sql` source is fetched twice per run (once for validation's
schema check, once for real execution) — a known, narrower version of the same
double-fetch concern the main source avoids; harmless for file/in-memory baselines.

## Running it

**CLI** (`xtab`):

```bash
xtab validate --config path/to/config.yaml              # schema/column checks only
xtab run      --config path/to/config.yaml              # print results to terminal
xtab run      --config path/to/config.yaml --out results/ --format csv   # or parquet (default)
xtab schema                                              # print CrosstabSpec's JSON Schema
xtab --verbose run --config path/to/config.yaml          # -v/--verbose goes BEFORE the subcommand
```

`--verbose`/`-v` bumps logging from INFO to DEBUG — see [Logging](#logging) below.

**`run_crosstab.sh`** — a convenience wrapper at the repo root that validates then runs
in one step, auto-activating `./.venv` if present. Defaults to
`examples/configs/my_crosstab.yaml` if no config path is given:

```bash
./run_crosstab.sh                                                    # uses examples/configs/my_crosstab.yaml
./run_crosstab.sh path/to/config.yaml
./run_crosstab.sh path/to/config.yaml --out results/ --format csv   # extra args pass through to xtab run
```

**Python API** — same spec, same execution path, no CLI involved:

```python
from crosstab_tool import CrosstabSpec, run_crosstab

spec = CrosstabSpec.model_validate({...})   # or build a config dict / load from YAML
result = run_crosstab(spec)

result.frames["region"]              # a pl.DataFrame, keyed by groupset (e.g. "region", "region__product", "__overall__")
result.to_pandas()["region"]          # same, as pandas, if you'd rather work in pandas downstream
```

`spec/serde.py` has the YAML/JSON round-trip helpers (`spec_from_file`,
`spec_from_yaml`, `spec_to_dict`, ...) if you want to load a config file from Python
without going through the CLI.

## Logging

Every run logs its progress via Python's standard `logging`, rendered through `rich`:

- **Default (INFO):** config loading, source-adapter construction, validation
  pass/fail, which groupsets are being computed, filter application, comparison
  mode/metrics, cardinality-guardrail arithmetic, SQL query execution with row count
  and elapsed time, join operations, and a final per-groupset row-count summary.
- **`--verbose`/`-v` (DEBUG):** additionally, the full source schema, the raw SQL query
  text, and per-source scan detail.
- **Never logged, at any level:** the SQL password — only username/host/database, and
  only after the connection string is already built.

If you're calling the Python API directly rather than through `xtab`, nothing
configures a log handler for you (standard library convention) — configure the
`"crosstab_tool"` logger yourself, or call
`crosstab_tool.logging_config.configure_logging(verbose=...)` to get the same
`RichHandler` setup the CLI uses.

## Known limitations (v1)

These are documented tradeoffs, not silent gaps — see `docs/POLARS_SCALE.md` and
`docs/implementation-plan.md` for the full reasoning:

- **No `GROUP BY` pushdown for `sql` sources.** The full query result is pulled
  client-side before Polars aggregates. Fine when `query` already narrows the data;
  impractical otherwise at 100M+ row scale.
- **No stratified-sampling mitigation yet.** `docs/POLARS_SCALE.md` sketches a
  `sampling` config block for the SQL-scale gap above; it isn't implemented, and a
  config with a `sampling` key under `source` will correctly fail validation rather
  than silently doing nothing.
- **Cube groupsets cost one full pass each.** Unlike SQL's `GROUPING SETS`/`CUBE`,
  Polars has no single-scan equivalent — a k-column cube is up to `2^k` separate
  `group_by().agg()` passes (batched via `pl.collect_all()` so Polars can overlap
  them, but not a single-scan guarantee).
- **`options.streaming` in some example configs is currently a no-op placeholder** —
  nothing reads it yet. Only `options.cardinality_guardrail` is actually wired up.
- **A `sql` comparison baseline is fetched twice** (validation + execution) per run.

## Where to go next

See `README.md`'s Status section for milestone-by-milestone build history. As of this
writing, M0–M6 are complete (core spec, file/DataFrame/SQL/join sources, comparison,
cube groupbys, the CLI); M7 (exporters as a first-class `Exporter` protocol, optional
entry-point stat plugins, perf/synthetic-scale tests, and folding the Engine Strategy
section of `docs/implementation-plan.md` into a living `docs/architecture.md`) is not
yet started.
