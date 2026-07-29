# Crosstab Tool — Implementation Plan

_This is the approved design/proposal document this project was built from. Saved here verbatim
for reference; `docs/architecture.md` will hold the living, updated architecture record as the
project evolves (starting with the Engine Strategy section below, per the M7 milestone)._

## Context

We're building a new Python package to generate descriptive-statistics crosstabs of ML model scores against arbitrary covariate groupings. The motivating need: analysts want to quickly see how score distributions (count, mean, std, percentiles, etc.) vary across single or multi-column groupings (e.g. region, region+product, age_bucket), including auto-generated combinations of a covariate set (cube/rollup style), over datasets that can reach 100M+ rows. The tool needs to work as both an importable library and a CLI (config-driven, for batch/scheduled jobs), and must be extensible so new statistics (distribution-shape stats now, label-based performance metrics like AUC/calibration later) can be added without touching core execution code.

Data sources vary by project: sometimes flat files (Parquet/CSV), sometimes an existing SQL warehouse, sometimes an in-memory DataFrame from an upstream pipeline. The tool should support all three via a pluggable source-adapter layer, without hardcoding a single source assumption into the core logic.

**Engine decision for v1: build natively on Polars** (not DuckDB), based on discussion with the user. Rationale and tradeoffs are captured below and must be written into `docs/architecture.md` in the repo so the decision and its migration path are documented for future contributors.

## Engine strategy: Polars-native v1 (documented decision)

### Why Polars-native for v1

- **Single technology to build, debug, and hire/onboard for.** One expression language (`pl.Expr`) covers scanning, filtering, grouping, and stats — no second SQL dialect layer to maintain alongside it.
- **Unified stat-function protocol.** Every stat (built-in or future) is just a function returning one or more `pl.Expr` (or, for stats that don't reduce cleanly to an expression, a `map_batches`/custom callable operating on a Polars group). There's no split between "SQL-expressible" and "DataFrame-function" stats like a DuckDB-hybrid design would need — one code path, simpler registry, simpler planner (in fact, no separate planner/router needed in v1).
- **Good performance at 100M+ rows for the sources that matter most today (files, in-memory DataFrames).** Polars' lazy engine (`scan_parquet`/`scan_csv` + `.lazy()`) does predicate/projection pushdown and multi-threaded execution, and its streaming engine can process larger-than-memory data without manual chunking.
- **Faster to ship.** No dialect-translation layer, no SQLAlchemy connector matrix to stand up for v1 — less surface area before the core value (descriptive crosstabs) is usable.

### Known tradeoffs vs. an eventual DuckDB layer (for future migration)

- **SQL warehouse sources won't get true pushdown in v1.** Polars can pull data from a warehouse via `pl.read_database_uri` (ConnectorX), but that fetches rows into the client to aggregate locally — it does not push the `GROUP BY` into the warehouse's own MPP engine. For v1, warehouse sources are practical only when the user's query already filters/pre-aggregates down to a manageable size (documented limitation, not silently unsupported). DuckDB (or direct SQLAlchemy-pushdown SQL generation) would close this gap by computing the aggregation inside the warehouse and only returning the small aggregated result.
- **Cube/auto-combination groupbys require multiple passes.** SQL's `GROUPING SETS`/`CUBE` lets an engine compute many groupset aggregations in a single table scan. Polars has no equivalent single-query construct — a cube over k covariates means up to 2^k separate `group_by().agg()` passes (mitigated by batching them with `pl.collect_all()` so Polars can overlap execution, but not a single-scan guarantee). This is the most consequential migration driver: if cube usage grows to be a primary workload at 100M+ rows, a DuckDB-backed engine collapsing that to one scan would meaningfully reduce runtime/IO.
- **Out-of-core maturity.** DuckDB's disk-spilling for aggregations that exceed RAM is longer-battle-tested than Polars' streaming engine, which has been rapidly evolving. Worth re-benchmarking before scaling far past 100M rows per run.
- **SQL as a portable, inspectable artifact.** A `pl.Expr` is Python/Polars-specific; SQL stat expressions are more easily reviewed, ported, or reused outside Python (e.g. directly in a warehouse console). Not a blocker, just a portability cost of the Polars-only design.

## Counterfactual / baseline comparison (explicit requirement)

A hard requirement, not a stretch goal: the tool must optionally compare each group's scores against a **user-defined counterfactual/baseline** (most commonly a prior model vintage's scores for the same population), producing both **difference metrics** and **statistical tests of difference** per group.

### `ComparisonSpec` (new field on `CrosstabSpec`)

- `baseline`: one of
  - `column: str` — the counterfactual score lives in another column of the *same* source (e.g. `prior_score`) → inherently **paired** (row-aligned) comparison.
  - `source: DataSourceSpec, join_keys: list[str]` — the counterfactual lives in a *separate* dataset (e.g. last quarter's scoring file) → **paired** comparison after joining on `join_keys` (e.g. entity/account ID).
  - `source: DataSourceSpec` (no `join_keys`) — separate dataset, **unpaired/distributional** comparison: each groupset is computed independently against the same groupby columns in both datasets and compared as two distributions, not row-matched.
- `metrics: list[str]` — resolved against a comparison-metric registry (see below), e.g. `["mean_diff", "paired_ttest"]` or `["ks_test", "mannwhitney"]`.

### Two computation paths (both reuse the Polars-native, aggregate-first philosophy — avoid raw per-row scipy calls at 100M-row scale wherever the math allows it)

1. **Paired path** (`column` or `source`+`join_keys`): construct a `__diff = current_score - baseline_score` column via a join (or direct column subtraction if same source), *before* grouping. Then:
   - Simple difference stats (`mean_diff`, `median_diff`, `std_diff`, etc.) are just the existing stat registry applied to `__diff` — no new execution machinery needed, this is the main reason paired mode is cheap.
   - `paired_ttest` only needs `mean(__diff)`, `std(__diff)`, `count(__diff)` per group — all plain aggregate expressions — with the t-statistic/p-value computed as a lightweight `scipy.stats.t.sf(...)` post-processing step on the small (rows = #groups) aggregated summary table, not on raw data. This keeps the expensive part (scanning/joining/aggregating 100M rows) fully in Polars and only hands scipy a tiny table.
   - `wilcoxon` (nonparametric paired) is the exception requiring the raw per-group `__diff` array (rank-based test) — computed via `group_by(...).agg(pl.col("__diff").implode())` then scipy applied per group; see guardrail below.
2. **Unpaired/distributional path** (`source`, no `join_keys`): run the normal groupby aggregation against *both* the current and baseline sources independently (same groupby columns), additionally collecting each group's raw score array via `.implode()`, join the two per-group results on the groupby key, and apply `scipy.stats.ks_2samp` / `mannwhitneyu` per group in a small Python loop over the (group-count-sized, not row-count-sized) joined table.

### Guardrail

Because `wilcoxon`/`ks_test`/`mannwhitney` require materializing raw per-group arrays (via `.implode()`), a group with millions of rows would create a very large in-memory list. `core/validation.py` should support an optional `max_sample_size` per comparison (with deterministic sampling applied before `.implode()` when a group exceeds it), and this limitation must be documented in `docs/architecture.md` alongside the engine-strategy notes.

### New/updated modules

- `spec/comparison_spec.py` — `ComparisonSpec`, `BaselineSpec` (discriminated union: `column` / `source+join_keys` / `source`).
- `stats/comparison_registry.py` — comparison-metric registry, separate from (but following the same `@register_*` pattern as) `stats/registry.py`, with two categories: `paired_aggregate` (mean_diff, median_diff, paired_ttest) and `distributional` (wilcoxon, ks_test, mannwhitney), each declaring whether it needs raw per-group arrays or just aggregate expressions.
- `engine/polars_engine.py` gains the diff-column construction (paired) and dual-source group+join (unpaired) execution paths described above.
- `core/result.py` — `CrosstabResult` frames include comparison columns (e.g. `mean_diff`, `paired_ttest_stat`, `paired_ttest_pvalue`) alongside descriptive stats when `comparison` is configured.

### Milestone placement

Insert as **M3 — Counterfactual comparison**, immediately after M2 (DataFrame adapter + non-trivial stat), since the paired path builds directly on the same "non-expression stat via `map_batches`/`.implode()`" infrastructure proven in M2, and this is an explicit hard requirement rather than a later nice-to-have. Subsequent milestones shift down: SQL source → M4, CLI → M5, Cube/auto-groupby → M6, Extensibility/exporters/perf/docs → M7.

### New dependency

- `scipy` (`scipy.stats`: `ttest_1samp`/manual t-stat, `wilcoxon`, `ks_2samp`, `mannwhitneyu`) — add to core dependencies.

### Migration path if/when DuckDB is added later

The registry/spec design below is intentionally engine-agnostic at the interface level (a stat is identified by name + params + required dtype; a source is identified by a spec + adapter), so a future `duckdb`-backed engine could be added as an alternate execution backend behind the same `ExecutionEngine`-style seam without changing `CrosstabSpec`, the CLI, or the stats registry's public shape — only `engine/polars_engine.py`'s internals plus a new `engine/duckdb_engine.py` and a routing decision would need to be added. Document this explicitly in `docs/architecture.md` as the intended extension point, but do not build the routing/planner abstraction now — that would be speculative complexity for a v1 that has exactly one engine.

## Package name & repo

- **Package name:** `crosstab_tool` (import name), repo `crosstab-tool`, CLI entry point `xtab`.
- **Location:** new standalone repo at `~/Documents/crosstab-tool`.

## Directory structure

```
crosstab-tool/
├── pyproject.toml                # deps, entry_points (console_scripts: xtab)
├── README.md
├── src/
│   └── crosstab_tool/
│       ├── __init__.py            # public API surface (e.g. run_crosstab)
│       ├── spec/
│       │   ├── crosstab_spec.py   # CrosstabSpec (pydantic model, the central contract)
│       │   ├── source_spec.py     # DataSourceSpec + subtypes (discriminated union)
│       │   ├── groupby_spec.py    # GroupBySpec (explicit) + CubeSpec (auto-combination)
│       │   ├── stat_spec.py       # StatSpec (name + params + target column)
│       │   ├── comparison_spec.py # ComparisonSpec + BaselineSpec (column / source+join_keys / source)
│       │   └── serde.py           # YAML/JSON <-> CrosstabSpec (round-trip)
│       ├── sources/
│       │   ├── base.py            # DataSourceAdapter protocol: to_polars_lazyframe(), describe_schema(), estimated_row_count()
│       │   ├── files.py           # ParquetSource / CSVSource, local + cloud (fsspec), via pl.scan_parquet/scan_csv
│       │   ├── dataframe.py       # PandasSource / PolarsSource / ArrowSource -> pl.LazyFrame
│       │   ├── sql.py             # SQLSource via pl.read_database_uri (ConnectorX); documented scale limits
│       │   └── registry.py        # string type -> adapter class, used by spec/serde.py
│       ├── engine/
│       │   └── polars_engine.py   # the single v1 execution engine: lazy scan -> filter -> group_by/agg per groupset -> collect
│       ├── stats/
│       │   ├── registry.py        # StatFunction protocol + @register_stat + lookup, all Polars-expression based
│       │   ├── builtin.py         # count/mean/std/min/max/percentile as pl.Expr builders
│       │   ├── builtin_custom.py  # skew/kurtosis or other non-trivial-expr stats via map_batches, proves 2nd path
│       │   ├── comparison_registry.py  # paired_aggregate (mean_diff, paired_ttest) + distributional (wilcoxon, ks_test, mannwhitney)
│       │   └── contrib/           # future: auc.py, calibration.py, lift.py (label-based metrics)
│       ├── core/
│       │   ├── runner.py          # CrosstabRunner.run(spec) -> CrosstabResult; single orchestration path for API+CLI
│       │   ├── result.py          # CrosstabResult: dict[groupset_key -> pl.DataFrame] + metadata (timing, engine, row counts)
│       │   └── validation.py      # column existence, stat/dtype compatibility, cube cardinality-explosion guardrails
│       ├── export/
│       │   ├── base.py            # Exporter protocol
│       │   └── builtin.py         # ParquetExporter, CSVExporter (Excel/HTML deferred)
│       └── cli/
│           ├── app.py             # Typer app: `xtab run/validate/schema`, thin wrapper over core.runner
│           └── config_loader.py   # YAML/JSON -> CrosstabSpec via spec.serde
├── tests/
│   ├── unit/            # spec validation, stat registry, groupby expansion
│   ├── integration/     # one module per source type (files, dataframe, sql-via-local-postgres)
│   ├── property/        # hypothesis-based: crosstab output vs. naive pandas/polars groupby, incl. nulls/ties/single-row groups
│   └── perf/             # synthetic-scale test (10-20M rows in routine CI, 100M+ nightly/manual)
├── examples/configs/     # explicit_groupbys.yaml, cube_groupbys.yaml
└── docs/
    └── architecture.md   # includes the Engine Strategy section above verbatim, as the durable decision record
```

## Core abstractions

- **`CrosstabSpec`** (pydantic v2 model): `source`, `score_columns`, `groupby` (`GroupBySpec | CubeSpec`), `stats: list[StatSpec]`, `filters` (Polars expression strings or a small structured filter list), `options` (e.g. streaming hints). Config YAML is a direct serialization of this model (`model_dump`/`model_validate`), so CLI and Python API share exactly one spec shape and one execution path (`core/runner.py`).
- **`DataSourceAdapter`**: narrow interface — `to_polars_lazyframe() -> pl.LazyFrame`, `describe_schema() -> dict[str, str]`, `estimated_row_count() -> int | None`. `ParquetSource`/`CSVSource` use `pl.scan_parquet`/`scan_csv` (local or cloud via `fsspec`, no eager load). `PandasSource`/`PolarsSource`/`ArrowSource` wrap already-in-memory data as a `pl.LazyFrame` (`pl.from_pandas(...).lazy()`, zero-copy for Arrow/Polars). `SQLSource` uses `pl.read_database_uri` (ConnectorX) against a user-supplied query/table — documented as v1's scale-limited path (see tradeoffs above).
- **`StatFunction` registry** (`stats/registry.py`): each stat has `name`, a builder `(column, params) -> pl.Expr` (or a `map_batches`-based callable for non-expression stats), `output_columns(column, params) -> list[str]`, and `required_dtype`. Adding a new stat = writing one function/class and `@register_stat`-ing it in a new or existing module — zero changes to `core/runner.py` or `engine/polars_engine.py`. Build `skew`/`kurtosis` in v1 (not deferred) specifically to prove the non-trivial-expression path works, not just the simple-aggregate path.
- **`GroupBySpec`/`CubeSpec`** (`spec/groupby_spec.py`): `GroupBySpec(groups: list[list[str]])` for explicit groupsets; `CubeSpec(columns, max_depth=None, include_empty=True)` for auto-generated combinations (power set, optionally depth-capped). Both normalize via `expand_to_groupsets(...)` into a flat list of column-lists that `engine/polars_engine.py` consumes identically — the engine doesn't know or care which mode produced them. `core/validation.py` should warn/error when a cube's groupset-count × estimated cardinality exceeds a configurable threshold, to guard against accidental combinatorial blowups on 100M-row data.
- **`engine/polars_engine.py`**: for each normalized groupset, builds and executes a lazy pipeline (scan → filter → `group_by(...).agg([...exprs])` → `collect`), batching multiple groupset queries via `pl.collect_all()` so Polars can overlap their execution rather than running strictly serially. This is the sole v1 engine — no routing/planner layer, per the "don't build speculative complexity" note above.
- **`CrosstabResult`**: `frames: dict[str, pl.DataFrame]` keyed by groupset identifier (e.g. `"region"`, `"region__product"`) + metadata (spec, timing, rows scanned). `.to_pandas()` convenience method for pandas-only downstream consumers; Polars remains the default in-memory type.

## CLI design

- **Typer**-based (`cli/app.py`), thin wrapper: `xtab run --config path.yaml [--out path] [--format parquet|csv]`, plus `xtab validate --config ...` and `xtab schema` (dumps JSON schema generated from the pydantic `CrosstabSpec` model). `cli/config_loader.py` only does file I/O + spec parsing + calling `CrosstabRunner.run(spec)` — zero business logic in the CLI layer.

## Build order / milestones

- **M0 — Scaffolding.** Repo init, `pyproject.toml` (hatchling), ruff/mypy/pre-commit, GitHub Actions CI (lint + unit tests), trivial smoke test green.
- **M1 — Core spec + Polars file engine + basic stats.** `spec/`, `sources/files.py`, `engine/polars_engine.py` (explicit groupby only), `stats/registry.py` + `builtin.py` (count/mean/std/min/max/percentile), `core/runner.py` + `result.py`. Deliverable: `run_crosstab(spec)` works end-to-end against a local Parquet file with an explicit groupby list.
- **M2 — In-memory DataFrame adapter + non-trivial stat.** `sources/dataframe.py` (pandas/Polars/Arrow), `skew`/`kurtosis` in `stats/builtin_custom.py` to validate the `map_batches` path.
- **M3 — Counterfactual/baseline comparison.** `spec/comparison_spec.py`, `stats/comparison_registry.py`, the paired diff-column path and unpaired dual-source path in `engine/polars_engine.py`, `max_sample_size` guardrail in `core/validation.py`. Deliverable: a spec with `comparison: {baseline: {column: prior_score}, metrics: [mean_diff, paired_ttest]}` produces difference + test-statistic columns per group; a second test validates the unpaired `source`+`ks_test`/`mannwhitney` path.
- **M4 — SQL source (scale-limited).** `sources/sql.py` via ConnectorX, with documentation of the pull-then-aggregate limitation; validated against a local Postgres/small dataset.
- **M5 — CLI.** `cli/app.py`, `config_loader.py`, `xtab run/validate/schema`, example configs (including a comparison example config).
- **M6 — Cube/auto-combination groupbys.** `CubeSpec`, `expand_to_groupsets`, `pl.collect_all()` batching, cardinality guardrails in `core/validation.py`.
- **M7 — Extensibility polish + exporters + perf hardening + `docs/architecture.md`.** Entry-point-based external stat plugins (optional), `export/` (Parquet/CSV), perf/synthetic-scale tests, and writing up the Engine Strategy documentation (from this plan) into `docs/architecture.md`, including the DuckDB migration-path notes and the comparison-guardrail documentation.

M1–M3 are architecturally load-bearing (prove the stats-registry, source-adapter, and comparison abstractions — comparison is a hard requirement, not deferred breadth); M4–M7 are further breadth/productionization.

## Testing / verification

- **Unit** (`tests/unit/`): spec validation errors, stat registry lookup/expression generation, groupby expansion (explicit + cube, incl. `max_depth` and `include_empty` edge cases).
- **Property-based correctness** (`tests/property/`): Hypothesis-generated small DataFrames (nulls, single-row groups, all-null groups, percentile ties) checked against naive `pandas.groupby(...)`/`polars.group_by(...)` ground truth, run against both the plain-expression stats and the `map_batches` stats.
- **Integration** (`tests/integration/`): one module per source type — local Parquet/CSV, in-memory pandas/Polars/Arrow, and a containerized/local Postgres for the SQL path.
- **Perf/synthetic-scale** (`tests/perf/`): generate a cached synthetic Parquet fixture (10-20M rows for routine CI; 100M+ manual/nightly), run a representative spec (multiple groupbys + full stat list + a cube), assert wall-clock/memory ceilings.
- **Tooling:** `pytest`, `pytest-cov`, `hypothesis`, `testcontainers`/docker-compose fixture for Postgres, `pytest-benchmark` for perf tests.

## Key dependencies

- **Core:** `polars`, `pyarrow` (Arrow interop), `connectorx` (SQL source reads), `scipy` (`scipy.stats`: paired t-test p-values, Wilcoxon, KS test, Mann-Whitney U for baseline comparisons).
- **Optional:** `pandas` (only for `.to_pandas()` convenience and `PandasSource` input, not a hard core dependency).
- **Spec/validation:** `pydantic` (v2), `pyyaml`.
- **CLI:** `typer` (+ `rich`).
- **Cloud files:** `fsspec` + `s3fs`/`gcsfs`/`adlfs` as optional extras.
- **Testing:** `pytest`, `pytest-cov`, `hypothesis`, `testcontainers`, `pytest-benchmark`.
- **Tooling:** `ruff`, `mypy`, `pre-commit`.

## Critical files

- `src/crosstab_tool/spec/crosstab_spec.py` — the central contract; get this shape right first.
- `src/crosstab_tool/stats/registry.py` — the extensibility mechanism every future stat (skew, AUC, calibration, lift) depends on.
- `src/crosstab_tool/sources/base.py` — the adapter interface every current/future source must satisfy (and the seam a future DuckDB engine would slot behind).
- `src/crosstab_tool/spec/groupby_spec.py` — explicit + cube logic and the `pl.collect_all()` batching strategy for multi-groupset performance.
- `docs/architecture.md` — durable record of the Polars-native-v1 decision and DuckDB migration path, so future contributors understand why and when to revisit it.
