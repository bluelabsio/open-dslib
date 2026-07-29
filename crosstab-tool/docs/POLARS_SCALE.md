# How the Polars-only approach meets the scale requirement

This document expands on requirement 1 in `docs/requirements.md` — *"Must handle datasets of
100M+ rows and support many aggregations/groupbys efficiently"* — and explains, mechanically,
how a Polars-native engine (no DuckDB, no Spark, no chunked pandas) satisfies it. The high-level
engine decision and its tradeoffs already live in `docs/implementation-plan.md` (Engine
strategy section) and will move into `docs/architecture.md`; this document goes one level
deeper into *why the specific Polars mechanisms actually get you to 100M+ rows*, since that
detail wasn't spelled out there.

There are really two separate scale problems buried in requirement 1, and Polars addresses them
with two different mechanisms:

1. **Row scale** — one aggregation over 100M+ rows must complete without blowing up memory or
   taking forever.
2. **Aggregation/groupby-count scale** — the tool must run *many* groupby specs (explicit lists,
   or cube/rollup auto-generated combinations) efficiently, not just one.

## 1. Row scale: the lazy, pushdown-optimized query engine

The core reason a single-engine Polars approach can handle 100M+ rows is that Polars never
requires you to materialize the full dataset in memory to do an aggregation — provided the
pipeline is built the right way.

### Lazy scanning instead of eager loading

`sources/files.py` uses `pl.scan_parquet(...)` / `pl.scan_csv(...)`, not `pl.read_parquet`.
The difference matters at 100M+ rows:

- `pl.read_*` eagerly loads the full file into a `DataFrame` in RAM before any filtering or
  grouping happens.
- `pl.scan_*` returns a `LazyFrame` — a *query plan*, not data. Nothing is read off disk until
  `.collect()` is called on the assembled pipeline (scan → filter → group_by → agg).

Because `engine/polars_engine.py` builds the entire pipeline (scan, filters, group_by, stat
expressions) before ever calling `.collect()`, Polars' query optimizer gets to see the whole
plan at once and apply:

- **Projection pushdown** — if the spec's stats/groupby only touch 5 of a Parquet file's 40
  columns, Polars reads only those 5 columns' column chunks off disk. For a wide file, this
  alone can cut I/O by an order of magnitude before any row-count concern even applies.
- **Predicate pushdown** — if `CrosstabSpec.filters` narrows rows (e.g. `region == "west"`),
  Parquet row-group statistics let Polars skip entire row groups without decompressing them,
  and for CSV it avoids materializing rows that would just be filtered out.
- **Multi-threaded, vectorized execution** — the physical `group_by().agg()` execution is
  written in Rust and parallelizes across cores automatically; this is what makes a 100M-row
  aggregation take seconds rather than minutes on a single machine, without the tool writing any
  concurrency code itself.

### Streaming for larger-than-memory data

Even with pushdown, 100M+ rows of relevant columns can still exceed available RAM, especially
on a laptop-class machine or a shared batch node. This is what Polars' **streaming engine**
solves: instead of computing the full pipeline in one shot, it processes the data in chunks
(morsels), maintaining only partial/intermediate aggregation state in memory at any time, and
spills what it needs to. For an operation family that's fundamentally reduction-shaped — count,
sum (for mean/std), min, max, and approximate percentiles — this is exactly the workload
streaming aggregation is built for: the *output* is small (one row per group) even when the
*input* is huge, so a chunked/streaming execution model never needs the whole 100M-row table
resident at once.

Concretely, `engine/polars_engine.py`'s job at row-scale is narrow: build the LazyFrame
pipeline correctly (scan with only needed columns → apply filters → `group_by(cols).agg(exprs)`)
and collect it in streaming mode. It does not need custom chunking logic, a manual
out-of-core spill strategy, or a separate "big data path" vs. "small data path" — one code path
handles both a 10K-row in-memory pandas DataFrame (M2's `PandasSource`) and a 100M-row Parquet
scan (M1's file source) identically, because both become the same `LazyFrame` abstraction before
execution.

### What this does *not* solve (documented honestly)

- **SQL warehouse sources.** `SQLSource` (M4) uses `pl.read_database_uri` / ConnectorX, which
  pulls rows to the client before Polars aggregates them — there is no pushdown of the
  `GROUP BY` into Snowflake/BigQuery/Redshift/Postgres itself. At 100M+ rows, this path only
  scales if the user's query already filters/pre-aggregates before Polars sees it. This is the
  single biggest scale caveat of a Polars-only design and is the main argument for the
  documented future DuckDB migration (which could push aggregation into the warehouse, or at
  minimum give a faster local re-aggregation layer over a ConnectorX pull).
- **Wilcoxon/KS/Mann-Whitney comparison paths.** These require `.implode()`-ing raw per-group
  score arrays for a rank-based test. A group with millions of rows would build a very large
  in-memory list despite the rest of the pipeline being aggregate-only. This is why
  `core/validation.py` has a `max_sample_size` guardrail (deterministic sampling before
  `.implode()`) — it's a targeted exception to the "stay aggregate-only" scaling story, not a
  gap in it.

### Mitigating the SQL warehouse gap: stratified sampling

The most direct fix for the SQL warehouse gap is generating real aggregation SQL
(`SELECT groupcols, COUNT(*), AVG(score), ... GROUP BY ...`) so the `GROUP BY` executes inside
the warehouse and only the small aggregated result crosses the wire. That is not always
available or worth building for v1 (not every stat is trivially SQL-expressible across every
warehouse dialect, and it reintroduces the "second dialect" complexity the Polars-native
decision was explicitly trying to avoid). Where the user is willing to accept an approximate,
sampled read via `pl.read_database_uri`/ConnectorX instead, **stratified sampling** is a
practical way to control both the total volume pulled and which groups stay well-represented.

**Why proportional sampling isn't enough.** Uniformly sampling 50% of a 100M-row table also
takes 50% of every group — a rare group with 40 rows nationally is left with ~20 rows, which may
or may not be a problem depending on how much you care about that group's stats. The fix isn't
"sample less aggressively," it's changing *how* the sample is allocated across groups.

**Floor-then-cap allocation.** Stratify by the same columns the groupby spec operates over
(ideally the finest-grained groupset — e.g. a cube's deepest combination — since coarser
groupings inherit adequate coverage from their finer children), then apply an allocation rule
instead of a flat percentage:

- If a group's row count is at or below a configurable floor (e.g. 100K rows), take **all** of
  it — no sampling loss for rare/small groups.
- If a group's row count exceeds the floor, subsample it down to a cap (flat, or proportional to
  size above the floor) so the total pulled volume stays bounded.

**How this gets pushed into the warehouse rather than done client-side, in two queries:**

1. A cheap pre-query to get group sizes. This is itself a `GROUP BY`, so — unlike the raw data
   pull — it's something the warehouse can compute and return a tiny result for regardless of
   the 100M+ row count:

   ```sql
   SELECT group_cols, COUNT(*) AS n FROM t GROUP BY group_cols
   ```

2. A sampling query using per-group row numbering, driven by the group sizes from step 1:

   ```sql
   WITH ranked AS (
     SELECT *, ROW_NUMBER() OVER (PARTITION BY group_cols ORDER BY <sort_key>) AS rn
     FROM t
   )
   SELECT * FROM ranked
   JOIN group_counts USING (group_cols)
   WHERE rn <= CASE WHEN group_counts.n <= 100000
                     THEN group_counts.n
                     ELSE <cap> END
   ```

**Two practical caveats:**

- `ORDER BY RANDOM()` as the row-numbering sort key forces a real per-partition sort, which has
  meaningful cost even inside the warehouse at 100M+ rows. A cheaper, reproducible alternative is
  **hash-based deterministic sampling** — e.g. `WHERE MOD(ABS(HASH(id)), 1000) < threshold` for
  rows in groups above the floor, `UNION ALL`'d with an unfiltered pull of rows in groups at or
  below the floor. No sort required, and — importantly for the CLI/batch reproducibility goal in
  requirement 7 — the same config produces the same sample on every run, since it's a
  deterministic function of the row's key rather than a fresh random draw.
- Stats computed on a stratified sample are **sample-level, not population-reweighted**. If a
  group was subsampled below the floor, its `count` reflects the sample, not the true population
  count, and `min`/`max` are no longer guaranteed to be the population's true min/max. Getting
  population-level estimates back out would require tracking each row's sampling fraction and
  applying an inverse-probability (Horvitz–Thompson-style) reweighting — worth calling out as a
  possible extension, but not needed unless a future user explicitly cares about population-level
  (rather than sample-level) counts for oversampled groups.

**Where this fits the tool's design.** A `SamplingSpec` (e.g.
`{strategy: "stratified", by: <groupby cols>, min_per_group: 100_000, target_total_rows: 50_000_000}`)
that `SQLSource` compiles into the two-query pattern above before handing the resulting query to
ConnectorX — additive to the existing `sources/sql.py` design from `docs/implementation-plan.md`,
not a rework of it, and consistent with the rest of the tool's declarative-config philosophy
(requirement 7): the sampling strategy is part of the reproducible spec, not an ad hoc query
tweak.

## 2. Aggregation/groupby-count scale: batched query execution, not serial looping

The second half of requirement 1 — "support many aggregations/groupbys efficiently" — is a
different axis from row count. A spec can ask for:

- an explicit list of groupby specs (e.g. `[region]`, `[region, product]`, `[age_bucket]`), or
- a cube/rollup auto-generated from a covariate set, which for `k` covariates can mean up to
  `2^k` distinct groupset combinations.

The naive way to run N groupset queries against a 100M-row source is a Python `for` loop calling
`.collect()` N times — each iteration re-scanning/re-filtering the source from scratch. That is
the approach that would *not* scale: N full scans of a 100M-row file for N groupsets.

Polars avoids this with `pl.collect_all()`. Instead of building one `LazyFrame` and collecting
it immediately, `engine/polars_engine.py` builds all N groupset pipelines as separate
`LazyFrame` query plans (they can share the same upstream scan/filter subplan) and hands the
whole list to `pl.collect_all([lf1, lf2, ..., lfN])` in one call. This lets Polars:

- overlap execution of independent groupset aggregations rather than running them strictly
  serially, using otherwise-idle cores/IO bandwidth while one groupset's aggregation is
  bottlenecked on something another isn't;
- in favorable cases, recognize and reuse a common scan/filter prefix across plans instead of
  re-reading the source file once per groupset.

This is *not* a single-scan guarantee the way SQL's `GROUPING SETS`/`CUBE` is (a true DuckDB/SQL
engine could, in principle, compute an entire cube in one pass over the data) — that gap is
explicitly called out in `docs/implementation-plan.md` as the most consequential reason a future
DuckDB migration might be warranted if cube workloads become dominant at 100M+ row scale. But
for v1, batching via `collect_all()` is what keeps "many groupbys" from turning into "many times
the runtime" — it converts an O(N) series of full-cost scans into N plans executed with shared
scheduling and, where possible, shared I/O.

`core/validation.py`'s cardinality guardrail (warn/error when groupset-count × estimated
cardinality exceeds a configurable threshold) exists precisely because this axis is
multiplicative with row-count cost: a `CubeSpec` over a covariate set with high-cardinality
columns can generate an enormous number of tiny groups even before hitting row-scale limits, so
the tool validates against combinatorial blowup rather than relying on collect_all() alone to
absorb arbitrary N.

## Summary: why one engine is enough for v1's scale target

| Scale dimension | Mechanism | Where it lives |
|---|---|---|
| Wide files, few needed columns | Projection pushdown | `pl.scan_parquet`/`scan_csv` lazy plan |
| Filtered subsets of 100M+ rows | Predicate pushdown | Same lazy plan, `CrosstabSpec.filters` |
| Raw aggregation compute at 100M+ rows | Multi-threaded vectorized `group_by().agg()` | Polars' Rust execution engine |
| Data larger than RAM | Streaming/chunked collection | `.collect(streaming=True)`-style execution in `engine/polars_engine.py` |
| Many groupby specs / cube combinations | Batched, overlapped execution | `pl.collect_all()` over all groupset `LazyFrame`s |
| Combinatorial groupset explosion | Cardinality guardrail (fail fast, not silently slow) | `core/validation.py` |
| Distributional comparison tests on huge groups | Deterministic pre-sampling before materializing arrays | `max_sample_size` guardrail, `core/validation.py` |
| SQL warehouse volume with rare-group protection | Floor-then-cap stratified sampling, pushed into the warehouse via a two-query pattern | `SamplingSpec` in `sources/sql.py` |

The throughline: every mechanism above keeps the *expensive* part (touching the 100M+ raw rows)
inside Polars' lazy, pushdown-optimized, multi-threaded, streaming-capable execution engine, and
only ever hands a *small* (group-count-sized, not row-count-sized) result to Python-level
post-processing (scipy stat tests, pydantic validation, export formatting). That's the general
principle a Polars-only v1 leans on to hit the 100M+ row / many-groupby requirement without
needing a second SQL engine underneath it — and it's also exactly why the two known gaps (SQL
warehouse pushdown, and raw-array-requiring nonparametric tests) are the two places that
principle breaks down, which is why they're called out explicitly rather than left as silent
limitations.
