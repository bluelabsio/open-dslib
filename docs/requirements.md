# Crosstab Tool — Requirements

This document captures the functional requirements as gathered directly from the requester,
independent of the implementation approach (see `docs/implementation-plan.md` for the
architecture/design that satisfies these requirements).

## Purpose

Given a set of ML model scores, aggregate them by various covariate features of interest and
produce descriptive statistics of those scores across those aggregations ("crosstabs").

## Functional requirements

1. **Scale.** Must handle datasets of 100M+ rows and support many aggregations/groupbys
   efficiently.

2. **Data sources — must support a mix, not a single fixed source:**
   - Flat files (Parquet/CSV) on local disk or cloud object storage.
   - SQL warehouses (e.g. Snowflake, BigQuery, Redshift, Postgres).
   - In-memory DataFrames already produced by an upstream Python pipeline/notebook.
   - The same groupby + stats specification should be able to run against any of these
     sources without rewriting logic.

3. **Descriptive statistics:**
   - v1 scope: basic summary statistics — count, mean, std, min/max, percentiles
     (e.g. median, p90).
   - The package must be designed so new statistics can be added easily later (e.g.
     distribution-shape stats like skew/kurtosis, or label-based performance metrics like
     AUC/calibration/lift once ground-truth labels are available) without modifying core
     execution code.

4. **Groupby shape — must support both:**
   - An explicit, user-defined list of groupby specs (e.g. by region, by region+product, by
     age_bucket), each computed and reported as its own crosstab.
   - Auto-generation of combinations/subsets from a given covariate set (cube/rollup style),
     configurable by the user (e.g. max depth).

5. **Counterfactual / baseline comparison (explicit hard requirement).** The tool must
   optionally compare each group's scores against a user-defined counterfactual/baseline —
   most commonly a prior model vintage's scores for the same population — producing:
   - **Difference metrics** (e.g. mean difference, median difference).
   - **Statistical tests of difference** (e.g. paired t-test, Wilcoxon signed-rank for
     row-matched/paired comparisons; KS test, Mann-Whitney U for unpaired/distributional
     comparisons when the baseline isn't row-matched to the current data).
   - The baseline may come from another column in the same source, or from an entirely
     separate dataset (joined on entity keys for a paired comparison, or compared at the
     group-distribution level when no join key is available).

6. **Usage modes — must support both:**
   - As a Python library, called programmatically in notebooks/scripts.
   - As a CLI tool, for batch/scheduled jobs.
   - Core logic must be a single reusable library; the CLI is a thin wrapper over the same
     API — not a second implementation.

7. **Configuration.** Groupby specs, stats, comparison settings, and data source must be
   specifiable via YAML/JSON config files (declarative, reproducible, suited to CLI/batch
   use). The Python API constructs the same underlying spec objects directly; config files
   are a serialization of that spec, not a separate schema to maintain.

8. **Output format.** Not fully decided at requirements time — at minimum, results must be
   returned as an in-memory DataFrame; file export (Parquet/CSV, possibly Excel/HTML later)
   should be pluggable but was explicitly left open for later refinement.

## Non-functional / process requirements

- **Engine choice must be documented, including tradeoffs.** The requester asked that the
  rationale for the v1 engine decision (Polars-native, not DuckDB) and the pros/cons of an
  eventual migration to DuckDB be written into project documentation, not just discussed in
  conversation. This lives in `docs/implementation-plan.md` (Engine strategy section) and is
  intended to be carried into `docs/architecture.md` as the project matures.
- **New standalone repo**, no integration with an existing codebase required.
