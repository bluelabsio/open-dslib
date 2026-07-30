# crosstab-tool

Descriptive-statistics crosstabs for ML model scores: aggregate scores across covariate
groupings (explicit or auto-generated combinations), compute extensible summary statistics,
and optionally compare against a counterfactual/baseline (e.g. a prior model vintage) with
difference metrics and statistical tests.

Built natively on [Polars](https://pola.rs). See `docs/architecture.md` for the engine
strategy and design decisions.

## Status

M0-M6 complete: core spec + Polars file engine + basic stats, the in-memory DataFrame
adapter, counterfactual/baseline comparison, the SQL source, the `xtab` CLI, and
cube/auto-groupby. See `docs/implementation-plan.md` for milestone sequencing. Not yet
built: exporters/extensibility polish (M7).

## Install (dev)

```bash
pip install -e ".[dev]"
```

## Usage

As a library:

```python
from crosstab_tool import run_crosstab, CrosstabSpec

result = run_crosstab(CrosstabSpec.model_validate({...}))
```

As a CLI, config-driven (see `examples/configs/` for the full shape of a config file):

```bash
xtab run --config examples/configs/explicit_groupbys.yaml
xtab run --config path/to/config.yaml --out results/ --format parquet
xtab validate --config path/to/config.yaml   # schema/column/dtype checks, no execution
xtab schema                                   # CrosstabSpec's JSON Schema, e.g. for editor autocompletion
```
