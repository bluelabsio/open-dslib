# crosstab-tool

Descriptive-statistics crosstabs for ML model scores: aggregate scores across covariate
groupings (explicit or auto-generated combinations), compute extensible summary statistics,
and optionally compare against a counterfactual/baseline (e.g. a prior model vintage) with
difference metrics and statistical tests.

Built natively on [Polars](https://pola.rs). See `docs/architecture.md` for the engine
strategy and design decisions.

## Status

M0-M4 complete: core spec + Polars file engine + basic stats, the in-memory DataFrame
adapter, counterfactual/baseline comparison, and the SQL source. See
`docs/implementation-plan.md` for milestone sequencing. Not yet built: the CLI (M5),
cube/auto-groupby (M6), and exporters/extensibility polish (M7).

## Install (dev)

```bash
pip install -e ".[dev]"
```

## Usage

```python
from crosstab_tool import run_crosstab, CrosstabSpec

result = run_crosstab(CrosstabSpec.model_validate({...}))
```

The `xtab` CLI (referenced in `examples/notebooks/`) is the M5 target interface and
isn't built yet -- for now, `CrosstabSpec` is constructed directly (or via
`CrosstabSpec.model_validate(yaml.safe_load(...))`, since the YAML files in
`examples/configs/` already round-trip against the real spec model).
