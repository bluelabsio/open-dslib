# crosstab-tool

Descriptive-statistics crosstabs for ML model scores: aggregate scores across covariate
groupings (explicit or auto-generated combinations), compute extensible summary statistics,
and optionally compare against a counterfactual/baseline (e.g. a prior model vintage) with
difference metrics and statistical tests.

Built natively on [Polars](https://pola.rs). See `docs/architecture.md` for the engine
strategy and design decisions.

## Status

Early scaffolding (M0). See the implementation plan for milestone sequencing.

## Install (dev)

```bash
pip install -e ".[dev]"
```

## Usage

```python
from crosstab_tool import run_crosstab

result = run_crosstab(spec)
```

```bash
xtab run --config examples/configs/explicit_groupbys.yaml
```
