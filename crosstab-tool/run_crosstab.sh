#!/usr/bin/env bash
# Convenience wrapper around `xtab validate` + `xtab run` for a given config file.
#
# Usage:
#   ./run_crosstab.sh [path/to/config.yaml] [extra xtab-run args...]
#
# Examples:
#   ./run_crosstab.sh
#   ./run_crosstab.sh examples/configs/my_crosstab.yaml
#   ./run_crosstab.sh examples/configs/my_crosstab.yaml --out results/ --format csv
#
# Defaults to examples/configs/my_crosstab.yaml if no config path is given. Activates
# ./.venv if present; otherwise assumes `xtab` is already on PATH (e.g. a venv you've
# activated yourself, or a global install).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CONFIG="${1:-examples/configs/my_crosstab.yaml}"
shift || true

if [ -f ".venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

if ! command -v xtab >/dev/null 2>&1; then
  echo "Error: 'xtab' not found on PATH. Set up the venv first, e.g.:" >&2
  echo "  python3.12 -m venv .venv && source .venv/bin/activate && pip install -e \".[dev,sql]\"" >&2
  exit 1
fi

if [ ! -f "$CONFIG" ]; then
  echo "Error: config file not found: $CONFIG" >&2
  exit 1
fi

echo "==> Validating $CONFIG"
xtab validate --config "$CONFIG"

echo "==> Running $CONFIG"
xtab run --config "$CONFIG" "$@"
