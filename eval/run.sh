#!/usr/bin/env bash
# Run the logistic-map evaluation.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"

if [ ! -x .venv/bin/python ]; then
  ./bootstrap.sh
fi

exec .venv/bin/python run_eval.py "$@"
