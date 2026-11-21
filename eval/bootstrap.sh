#!/usr/bin/env bash
# Create eval/.venv with a Python that Inspect AI supports (>= 3.11).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"

python_bin=""
for candidate in python3.12 python3.13 python3.14 python3.11 python3; do
  if command -v "$candidate" >/dev/null 2>&1; then
    if "$candidate" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)'; then
      python_bin="$candidate"
      break
    fi
  fi
done

if [ -z "$python_bin" ]; then
  echo "Need Python 3.11+ to run Inspect AI (python3 found: $(python3 --version 2>&1))." >&2
  exit 1
fi

if [ ! -d .venv ]; then
  "$python_bin" -m venv .venv
fi

.venv/bin/python -m pip install --quiet --upgrade pip
.venv/bin/python -m pip install --quiet -r requirements.txt
echo "eval environment ready: $("$here/.venv/bin/python" --version) (.venv)"
