#!/usr/bin/env bash
# Create the project-local venv, install Kuzu and initialize the graph.
#
#   bash scripts/bootstrap.sh [--no-init]
#
# Environment overrides:
#   PYTHON        interpreter used to create the venv (default: python3)
#   DATAFLOW_VENV venv directory (default: <cwd>/.dataflow/.venv)
#   DATAFLOW_DB   database path (default: <cwd>/.dataflow/graph.kuzu)
set -euo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(pwd)}"
PYTHON="${PYTHON:-python3}"
VENV="${DATAFLOW_VENV:-$PROJECT_ROOT/.dataflow/.venv}"
DB="${DATAFLOW_DB:-$PROJECT_ROOT/.dataflow/graph.kuzu}"
RUN_INIT=1
for arg in "$@"; do
  case "$arg" in
    --no-init) RUN_INIT=0 ;;
    -h|--help) sed -n '2,9p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

if ! command -v "$PYTHON" >/dev/null 2>&1; then
  echo "error: $PYTHON not found; set PYTHON to a Python 3.8+ interpreter" >&2
  exit 1
fi

PY_VERSION="$("$PYTHON" -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
if ! "$PYTHON" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 8) else 1)'; then
  echo "error: Python 3.8+ is required (found $PY_VERSION)" >&2
  exit 1
fi

mkdir -p "$PROJECT_ROOT/.dataflow"
if [ ! -x "$VENV/bin/python" ]; then
  echo "creating venv at $VENV"
  "$PYTHON" -m venv "$VENV"
fi

echo "installing kuzu (this may take a moment)"
"$VENV/bin/python" -m pip install --quiet --upgrade pip
"$VENV/bin/python" -m pip install --quiet "kuzu>=0.11,<0.12"

KUZU_VERSION="$("$VENV/bin/python" -c 'from importlib.metadata import version; print(version("kuzu"))')"
echo "kuzu $KUZU_VERSION installed in $VENV"

if [ "$RUN_INIT" -eq 1 ]; then
  "$VENV/bin/python" "$SKILL_DIR/scripts/dataflow.py" --db "$DB" init
fi

cat <<EOF

next steps (run from the project root):
  PY=$VENV/bin/python
  \$PY $SKILL_DIR/scripts/dataflow.py report
  \$PY $SKILL_DIR/scripts/dataflow.py render

database: $DB
EOF
