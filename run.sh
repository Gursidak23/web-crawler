#!/usr/bin/env bash
#
# One-command launcher for the Moonshot Web Crawler.
#
# Creates an isolated virtual environment (so the crawler's dependencies never
# clash with your system / global Python packages), installs the project into
# it, then starts the API + dashboard. Safe to re-run: the venv and the install
# step are created only once.
#
#   bash run.sh                                   # serve the dashboard on :8010
#   bash run.sh --seeds https://example.com --max-pages 100
#   bash run.sh --help                            # all options (forwarded to run.py)
#
# Requires Python 3.11+ . Override the interpreter with:
#   PYTHON=python3.12 bash run.sh
#
set -euo pipefail
cd "$(dirname "$0")"

PYTHON_BIN="${PYTHON:-python3.11}"
VENV_DIR=".venv"

# If unpacked from a mail-safe zip, JS assets ship as .txt (mail filters block
# .js). Restore each one - the dashboard bundle plus its vendored Chart.js and
# Tailwind - to its .js name so the web UI loads. (run.py does this too.)
for txt in \
  crawler/api/static/app.txt \
  crawler/api/static/vendor/chart.umd.min.txt \
  crawler/api/static/vendor/tailwind.txt; do
  js="${txt%.txt}.js"
  if [ -f "$txt" ] && [ ! -f "$js" ]; then
    cp "$txt" "$js"
  fi
done

# Fall back to python3 if the requested interpreter isn't on PATH.
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  PYTHON_BIN="python3"
fi

FRESH=0
if [ ! -d "$VENV_DIR" ]; then
  echo ">> Creating virtualenv ($("$PYTHON_BIN" --version 2>&1)) in $VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
  FRESH=1
fi

# Resolve the venv interpreter (Linux/macOS vs Git-Bash on Windows).
if [ -x "$VENV_DIR/bin/python" ]; then
  VENV_PY="$VENV_DIR/bin/python"
else
  VENV_PY="$VENV_DIR/Scripts/python.exe"
fi

# Install the project + dependencies only when needed.
if [ "$FRESH" = "1" ] || ! "$VENV_PY" -c "import crawler" >/dev/null 2>&1; then
  echo ">> Installing dependencies (first run only)"
  "$VENV_PY" -m pip install --upgrade pip
  "$VENV_PY" -m pip install -e .
fi

echo ">> Starting crawler - dashboard at http://localhost:8010/  (Ctrl+C to stop)"
exec "$VENV_PY" run.py "$@"
