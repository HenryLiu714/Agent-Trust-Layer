#!/usr/bin/env bash
# One-shot setup for irimi contributors. To use irimi, install it from PyPI (see the README).
# Creates the project venv, installs irimi in editable mode, and generates the local CA.
# Needs uv; it does not install it for you. Safe to run more than once.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v uv >/dev/null 2>&1; then
  echo "error: uv not found; install it, then run this script again:" >&2
  echo "  curl -LsSf https://astral.sh/uv/install.sh | sh" >&2
  echo "  (other ways to install it: https://docs.astral.sh/uv/getting-started/installation/)" >&2
  exit 1
fi

# mitmproxy's dependencies do not build on a free-threaded Python, which uv picks for a bare 3.14
# when it manages one. `3.14+gil` matches only an installed Python, so install a GIL 3.14 first.
if [ -z "${UV_PYTHON:-}" ]; then
  uv python find 3.14+gil >/dev/null 2>&1 || uv python install 3.14
  export UV_PYTHON=3.14+gil
fi

echo "Syncing project (this downloads Python 3.14 and dependencies on first run)..."
uv sync

echo
uv run irimi init

echo
echo "Setup complete. Next, with no keys and no network:"
echo "  make demo           an agent under irimi shadow, against fake services"
echo "  make check          everything CI runs"
echo "  uv run irimi --help"
