#!/usr/bin/env bash
# One-shot setup for irimi contributors and first-time users.
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

# mitmproxy's dependencies do not build on a free-threaded Python, which uv may pick for a bare 3.14.
export UV_PYTHON="${UV_PYTHON:-3.14+gil}"

echo "Syncing project (this downloads Python 3.14 and dependencies on first run)..."
uv sync

echo
uv run irimi init

echo
echo "Setup complete."
echo "  Run the CLI:        uv run irimi --help"
echo "  Run every check:    make check      (or: uv run pytest)"
echo "  Try the example:    uv sync --group examples, then see CONTRIBUTING.md"
