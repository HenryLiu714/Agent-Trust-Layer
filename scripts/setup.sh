#!/usr/bin/env bash
# One-shot setup for irimi contributors and first-time users.
# Installs uv if missing, creates the project venv, installs irimi in editable mode,
# and generates the local CA. Safe to run more than once.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v uv >/dev/null 2>&1; then
  echo "uv not found. Installing from https://astral.sh/uv ..."
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
fi

echo "Syncing project (this downloads Python 3.14 and dependencies on first run)..."
uv sync

echo
uv run irimi init

echo
echo "Setup complete."
echo "  Run the CLI:   uv run irimi --help"
echo "  Run the tests: uv run pytest"
