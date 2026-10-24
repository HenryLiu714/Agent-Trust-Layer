#!/usr/bin/env bash
# Install a built wheel into a clean Python 3.12 venv, away from the project's own environment,
# and check that the command it ships runs: `irimi --version`, then `irimi init` into a temporary
# IRIMI_HOME. With a second argument, the version irimi reports must equal it.
#
#   scripts/smoke-test-wheel.sh dist/irimi-0.2.0-py3-none-any.whl [0.2.0]
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: $0 WHEEL [VERSION]" >&2
  exit 2
fi
wheel="$1"
expected="${2:-}"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

uv venv --no-project --python 3.12 "$work/venv"
uv pip install --python "$work/venv/bin/python" "$wheel"

reported="$("$work/venv/bin/irimi" --version)"
echo "$reported"
if [ -n "$expected" ] && [ "$reported" != "irimi $expected" ]; then
  echo "error: the wheel reports '$reported', expected 'irimi $expected'" >&2
  exit 1
fi

IRIMI_HOME="$work/home" "$work/venv/bin/irimi" init
if [ ! -f "$work/home/ca/ca.pem" ]; then
  echo "error: irimi init did not write $work/home/ca/ca.pem" >&2
  exit 1
fi
echo "smoke test passed: $wheel"
