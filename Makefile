# Contributor shortcuts. Every target is a one-line `uv` command; see CONTRIBUTING.md.
.DEFAULT_GOAL := help
.PHONY: help setup test test-fast workflows cov live demo lint fmt typecheck check

help:  ## list the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-10s %s\n", $$1, $$2}'

setup:  ## create the venv, install irimi in editable mode, generate the local CA
	scripts/setup.sh

test:  ## run the test suite, in parallel
	uv run pytest -q -n auto

test-fast:  ## the unit tests only: no end-to-end runs, no live calls, no workflow corpus
	uv run pytest -q -n auto -m "not e2e and not live" --ignore=tests/e2e/workflows

workflows:  ## the sample-workflow corpus alone
	uv run pytest -q -n auto tests/e2e/workflows

cov:  ## the test suite with a coverage report
	uv run pytest -q -n auto --cov --cov-report=term

live:  ## the live checks; they call real services and skip without STRIPE_API_KEY
	uv run pytest -q -rs -m live

demo:  ## one sample workflow, bare and then under irimi shadow, with no keys
	uv run python -m tests.e2e.workflows w01_ticket_triage double_refund

lint:  ## ruff: lint and check formatting (what CI runs)
	uv run ruff check .
	uv run ruff format --check .

fmt:  ## ruff: format and apply the safe fixes
	uv run ruff format .
	uv run ruff check --fix .

typecheck:  ## mypy over src/, examples/ and tests/
	uv run mypy

check: lint typecheck test  ## everything CI runs, in CI's order
