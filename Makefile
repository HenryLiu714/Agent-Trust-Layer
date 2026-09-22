# Contributor shortcuts. Every target is a one-line `uv` command; see CONTRIBUTING.md.
.PHONY: setup test lint fmt typecheck check

setup:  ## create the venv, install irimi in editable mode, generate the local CA
	scripts/setup.sh

test:  ## run the test suite
	uv run pytest -q

lint:  ## ruff: lint and check formatting (what CI runs)
	uv run ruff check .
	uv run ruff format --check .

fmt:  ## ruff: format and apply the safe fixes
	uv run ruff format .
	uv run ruff check --fix .

typecheck:  ## mypy over src/
	uv run mypy

check: lint typecheck test  ## everything CI runs, in CI's order
