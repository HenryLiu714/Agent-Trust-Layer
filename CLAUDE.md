# irimi

Shadow mode for AI agents: a local MITM proxy where reads are real and writes are virtual.
Python 3.12+, managed with `uv`. See `docs/architecture.md` for the module map and
`CONTRIBUTING.md` for setup and conventions.

## Commands

- `make check` runs everything CI runs: `ruff check`, `ruff format --check`, `mypy`, `pytest`.
- `uv run pytest -q` for the tests alone (about ten seconds, no network, no keys).
- `uv run irimi --help` for the CLI.

## Rules that tests enforce

- `mitmproxy` is imported only in `src/irimi/engine/mitm.py`. Everything else uses the `Engine`
  protocol.
- A module imports only from lower layers. The layer table is in `tests/test_import_boundary.py`
  and `docs/architecture.md`; a new module has to be placed in both.
- Nothing in a mitmproxy hook may raise: a raised hook forwards the flow, and a forwarded write
  escapes shadow mode. Fail closed and flag it.
- THE SCOPE RULE (`servicemap/rules.py`): no classification that forwards live may apply to a
  method it did not name explicitly. Enforce a safety rule at load time and at the decision.
- Every response irimi decided carries `Irimi-Answered-By`; a live forward carries none.

## Conventions

- Branch per issue, `<issue-number>-<short-slug>`; PRs against `main` with `Closes #<n>`.
- Shipped maps in `src/irimi/maps/` never set a `target:`.
- Comments state the rule and name the issue that motivated it.
