# irimi

Shadow mode for AI agents: a local MITM proxy where reads are real and writes are virtual.
Python 3.12+, managed with `uv`. See `docs/architecture.md` for the module map and
`CONTRIBUTING.md` for setup and conventions.

## Commands

- `make check` runs everything CI runs: `ruff check`, `ruff format --check`, `mypy`, `pytest`.
- `uv run pytest -q` for the tests alone (about five minutes, no network, no keys).
- `uv run pytest -q tests/e2e/workflows` for the sample-workflow corpus alone; see
  `tests/e2e/workflows/README.md`.
- `uv run irimi --help` for the CLI.

## Rules that tests enforce

`tests/safety/README.md` names the test that holds each rule and what breaks if it fails.

- `mitmproxy` is imported only in `src/irimi/engine/mitm.py`. Everything else uses the `Engine`
  protocol.
- A module imports only from lower layers. The layer table is in `tests/safety/test_import_boundary.py`
  and `docs/architecture.md`; a new module has to be placed in both.
- Nothing in a mitmproxy hook may raise: a raised hook forwards the flow, and a forwarded write
  escapes shadow mode. Fail closed and flag it.
- THE SCOPE RULE (`servicemaps/rules.py`): no classification that forwards live may apply to an
  unsafe method it did not name explicitly. Enforce a safety rule at load time and at the decision.
- Every response irimi decided carries `Irimi-Answered-By`; a live forward carries none.
- Comments follow CONTRIBUTING's style guide; `tests/test_prose_style.py` holds the counts to a
  ratchet.

## Conventions

- Branch per issue, `<issue-number>-<short-slug>`; PRs against `main` with `Closes #<n>`.
- Shipped maps in `src/irimi/maps/` never set a `target:`.
- Comments explain why, in at most three lines. No history, no phase numbers, no private-doc
  references. Issue numbers only as a trailing "See #NN." when the issue is needed to understand
  the code. Never in messages, help text or shipped maps. `tests/test_prose_style.py` enforces
  this. See CONTRIBUTING.md "Writing comments, docstrings and messages".
- Every feature is tested end to end on the sample workflows, not only in unit tests: drive it
  through the real `irimi shadow`, and extend the workflow scenarios and pins that cover the
  behaviour you change rather than writing a new agent, and keep the five invariants holding. A
  fix that flips a `KNOWN BUG:` pin updates the pin and drops the marker in the same PR.
