# Contributing to irimi

## Prerequisites

- macOS or Linux.
- [`uv`](https://docs.astral.sh/uv/). The setup script prints the install command if it is
  missing. `uv` fetches the Python it needs (3.14 for development, per `.python-version`); you do
  not install Python.
- `make`, optionally. Every `make` target is one `uv` command, listed in the `Makefile`.

## Set up

```
git clone https://github.com/HenryLiu714/Irimi.git
cd Irimi
scripts/setup.sh          # or: make setup
```

That creates `.venv`, installs irimi in editable mode with the dev tools, and runs `irimi init`
to generate the local CA under `~/.irimi/ca/`. Set `IRIMI_HOME` first if you want it elsewhere
(see `.env.example`). Then:

```
uv run irimi --help
```

## Releasing

A `v<version>` tag publishes irimi to PyPI through `.github/workflows/release.yml`, with PyPI
Trusted Publishing, so there is no token to keep (#169). Bump `version` in `pyproject.toml` and
irimi's own entry in `uv.lock`, and add a `## [<version>]` section to `CHANGELOG.md`, in a PR;
once it is merged, tag that commit on `main` and push the tag:

```
git tag v0.1.0 && git push origin v0.1.0
```

The workflow runs the whole CI suite, builds the wheel and sdist, and installs the wheel into a
clean Python 3.12 venv to run `irimi --version` and `irimi init`. It refuses a tag that does not
match `pyproject.toml`, a tag on a commit that is not on `main`, and a version with no
`CHANGELOG.md` section. Only then does it publish, with attestations, and create a GitHub Release
with that section as its notes. A version, once on PyPI, can never be uploaded again, even after it
is deleted.

## Day to day

```
make              # list the targets
make check        # what CI runs: lint, format check, mypy, tests
make test         # uv run pytest -q -n auto
make test-fast    # the unit tests: no end-to-end runs, no live calls, no workflow corpus
make workflows    # the sample-workflow corpus alone
make cov          # the tests with a coverage report
make lint         # uv run ruff check . && uv run ruff format --check .
make fmt          # uv run ruff format . && uv run ruff check --fix .
make typecheck    # uv run mypy
```

`uvx pre-commit install` runs ruff and the prose lint on every commit, with the versions `uv.lock`
pins. Each test has a 120-second limit (`pytest-timeout`), so a hang fails its test.

The suite runs in about five minutes and needs no network and no keys: the engine tests start a
real mitmproxy on a random loopback port against local HTTP servers. `tests/conftest.py` points
`IRIMI_HOME` and the working directory at empty temporary directories, so your own overrides file
and CA are never in play.

The examples (`examples/README.md`) need the official Stripe and Anthropic SDKs, an opt-in group
(`uv sync --group examples`). `make demo EXAMPLE=quickstart` runs one under `irimi shadow` against
fake services, with no keys. Each example is driven end to end on every `uv run pytest -q`:
`tests/e2e/test_quickstart.py`, `test_replay_example.py`, `test_sidecar.py` and `test_demo.py`.

The live checks call real services. `tests/live/test_stripe_test_mode.py` runs the quickstart agent
against Stripe test mode and checks that Stripe holds no refund afterwards. It is marked `live`,
which a plain `uv run pytest` deselects even when `STRIPE_API_KEY` is exported. Run it with
`uv run pytest -m live`; it still skips unless `STRIPE_API_KEY` is set:

```
uv run python -m examples.seed_stripe          # once, outside shadow: seeds a charge
STRIPE_API_KEY=sk_test_... uv run pytest -m live -q -rs tests/live
```

`scripts/live-checks/sidecar.sh` runs the sidecar example and `irimi serve` in two containers. It
needs Docker with compose, `uv` and a test-mode key. It seeds two charges, builds both images,
refunds both charges at once through the app under irimi, checks that irimi stored two ok runs and
that Stripe holds no refund, and tears down:

```
STRIPE_API_KEY=sk_test_... scripts/live-checks/sidecar.sh
```

`examples/replay/README.md` has the replay example's live run: record the agent against Stripe test
mode and the real Anthropic API, replay it with `prompt_half.txt`, and check that the replay names
the model call as where it diverged, the refund as the write that changed, and that Stripe holds no
refund. Run the live checks by hand before a release.

## Sample workflows

`tests/e2e/workflows/` holds eleven small agents, each built to exercise one part of irimi: a
multi-turn LLM tool loop, a nightly batch job, a concurrent queue worker, Slack and Stripe webhook
handlers, database writes behind `@sdk.write_tool`, SSE streaming, internal services, failure paths,
and agents that get around the proxy. A harness runs each scenario bare, under `irimi shadow` and
under `irimi serve`, against a fake internet that answers for the real host names, so irimi's
shipped maps decide every call. Its tests hold every scenario to five universal invariants and
pin what irimi does in each one. `tests/e2e/workflows/README.md` explains the harness.

```
uv run python -m tests.e2e.workflows                             # workflows, and where to start
uv run python -m tests.e2e.workflows w09_scope_gauntlet verbs    # one scenario, every mode
uv run pytest -q -n auto tests/e2e/workflows
```

When a change alters what irimi answers, a workflow test pins the old answer and fails. Update the
pin in the same PR, and say in the PR why the new answer is right.

Every feature is tested on the corpus end to end, as well as in unit tests. Each open issue's
`Since #89 landed` section names the workflows its feature has to run through and the pins it
extends or flips. Extend an existing workflow with a scenario before writing a new agent. A pin
under `KNOWN BUG:` is a known bug: the PR that fixes it updates the pin and removes the marker.

## Where things are

`docs/architecture.md` is the map: each module's job, the layer it sits in, how one request moves
through the engine, and the rules the tests hold the code to. Read it before adding a module.
The short version:

```
src/irimi/
  core/                               vocabulary: exchanges, addresses, bounds, the body parser
  protocol/                           the engine-SDK contract and the trace format
  config/                             where irimi keeps state; private files; the local CA
  sdk/                                what an agent imports; `__init__` is the public surface
  servicemaps/                        the service maps as code, and what a request is by them
  services/                           one spec per service: answers, effects, checks
  record/                             redaction and the run store
  decide/                             targets, the check read, the write log, the policy
  engine/                             the Engine protocol and session core; mitm.py imports mitmproxy
  report/                             what a run prints
  replay/                             re-running a stored run, and comparing two runs
  cli/  __main__.py  testing.py       the command and its composition root; `python -m irimi`
  maps/*.yaml                         the shipped service maps
  fixtures/*.json                     the vendored response objects L1 answers start from
tests/                                unit tests of the modules that need no running engine
tests/engine/  cli/  sdk/             the engine, the commands and the SDK, each run for real
tests/safety/                         the rules every run keeps, one file each (see its README)
tests/e2e/  live/                     whole agents under irimi; `live` calls real services
tests/support/                        the helpers and fakes the tests share; never collected
examples/                             the examples, each with a README; examples/README.md lists them
tests/e2e/workflows/                  the sample workflows, the harness that runs them, their pins
```

Two tests guard the layout itself. `tests/safety/test_import_boundary.py` fails if mitmproxy is imported
outside `engine/mitm.py`, or if a module imports a higher layer, another package of its own
layer, or a module after it in its own package; when you add a module it asks you to place it in
the layer table (and in `docs/architecture.md`, which it checks against the same table).

## Adding a service map

Maps are YAML in `src/irimi/maps/` and need no Python. The README's "Service maps" section is the
schema; the loader refuses anything outside it by name. Things to know before you write one:

- Write `match:` in block style. A flow mapping cannot hold a path containing `{`.
- A write may name a `fixture:`, which is an object in `src/irimi/fixtures/<service>.json`.
  Add the object there first, with its provenance in the file's `_source` entry; a `fixture:`
  the package does not ship answers at L0 and flags the exchange rather than failing.
- A write may name a `fires:` list of the webhook event names the real service would have sent.
  It is free text — the loader checks the key, but there is no table of a service's events to check
  a name against — so name only events the service really sends for that call, and only on a write
  the service's effects table in `src/irimi/services/` models, so a later read and the events agree.
  `tests/test_servicemap.py` pins every shipped list; update it with the map.
- Quote a wildcard host (`"*.posthog.com"`) and any `human:` template containing `#`.
- A live-forwarded kind (`read`, `llm`, `telemetry`) must name its methods and must justify a
  destructive one with `live_because:`. A `default_kind:` may never be a live
  kind. This is THE SCOPE RULE; `servicemaps/rules.py` explains why.
- Shipped maps never set a `target:`. Targets belong in a user's overrides file.
- `tests/test_servicemap.py` pins the shipped maps' hosts, route counts and a round-trip through a
  built wheel; `tests/test_classify.py` pins how each shipped route classifies. Add to both.

## Conventions

- Branch per issue, named `<issue-number>-<short-slug>`. Open a PR against `main` with
  `Closes #<n>` in the body.
- Commit messages say what changed and why, in the imperative. The issue a change closes belongs
  in the commit message and the PR, not in the comment that states the rule: `git blame` finds it.
  The next section says when a comment may cite an issue.
- A safety rule is enforced where the configuration is loaded *and* at the decision it protects.
  One check is a rule-shaped hole.
- Nothing in a mitmproxy hook may raise. Fail closed: answer locally and flag it.
- CI runs lint, format check, mypy and the tests on Ubuntu and macOS, on Python 3.14 and on 3.12,
  the floor `pyproject.toml` promises.

## Writing comments, docstrings and messages

### Comments

1. A comment says **why**: a constraint, an invariant, or a consequence that is not obvious. It
   never says **what** the next line does, or **when** or **how** it came to be.
2. One to three lines is normal. If you need more than six, the content belongs in a docstring or
   in `docs/` (link it by section name).
3. Write plain present-tense sentences in sentence case. No all-caps except constants, and no
   headings made of capitals. Use "-" rather than em-dashes.
4. No history: no "since #NN", "used to", "now", "Phase N", dates, "Decided ...", "this PR". Git
   and the PR hold history.
5. No private references: no design-doc sections or decision numbers, no planning ids, no "use
   case N", no agent-instruction files, no Notion. If a design decision matters, it lives in
   `docs/architecture.md`.
6. Issue numbers: at most one per comment, only as a trailing `See #NN.`, and only when the issue
   holds a reproduction or discussion that a reader needs. The comment must make sense with the
   reference deleted. Never use an issue number as a noun ("#30's bug"). Never put one in a
   docstring summary line, help text, a log or error message, printed output or a shipped map.
   Prefer pointing at the test that pins the behaviour: `Pinned by tests/test_overlay.py::test_x`.
7. Don't defend the absence of duplication ("the one place", "rather than a second spelling",
   "cannot drift"). The structure shows it.
8. Use the project's terms: a faked write, answered by irimi, replay (re-running a stored run
   only), retry (an idempotent repeat), target, fake (a write tool's replacement), echo and
   fixture, precondition check. Do not coin metaphors (door, world, untruth) in prose.

### Docstrings

PEP 257, Google style where a body is needed.

1. The summary line is one line of at most 100 characters, saying what the function returns or
   does: "Return the run's summary lines.", not "What a run prints: ...".
2. Add a body only when the contract is not obvious from the signature: arguments with
   constraints, return semantics, raised exceptions, thread-safety, invariants. As a rough limit,
   the body is no longer than the code.
3. A module docstring is its purpose (one paragraph) plus a bullet list of guarantees, 15 lines at
   most. Protocol and design rationale go in `docs/`.
4. Tables are fine where they replace prose, as the decision tables in `sdk/_tools.py` and
   `replay.policy.ReplayPolicy` do.

### User-facing text

1. CLI lines to the terminal: `error: <what happened>[; <what to do>]`, `warning: ...`,
   `hint: ...`. Lower case, one line, no trailing period, flags named exactly as typed. No
   `WARNING:` or `Note:`.
2. Logs: `irimi: <what happened>[; <consequence>]` on every logger, the SDK's included.
3. Never show issue numbers, phase numbers or internal level codes, unless the output has a
   documented legend (an `irimi --help` epilog or the README).
4. argparse help is a lower-case verb phrase with no trailing period, two lines or fewer. Every
   subcommand gets a `description=` and an `epilog=` with one example.
5. Output uses one arrow, `->`, which is ASCII-safe.
6. Comments in a shipped map are written for a user copying the map: explain the map's choices in
   the map's own terms, with no issue or design references.

### Enforcement

`tests/test_prose_style.py` scans comments and docstrings in `src/`, `tests/` and `examples/`, and
every line of the shipped maps, for private references, history markers, all-caps runs and issue
numbers. It holds each file to the counts in `tests/prose_baseline/*.json`, which may only fall:

- A change that adds a match fails the test. Rewrite the comment instead of raising a count.
- A change that removes matches fails the test until you lower the file's counts to match (delete
  an entry that reaches zero). The failure message prints the counts to write.
- A file that moves or is renamed takes its baseline entry with it, with the same counts.
