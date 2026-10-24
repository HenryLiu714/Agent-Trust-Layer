# Contributing to irimi

Thank you for helping. This page covers how to propose a change, how to set up and test, and the
conventions the code follows.

## How to contribute

- **Bugs.** Open an issue with the bug template, or send a pull request with the fix and a test
  that fails without it. A write that reached the real service has its own template. Report a
  security problem privately, as [`SECURITY.md`](SECURITY.md) says.
- **Features and new services.** Open an issue first, so we can agree on the shape before you
  write the code.
- **Pull requests.** Fork the repository, make your change on a branch, and open a pull request
  against `main`. Name the branch `<issue-number>-<short-slug>` when there is an issue, and put
  `Closes #<n>` in the pull request. CI must pass before review: lint, format check, mypy and the
  tests, on Ubuntu and macOS.
- **License.** Contributions are licensed under Apache-2.0, the project's license
  ([`LICENSE`](LICENSE)).
- **Conduct.** Everyone taking part follows the [code of conduct](CODE_OF_CONDUCT.md).

## Prerequisites

- macOS or Linux.
- [`uv`](https://docs.astral.sh/uv/). The setup script prints the install command if it is
  missing. `uv` fetches the Python it needs (3.14 for development, per `.python-version`); you do
  not install Python.
- `make`, optionally. Every `make` target is one `uv` command, listed in the `Makefile`.

## Set up

```
git clone https://github.com/<you>/Irimi.git
cd Irimi
scripts/setup.sh          # or: make setup
```

That creates `.venv`, installs irimi in editable mode with the dev tools, and runs `irimi init`
to generate the local CA under `~/.irimi/ca/`. Set `IRIMI_HOME` first if you want it elsewhere
(see `.env.example`). Then `make demo` runs an agent under `irimi shadow` against fake services,
with no keys and no network.

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

The suite (about 7,000 tests) needs no network and no keys. On the maintainer's laptop it takes
about six and a half minutes serially (`uv run pytest -q`) and about two and a half with
`make test`, which runs it in parallel. The engine tests start a real mitmproxy on a random
loopback port against local HTTP servers. `tests/conftest.py` points `IRIMI_HOME` and the working
directory at empty temporary directories, so your own overrides file and CA are never in play.

The examples ([`examples/README.md`](examples/README.md)) need the official Stripe and Anthropic
SDKs, an opt-in group (`uv sync --group examples`). `make demo EXAMPLE=quickstart` runs one under
`irimi shadow` against fake services, with no keys. Each example is driven end to end on every test
run: `tests/e2e/test_quickstart.py`, `test_replay_example.py`, `test_sidecar.py` and
`test_demo.py`.

### Live checks

These are optional, call real services, and need test-mode keys. Run them before a release.

`tests/live/test_stripe_test_mode.py` runs the quickstart agent against Stripe test mode and
checks that Stripe holds no refund afterwards. It is marked `live`, which a plain `uv run pytest`
deselects even when `STRIPE_API_KEY` is exported:

```
uv run python -m examples.seed_stripe          # once, outside shadow: seeds a charge
STRIPE_API_KEY=sk_test_... uv run pytest -m live -q -rs tests/live
```

`scripts/live-checks/sidecar.sh` runs the sidecar example and `irimi serve` in two containers. It
needs Docker with compose, `uv` and a test-mode key. It seeds two charges, refunds both at once
through the app under irimi, checks that irimi stored two runs and that Stripe holds no refund, and
tears down:

```
STRIPE_API_KEY=sk_test_... scripts/live-checks/sidecar.sh
```

[`examples/replay/README.md`](examples/replay/README.md) has the replay example's live run, against
Stripe test mode and the real Anthropic API.

## Sample workflows

`tests/e2e/workflows/` holds eleven small agents, each built to exercise one part of irimi, and a
harness that runs each scenario bare, under `irimi shadow` and under `irimi serve`, against a fake
internet that answers for the real host names, so irimi's shipped maps decide every call. Its tests
hold every scenario to five invariants and pin what irimi does in each one.
[`tests/e2e/workflows/README.md`](tests/e2e/workflows/README.md) explains the harness, lists the
workflows, and says how to add one.

```
uv run python -m tests.e2e.workflows                             # workflows, and where to start
uv run python -m tests.e2e.workflows w09_scope_gauntlet verbs    # one scenario, every mode
make workflows                                                   # the whole corpus
```

Every feature is tested on the corpus end to end, as well as in unit tests. If your change touches
behaviour a workflow covers, extend that workflow's scenarios and update its pins rather than
writing a new agent, and say in the pull request why the new answer is right. Ask in the issue if
you are unsure which workflow covers it. A pin under `KNOWN BUG:` is a known bug: the change that
fixes it updates the pin and removes the marker.

## Where things are

[`docs/architecture.md`](docs/architecture.md) is the map of the code: each package's job, the
layer it sits in, how one request moves through the engine, and the rules the tests hold the code
to. Read it before adding a module.

`tests/safety/test_import_boundary.py` fails if mitmproxy is imported outside `engine/mitm.py`, or
if a module imports a higher layer, another package of its own layer, or a module after it in its
own package. When you add a module, it asks you to place it in its layer table and in
`docs/architecture.md`, which it checks against the same table. `tests/safety/README.md` lists
the other rules every run keeps and the test that holds each.

## Adding a service map

Maps are YAML in `src/irimi/maps/` and need no Python.
[`docs/service-maps.md`](docs/service-maps.md) is the schema, and the loader refuses anything
outside it by name. [`docs/adding-a-service.md`](docs/adding-a-service.md) covers the Python a
service needs for whole objects, later reads that show a write, and precondition checks. Things to
know before you write a map:

- Write `match:` in block style. A flow mapping cannot hold a path containing `{`.
- A write may name a `fixture:`, which is an object in `src/irimi/fixtures/<service>.json`.
  Add the object there first, with its provenance in the file's `_source` entry.
- A write may name a `fires:` list of the webhook event names the real service would have sent.
  The loader cannot check the names, so name only events the service really sends for that call,
  and only on a write the service's effects in `src/irimi/services/` model, so a later read and
  the events agree. `tests/test_servicemap.py` pins every shipped list; update it with the map.
- Quote a wildcard host (`"*.posthog.com"`) and any `human:` template containing `#`.
- A live-forwarded kind (`read`, `llm`, `telemetry`) must name its methods and must justify a
  destructive one with `live_because:`. A `default_kind:` may never be a live kind. This is the
  scope rule; `servicemaps/rules.py` enforces it.
- Shipped maps never set a `target:`. Targets belong in a user's overrides file.
- `tests/test_servicemap.py` pins the shipped maps' hosts, route counts and a round-trip through a
  built wheel; `tests/test_classify.py` pins how each shipped route classifies. Add to both.

## Conventions

- Commit messages say what changed and why, in the imperative. The issue a change closes belongs
  in the commit message and the pull request, not in the comment that states the rule: `git blame`
  finds it. The next section says when a comment may cite an issue.
- A safety rule is enforced where the configuration is loaded *and* at the decision it protects.
- Nothing in a mitmproxy hook may raise. Fail closed: answer locally and flag it.
- A user-visible change gets a line under `[Unreleased]` in [`CHANGELOG.md`](CHANGELOG.md),
  marked **Breaking** with how to migrate when it breaks something.

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

## For maintainers: releasing

A `v<version>` tag publishes irimi to PyPI through `.github/workflows/release.yml`, with PyPI
Trusted Publishing, so there is no token to keep. In a pull request:

1. Bump `version` in `pyproject.toml` and irimi's own entry in `uv.lock`.
2. In `CHANGELOG.md`, rename `## [Unreleased]` to `## [<version>] - <YYYY-MM-DD>`, add a new empty
   `## [Unreleased]` above it, and update the link references at the bottom.

Once it is merged, tag that commit on `main` and push the tag:

```
git tag v0.2.0 && git push origin v0.2.0
```

The workflow runs the whole CI suite, builds the wheel and sdist, and installs the wheel into a
clean Python 3.12 venv to run `irimi --version` and `irimi init`. It refuses a tag that does not
match `pyproject.toml`, a tag on a commit that is not on `main`, and a version with no
`CHANGELOG.md` section. Only then does it publish, with attestations, and create a GitHub Release
with that section as its notes. A version, once on PyPI, can never be uploaded again, even after it
is deleted.
