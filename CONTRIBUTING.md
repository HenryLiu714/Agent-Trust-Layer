# Contributing to irimi

## Prerequisites

- macOS or Linux.
- [`uv`](https://docs.astral.sh/uv/). The setup script installs it if it is missing. `uv` fetches
  the Python it needs (3.14 for development, per `.python-version`); you do not install Python.
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
irimi's own entry in `uv.lock` in a PR; once it is merged, tag that commit on `main` and push the
tag:

```
git tag v0.1.0 && git push origin v0.1.0
```

The workflow refuses a tag that does not match `pyproject.toml`. A version, once on PyPI, can never
be uploaded again, even after it is deleted.

## Day to day

```
make check        # what CI runs: lint, format check, mypy, tests
make test         # uv run pytest -q
make lint         # uv run ruff check . && uv run ruff format --check .
make fmt          # uv run ruff format . && uv run ruff check --fix .
make typecheck    # uv run mypy
```

The suite runs in about five minutes and needs no network and no keys: the engine tests start a
real mitmproxy on a random loopback port against local HTTP servers. `tests/conftest.py` points
`IRIMI_HOME` and the working directory at empty temporary directories, so your own overrides file
and CA are never in play.

The example agent needs the Stripe SDK (and optionally Slack's). They are an opt-in group:

```
uv sync --group examples
export STRIPE_API_KEY=sk_test_...
uv run python examples/refund_agent/seed.py         # once, outside shadow: seeds a charge
uv run irimi shadow -- python examples/refund_agent/agent.py
```

`tests/test_phase_exit.py` holds a hermetic criterion per phase, and Phase 2's twin through the
real `irimi shadow`, all of which run on every `uv run pytest -q`; and one live check of that run.
The live check is marked `live`, which a plain `uv run pytest` deselects even when `STRIPE_API_KEY`
is exported. Run it with `uv run pytest -m live`; it still skips unless `STRIPE_API_KEY` is set and
the `examples` group is installed. Run it by hand before closing a phase:

```
STRIPE_API_KEY=sk_test_... uv run pytest -m live -q -rs tests/test_phase_exit.py
```

With `SLACK_BOT_TOKEN` and `SLACK_CHANNEL` also set, the agent posts to Slack and reads the channel
back, and the check asserts the post is in what it read. Give `SLACK_CHANNEL` as a channel **id**
(`C0123`), not `#general`: that is what makes the read-back `overlay: full`. `chat.postMessage`
accepts the name but `conversations.history` wants the id, and irimi's faked post echoes back the
spelling it was sent, so a post to `#general` is read back from a channel irimi cannot match to it
(#44).

```
STRIPE_API_KEY=sk_test_... SLACK_BOT_TOKEN=xoxb-... SLACK_CHANNEL=C0123 \
  uv run pytest -m live -q -rs tests/test_phase_exit.py
```

Phase 3's exit for a server agent has a hermetic half, `tests/test_phase3_exit.py`, which runs on
every `uv run pytest -q`, and a container half, which needs Docker with compose, `uv` and a
test-mode key, and is run by hand before closing the phase (`examples/server_agent/README.md`).
It seeds two charges, builds both images, refunds both charges at once through the agent under
irimi, checks that irimi stored two ok runs and that Stripe holds no refund, and tears down:

```
STRIPE_API_KEY=sk_test_... examples/server_agent/check.sh
```

Phase 3's exit for an LLM agent (use case 1) has a hermetic half too,
`tests/test_phase3_replay_exit.py`, and a live half run by hand before closing the phase
(`examples/llm_refund_agent/README.md`): record the agent against Stripe test mode and the real
Anthropic API, replay it with `prompt_half.txt`, and check that the replay names the model call as
where it diverged, the refund as the write that changed, and that Stripe holds no refund.

## Sample workflows

`examples/workflows/` holds eleven small agents, each built to exercise one part of irimi: a
multi-turn LLM tool loop, a nightly batch job, a concurrent queue worker, Slack and Stripe webhook
handlers, database writes behind `@sdk.write_tool`, SSE streaming, internal services, failure paths,
and agents that get around the proxy. A harness runs each scenario bare, under `irimi shadow` and
under `irimi serve` (#77), against a fake internet that answers for the real host
names, so irimi's shipped maps decide every call. `tests/workflows/` holds every scenario to
five universal invariants and pins what irimi does in each one. `examples/workflows/README.md`
explains the harness.

```
uv run python -m examples.workflows                              # list workflows and scenarios
uv run python -m examples.workflows w09_scope_gauntlet verbs     # one scenario, bare then shadow
uv run pytest -q tests/workflows
```

When a change alters what irimi answers, a workflow test pins the old answer and fails. Update the
pin in the same PR, and say in the PR why the new answer is right.

Every feature is tested on the corpus end to end, as well as in unit tests. Each open issue's
`Since #89 landed` section names the workflows its feature has to run through and the pins it
extends or flips. Extend an existing workflow with a scenario before writing a new agent. A pin
under `LOOKS WRONG:` is a known bug: the PR that fixes it updates the pin and removes the marker.

## Where things are

`docs/architecture.md` is the map: each module's job, the layer it sits in, how one request moves
through the engine, and the rules the tests hold the code to. Read it before adding a module.
The short version:

```
src/irimi/
  exchange.py paths.py netaddr.py     vocabulary, state locations, "is this loopback?"
  limits.py files.py opener.py        size bounds; private files; the no-redirect opener
  ca.py  servicemap/  bodies.py       the local CA; the service maps; the never-raising body parser
  classify.py  wire.py                what a request is; parse, attribute and stamp
  reverse_door.py                     the /<host>/ door
  delegation.py                       answer targets
  services/                           one spec per service: answers, effects, checks; the L0/L1 body
  reader.py                           the one real read L3 issues
  recording.py                        a stored run as a replay answers from it
  policy.py  overlay.py  store.py     the decision (shadow and replay), and the two seams beside it
  engine/                             the Engine protocol; mitm.py is the only mitmproxy importer
  report.py  runner.py                what a run prints; the child env and engine thread
  compare.py                          two stored runs' writes side by side
  replay.py                           what `irimi replay` runs, and what it says a replay did
  cli/  __main__.py                   argparse and the composition root; `python -m irimi`
  maps/*.yaml                         the shipped service maps
  fixtures/*.json                     the vendored response objects L1 answers start from
tests/                                one file per module, plus the invariant and exit tests
examples/refund_agent/                the fixture agent the proxy is tested against
examples/server_agent/                a server agent beside `irimi serve`, in containers
examples/llm_refund_agent/            an LLM agent recorded, its prompt edited, and replayed
examples/workflows/                   the sample workflows and the harness that runs them
tests/workflows/                      the workflows' invariants and per-workflow pins
```

Two tests guard the layout itself. `tests/test_import_boundary.py` fails if mitmproxy is imported
outside `engine/mitm.py` or if a module imports from its own layer or a higher one; when you add a
module it asks you to place it in the layer table (and in `docs/architecture.md`).

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
  kind. This is THE SCOPE RULE; `servicemap/rules.py` explains why.
- Shipped maps never set a `target:`. Targets belong in a user's overrides file.
- `tests/test_servicemap.py` pins the shipped maps' hosts, route counts and a round-trip through a
  built wheel; `tests/test_pipeline.py` pins how each shipped route classifies. Add to both.

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
4. Tables are fine where they replace prose, as the decision tables in `sdk/tools.py` and
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
