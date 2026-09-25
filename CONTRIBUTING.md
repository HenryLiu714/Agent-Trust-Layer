# Contributing to irimi

## Prerequisites

- macOS or Linux.
- [`uv`](https://docs.astral.sh/uv/). The setup script installs it if it is missing. `uv` fetches
  the Python it needs (3.14 for development, per `.python-version`); you do not install Python.
- `make`, optionally. Every `make` target is one `uv` command, listed in the `Makefile`.

## Set up

```
git clone https://github.com/HenryLiu714/Agent-Trust-Layer.git
cd Agent-Trust-Layer
scripts/setup.sh          # or: make setup
```

That creates `.venv`, installs irimi in editable mode with the dev tools, and runs `irimi init`
to generate the local CA under `~/.irimi/ca/`. Set `IRIMI_HOME` first if you want it elsewhere
(see `.env.example`). Then:

```
uv run irimi --help
```

## Day to day

```
make check        # what CI runs: lint, format check, mypy, tests
make test         # uv run pytest -q
make lint         # uv run ruff check . && uv run ruff format --check .
make fmt          # uv run ruff format . && uv run ruff check --fix .
make typecheck    # uv run mypy
```

The suite runs in about ten seconds and needs no network and no keys: the engine tests start a
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
real `irimi shadow`, all of which run on every `uv run pytest -q`; and one live check of that run,
which skips unless `STRIPE_API_KEY` is set and the `examples` group is installed. Run the live check by hand before closing a phase:

```
STRIPE_API_KEY=sk_test_... uv run pytest -q -rs tests/test_phase_exit.py
```

With `SLACK_BOT_TOKEN` and `SLACK_CHANNEL` also set, the agent posts to Slack and reads the channel
back, and the check asserts the post is in what it read. Give `SLACK_CHANNEL` as a channel **id**
(`C0123`), not `#general`: that is what makes the read-back `overlay: full`. `chat.postMessage`
accepts the name but `conversations.history` wants the id, and irimi's faked post echoes back the
spelling it was sent, so a post to `#general` is read back from a channel irimi cannot match to it
(#44).

```
STRIPE_API_KEY=sk_test_... SLACK_BOT_TOKEN=xoxb-... SLACK_CHANNEL=C0123 \
  uv run pytest -q -rs tests/test_phase_exit.py
```

## Where things are

`docs/architecture.md` is the map: each module's job, the layer it sits in, how one request moves
through the engine, and the rules the tests hold the code to. Read it before adding a module.
The short version:

```
src/irimi/
  exchange.py paths.py netaddr.py     vocabulary, state locations, "is this loopback?"
  ca.py  servicemap/                  the local CA; the service maps (model, rules, loader)
  pipeline.py  reverse_door.py        parse -> classify -> annotate -> respond; the /<host>/ door
  delegation.py  echo.py              answer targets; the L0 echo and the L1 fixture answer
  policy.py  overlay.py  store.py     the decision, and the two seams later phases fill
  engine/                             the Engine protocol; mitm.py is the only mitmproxy importer
  report.py  runner.py                what a run prints; the child env and engine thread
  cli.py                              argparse and the composition root
  maps/*.yaml                         the shipped service maps
  fixtures/*.json                     the vendored response objects L1 answers start from
tests/                                one file per module, plus the invariant and exit tests
examples/refund_agent/                the fixture agent the proxy is tested against
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
  destructive one with `persists: false` and a `comment:`. A `default_kind:` may never be a live
  kind. This is THE SCOPE RULE; `servicemap/rules.py` explains why.
- Shipped maps never set a `target:`. Targets belong in a user's overrides file.
- `tests/test_servicemap.py` pins the shipped maps' hosts, route counts and a round-trip through a
  built wheel; `tests/test_pipeline.py` pins how each shipped route classifies. Add to both.

## Conventions

- Branch per issue, named `<issue-number>-<short-slug>`. Open a PR against `main` with
  `Closes #<n>` in the body.
- Commit messages say what changed and why, in the imperative. The `#<n>` of the issue that
  motivated a rule belongs in the comment that states the rule, so the next reader can find the
  bug it closed.
- A safety rule is enforced where the configuration is loaded *and* at the decision it protects.
  One check is a rule-shaped hole.
- Nothing in a mitmproxy hook may raise. Fail closed: answer locally and flag it.
- CI runs lint, format check, mypy and the tests on Ubuntu and macOS, on Python 3.14 and on 3.12,
  the floor `pyproject.toml` promises.
