# Sample workflows

Eleven small agents, each built to exercise part of irimi, and a harness that runs any of them
bare, under `irimi shadow`, beside `irimi serve`, and as a replay of a run one of them recorded.
Every host they call is served by a fake internet in the test process, so they need no keys and
no network, and they run with the rest of the suite.

The agents call the real host names (`api.stripe.com`, `slack.com`, `api.anthropic.com`, ...), so
irimi classifies their calls with its real, shipped maps. Each scenario runs bare first, as the
baseline whose writes really land on the fake services, then under irimi, where none may. The
tests then pin what irimi printed, stored and answered in each mode.

## Running it

```
uv run python -m tests.e2e.workflows                                 # workflows, and where to start
uv run python -m tests.e2e.workflows --all                           # every scenario
uv run python -m tests.e2e.workflows w01_ticket_triage double_refund # bare, shadow, then serve
uv run python -m tests.e2e.workflows w09_scope_gauntlet verbs --mode shadow
uv run pytest -q -n auto tests/e2e/workflows                         # the whole corpus
uv run pytest -q tests/e2e/workflows/test_w04_slack_ops_bot.py       # one workflow's pins
```

A run prints what the agent saw per call, what irimi printed, what reached the fake services,
and whether the invariants below held.

## The workflows

| | Workflow | Trigger | What it exercises |
|---|---|---|---|
| W1 | `w01_ticket_triage` | support-ticket webhook | a multi-turn LLM tool loop with Stripe refunds; precondition checks, idempotency, `irimi compare`, most of `irimi replay`, and deleting runs mid-run |
| W2 | `w02_nightly_reconcile` | scheduled `sdk.run` | pagination over a cursor irimi minted, 40 writes in one run, a block with no entrypoint to replay, look-alike fields and a key across a run name's cut |
| W3 | `w03_queue_worker` | queue, threads and asyncio | run attribution under concurrency across six HTTP clients, shared connection pools, write tools on many threads |
| W4 | `w04_slack_ops_bot` | Slack Events API | Slack fidelity: minted threads, names and ids, missing scopes, archived channels, incoming webhooks, posts that print alike |
| W5 | `w05_dispute_responder` | signed Stripe webhook | a `bytes` trigger, the webhooks a write would fire, a live read whose response holds a credential |
| W6 | `w06_crm_db_agent` | CLI | tools the proxy cannot see: database and file writes as write tools, their fakes, and replayed read tools |
| W7 | `w07_streaming_assistant` | streaming HTTP endpoint | SSE streams recorded chunk by chunk, hang-ups and resets mid-stream, a stream that outlives its run |
| W8 | `w08_orchestrator` | nested triggers and an internal service | nested runs, unmapped internal hosts, `Irimi-Run` across a hop, a target nothing listens on, a remote target refused |
| W9 | `w09_scope_gauntlet` | none (a plain script) | every classification edge: the scope rule, unsafe verbs, odd and oversized bodies, forged irimi headers, base-URL mode and its refusals (`HEAD` included), the control endpoint by hand |
| W10 | `w10_flaky_upstream` | CLI | retries, timeouts, resets, a failed check read, two checks at once, an error on a faked refund's read, a run that raises or is killed, a control endpoint that is gone or stalls |
| W11 | `w11_leaky_agent` | CLI | what irimi cannot see: clients that ignore the proxy or refuse its CA, raw sockets, loopback sidecars |

Each workflow is a package under `agents/`: `agent.py` is the agent, started with `python -m
<module>` so the SDK records its real module name, and `scenarios.py` defines `WORKFLOW`, which
seeds the fake services and names the agent's argv and environment per scenario. The workflow's
pins are in `test_wNN_<name>.py` beside this file. The cross-workflow tests hold every scenario
to more: `test_stored_runs.py` (what the trace store keeps), `test_replays.py` (what every replay
owes) and `test_control_endpoint.py` (what the SDK posts).

## Layout

- `harness/internet.py` is the fake internet: one loopback server that answers for every host a
  workflow calls, and a resolver, in irimi's process only, that refuses every other name.
- `harness/services.py` holds the fake services. They keep state, so a bare run's second refund of
  a charge is really refused, and the scripted LLM answers from a function of the prompt.
- `harness/scenario.py`, `execute.py`, `result.py`, `invariants.py` and `compare.py` define a
  scenario, run it in a mode, read what the run left (irimi's stdout and stderr, its store and
  its write log), check the invariants, and read `irimi compare`'s rows. `tests/e2e/workflows/conftest.py` runs each scenario once per mode per session.
- `agents/agentkit.py` is what the agents share: a urllib client, configuration from the
  environment, and the observation log the tests read. `agents/observe.py` wraps the SDK's
  `trigger`, `run`, `read_tool` and `write_tool` to log which runs started and which body ran.

## The five invariants

Every shadow run, serve run and replay of every scenario is held to these
(`test_invariants.py`):

1. No write reached a fake service (inverted for a scenario marked `leaks`), and every live answer
   the agent saw came from a fake service.
2. No request that reached a fake service carried `Irimi-Run`.
3. No write tool's real function ran.
4. No canary credential, sent by the agent or handed back by a live read, reached disk where irimi
   writes, whole or as the start a cut before redaction would leave.
5. The agent exited as it did bare, unless the scenario is marked `diverges`.

A new workflow is found by name, so it is held to these without an edit to the tests.

## Adding a workflow or a scenario

1. Copy W2 (`agents/w02_nightly_reconcile/`): an agent, its run, and a `scenarios.py`.
2. In `agent.py`, call `agentkit.start()` first, label each call a test keys on (`label=`), and
   log a final `result`. Take `trigger`, `run` and the tool decorators from `observe`, and the rest
   of the SDK from `irimi.sdk`. Give the agent real inputs through argv and environment, set by
   the scenario, rather than branching on the scenario's name.
3. Add `test_wNN_<name>.py` beside this file, pinning what irimi does in each scenario.
4. Run `uv run pytest -q -n auto tests/e2e/workflows`; the invariants and the cross-workflow
   tests pick the new scenarios up.

## Known bugs

A behaviour that is wrong is pinned as it is, not worked around, under a comment of one form:

```
# KNOWN BUG (#123): <the bug, in one sentence>.
# KNOWN BUG: <the bug, in one sentence>.
```

The first names the issue that tracks it, the second has none yet. A fix that flips such a pin
updates the pin and drops the marker in the same change, and `test_invariants.py` holds every
marker in the corpus to this form.
