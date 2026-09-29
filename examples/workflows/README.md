# Sample agent workflows

Eleven small agents, each built to exercise one part of irimi, and a harness that runs any of
them bare or under `irimi shadow` against a fake internet. They need no keys and no network, and
their tests run on every `uv run pytest -q`.

```
uv run python -m examples.workflows                                  # list workflows and scenarios
uv run python -m examples.workflows w09_scope_gauntlet verbs         # run one scenario, both modes
uv run python -m examples.workflows w01_ticket_triage full_refund --mode shadow
uv run pytest -q tests/workflows                                     # the whole corpus
```

## The workflows

| | Workflow | Trigger | What it is the regression test for |
|---|---|---|---|
| W1 | `w01_ticket_triage` | support-ticket webhook | a multi-turn LLM tool loop; L3 on the model's refunds; replay divergence mid-conversation (#82-#85) |
| W2 | `w02_nightly_reconcile` | scheduled `sdk.run` | pagination, a cursor naming a minted id (#53), 40 writes in one run, a non-replayable trigger |
| W3 | `w03_queue_worker` | queue, threads and asyncio | run attribution under concurrency (#74, #75), several HTTP clients |
| W4 | `w04_slack_ops_bot` | Slack Events API | Slack fidelity: minted threads (#52), names vs ids (#44), `missing_scope`, duplicate delivery, the webhook path (#87) |
| W5 | `w05_dispute_responder` | signed Stripe webhook | bytes trigger args, `would_fire` (#47), an event the agent's own write would fire |
| W6 | `w06_crm_db_agent` | CLI | tools the proxy cannot see (#76, #83): database and file writes, stand-ins, decoration-time errors |
| W7 | `w07_streaming_assistant` | streaming HTTP endpoint | SSE through the proxy (#71), a caller that disconnects, an upstream reset |
| W8 | `w08_orchestrator` | nested triggers + an internal service | nested runs, unmapped internal hosts, `Irimi-Run` across a hop (#67) |
| W9 | `w09_scope_gauntlet` | none (plain script) | THE SCOPE RULE, the L0 floor, idempotency (#46), odd bodies, forged irimi headers |
| W10 | `w10_flaky_upstream` | CLI | retries, timeouts, resets, a run that raises or is killed after a write |
| W11 | `w11_leaky_agent` | CLI | what irimi cannot see: clients that ignore the proxy, loopback services (Phase 4 readiness) |

Each workflow is a package: `agent.py` is the agent, run with `python -m`; `scenarios.py` seeds
the fake services per scenario; `tests/workflows/test_wNN_*.py` pins what irimi does in each.

## How it works

- **The fake internet** (`harness/internet.py`) is one loopback server that answers for every
  host a workflow calls: `api.stripe.com`, `slack.com`, `api.anthropic.com`, `api.openai.com`,
  and a workflow's own `*.internal` services. The agents call the real host names, so irimi
  classifies them with its real, shipped maps. Only name resolution is faked, in the process that
  runs irimi. A name the fake internet does not serve fails to resolve, so no harness run can
  reach the real internet.
- **The fake services** (`harness/services.py`) are stateful. A bare run really refunds, and a
  second refund of the same charge is really refused. The scripted LLM answers from a per-scenario
  function of the prompt, so a changed prompt changes the answer deterministically.
- **Bare vs shadow.** Each scenario runs twice. The bare run is the baseline, and its writes land
  on the fake services. The shadow run goes through `irimi shadow`, and nothing may land.
- **The observation log.** Each agent logs what it saw (status, `Irimi-Answered-By`, which tool
  ran) to a JSON-lines file. The tests assert on that as well as on irimi's output.
- **The SDK stand-in** (`sdk.py`). The agents use the SDK API from #74 and #76 (`@sdk.trigger`,
  `sdk.run`, `@sdk.tool(kind=..., shadow=...)`). Until `irimi.sdk` exists, `sdk.py` implements its
  call-time rules itself, so an active write tool already runs its stand-in and never the real
  function. Once `irimi.sdk` lands, `sdk.py` re-exports it.

## The universal invariants

`tests/workflows/test_invariants.py` holds every scenario of every workflow to five rules:

1. No write reached a fake service under shadow. A scenario marked `leaks` inverts this rule,
   because it exists to show an escape.
2. No request that reached a fake service carried `Irimi-Run`.
3. No write tool's real function ran under shadow.
4. No canary credential reached disk under irimi's home.
5. The agent exited the same way under shadow as bare, unless the scenario is marked `diverges`.

A new workflow package is found by its name, `wNN_<name>`, and is held to all five rules without
any registry edit.

## Adding a workflow

1. Create `examples/workflows/wNN_<name>/` with `__init__.py`, `agent.py` and `scenarios.py`
   (defining `WORKFLOW`). Copy W9 for the shape.
2. In `agent.py`, call `agentkit.start()` first, label every call, and log a final `result`.
3. Add `tests/workflows/test_wNN_<name>.py`, pinning what irimi actually does in each scenario. A
   behaviour that looks wrong is pinned with a comment naming the issue, not worked around.
