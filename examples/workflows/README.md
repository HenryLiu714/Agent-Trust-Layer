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
| W1 | `w01_ticket_triage` | support-ticket webhook | a multi-turn LLM tool loop; L3 on the model's refunds (one too large, one refused because the overlay shows the first); one refund sent twice under one key (#46); replay divergence mid-conversation (#82-#85) |
| W2 | `w02_nightly_reconcile` | scheduled `sdk.run` | pagination, a cursor naming a minted id (#53), 40 writes in one run, a non-replayable trigger |
| W3 | `w03_queue_worker` | queue, threads and asyncio | run attribution under concurrency (#74, #75), several HTTP clients, two runs sharing one engine's write log |
| W4 | `w04_slack_ops_bot` | Slack Events API | Slack fidelity: minted threads (#52), names vs ids (#44), `missing_scope`, an archived channel, an L0 `reactions.add`, duplicate delivery, the webhook path (#87) |
| W5 | `w05_dispute_responder` | signed Stripe webhook | bytes trigger args, `would_fire` (#47), an event the agent's own write would fire, a live read whose response carries a credential (a payment intent's `client_secret`) stored redacted (#70) |
| W6 | `w06_crm_db_agent` | CLI | tools the proxy cannot see (#76, #83): database and file writes, stand-ins, decoration-time errors |
| W7 | `w07_streaming_assistant` | streaming HTTP endpoint | SSE through the proxy, recorded chunk by chunk (#71): two streams at once, chunks that end mid-line and mid-secret; a caller that disconnects, an upstream reset mid-stream and before the stream opens, a LangSmith trace stored only as having happened (#70) |
| W8 | `w08_orchestrator` | nested triggers + an internal service | nested runs, unmapped internal hosts, `Irimi-Run` across a hop (#67) |
| W9 | `w09_scope_gauntlet` | none (plain script) | THE SCOPE RULE, the L0 floor, idempotency (#46), odd bodies, reads past the body limit, forged irimi headers, the reverse door, the control endpoint (#73): runs started, given tool calls and ended over it by hand, two at once, and every refusal |
| W10 | `w10_flaky_upstream` | CLI | retries, timeouts, resets on a read and a write, irimi's own L3 read failing, a run that raises or is killed after a write |
| W11 | `w11_leaky_agent` | CLI | what irimi cannot see: clients that ignore the proxy, loopback services (Phase 4 readiness) |

Each workflow is a package: `agent.py` is the agent, started through `examples.workflows.launch`
so it keeps its real module name; `scenarios.py` seeds the fake services per scenario;
`tests/workflows/test_wNN_*.py` pins what irimi does in each.

## How it works

- **The fake internet** (`harness/internet.py`) is one loopback server that answers for every
  host a workflow calls: `api.stripe.com`, `slack.com`, `api.anthropic.com`, `api.openai.com`,
  and a workflow's own `*.internal` services. The agents call the real host names, so irimi
  classifies them with its real, shipped maps. Only name resolution is faked, in the process that
  runs irimi. A name the fake internet does not serve fails to resolve, and so does any IP
  address but loopback or `0.0.0.0`. The one gap: asyncio skips the resolver for an IP literal,
  so a map that sent irimi to a public IP would not be stopped; no workflow uses one. An agent inherits only
  `PATH`, `HOME`, the locale and a few like them from your shell; everything else it is given.
- **The fake services** (`harness/services.py`) are stateful. A bare run really refunds, and a
  second refund of the same charge is really refused. The scripted LLM answers from a per-scenario
  function of the prompt, so a changed prompt changes the answer deterministically.
- **Bare vs shadow.** Each scenario runs twice. The bare run is the baseline, and its writes land
  on the fake services. The shadow run goes through `irimi shadow`, and nothing may land.
- **The observation log.** Each agent logs what it saw (status, `Irimi-Answered-By`, which tool
  ran) to a JSON-lines file. The tests assert on that as well as on irimi's output, through
  `Result` (`harness/run.py`): `calls()`, `answered()`, `by_label()`, `tools()`, `events()`,
  `exchange_lines()` and `summary()` for what irimi printed, and `stored()` and `stored_events()`
  for what it recorded in its trace store under the run's `IRIMI_HOME` (#70), with
  `process_run_id()`, `runs(...)`, which runs `irimi runs list` or `runs show` over it, and
  `maps()`, the maps `runs show` renders writes with (#72).
  `reported` holds the exchanges irimi printed a line for and `handed` those its engine gave the
  store, so a test can compare what was kept with what happened.
- **The agent kit** (`agentkit.py`) is what every agent shares, all stdlib: its HTTP client and
  the Stripe, Slack and Anthropic calls on it, the observation log, its SQLite files, and, for an
  agent that serves (W1, W4, W5, W7), `serve()`, and `deliver()` (W1, W4, W5), which sends an inbound call to the
  agent's own loopback server through no proxy, because inbound traffic is not its egress.
- **The SDK** (`sdk.py`). The agents use the SDK API from #74 and #76 (`@sdk.trigger`,
  `sdk.run`, `@sdk.tool(kind=..., shadow=...)`). `sdk.py` re-exports `irimi.sdk` name by name, so
  `trigger` and `run` are the real ones (#74): they post each run's start and end to irimi, and
  are wrapped only so the agent's log still gets `run.start` and `run.end` with the real run id.
  Until `irimi.sdk` has `tool` (#76), `sdk.py` implements its call-time rules itself, so an active
  write tool already runs its stand-in and never the real function.

## The universal invariants

`tests/workflows/test_invariants.py` holds every scenario of every workflow to five rules:

1. No write reached a fake service under shadow. A scenario marked `leaks` inverts this rule,
   because it exists to show an escape. Every live answer the agent saw must have come from a
   fake service, or the rule is watching the wrong place. A fake counts a request as a write
   unless it knows it is a read: a Slack method it does not implement is a write, and so is an
   unsafe method on a host no fake serves.
2. No request that reached a fake service carried `Irimi-Run`.
3. No write tool's real function ran under shadow.
4. No canary credential reached disk where irimi writes: its home, its working directory, TMPDIR.
   The trace store is under its home (#70), so this is the redaction test across the corpus: a
   credential shape `redact` misses is fixed in `redact`, not in the canary. `CANARIES` are the
   credentials the agent is given and sends; `SERVED_CANARIES` are those a fake service hands
   back in a response body (W5's payment intent), which a live read stores.
5. The agent exited the same way under shadow as bare, unless the scenario is marked `diverges`.

`tests/workflows/test_stored_runs.py` holds every scenario to what the store keeps (#70): a run
that started irimi stores exactly one process run, with the agent's argv and exit; every exchange
irimi printed is stored exactly once, equal field by field to itself redacted, and telemetry only
as having happened, in its run's order among the tool calls the control endpoint passed on;
every other run is an `sdk` run: one the SDK started (#74), or, in W9's control scenarios, one W9
posted by hand (#73); every run the agent logged (`run.start`, `run.end`) is stored, with the
trigger name and outcome it logged and its error under the exception's `module.qualname`, and a
run killed before its end is stored with none; every store directory is 0700, every file 0600, and
no `events.jsonl` ends in a half-written line; and a bare run stores nothing. A streamed answer is stored as the chunks the agent was sent (#71). For
W9 (but for its control scenarios) and W11, which send no `Irimi-Run`, `irimi runs show <process
run>` ends in the summary
`irimi shadow` printed, but for the elapsed seconds; for every workflow, the summary over all of
a process's stored runs together is that block (#72).

`tests/workflows/test_control_endpoint.py` holds every scenario to the control endpoint's two
promises (#73): no control request is an exchange, printed, handed to the store or stored, and
`IRIMI_CONTROL` reaches every agent under shadow, naming the listener its proxy does, and no bare
agent. `agentkit.start()` logs both variables, which is what that check reads; every shadow run
starts its agent as often as its bare run does, but for W8 `map_refused`, where irimi refuses to
start. And a bare agent, with no irimi, starts no run and calls no control endpoint (#74). Every
SDK workflow posts its runs' starts and ends to the endpoint, and three W9 scenarios call it by
hand:

- `self_addressed`: its health check and a path no route names, at each of the three spellings of
  the listener.
- `control_runs`: the SDK's job by hand, with posts the test controls exactly. Runs are started,
  given two tool calls with a Stripe read labelled `Irimi-Run: <id>` between them, and ended: one
  posted direct (`127.0.0.1`), one through the proxy to itself (`127.1`), two at once from threads
  kept in lockstep, and one ended in error with a message past the 1000 characters a trace keeps.
  Then every refusal (405, 404, 400 for `../x`, `unattributed`, `NaN`, a missing field and a tool
  call naming its run, 413), and the health check before and after. The tests pin what is stored:
  `attribution: sdk`, the posted trigger, the tool calls in order with the read between them, the
  outcome, and `irimi runs list` no longer saying `incomplete`. Bare, the agent has no
  `IRIMI_CONTROL` and posts to the same URL shape at its proxy, the fake internet, which answers
  502, so it exits as it does under shadow.
- `control_hazards`: posts the SDK must never make. A start re-posted after its run ended is
  accepted and reopens the run (pinned `LOOKS WRONG`); a start or an end for the process run's own
  id (`IRIMI_RUN`) is refused 400, while a tool call posted to it is stored in it.

A new workflow package is found by its name, `wNN_<name>`, and is held to all five rules without
any registry edit. A last test checks that the corpus gives rules 1 to 4 something to catch: a rule
that nothing could break passes by default. Rule 5 compares two real exits in every scenario.

## Adding a workflow

1. Create `examples/workflows/wNN_<name>/` with `__init__.py`, `agent.py` and `scenarios.py`
   (defining `WORKFLOW`). Copy W9 for the shape.
2. In `agent.py`, call `agentkit.start()` first (it logs the proxy and `IRIMI_CONTROL` the agent
   was given), label each call a test keys on (`label=`), and
   log a final `result`. A call made with another client logs itself with `agentkit.obs_http()`.
3. Add `tests/workflows/test_wNN_<name>.py`, pinning what irimi actually does in each scenario. A
   behaviour that looks wrong is pinned under a `LOOKS WRONG:` comment (naming its issue once one
   is filed), not worked around.
