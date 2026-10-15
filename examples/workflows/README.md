# Sample agent workflows

Eleven small agents, each built to exercise one part of irimi, and a harness that runs any of
them bare, under `irimi shadow`, or beside `irimi shadow --serve` against a fake internet. They
need no keys and no network, and their tests run on every `uv run pytest -q`.

```
uv run python -m examples.workflows                                  # list workflows and scenarios
uv run python -m examples.workflows w09_scope_gauntlet verbs         # one scenario, every mode
uv run python -m examples.workflows w01_ticket_triage full_refund --mode shadow
uv run pytest -q tests/workflows                                     # the whole corpus
```

## The workflows

| | Workflow | Trigger | What it is the regression test for |
|---|---|---|---|
| W1 | `w01_ticket_triage` | support-ticket webhook | a multi-turn LLM tool loop; L3 on the model's refunds (one too large, one refused because the overlay shows the first); one refund sent twice under one key (#46); `irimi compare` of each prompt's run against the full refund's, both recorded into one store, and its exit 2 for a run id the store does not hold or a run it holds damaged, and a run whose last line a crash cut, compared without the write on it (pinned `LOOKS WRONG`) (#79); replay divergence mid-conversation (#82-#85) |
| W2 | `w02_nightly_reconcile` | scheduled `sdk.run` | pagination, a cursor naming a minted id (#53), 40 writes in one run, a non-replayable trigger; `irimi compare` of a clean night against forty mismatches, eighty writes apart (#79) |
| W3 | `w03_queue_worker` | queue, threads and asyncio | run attribution under concurrency (#74, #75), six HTTP clients the SDK labels (urllib, `http.client`, `requests`, `httpx` sync and async) or that labels itself (a raw asyncio client, #75), eight runs sharing two one-connection pools (#75), two runs sharing one engine's write log, as decided in #77, whose second run's summary calls the first run's faked refund its own (pinned `LOOKS WRONG`, #129); under `irimi shadow --serve`, each run's summary printed as it ends and holding its own events alone, eight at once, and a lost run's refunds and tool calls `unattributed` (#77); Phase 3's exit (#78) over the five concurrent scenarios, under shadow and serve: eight `sdk` runs ended `ok`, each run's trigger, exchanges and tool calls naming its own charge alone, nothing `unattributed`, `irimi runs list` one line per run, every write tool stood in and its ledger row unwritten, every charge read the fake answered stored as live; each way the SDK marks a run: triggers on functions, on a worker's methods and on a sync function that returns a coroutine, `async with sdk.run`, and a run cancelled at shutdown (#74); a Task a sync trigger returns, whose calls land in its ended run (#75, pinned `LOOKS WRONG`, #111) and are missing from the summary serve printed at its end (#77, pinned `LOOKS WRONG`, #125); the worker's own ledger as `@sdk.tool`s, called from four threads, from eight tasks and from a thread that lost its run, and as a worker's static, class and plain methods, each call in its own run (#76); `irimi compare` of two of the eight concurrent runs, each showing its own refund and ledger write alone, of a run that failed, was cancelled, was refused through the shared write log, outlived its trigger or ran under a named version, and of a lost run's writes found in the engine's own run (#79) |
| W4 | `w04_slack_ops_bot` | Slack Events API | Slack fidelity: minted threads (#52), names vs ids (#44), `missing_scope`, an archived channel, an L0 `reactions.add`, duplicate delivery, the webhook path (#87); `irimi compare` of an archived channel's refused post, a `~` with `✗`, of a missing scope, which L3 cannot see and compare cannot either, and of a post holding a newline against one holding a typed backslash-n (`two_line_answer`, `typed_escape_answer`), changed though the summary prints them alike, and printed with each backslash escaped so the two lines differ (#79) |
| W5 | `w05_dispute_responder` | signed Stripe webhook | bytes trigger args, `would_fire` (#47), an event the agent's own write would fire, a live read whose response carries a credential (a payment intent's `client_secret`) stored redacted (#70); `irimi compare` calling a refund of the whole charge and one of what an earlier refund left the same, both `refund ? on ch_SMALL` (`partly_refunded`, pinned `LOOKS WRONG`, #79) |
| W6 | `w06_crm_db_agent` | CLI | tools the proxy cannot see (#76, #83): database and file writes, stand-ins, decoration-time errors; a stand-in that raises, re-raised as the same object, and a retry, which `irimi compare` shows against the merge made right first time (`merge_once`) as the failed call it added (#79); two writes at once, the first started ending last (`overlapping_writes`), stored and compared in the order they ended (#79); tools called in no run, filed in the process run; HTTP made inside a stand-in; an async read, a sync write that returns a coroutine, and an async static method whose stand-in is a static method beside it |
| W7 | `w07_streaming_assistant` | streaming HTTP endpoint | SSE through the proxy, recorded chunk by chunk (#71): two streams at once, chunks that end mid-line and mid-secret; a caller that disconnects, an upstream reset mid-stream and before the stream opens, a LangSmith trace stored only as having happened (#70); every stream stored in the chat's run, labelled by the SDK, read with urllib or with httpx as the model SDKs read them, and two chats' streams each in its own run (#75); a stream that comes gzip-encoded and decodes to a GiB, which the agent decodes only so far and irimi records up to its cap (#94); a stream its trigger returns still open, as a handler returns a streaming response, recorded after its run's end and so missing from the summary serve printed at that end (#77, pinned `LOOKS WRONG`); `irimi compare` of two chats streamed at once, no write between them, and of a chat whose streamed tool call became a post against one whose did not (#79) |
| W8 | `w08_orchestrator` | nested triggers + an internal service | nested runs, unmapped internal hosts, `Irimi-Run` across a hop: labelled by the SDK, stripped by irimi (#67, #75); a tool called in a nested trigger, filed in its parent's run (#76); under serve, the two nested runs' summaries each naming its own reservation, and a map irimi refuses ending serve before it listens (#77); `irimi compare` of the two orchestrator runs, a write made in a nested trigger (`nested_write`) compared as its parent's, of what a map changed about the writes, and of a reservation delegated to an answer target nothing listens on (`target_down`, #16), answered 502 and compared as its summary printed it (#79) |
| W9 | `w09_scope_gauntlet` | none (plain script) | THE SCOPE RULE, the L0 floor, idempotency (#46), odd bodies, reads past the body limit, forged irimi headers, the reverse door, the control endpoint (#73): runs started, given tool calls and ended over it by hand, two at once, every refusal, and a tool call made in no run, filed in `unattributed` (#76); bodies that decode past irimi's cap (#94): a write refused, a model call forwarded byte for byte, a read-back never overlaid and said not to show the write it follows, run starts refused 413; under serve, every spelling of the listener bound to `0.0.0.0`, and the script's every request `unattributed` (#77); `irimi compare` of two writes whose bodies were cut (`cut_refund`, `cut_update`), a `?` row, of two cut refunds of different amounts, `?` though they print alike, and of one key used three times, the process run against `unattributed` (#79) |
| W10 | `w10_flaky_upstream` | CLI | retries, timeouts, resets on a read and a write, irimi's own L3 read failing, a run that raises, is interrupted (Ctrl-C) or is killed after a write, an SDK whose control endpoint is unset, gone or refusing (#74), and a read answered with a bomb in gzip, br or zstd that decodes to a GiB, held to irimi's memory bound (#94); under serve, a run killed or whose end never reached irimi printing no summary, its refund in none (#77, pinned `LOOKS WRONG`); `irimi compare` of each failure against a retried read: retries show nothing, and a crash, a kill, a prose answer, a bomb and irimi's own failed check each show what they changed (#79) |
| W11 | `w11_leaky_agent` | CLI | what irimi cannot see: clients that ignore the proxy, loopback services (Phase 4 readiness); each escape an `sdk.run` whose escaped request carries no `Irimi-Run`, and a read whose redirects lead it off irimi's route, labelled on each hop through irimi and on no other (#75); the sidecar's write made by a write tool, the one escape an agent can close itself (#76); `irimi compare` showing each escape as a run that wrote nothing (pinned `LOOKS WRONG`), and the closed one as its write tool (#79) |

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
- **Serve** (#77). Every scenario runs a third time, under `irimi shadow --serve` with its
  `serve_flags` (none but W9 `self_addressed`'s `--listen-host 0.0.0.0`): irimi in the harness's
  process on a thread of its own, where the fake internet resolves, and the agent beside it as its
  own process, given the README's serve-mode variables and no `IRIMI_RUN`, so a request or a tool
  call no SDK run names is `unattributed`. irimi is stopped by its `stop` event, not a signal,
  when the agent exits, and must then have left no thread of its own running: its engine stopped
  and its store closed. A serve that refuses to start (W8 `map_refused`) starts no agent, as
  `irimi shadow` starts none. Serve applies to every workflow, the plain script W9 included, whose
  every request is then `unattributed` (`irimi runs show unattributed` ends in its summary), and
  W9 `control_hazards` posts for serve's own run, `unattributed`, where it has no `IRIMI_RUN`.
  `Result.run_blocks()` is each run's printed summary by run id; `Result.summaries()` is every
  summary line irimi printed in either mode, and `Result.engine_run_id()` the run a request no run
  names lands in: the process run under shadow, `unattributed` under serve. Each workflow's tests
  run under `irimi shadow` and serve alike (`MODES`), and pin where the two differ.
- **The observation log.** Each agent logs what it saw (status, `Irimi-Answered-By`, which tool
  ran) to a JSON-lines file. The tests assert on that as well as on irimi's output, through
  `Result` (`harness/run.py`): `calls()`, `answered()`, `by_label()`, `tools()`, `events()`,
  `exchange_lines()`, `tool_call_lines()` (#76) and `summary()` for what irimi printed, and
  `stored()` and `stored_events()` for what it recorded in its trace store under the run's
  `IRIMI_HOME` (#70), with
  `process_run_id()`, `runs(...)`, which runs `irimi runs list` or `runs show` over it, and
  `maps()`, the maps `runs show` renders writes with (#72). `run(..., store=)` records into a
  store runs share, `command(...)` runs any `irimi` command as `runs(...)` does, `compare_runs(a,
  b)` runs `irimi compare` over two runs in this run's store, `compare(a, b)` over the SDK runs two
  results started, and `shown_writes(run)` is what `runs show` prints of a run that compare prints
  too; `rows_not_shown(...)` checks every compared row is a line its own run's summary printed.
  The `shared_store_run` fixture records each scenario of a workflow into one store, or into one
  of its stores by name, once; in a shared store `process_run_id()` is the run the banner named
  (#79). The `traced_run` fixture runs a
  shadow run once more under `tracemalloc`, for the most memory irimi's process held (#94).
- **Bombs** (#94). `agentkit.gzip_bomb()` builds a gzip body of a few hundred KB that decodes to
  as much as it is asked, splicing one compressed padding between a head and a tail, so a second
  bomb costs a CRC rather than a compression; the fake internet's `gzip-bomb`, `br-bomb` and
  `zstd-bomb` faults answer with a GiB, and `gzip-padded` with the service's own answer padded.
  A fake service decodes a gzip request body only so far (`MAX_REQUEST_DECODED`) and refuses one
  past it 413. Each is built in the scenario's `setup`: built inside the request it answers, it
  took longer than the agent's timeout.
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
  Since #76 `tool` is irimi's own too, wrapped only so a tool's real function and its stand-in
  each log that it ran, and in which run (`tool` events). The real `tool` is given what the agent
  wrote, so its checks judge that: a `staticmethod` or `classmethod`, a `staticmethod` stand-in, a
  stand-in that is the function itself, and the agent's own `name`, so the name irimi stores is
  the real default. `sdk.py` still has the call-time rules for a checkout with no
  `irimi.sdk`, so an active write tool there runs its stand-in and never the real function. The
  corpus labels no request with `Irimi-Run` itself whenever the real `irimi.sdk` imports
  (`sdk.labels_requests()`), only in the stand-in: the SDK does it (#75), so a scenario that pins
  a run's requests fails without the SDK's patch. W3's raw asyncio client is the one exception, a
  client the SDK does not cover.

## The universal invariants

`tests/workflows/test_invariants.py` holds every scenario of every workflow to five rules, and
every serve run to the same five (#77), and to printing one `--- run <id> ---` block per end it
took, none for a run that did not end (W10 `sigterm_mid_run`) or whose end never reached it (W10's
`control_*`), each equal to that run's stored summary. But two, pinned `LOOKS WRONG`: W3
`task_outlives_trigger` and W7 `hand_off` store events in a run after its end, and their blocks,
printed at the end, hold every event that ended by then and none after.

1. No write reached a fake service under shadow. A scenario marked `leaks` inverts this rule,
   because it exists to show an escape. Every live answer the agent saw must have come from a
   fake service, or the rule is watching the wrong place. A fake counts a request as a write
   unless it knows it is a read: a Slack method it does not implement is a write, and so is an
   unsafe method on a host no fake serves.
2. No request that reached a fake service carried `Irimi-Run`.
3. No write tool's real function ran under shadow.
4. No canary credential reached disk where irimi writes: its home, its working directory, TMPDIR,
   and a store it was given outside its home (#79).
   The trace store is under its home (#70), so this is the redaction test across the corpus: a
   credential shape `redact` misses is fixed in `redact`, not in the canary. `CANARIES` are the
   credentials the agent is given and sends; `SERVED_CANARIES` are those a fake service hands
   back in a response body (W5's payment intent, and the first lines of W10's bombs, #94), which a
   live read stores.
5. The agent exited the same way under shadow as bare, unless the scenario is marked `diverges`.

`tests/workflows/test_stored_runs.py` holds every scenario to what the store keeps (#70): a run
that started irimi stores exactly one process run, with the agent's argv and exit; every exchange
irimi printed is stored exactly once, equal field by field to itself redacted, and telemetry only
as having happened, in its run's order among the tool calls the control endpoint passed on;
every other run is an `sdk` run: one the SDK started (#74), or, in W9's control scenarios, one W9
posted by hand (#73), but where the SDK cannot reach irimi (W10's `control_*`), where each run is
the `header` run its labelled requests made; every run the agent logged (`run.start`, `run.end`)
is stored, with the trigger name and outcome it logged and its error under the exception's
`module.qualname`, a run killed before its end is stored with none, and every SDK workflow's
scenario logs one but those that start no run; every call the agent made inside a run, through
irimi, is stored in that run, by the `Irimi-Run` the SDK put on it (#75); every tool call the
agent logged (`tool` events) is stored in the run it was made in, or in the process run when it
was made in none, by name, kind and which body ran, none lost, doubled or mixed between runs,
and irimi printed one line for each and for nothing else, while a bare agent's every tool ran its
real function (#76); every store directory is 0700, every file 0600, and no `events.jsonl` ends
in a half-written line; and a bare run stores nothing. A streamed answer is stored as the chunks
the agent was sent (#71). For W9 (but for its control scenarios), which sends no `Irimi-Run`,
`irimi runs show <process run>` ends in the summary `irimi shadow` printed, but for the elapsed
seconds; for every workflow, the summary over all of a process's stored runs together is that
block, its tool writes' lines included (#72, #76). Under serve mode (#77) the same checks hold
but for the process run, which serve does not have: what no run names is `unattributed`; and serve
prints no closing block, so the summary over all of its runs and `unattributed/` together is the
summary of what it reported as it happened. And `irimi compare <r> <r>`, through the real CLI, for
every run the store holds and `unattributed`, in both modes (#79): each side named as `irimi
runs show <r>` names the run, one `=` row for each write line its summary printed, or a `?` pair
of that line for a write whose body was cut, which exits 1 (every other run exits 0), HTTP writes
and tool writes each in the summary's order, and the totals; a last check makes sure the
corpus gives it answered, refused, stood-in, cut and interleaved writes to compare, and, under
serve, `unattributed` ones.

`tests/workflows/test_control_endpoint.py` holds every scenario to the control endpoint's two
promises (#73): no control request is an exchange, printed, handed to the store or stored, and
`IRIMI_CONTROL` reaches every agent under shadow, naming the listener its proxy does, and no bare
agent. `agentkit.start()` logs both variables, which is what that check reads; every shadow run
starts its agent as often as its bare run does, but for W8 `map_refused`, where irimi refuses to
start. And a bare agent, with no irimi, starts no run and calls no control endpoint (#74).
`agentkit.start()` also logs each record an irimi logger emits, so the SDK's one way to complain,
a WARNING on `irimi.sdk` when a post fails, is held to W10's `control_*` scenarios, once each. Every
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
  id (`IRIMI_RUN`) is refused 400, while a tool call posted to it is stored in it; and a tool call
  posted to `unattributed`, as the SDK posts one made in no run where there is no process run
  (`irimi serve`, #77), is taken and filed there, the one post that may name it (#76). Under serve
  mode, irimi's own run is `unattributed`, so the start and the end are refused as naming no run,
  and the reopened run's block, printed at its first end, no longer matches what the store holds
  (pinned `LOOKS WRONG`, #97).

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
