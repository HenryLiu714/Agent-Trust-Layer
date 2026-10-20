# Sample agent workflows

Eleven small agents, each built to exercise one part of irimi, and a harness that runs any of
them bare, under `irimi shadow`, or beside `irimi shadow --serve` against a fake internet, and
replays a run one of them recorded with `irimi replay` (#82, #84). They need no keys and no
network, and their tests run on every `uv run pytest -q`.

```
uv run python -m examples.workflows                                  # list workflows and scenarios
uv run python -m examples.workflows w09_scope_gauntlet verbs         # one scenario, every mode
uv run python -m examples.workflows w01_ticket_triage full_refund --mode shadow
uv run pytest -q tests/workflows                                     # the whole corpus
```

## The workflows

| | Workflow | Trigger | What it is the regression test for |
|---|---|---|---|
| W1 | `w01_ticket_triage` | support-ticket webhook | a multi-turn LLM tool loop; L3 on the model's refunds (one too large, one refused because the overlay shows the first); one refund sent twice under one key (#46); `irimi compare` of each prompt's run against the full refund's, both recorded into one store, and its exit 2 for a run id the store does not hold or a run it holds damaged, and a run whose last line a crash cut, compared without the write on it (pinned `LOOKS WRONG`) (#79); a full refund replayed unchanged, answered from its recording with nothing sent anywhere, and with the half prompt, diverging at the first model call and its refund checked against the recorded read of the charge; a retried refund replayed as its two recorded writes; a refund read back after it (`verified_refund`), the overlay's read served from the recording as recorded, though each model call's `metadata` names a session new in every process, and replayed with a prompt that refunds nothing, its recorded read of the refund a miss that diverges the replay, read live, and its inverse, two reads and no refund (`checked_charge`) replayed with a prompt that refunds, the read after the refund live and overlaid (#82); the customer read from the recording and the reply through its stand-in, replayed unchanged, with the half prompt, and with the helpdesk database gone, so that a real read would fail, and a customer looked up after the refund (`late_lookup`) replayed with a prompt that looks it up before, so the recorded lookup shows a refund the replay has not made yet: it runs for real, and the replay, not diverged by it, answers the refund and the read after it from the recording (#83); a full refund replayed through its trigger's entrypoint, `replay_child` reviving its `Ticket`, its result saying it did not diverge, and with the half prompt saying where it did, the first model call, and the refund it changed; `sdk.replay()` from a test, by the run's own trigger and by `handle_ticket` with its ticket, returning what `irimi replay --result-file` writes; the full refund replayed with another ticket (`--args`), diverging at the first model call with the same writes, its result saying the triggers' args differ, and with the read tool called in the trigger's place (`--call`), the recording's other calls never made, its run named after the function it called; and a replay replayed by the trigger it called, with each prompt (#84); and #85's exit on the corpus: the full refund replayed with its database gone, `no divergence`; with the half prompt by its command, the `$24.50` refund and no row in `replies`; `retried_refund`, `double_refund` and `runaway_loop` replayed unchanged, `no divergence`, each its whole result pinned; a run recorded into a shared store replayed into a copy of it, the shared store left as it was; and a full refund replayed with its tool results changed (`TOOL_RESULT_TAG`), an edit to the agent's code rather than its prompt: the first model call still answered from the recording, the replay diverging mid-conversation at the second, and no write changed; the full refund, recorded under shadow and under serve, replayed with each prompt `COMPARED` holds, diverging at the first model call and showing, row for row, what `irimi compare` showed of the two recorded runs, and each scenario replayed by its command with the full refund's prompt, the other way round, but for a refund its recording never made, `not_evaluable` (L2) and in minor units; and the serve recording replayed unchanged, `no divergence`, `runs list` naming serve's run `sdk` and the replay `replay of` it, and a replay of a replay `replay of` the first replay (#85, #154) |
| W2 | `w02_nightly_reconcile` | scheduled `sdk.run` | pagination, a cursor naming a minted id (#53), 40 writes in one run, a non-replayable trigger; `irimi compare` of a clean night against forty mismatches, eighty writes apart (#79); a replay whose page names the refund it minted, answered from the recording because the refund kept its recorded id, still the overlay's answer with its `↳` line, and a replay of that replay (#82), each by its command, since an `sdk.run` block records no entrypoint, which `irimi replay` refuses first for `datetime_trigger`, before its args stored by their repr (#84); `irimi runs list` listing each of those replays as `replay of` the run it replayed, and the recording's runs as they were, and `runs show` naming the run each replayed (#154) |
| W3 | `w03_queue_worker` | queue, threads and asyncio | run attribution under concurrency (#74, #75), six HTTP clients the SDK labels (urllib, `http.client`, `requests`, `httpx` sync and async) or that labels itself (a raw asyncio client, #75), eight runs sharing two one-connection pools (#75), two runs sharing one engine's write log, as decided in #77, whose second run's summary calls the first run's faked refund its own (pinned `LOOKS WRONG`, #129); under `irimi shadow --serve`, each run's summary printed as it ends and holding its own events alone, eight at once, and a lost run's refunds and tool calls `unattributed` (#77); Phase 3's exit (#78) over the five concurrent scenarios, under shadow and serve: eight `sdk` runs ended `ok`, each run's trigger, exchanges and tool calls naming its own charge alone, nothing `unattributed`, `irimi runs list` one line per run, every write tool stood in and its ledger row unwritten, every charge read the fake answered stored as live; each way the SDK marks a run: triggers on functions, on a worker's methods and on a sync function that returns a coroutine, `async with sdk.run`, and a run cancelled at shutdown (#74); a Task a sync trigger returns, whose calls land in its ended run (#75, pinned `LOOKS WRONG`, #111) and are missing from the summary serve printed at its end (#77, pinned `LOOKS WRONG`, #125); the worker's own ledger as `@sdk.tool`s, called from four threads, from eight tasks and from a thread that lost its run, and as a worker's static, class and plain methods, each call in its own run (#76); `irimi compare` of two of the eight concurrent runs, each showing its own refund and ledger write alone, of a run that failed, was cancelled, was refused through the shared write log, outlived its trigger or ran under a named version, and of a lost run's writes found in the engine's own run (#79); each of eight concurrent runs replayed in turn, alone, its read and refund from its own recording and nothing sent anywhere, and `shared_charge`'s second run replayed alone, its recorded refusal its own again (#82, #84; before #84 another run of the process took its answers, #155); the worker's methods, whose runs `replay_child` will not call without the instance a run does not record, replayed by the worker's command, the first run's static read tool answered from its recording and the later runs' for real (#83, #84); the stuck message's trigger, called alone, finding no shutdown to wait for and raising where its recorded run was cancelled (#84) |
| W4 | `w04_slack_ops_bot` | Slack Events API | Slack fidelity: minted threads (#52), names vs ids (#44), `missing_scope`, an archived channel, an L0 `reactions.add`, duplicate delivery, the webhook path (#87); `irimi compare` of an archived channel's refused post, a `~` with `✗`, of a missing scope, which L3 cannot see and compare cannot either, and of a post holding a newline against one holding a typed backslash-n (`two_line_answer`, `typed_escape_answer`), changed though the summary prints them alike, and printed with each backslash escaped so the two lines differ (#79); a minted thread whose posts' fixture is unreadable (`own_thread_fixture_failed`), both posts `fake-L0`, each read of the thread `partial`, the one before the reply placed by the parent's minted `ts` alone, and so again in its replay, the overlay run over the recorded envelopes (#64) |
| W5 | `w05_dispute_responder` | signed Stripe webhook | bytes trigger args, `would_fire` (#47), an event the agent's own write would fire, a live read whose response carries a credential (a payment intent's `client_secret`) stored redacted (#70); `irimi compare` calling a refund of the whole charge and one of what an earlier refund left the same, both `refund ? on ch_SMALL` (`partly_refunded`, pinned `LOOKS WRONG`, #79); the trigger replayed alone, its raw body revived as the `bytes` it was captured from (#84) |
| W6 | `w06_crm_db_agent` | CLI | tools the proxy cannot see (#76, #83): database and file writes, stand-ins, decoration-time errors; replays answering every read tool from the recording but the one whose result does not revive (`score_account`), every write through its stand-in, a credential in a tool's args matching its placeholder, two identical reads with different recorded results taken in order, an async read and a sync read whose recorded call returned a coroutine each answered as its `await` expects, and a read made while that coroutine was pending answered from its own record, a recorded `KeyError` raised as `sdk.RecordedToolError` that the agent's `except KeyError` catches, a stale `IRIMI_REPLAY_RUN` exported by hand, which `irimi shadow` removes, a bare agent's SDK ignores, and a serve agent's SDK ignores too, its `IRIMI_RUN` not naming it (#84), and `enrich` replayed behind a gateway that refuses the lookup route (`WORKFLOW_LOOKUP_REFUSED`), every read run for real, one warning for six failed lookups, and the agent unharmed (#83); a stand-in that raises, re-raised as the same object, and a retry, which `irimi compare` shows against the merge made right first time (`merge_once`) as the failed call it added (#79); two writes at once, the first started ending last (`overlapping_writes`), stored and compared in the order they ended (#79); tools called in no run, filed in the process run; HTTP made inside a stand-in; an async read, a sync read and a sync write that each return a coroutine, and an async static method whose stand-in is a static method beside it |
| W7 | `w07_streaming_assistant` | streaming HTTP endpoint | SSE through the proxy, recorded chunk by chunk (#71): two streams at once, chunks that end mid-line and mid-secret; a caller that disconnects, an upstream reset mid-stream and before the stream opens, a LangSmith trace stored only as having happened (#70); every stream stored in the chat's run, labelled by the SDK, read with urllib or with httpx as the model SDKs read them, and two chats' streams each in its own run (#75); a stream that comes gzip-encoded and decodes to a GiB, which the agent decodes only so far and irimi records up to its cap (#94); a stream its trigger returns still open, as a handler returns a streaming response, recorded after its run's end and so missing from the summary serve printed at that end (#77, pinned `LOOKS WRONG`); `irimi compare` of two chats streamed at once, no write between them, and of a chat whose streamed tool call became a post against one whose did not (#79); replays answering both streams from the recording as joined bodies, a secret stored in pieces handed back whole and matching its placeholder, and one stored redacted handed back as its placeholder; a stream recorded cut (a hang-up, a reset, irimi's cap) a miss that goes to the model again, a stream that comes gzip-encoded (`gzip_stream`), stored decoded and served decoded without its encoding, and one of `two_callers`' lockstep chats replayed, the other's stream left waiting (exit 4) (#82), each by its command, since a chat's trigger is given its caller's `send`, a callable `irimi replay` refuses to call it with (#84) |
| W8 | `w08_orchestrator` | nested triggers + an internal service | nested runs, unmapped internal hosts, `Irimi-Run` across a hop: labelled by the SDK, stripped by irimi (#67, #75); a tool called in a nested trigger, filed in its parent's run (#76); under serve, the two nested runs' summaries each naming its own reservation, and a map irimi refuses ending serve before it listens (#77); `irimi compare` of the two orchestrator runs, a write made in a nested trigger (`nested_write`) compared as its parent's, of what a map changed about the writes, and of a reservation delegated to an answer target nothing listens on (`target_down`, #16), answered 502 and compared as its summary printed it (#79); the second of two identical runs replayed alone, every answer its own recording's (#82, #84; before #84 the first run took them, #155) |
| W9 | `w09_scope_gauntlet` | none (plain script) | THE SCOPE RULE, the L0 floor, idempotency (#46), odd bodies, reads past the body limit, forged irimi headers, the reverse door, the control endpoint (#73): runs started, given tool calls and ended over it by hand, two at once, every refusal, and a tool call made in no run, filed in `unattributed` (#76); bodies that decode past irimi's cap (#94): a write refused, a model call forwarded byte for byte, a read-back never overlaid and said not to show the write it follows, run starts refused 413; under serve, every spelling of the listener bound to `0.0.0.0`, and the script's every request `unattributed` (#77); `irimi compare` of two writes whose bodies were cut (`cut_refund`, `cut_update`), a `?` row, of two cut refunds of different amounts, `?` though they print alike, and of one key used three times, the process run against `unattributed` (#79); the GET that deletes, replayed: answered from the recording, so it deletes nothing; a run replayed whose agent names it and the process's other recorded runs again, every recorded run's files untouched and the replay's events for them in `unattributed` flagged `bad-run-id`; a cut write refused again, and the checked refund after it answered from the recording (#82); `verbs`, a process run with no trigger, replayed by its argv (#84) |
| W10 | `w10_flaky_upstream` | CLI | retries, timeouts, resets on a read and a write, irimi's own L3 read failing, a run that raises, is interrupted (Ctrl-C) or is killed after a write, an SDK whose control endpoint is unset, gone or refusing (#74), and a read answered with a bomb in gzip, br or zstd that decodes to a GiB, held to irimi's memory bound (#94); under serve, a run killed or whose end never reached irimi printing no summary, its refund in none (#77, pinned `LOOKS WRONG`); `irimi compare` of each failure against a retried read: retries show nothing, and a crash, a kill, a prose answer, a bomb and irimi's own failed check each show what they changed (#79); a refund whose precondition read failed, replayed as its recorded L2 answer with no read at all; a 429 and a 500 served again and the retry after each; a read that lost its upstream failing again live, flagged `replay-miss` and `upstream-error`; a run that raised, was killed or interrupted after its write, replayed to the same end, the replay's run ending on the recorded builtin's type (#82, #84); the `control_*` runs, `header` runs with no trigger, replayed by their command, whose result says nothing of args, as none were given (#84) |
| W11 | `w11_leaky_agent` | CLI | what irimi cannot see: clients that ignore the proxy, loopback services (Phase 4 readiness); each escape an `sdk.run` whose escaped request carries no `Irimi-Run`, and a read whose redirects lead it off irimi's route, labelled on each hop through irimi and on no other (#75); the sidecar's write made by a write tool, the one escape an agent can close itself (#76); `irimi compare` showing each escape as a run that wrote nothing (pinned `LOOKS WRONG`), and the closed one as its write tool (#79); each escape escaping its replay too, as a replay has nothing of it to answer from (#82) |

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
- **Replay** (#82, #84). `replay(recorded, run_id, workdir, env, options, command=)` replays the
  stored run `run_id`, recorded under `irimi shadow` or serve (#85) or itself a replay, with the
  real `irimi replay <run id>`, in a copy of the recording's `IRIMI_HOME` and store under
  `workdir`, so a replay never adds its run to a recording or a replay another test reads (#85),
  with `env` over the scenario's environment, so `PROMPT=half` is the prompt edit, and `options`
  given to `irimi replay` as well (`--call`, `--args`). It gets a fresh
  `World` from the same `setup` and a fresh fake internet, so whatever the recording did not answer
  shows up in `internet.requests()`. What runs is what `irimi replay` runs for the run: its trigger, called
  alone by `irimi.sdk.replay_child`, which the agent's `main()` never reaches, or a process run's
  argv; with `command=True`, the scenario's own agent command (`-- <cmd>`), the way to replay a run
  `irimi replay` will not call alone (an `sdk.run` block, args stored by their description) or one
  whose trigger needs what `main()` sets up. Every run the child's SDK starts adopts the replay's
  one run, which `Result.process_run_id()` names. `irimi replay` prints its result in place of a
  summary and writes it with `--result-file`, which `Result.replay` holds: `Result.replay_result()`
  is its lines, `Result.summary()` the replay's run's summary as `irimi runs show` prints it,
  `Result.exit_code` the replay's, and `Result.agent_exit_code()` how the child exited. A trigger
  called alone logs no `result`, so `Result.returned()` reads what it returned off its `run.end`
  (`examples/workflows/sdk.py` logs it), and each trigger's `run.start` the type of each argument it
  was `called_with`, so a replay is held to what `replay_child` revived. `sdk_replay(recorded,
  run_id, workdir, *call)` is a replay made by `sdk.replay()`, as a test of an agent would make it,
  through the agents' own `sdk`, which forwards `replay` to `irimi.sdk`, its `irimi replay` run in
  this process through `sdk.replays._run_irimi`, the one seam, so the fake internet resolves its
  names and the scenario's maps load; `Result.replay` is what it returned. `replayed_run_id` is the
  one run besides the engine's own (the process run, or serve's `unattributed`) that the
  recording's irimi put exchanges or tool calls in, else the engine's own; `replay_targets` is
  every run a recording stored but the process run, in the order they started, or the process run
  alone (W9's scripts), since a process that held several
  (W3, W5, W8) is replayed one run at a time; and `counterpart` is the replay's one run, which
  stands for the run it replays. The `replayed(name, scenario, env, run_id, command, options,
  mode)` fixture replays a run of `run_workflow`'s shadow run, or its serve run, once per session,
  and a replay of that replay copies it in turn, so the shadow run's and the cached replay's own
  pins still read their stores whole.
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
  `listed_attributions()` is each run `runs list` lists in this run's store, by id, to its
  attribution column, `replay of <id>` for a replay's run (#154).
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
  are wrapped only so the agent's log still gets `run.start` and `run.end` with the real run id,
the start with the type of each argument a trigger was `called_with` and the end with what it
`returned` (#84).
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

`tests/workflows/test_replays.py` replays every run of every scenario's shadow recording, unchanged,
with the real `irimi replay` (#82, #84): the run's trigger called alone, or a process run's argv,
or, for a run `irimi replay` refuses to call (`REFUSED`, held to irimi's own rule,
`irimi.replay.plan`), the scenario's command. It holds each replay to nine rules: invariants 1 to 4
and no write forwarded, matched or missed; the exit code its recording had (a trigger called alone 0
for a run that ended `ok` and 1 for one that raised), and the replay's run ending as the recorded
run ended, a raised builtin under its recorded type; every exchange a policy decided flagged
`from-recording` or `replay-miss`, never both; every answer irimi decided stamped as recorded; the
replay's comparison of the recorded run with its own all `=`, or `?` for a cut write and exit 1, its
result never saying the triggers' args differ, and its result file naming the two runs, the exit and
the divergence point its lines name; nothing on the fake internet but a read or llm call forwarded
as a miss, telemetry, or an escape irimi never saw; for a scenario whose one run its recording holds
whole, no request there, no miss at all, and the replay saying so; the calls its run made, answered
with the statuses they were answered with when recorded, so a replay whose agent never ran fails;
and its tool calls (#83), the recorded run's made again, each read answered from the recording and
each write through its stand-in, with the corpus's log of which body ran agreeing with what irimi
stored, by an agent `IRIMI_REPLAY_RUN` told it replays whenever its `main()` ran. What a scenario
cannot owe it names in a table with the reason: a trigger that, called alone, does not do what it
did in its process, pinned to how each replay of it ended (`ALONE`: a method `replay_child` cannot
call, a database or flag or event loop `main()` sets up, a thread `main()` waited for, a delivery
the process remembered), a command that runs every run of the process as the one replay (W3
`async_run`, W7 `two_callers`, W9 `control_runs`), a read tool's recorded result that does not
revive (W6 `score_account`), a call made inside a read tool the recording answered (W6
`fetch_enrichment`'s GET), recorded answers that are not whole (`stream-truncated`,
`body-truncated`, `upstream-error`), telemetry, a port in a query (W11 `redirected`), an escape, W7
`two_callers`' lockstep (exit 4 replayed), a write refused for its cut body before any policy is
asked (W9 `bombs`, `cut_refund`, `cut_update`), a trigger that, alone, ends on another error but
exits as it did (`ENDS_DIFFER`: W3 `cancelled_on_shutdown`), and an agent that names its runs
itself, whose posts the replay files as `unattributed` (W9's control scenarios). An entry that is
no longer needed fails, and a last test holds a shadow run up as its own replay, so rule 9 has
something to catch.

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
agent, and `IRIMI_REPLAY_RUN` (#83) no shadow agent, even one whose scenario exported a stale one
(W6 `read_after_write`), which a bare or serve agent, not irimi's child, keeps. `agentkit.start()`
logs all three variables, which is what that check reads; every shadow run
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
  kept in lockstep, and one ended in error with a message past the 1000 characters a trace keeps
  and an error type past the 300 it keeps of a posted one (#99).
  Then every refusal (405, 404, 400 for `../x`, `unattributed`, `NaN`, a missing field and a tool
  call naming its run, 413), and the health check before and after. The tests pin what is stored:
  `attribution: sdk`, the posted trigger, the tool calls in order with the read between them, the
  outcome, and `irimi runs list` no longer saying `incomplete`. Bare, the agent has no
  `IRIMI_CONTROL` and posts to the same URL shape at its proxy, the fake internet, which answers
  502, so it exits as it does under shadow.
- `control_hazards`: posts the SDK must never make. A start re-posted after its run ended is
  answered 204 and dropped by the store, so the run keeps its end and its `dropped_events` 0;
  the health check after it counts the drop (#97). A start or an end for the process run's own
  id (`IRIMI_RUN`) is refused 400, while a tool call posted to it is stored in it; and a tool call
  posted to `unattributed`, as the SDK posts one made in no run where there is no process run
  (`irimi serve`, #77), is taken and filed there, the one post that may name it (#76). Under serve
  mode, irimi's own run is `unattributed`, so the start and the end are refused as naming no run,
  and the ended run's block, printed at its end, is the one `irimi runs show` ends in.

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
