# Record, compare and replay

For agent developers who want to know what a change to their agent would do differently: a new
prompt, a new model, new code. irimi stores every run. `irimi compare` lines up the writes of two
stored runs; `irimi replay` re-runs a stored run's trigger with every request answered from what
the run recorded, so a difference in its writes comes from the agent, not from the services.

## Record

Every `irimi shadow` run is stored. Mark the function that handles one unit of work with
`@sdk.trigger` ([sdk.md](sdk.md)), so each call is a run of its own with its arguments recorded,
and start the agent by module name so the trigger can be imported again:

```
irimi shadow -- python -m my_agent
irimi runs list
```

The shadow summary ends with `replay with: irimi replay <id>` for each run the SDK started.

## Replay

`irimi replay RUN` runs a stored run's agent again through irimi and prints what it did
differently. Here the sample workflow `w01_ticket_triage double_refund` was recorded, its prompt
changed to ask for half refunds, and replayed (only the CA's temporary path is shortened):

```
irimi replay · run 61d24b6e2d3d3322 · listening on 127.0.0.1:50745 · ca /tmp/irimi/home/ca/ca.pem
replaying 0d19bf2b53a14efb as 61d24b6e2d3d3322
only traffic sent through this proxy is shadowed; a client that ignores HTTPS_PROXY talks to the real service
real          llm       POST api.anthropic.com/v1/messages -> 200  [replay-miss]
recorded      read      GET api.stripe.com/v1/charges/ch_TICKET1 -> 200  [from-recording]
tool-recorded read      tests.e2e.workflows.agents.w01_ticket_triage.agent:lookup_customer -> ok
real          llm       POST api.anthropic.com/v1/messages -> 200  [replay-miss]
irimi-check   read      GET api.stripe.com/v1/charges/ch_TICKET1 -> 200  [from-recording]
faked         write     POST api.stripe.com/v1/refunds -> 200  [replay-miss]
real          llm       POST api.anthropic.com/v1/messages -> 200  [replay-miss]
tool-fake     write     tests.e2e.workflows.agents.w01_ticket_triage.agent:send_reply -> ok
real          llm       POST api.anthropic.com/v1/messages -> 200  [replay-miss]

irimi replay · 0d19bf2b53a14efb -> 61d24b6e2d3d3322
  agent exited 0
  diverged at the agent's 1st call: real          llm       POST api.anthropic.com/v1/messages -> 200  [replay-miss]
  2 from the recording · 5 missed

  - ○ stripe  refund $49.00 on ch_TICKET1  faked; checked against live state: would succeed
  ~ A ✗ stripe  refund $49.00 on ch_TICKET1  faked; would fail: charge_already_refunded
    B ○ stripe  refund $24.50 on ch_TICKET1  faked; checked against live state: would succeed
  ~ A ○ tool  tests.e2e.workflows.agents.w01_ticket_triage.agent:send_reply  fake ran instead
    B ○ tool  tests.e2e.workflows.agents.w01_ticket_triage.agent:send_reply  fake ran instead
      args.body: "Refund of 4900 requested (statuses: 200, 400)." -> "Refund of 2450 requested (statuses: 200)."

  0 same · 2 changed · 1 only in A · 0 only in B
```

The changed prompt sent a different first message to the model, which the recording could not
answer, so the replay diverged there and asked the model again. The agent then refunded once, for
half, instead of twice. irimi exits 1: a write differs. Replayed unchanged, every call is answered
from the recording and the report says `no divergence: every call was answered from the recording`
and `3 same`, exit 0.

### What runs

In order of precedence:

1. `-- COMMAND` given: that command, as `irimi shadow -- COMMAND` would run it.
2. `--entrypoint MODULE:FUNC` given, or the run's trigger has an entrypoint: that function, called
   in a child process with `--args` (or `--args-file`) or the trigger's recorded arguments. The
   child imports the function by name, rebuilds the arguments and calls it, running a coroutine to
   its end.
3. A process run: its recorded command line, run again. Only the argv is recorded: the
   environment, working directory and stdin are not, and the replay's header says so.

A run with none of these (an `sdk.run` block, or a run whose start never reached irimi) is refused
with the flag to pass. So are recorded arguments that are not values (a callable, a `datetime`
stored by its description), and an entrypoint in `__main__`: irimi prints the command to run
instead. A call the child then cannot make (a method, whose instance a run does not record; a
module that does not import) is a replay that could not run: irimi prints why, and exits 2.

### Which Python calls the trigger

irimi's own, unless `--python PATH` names the agent's. An irimi installed apart from the agent
(`uv tool install irimi`) runs under a Python without the agent's packages, so pass
`--python .venv/bin/python`, the interpreter itself. That Python must have irimi installed, as an
agent whose trigger the SDK recorded does; irimi checks it before the replay starts, and warns when
its version differs. A command names its own Python, so `--python` with `-- COMMAND` is refused.

## The same request

A replay answers a request from the recording when a recorded exchange matches it on all of:

- its kind, method, host and path;
- its query, with parameters sorted;
- the service's scope headers (Stripe's `Stripe-Account` and `Stripe-Version`);
- its body, as compared: the request's fields less the route's `compare_ignores:` (for an LLM
  call, the JSON body less those fields and `cache_control`);
- its idempotency standing: a retry matches only a recorded retry.

The live request is redacted with the store's key before it is compared, so a secret matches its
own placeholder. Each recorded exchange answers once, in recorded order.

## From the recording, and missed

| Request | In the recording (`from-recording`) | Not in it (`replay-miss`) |
| --- | --- | --- |
| an LLM call, or a read before the replay diverged | the recorded response | sent live |
| a read after the replay diverged | not used | sent live, with the overlay |
| a write | the recorded answer | faked as shadow mode fakes it; the replay diverges |
| telemetry, or a request sent to a target | not looked up | as in shadow mode |

A write is never forwarded in a replay. A recorded read made after a recorded write that the
replay has not made is stale: it would show a write that did not happen, so it is never served,
and reaching one diverges the replay. irimi's check reads are answered from the recording too.

**Divergence** is the first of the agent's own calls that the recording did not answer: an exchange
flagged `replay-miss`, or a read tool that ran for real. irimi's check reads and write tools' fakes
never count. The line under it counts the agent's exchanges and read tools answered from the
recording, and missed. Past the divergence the agent talks to the real services for its reads and
to the model again.

### Tools in a replay

A read tool asks the recording first, by its name and its arguments. When the recorded run made the
same call, nothing runs: the call returns the recorded result, rebuilt from its captured JSON, and
is recorded `ran: recording`. A recorded exception is raised as `sdk.RecordedToolError`. The real
function runs instead (`ran: real`, a miss) when the recording has no such call, when its result was
stored only as a description (a `Decimal`, anything past 1 MiB), when the recorded call was
cancelled before it finished, or when it was recorded after a write the replay has not made.

A write tool always runs its fake, as under shadow. Nothing shows a later read what a fake did, so
a write tool never diverges a replay.

## Compare

`irimi compare RUN_A RUN_B` prints the writes two stored runs made, side by side, as a replay's
report does: `=` same, `~` changed, `-` only in A, `+` only in B, `?` not compared.

- **What is compared.** Each run's writes, in the order irimi answered them: every faked, refused
  or targeted HTTP write, and every write tool whose fake ran. Reads, LLM calls, telemetry and read
  tools are not compared. A retry is the write it repeats, once.
- **By data, not by line.** Two writes are the same when they are the same operation, irimi
  answered them the same way (faked, refused, sent to the same target; the same precondition
  outcome and rejection code), and they carry the same fields: the objects the path names, the
  query, the scope headers and the body less `compare_ignores:`. A write tool compares its
  arguments. When a changed pair prints alike, the fields that differ are listed under it, as
  `args.body` is above.
- **Pairing.** Identical writes line up first; the writes between them pair by operation, so a
  refund of another amount is one changed write, not one removed and one added.
- **A body cut when stored is `?`.** Such a write is never `same`, even compared with itself.
- **Exit code.** As `diff(1)`'s: 0 when every write is `=`, 1 when any is not, `?` included, and 2
  when a run is not in the store or cannot be read. Both runs must be in one store.

Comparing two separate recorded runs is not a replay: each ran against the services as they were
then, so a difference can come from their data changing or the model sampling another answer, not
only from the agent.

## From a test

`irimi.testing.replay(run_id)` replays a run from a test and returns a `ReplayResult`;
`result.same` is true when the writes were the same. See [sdk.md](sdk.md#irimitesting).

## Exit codes

| Code | `irimi replay` |
| --- | --- |
| 0 | every write is the same |
| 1 | a write differs, or was cut when stored, or events were dropped |
| 2 | irimi could not do it: the replay was refused, could not start or call the agent, or its run could not be read back |
| 130, 143 | stopped by SIGINT or SIGTERM |
| 141 | whatever read irimi's output went away |

The agent's own exit code is printed and decides nothing. `--result-file PATH` writes the result
as JSON: `source_run_id`, `replay_run_id`, `exit_code`, `diverged_at` and `lines`.

What a replay cannot reproduce is in [limitations.md](limitations.md#replay).
