# Concepts

For anyone reading irimi's output, its docs or its code for the first time. These are the terms
the summary, the CLI, the trace format and the source use, each with one meaning. Read this page
before the others; they use these words without defining them again.

## The model in one paragraph

irimi is a proxy between an agent and the services it calls. It classifies each request. Reads
are forwarded to the real service, and their answers come back unchanged, or with the run's own
writes applied. Writes are answered by irimi itself and never reach the service. Each answer is
recorded, redacted, as part of a run, which can be printed, compared with another run, or replayed.

## Terms

**Run.** One unit of agent work, stored as a `run.json` and an `events.jsonl`. A run comes into
being in one of three ways (its `attribution`):

- `process`: the command `irimi shadow -- <cmd>` launched, its whole process tree;
- `sdk`: one call of a function marked `@sdk.trigger`, or one `with sdk.run(...)` block;
- `header`: a run id that arrived on requests (`Irimi-Run`) with no start from the SDK.

Events no run claimed are stored under `unattributed`, which is not a run.

**Exchange.** One HTTP request and its response, as irimi saw or made it. A run's events are its
exchanges, its tool calls and its telemetry records.

**Kind.** What a request is: `read`, `write`, `llm`, `telemetry` or `unknown`. The first of these
rules that answers decides it (its **classification**):

1. the route in a service map that matches the request's method and path;
2. the service's `default_kind:`, for a host a map claims but a route it does not list;
3. RFC 9110: `GET`, `HEAD` and `OPTIONS` are reads;
4. otherwise `unknown`, flagged `unclassified`.

The scope rule then refuses a live kind on an unsafe method its route did not name
([security.md](security.md#invariants)).

**Live kinds.** `read`, `llm` and `telemetry` are forwarded to the real service. `write` and
`unknown` are answered by irimi and never reach the network, so a route nobody has mapped is faked
rather than sent.

**Answered by irimi.** Any response that did not come from the real service unchanged. Every such
response carries `Irimi-Answered-By`, and the stored exchange's `answered_by` says the same:

| Value | Who answered |
| --- | --- |
| `live` (no header) | the real service; its bytes reach the agent unchanged |
| `fake-L0`, `fake-L1` | irimi, faking a write (echo or fixture, below) |
| `overlay` | the real service, with the run's faked writes applied by irimi |
| `delegated` | a target you configured |
| `recording` | a replay, sending again what the service answered the stored run |
| `irimi` | irimi's own error: a refusal, a decision that failed, a target it could not reach |

**Faked write.** A write irimi answered itself with a realistic success. The real service never
saw it. A faked write's body is one of two kinds:

- an **echo** (`fake-L0`): the request's own fields reflected back, with an id minted for each
  field the route's `ids:` names;
- a **fixture** (`fake-L1`): a whole response object from `irimi/fixtures/<service>.json`, with
  the request's fields written over it, for a route whose map names a `fixture:`.

**Overlay.** irimi applying the run's faked writes to a live read, so the agent reads back what it
wrote: a re-read charge shows its new `amount_refunded`, a refunds list shows the faked refund, a
Slack history shows the faked post. A read the overlay could express only partly is recorded
`overlay: partial`. An error the service sent about the caller or itself (a `401`, a `429`, a
`5xx`) is never edited into a success: it reaches the agent as sent, recorded `partial`. Only a
`404` for an object the run minted is answered from the write log.

**Precondition check.** Before faking a write whose route names a `precondition:`, irimi makes one
real read (**irimi's check read**), applies the run's faked writes to it, and judges whether the
real service would have accepted the write. The result is `passed`, `rejected` (the agent gets
the service's own error) or `not_evaluable` (irimi could not tell, and fakes the write). Check
reads are counted apart from the agent's reads.

**Write log.** The faked writes of one run, which the overlay and the precondition check apply.
Each run has its own; it is dropped when the run ends, and a write sent after that is answered
but not filed. A rejected write, a retry and an error never enter it.

**Retry.** A write repeating an idempotency key the run already used (Stripe's
`Idempotency-Key`). With the same parameters it gets the first answer again and counts as one
write; with other parameters it gets the service's idempotency error.

**Would have fired.** The webhooks the real service would have sent for an accepted faked write,
from the route's `fires:` list. irimi records and prints them; it delivers nothing.

**Target.** A URL you name that answers a service or route instead of irimi
(`--target`, or `target:` in an overrides file). Its answers are stamped `delegated`. Targets must be
on this machine unless you allow otherwise ([service-maps.md](service-maps.md#targets)).

**Proxy and base URL.** The two ways a request reaches irimi. Most clients send through the proxy
(`HTTPS_PROXY`). An SDK that ignores proxy settings can point its base URL at
`http://127.0.0.1:PORT/<upstream-host>` instead: base-URL mode. Each stored exchange says which in
its `door` field: `forward` for the proxy, `reverse` for base-URL mode.

**Tool call.** A function call the proxy cannot see (a database write, a file, a library call),
reported by the SDK. A **read tool** runs for real and is recorded. A **write tool** names a
**fake**, a function that runs in its place under irimi with the same arguments; the real function
never runs.

**Recording, replay and divergence.** A stored run is the **recording** a replay answers from.
**Replay** re-runs a stored run's trigger, or its command, under irimi: each request the recording
holds is answered with its recorded answer (`from-recording`), and anything else **misses**
(`replay-miss`) and is decided as shadow mode would decide it. The first miss is where the replay
**diverged**. See [replay.md](replay.md).

**Trace store.** The directory runs are stored in, `$IRIMI_HOME/store` by default. Its format is
[trace-format.md](trace-format.md).

## Shorthand: L0 to L3

The `answered_by` values and some of the source describe a faked write by level. The levels are
three separate facts, and irimi's output names each in plain words:

| Shorthand | Fact | Where it shows |
| --- | --- | --- |
| L0, L1 | how the write's body was built: echo or fixture | `answered_by` (`fake-L0`, `fake-L1`) |
| L2 | whether later reads show the write: the overlay | `answered_by: overlay`, `overlay: full/partial` |
| L3 | whether the write was checked against live state | `precondition` |

The summary prints none of these codes; [reading-the-summary.md](reading-the-summary.md) says what
it prints instead.
