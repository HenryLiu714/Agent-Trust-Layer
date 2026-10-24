# Trace format

For authors of tools that read the runs irimi stores: dashboards, CI checks, exporters. This page
is the specification of what irimi writes about a run: the files, every record and field, every
closed vocabulary and the compatibility rules. How the store writes these files safely is in
[architecture.md](architecture.md#store-internals), what redaction removes is in
[security.md](security.md#redaction), and how `irimi runs show` prints a run is in
[cli.md](cli.md#irimi-runs-show).

## About this format

- **Version.** Each run records the format version it was written in, `schema_version`. irimi 0.2
  writes version 3; irimi 0.1.0 wrote version 2. Version 1 was never released.
- **Compatibility.** A reader reads every older version. It ignores a field it does not know, so a
  field older readers can do without is added with no version bump. Adding a value to a closed
  vocabulary, or removing, renaming or changing the meaning of a field, bumps the version, and an
  older reader refuses a run written in a newer one rather than misreading it.
- **While irimi is 0.x**, a minor release may bump the version. Each bump is listed under
  [Versioning](#versioning), and the "Since" column of each table names the version a field or
  value first appeared in.
- **Held to the code.** The codecs are `src/irimi/protocol/trace.py`. `tests/test_trace.py` checks
  that the first column of each field table below is exactly the keys its encoder writes, in
  order, that each vocabulary table lists exactly the values the code knows, and that the example
  at the end decodes and re-encodes to itself.

## Layout

```
<store>/                        default $IRIMI_STORE, else $IRIMI_HOME/store
  blobs/<sha256-hex>            a redacted body, written once
  runs/<run_id>/run.json        the run record
  runs/<run_id>/events.jsonl    the run's events, one JSON object per line, in seq order
  unattributed/events.jsonl     events no usable run claimed
  serve.lock                    held by the one irimi serve writing this store
```

- **A run id** is also a directory name, so it matches `^[A-Za-z0-9_-]{1,64}$`. An `Irimi-Run`
  header that does not match is treated as absent.
- **Events with no usable run** go to `unattributed/events.jsonl`, which has no `run.json`. That
  covers the run id `unattributed`, an id the store cannot use as a directory, and an id that
  differs only in case from a run directory already present (on a case-insensitive filesystem the
  two would be one directory). An exchange filed there for its id carries the flag `bad-run-id`.
- **A header run.** The first event for a run id with no `run.json` creates one, with
  `attribution: "header"`, `started_at` that event's own (or `null` for an event built with no
  clock, whose `started_at` is `0.0`), and every other optional field `null`.
  A start that arrives later rewrites the record whole, keeping its events.
- **An ended run is not started again.** A start for a run whose `run.json` has an `outcome` is
  dropped, and the record is unchanged.
- **Runs written by a replay** name the run they replayed in `replay_of`. A replay never writes to
  a run it did not make.

## Bodies

A body is never inline in a record. It is stored once, as `blobs/<sha256 of its bytes>`, and named
by a [body ref](#body-ref). An empty body stores nothing, and its ref is `null`.

- **What the bytes are.** The body as the agent sent it or received it, after redaction, decoded
  from its `Content-Encoding` when that is `gzip`, `deflate`, `br`, `zstd` or `identity`. A body in
  any other encoding is stored as it came.
- **Cut bodies.** A body over 8 MiB keeps its first 8 MiB: its ref says `truncated: true` and its
  exchange carries `body-truncated`. A body that decodes past 64 MiB is stored empty, also flagged
  `body-truncated`. A stored body is always a prefix of the whole redacted body, never part of a
  secret.
- **Streams.** A `text/event-stream` response is stored as the chunks the agent was sent, joined,
  with each chunk's length in `stream_chunks`. A stream that is not whole (cut past 8 MiB, ended by
  an error, or not decodable) is stored up to its last complete line and flagged
  `stream-truncated`; the agent still got every chunk that arrived.
- **Redaction.** Every value in every record may hold `<redacted:` + 16 hex digits + `>` in place
  of a secret, or `<redaction-failed>` where a part could not be redacted (the exchange is then
  flagged `redaction-failed`). A name, repr or error the SDK cut before irimi could redact it ends
  in `<redacted>` where a secret crossed the cut. The rules are in
  [security.md](security.md#redaction).
- **Telemetry** bodies are never stored.

## Records

Every timestamp is wall-clock seconds since the epoch, a finite JSON number. Every field in the
tables is present in every record; "or `null`" says when its value may be `null`.

### Run record

`run.json` (`trace.RunRecord`):

| Field | Type | Since | Meaning |
| --- | --- | --- | --- |
| `schema_version` | integer | 2 | The format version this run was written in. |
| `run_id` | string | 2 | The run's id; also its directory name. |
| `mode` | [mode](#vocabularies) | 2 | `replay` for a run a replay wrote, else `shadow`. |
| `attribution` | [attribution](#vocabularies) | 2 | How the run came to exist. |
| `trigger` | trigger or `null` | 2 | What started the run. `null` for a `header` run. |
| `agent_version` | string or `null` | 2 | The agent's own version, from `IRIMI_AGENT_VERSION`. |
| `engine_version` | string | 2 | The irimi that recorded the run. |
| `sdk_version` | string or `null` | 2 | The SDK that started the run; `null` for a run it did not start. |
| `started_at` | number or `null` | 2 | When the run started; for a `header` run, when its first event started. |
| `ended_at` | number or `null` | 2 | When the run ended. `null` together with `outcome`. |
| `outcome` | [outcome](#vocabularies) or `null` | 2 | `null` means the run never ended, or irimi stopped first. |
| `error` | error or `null` | 2 | Why the run failed, when `outcome` is `error`. |
| `exit_code` | integer or `null` | 2 | The child's exit code; set only for a `process` run that ended. |
| `dropped_events` | integer | 2 | Events the store could not write. `0` means the run is whole. |
| `replay_of` | string or `null` | 2 | The run a replay answered this one from; set exactly when `mode` is `replay`. |

### Trigger

A run's `trigger` (`trace.Trigger`):

| Field | Type | Since | Meaning |
| --- | --- | --- | --- |
| `name` | string | 2 | The display name: a trigger's name, or a process run's program. |
| `entrypoint` | string or `null` | 2 | `module:qualname` of the function that started the run; `null` for an `sdk.run` block and a process run. |
| `args` | any JSON | 2 | The [captured arguments](#captured-arguments); for a process run, `{"argv": [...]}`, its command line. |
| `replayable` | boolean | 2 | Whether `args` holds everything needed to call `entrypoint` again. |

### Error

A run's or tool call's `error` (`trace.ErrorInfo`):

| Field | Type | Since | Meaning |
| --- | --- | --- | --- |
| `type` | string | 2 | The exception's `module.qualname`, at most 300 characters. |
| `message` | string | 2 | Its `str()`, at most 1000 characters. |

Both are redacted whole and only then cut; a secret across the cut is kept as its placeholder,
never dropped or split.

### Captured arguments

A trigger's `args`, and a tool call's `args` and `result`, are values captured as JSON. A
function's arguments are bound to its parameters, `{"param": value}`, leaving out a method's
`self` or `cls` and not applying defaults; a call that does not bind is captured as
`{"args": [...], "kwargs": {...}}` and is not replayable. `sdk.run(name, inputs=value)` captures
`value`. Each value is captured by the first row that matches it, and is replayable only when every
part of it was captured by a replayable row:

| Value | Captured as | Replayable |
| --- | --- | --- |
| `None`, `bool`, `int`, `str`, finite `float` | itself | yes |
| non-finite `float` | `{"__irimi_repr__": "nan"}` (or `"inf"`, `"-inf"`) | no |
| `list`, `tuple` | a JSON list | when every item is |
| `dict` whose every key is a `str` | a JSON object | when every value is |
| `dict` with any other key | `{"__irimi_repr__": repr(v)}` | no |
| dataclass instance | `{"__irimi_type__": "module:qualname", "value": <its init fields, captured>}` | when every field is, and its class can be imported |
| object with a callable `model_dump` (pydantic v2) | `{"__irimi_type__": "module:qualname", "value": <v.model_dump(mode="json"), captured>}` | the same |
| `bytes` | `{"__irimi_bytes__": <base64>}` | yes |
| anything else | `{"__irimi_repr__": repr(v)}`, or `"<unrepresentable {type}>"` when `repr` raises | no |

- **Types match exactly.** An `IntEnum`, a namedtuple, an `OrderedDict` or a `bytearray` is
  "anything else", since rebuilding the base type would not give back the value. An `int` too long
  to write as JSON is `"<unrepresentable int>"`.
- **The markers are reserved.** A `dict` with `__irimi_repr__`, `__irimi_type__`, `__irimi_bytes__`
  or `__irimi_truncated__` as a key of its own is captured by its repr, so a marker in a captured
  value was always written by capture.
- **A description is not the value.** `__irimi_repr__` and `__irimi_truncated__` say what a value
  was, wherever they appear in it; a replay never hands such a value back as the value.
- **Importable classes only.** A class defined inside a function, or in `__main__`, cannot be
  imported to rebuild it, so its instances are not replayable.
- **Depth.** A value inside more than 32 containers is `{"__irimi_repr__": "<too deep>"}`.
- **Size.** A repr is cut to 1000 characters outside any secret's shape, and a secret across the
  cut is marked `<redacted>`. Captured `args` over 1 MiB
  of JSON are replaced whole by `{"__irimi_truncated__": true, "bytes": n}`, where `n` is the size
  capture had reached when it stopped; so is a tool call's `result` when `args` and `result`
  together pass 1 MiB. Neither is replayable.

### Events

Each line of `events.jsonl` is a JSON object: `seq`, then `type` ([event type](#vocabularies)),
then the fields of the record `type` names.

```
{"seq": <integer>, "type": "exchange" | "tool_call" | "telemetry", ...fields}
```

`seq` numbers a run's events in the order the store received each finished event, so it is
completion order; `started_at` recovers start order. A write that waits on irimi's check read
starts first and finishes last, so the check read's `seq` is lower. In `unattributed/events.jsonl`
each process writing the file numbers its own lines, so a `seq` may repeat there.

### `exchange`

One HTTP exchange irimi saw or made (`irimi.core.exchange.Exchange`):

| Field | Type | Since | Meaning |
| --- | --- | --- | --- |
| `run_id` | string | 2 | The run it belongs to. |
| `service` | string | 2 | The service its map names, or its host when no map claims it. |
| `operation` | string | 2 | The route's operation (`refunds.create`), or `METHOD /path` when no route matched; `""` for a request irimi could not parse, or whose decision failed. |
| `kind` | [kind](#vocabularies) | 2 | What the request is. |
| `answered_by` | [answered by](#vocabularies) | 2 | Who produced the response the agent got. |
| `door` | [door](#vocabularies) | 2 | How the request reached irimi. |
| `issued_by` | [issued by](#vocabularies) | 2 | `engine` for irimi's check read. |
| `target` | string | 2 | The target URL a `delegated` exchange was sent to, or the one an `irimi` exchange flagged `target-failed` could not reach; `""` otherwise. |
| `flags` | list of [flags](#flags) | 2 | Facts about this exchange. |
| `overlay` | [overlay](#vocabularies) or `null` | 2 | How much of the run's faked writes this read could show; `null` when the overlay did not consider it. |
| `precondition` | [precondition](#vocabularies) or `null` | 2 | What the precondition check said before this write was faked; `null` when nothing was asked. |
| `rejection_code` | string | 2 | The service's code for a rejected write; `""` otherwise. |
| `would_fire` | list of strings | 2 | The webhooks an accepted faked write would have made the service send, from its route's `fires:`. |
| `currency` | string | 2 | The currency irimi's check read found; `""` when there was none. |
| `started_at` | number | 2 | When irimi parsed the request. |
| `ended_at` | number | 2 | When irimi finished the exchange; never before `started_at`. |
| `request` | request | 2 | What was asked, as irimi forwarded and recorded it, without `Irimi-Run`. |
| `response` | response or `null` | 2 | What came back; `null` when nothing did (a lost connection, an unreachable target). |
| `stream_chunks` | list of integers | 2 | The byte length of each chunk of a streamed response, in arrival order, summing to the stored body's length; `[]` when the response did not stream or its stored body is empty. |

### Request

An exchange's `request` (`exchange.Request`):

| Field | Type | Since | Meaning |
| --- | --- | --- | --- |
| `method` | string | 2 | Upper case. |
| `scheme` | string | 2 | `http` or `https`. |
| `host` | string | 2 | Lower case, without the port. |
| `port` | integer | 2 | The port. |
| `path` | string | 2 | Starts with `/`; no query. |
| `query` | string | 2 | The raw query string, without the `?`; `""` for none. |
| `headers` | list of pairs | 2 | `[name, value]` pairs, names lower case, in the order sent. |
| `body` | body ref or `null` | 2 | The request body. |

### Response

An exchange's `response` (`exchange.Response`):

| Field | Type | Since | Meaning |
| --- | --- | --- | --- |
| `status` | integer | 2 | The HTTP status. |
| `headers` | list of pairs | 2 | `[name, value]` pairs, as the service sent them; repeats (`set-cookie`) kept. |
| `body` | body ref or `null` | 2 | The response body. |

### Body ref

A request's or response's `body` (`trace.BodyRef`):

| Field | Type | Since | Meaning |
| --- | --- | --- | --- |
| `sha256` | string | 2 | 64 lower-case hex digits: the file under `blobs/`. |
| `size` | integer | 2 | The stored length in bytes. |
| `truncated` | boolean | 2 | The stored bytes are only the start of the body. |

### `tool_call`

A tool call the proxy cannot see, reported by the SDK (`trace.ToolCall`):

| Field | Type | Since | Meaning |
| --- | --- | --- | --- |
| `tool_call_id` | string | 2 | Its id, unique in the run. |
| `run_id` | string | 2 | The run it was made in; else the process run; else `unattributed`. |
| `name` | string | 2 | The tool's name: its `name=`, or the function's `module:qualname`. |
| `kind` | [tool kind](#vocabularies) | 2 | What the tool does. |
| `ran` | [ran](#vocabularies) | 2 | Which body ran. |
| `args` | any JSON | 2 | What it was called with, bound against the real function's signature and [captured](#captured-arguments). |
| `result` | any JSON | 2 | What it returned, captured the same way; `null` when it raised. |
| `error` | error or `null` | 2 | What it raised. A call stopped from outside (its task cancelled, its coroutine closed) records `asyncio.exceptions.CancelledError` or `builtins.GeneratorExit`, which a replay never hands back. |
| `started_at` | number | 2 | When the call began. |
| `ended_at` | number | 2 | When it returned or raised. |
| `awaited` | boolean | 2 | `true` for a sync tool that returned a coroutine: the call ended when the coroutine did, and `result` and `error` are the coroutine's. |

### `telemetry`

One telemetry exchange, recorded only as having happened (`trace.TelemetrySeen`):

| Field | Type | Since | Meaning |
| --- | --- | --- | --- |
| `run_id` | string | 2 | The run it belongs to. |
| `host` | string | 2 | The host it was sent to. |
| `started_at` | number | 2 | When it started. |

## Vocabularies

Each of these fields holds one of a closed set of values. A reader refuses a value it does not
know.

`mode` (`trace.Mode`):

| Value | Since | Meaning |
| --- | --- | --- |
| `shadow` | 2 | A run under `irimi shadow` or `irimi serve`. |
| `replay` | 2 | A run `irimi replay` wrote; `replay_of` names the run it replayed. |

`attribution` (`trace.Attribution`):

| Value | Since | Meaning |
| --- | --- | --- |
| `process` | 2 | The run `irimi shadow -- <cmd>` or `irimi replay` stores for its child process. |
| `sdk` | 2 | A run the SDK started: a `@sdk.trigger` call or an `sdk.run` block. |
| `header` | 2 | A run id seen on requests (`Irimi-Run`) with no start. |

`outcome` (`trace.RunStatus`):

| Value | Since | Meaning |
| --- | --- | --- |
| `ok` | 2 | The run ended normally. |
| `error` | 2 | The trigger raised, or the process exited non-zero; `error` or `exit_code` says how. |

`kind` (`exchange.Kind`):

| Value | Since | Meaning |
| --- | --- | --- |
| `read` | 2 | Forwarded to the real service. |
| `write` | 2 | Answered by irimi or a target; never forwarded. |
| `llm` | 2 | An LLM call, forwarded to the real service. |
| `telemetry` | 2 | Observability traffic, forwarded and stored only as a `telemetry` event. |
| `unknown` | 2 | Nothing classified it; answered by irimi as a presumed write. |

`answered_by` (`exchange.AnsweredBy`):

| Value | Since | Meaning |
| --- | --- | --- |
| `live` | 2 | The real service; the agent got its bytes unchanged. |
| `fake-L0` | 2 | irimi faked the write with an echo of its fields. |
| `fake-L1` | 2 | irimi faked the write from a fixture. |
| `delegated` | 2 | A target answered. |
| `overlay` | 2 | The real service answered, and irimi applied the run's faked writes to the body. |
| `recording` | 2 | A replay sent again what the service answered the stored run. Other answers a replay takes from the recording keep their recorded value and carry `from-recording`. |
| `irimi` | 3 | irimi's own error, `{"error": {"type": ..., "message": ...}}` (no body for a `HEAD`); its flags say why: `refused`, `decision-failed` or `target-failed`. |

`door` (`exchange.Door`):

| Value | Since | Meaning |
| --- | --- | --- |
| `forward` | 2 | Sent through the proxy. |
| `reverse` | 2 | Sent in base-URL mode, `http://127.0.0.1:PORT/<host>/<path>`. |

`issued_by` (`exchange.IssuedBy`):

| Value | Since | Meaning |
| --- | --- | --- |
| `agent` | 2 | The agent sent it. |
| `engine` | 2 | irimi's check read, made before faking a write. |

`overlay` (`exchange.OverlayCoverage`):

| Value | Since | Meaning |
| --- | --- | --- |
| `full` | 2 | The read shows every faked write it should. |
| `partial` | 2 | The read is incomplete and irimi could not fix it; set even when the body is unchanged. |

`precondition` (`exchange.PreconditionOutcome`):

| Value | Since | Meaning |
| --- | --- | --- |
| `passed` | 2 | The real service would have accepted the write. |
| `rejected` | 2 | It would have refused it; `rejection_code` says why. |
| `not_evaluable` | 2 | irimi could not tell; the write was faked. |

A tool call's `kind` (`trace.ToolKind`):

| Value | Since | Meaning |
| --- | --- | --- |
| `read` | 2 | A read tool: it runs for real. |
| `write` | 2 | A write tool: under irimi, its fake runs. |

`ran` (`trace.Ran`):

| Value | Since | Meaning |
| --- | --- | --- |
| `real` | 2 | The function ran. In a replay, a read tool's `real` is a miss. |
| `shadow` | 2 | The write tool's fake ran instead. |
| `recording` | 2 | A replay answered the read tool from the recording; its body never ran. |

An event's `type` (`trace.EventType`):

| Value | Since | Meaning |
| --- | --- | --- |
| `exchange` | 2 | An [exchange](#exchange). |
| `tool_call` | 2 | A [tool call](#tool_call). |
| `telemetry` | 2 | A [telemetry](#telemetry) record. |

### Flags

`flags` is open: a reader skips a flag it does not know. Each appears at most once per exchange
(`exchange.Flag`):

| Value | Since | Meaning |
| --- | --- | --- |
| `unclassified` | 2 | No map said what this request is; `kind` is `unknown` and irimi answered it. |
| `kind-downgraded` | 2 | The scope rule refused a live kind on an unsafe method its route did not name; irimi answered it. |
| `target-failed` | 2 | A target was chosen and could not be reached or applied; the agent got irimi's `502`. |
| `decision-failed` | 2 | irimi's decision failed; the agent got irimi's `502` and nothing was forwarded. |
| `refused` | 3 | irimi refused the request before deciding it: unparseable, unroutable, a base-URL host it does not relay, or a base-URL request it could not rewrite. |
| `upstream-error` | 2 | A forwarded request lost the real service before it answered. |
| `fixture-failed` | 2 | The route's fixture could not be read, so the write was answered with an echo. |
| `idempotent-replay` | 2 | A retry with a used idempotency key and the same parameters; it got the first answer again. |
| `idempotency-conflict` | 2 | A used idempotency key with other parameters; it got the service's idempotency error. |
| `redaction-failed` | 2 | Part of this record could not be redacted and is stored as `<redaction-failed>`. |
| `body-truncated` | 2 | A stored body is cut or empty; the bytes the agent got were whole. |
| `bad-run-id` | 2 | The run id could not name a directory, so the exchange is in `unattributed/`. |
| `stream-truncated` | 2 | The stored stream is incomplete; the agent got every chunk that arrived. |
| `replay-miss` | 2 | A replay found no recorded answer and decided the request as shadow mode would. |
| `from-recording` | 2 | A replay answered from the recording. |

`would_fire` is open too: its values are event names from a service map.

## Versioning

- `schema_version`, in `run.json`, is one integer for the whole format. Events carry no version: a
  reader decodes them with its own vocabulary, which holds every older version's values.
- A writer never appends to a run whose `run.json` has another `schema_version`, or that it cannot
  decode. It drops and counts that run's events, and leaves the record as it found it.
- A decoder refuses a `schema_version` newer than its own, or below 1.
- A record that is not a JSON object, lacks a field, or holds a value of the wrong type, outside a
  closed vocabulary or not finite, is refused with `trace.TraceFormatError`, whose message never
  quotes a whole value. A body ref is handed to the caller's blob reader, and whatever that raises
  reaches the caller unchanged.

| Version | Written by | Changes |
| --- | --- | --- |
| 2 | irimi 0.1.0 | The first released version. |
| 3 | irimi 0.2 | Added `irimi` to `answered_by` and the `refused` flag. Removed an exchange's `validation` field and the `fidelity:*` flags, which a reader ignores in a version 2 run. In a version 2 run, irimi's own errors appear as `fake-L0` flagged `decision-failed`, or `delegated` flagged `target-failed`. |

## Example

One SDK run recorded by `uv run python scripts/demo.py replay`: an LLM refund agent looks the
customer up in its own database (a read tool), irimi checks the charge before faking the refund,
and the agent records its decision through a write tool, whose fake runs. The demo's fake Stripe
answers plain HTTP on a loopback port, so `scheme` and `port` are not Stripe's. Two events are left
out (the agent's own read of the charge and its LLM call), and the refund's request headers are
abridged.

```json run.json
{
  "schema_version": 3,
  "run_id": "a0b7ded762e9ee2e",
  "mode": "shadow",
  "attribution": "sdk",
  "trigger": {
    "name": "examples.replay.agent:handle_ticket",
    "entrypoint": "examples.replay.agent:handle_ticket",
    "args": {
      "ticket": {
        "__irimi_type__": "examples.replay.agent:Ticket",
        "value": {"ticket_id": "T-1", "charge": "ch_demo1", "message": "I was charged twice"}
      }
    },
    "replayable": true
  },
  "agent_version": null,
  "engine_version": "0.1.0",
  "sdk_version": "0.1.0",
  "started_at": 1792803261.616826,
  "ended_at": 1792803261.737567,
  "outcome": "ok",
  "error": null,
  "exit_code": null,
  "dropped_events": 0,
  "replay_of": null
}
```

On disk each event is one line; here each is indented to be readable.

The read tool, which ran for real:

```json events.jsonl
{
  "seq": 1,
  "type": "tool_call",
  "tool_call_id": "fd35abfb55cc5f4d",
  "run_id": "a0b7ded762e9ee2e",
  "name": "db.lookup_customer",
  "kind": "read",
  "ran": "real",
  "args": {"charge": "ch_demo1"},
  "result": {"charge": "ch_demo1", "name": "Ada Lovelace"},
  "error": null,
  "started_at": 1792803261.621098,
  "ended_at": 1792803261.621583,
  "awaited": false
}
```

irimi's check read of the charge, which started after the refund and finished before it:

```json events.jsonl
{
  "seq": 4,
  "type": "exchange",
  "run_id": "a0b7ded762e9ee2e",
  "service": "stripe",
  "operation": "charges.retrieve",
  "kind": "read",
  "answered_by": "live",
  "door": "forward",
  "issued_by": "engine",
  "target": "",
  "flags": [],
  "overlay": null,
  "precondition": null,
  "rejection_code": "",
  "would_fire": [],
  "currency": "",
  "started_at": 1792803261.733286,
  "ended_at": 1792803261.734038,
  "request": {
    "method": "GET",
    "scheme": "http",
    "host": "api.stripe.com",
    "port": 58743,
    "path": "/v1/charges/ch_demo1",
    "query": "",
    "headers": [
      ["authorization", "<redacted:7752e97fb967214a>"],
      ["stripe-version", "2026-08-26.dahlia"],
      ["accept", "application/json"]
    ],
    "body": null
  },
  "response": {
    "status": 200,
    "headers": [
      ["Server", "BaseHTTP/0.6 Python/3.14.6"],
      ["Date", "Sat, 24 Oct 2026 00:54:21 GMT"],
      ["content-type", "application/json"],
      ["content-length", "208"]
    ],
    "body": {
      "sha256": "a6b0de1e835d8f626d8d997b0a852449ae15a491813da2140d07234b3e2ee23c",
      "size": 208,
      "truncated": false
    }
  },
  "stream_chunks": []
}
```

The agent's refund, faked from Stripe's fixture once the check passed:

```json events.jsonl
{
  "seq": 5,
  "type": "exchange",
  "run_id": "a0b7ded762e9ee2e",
  "service": "stripe",
  "operation": "refunds.create",
  "kind": "write",
  "answered_by": "fake-L1",
  "door": "forward",
  "issued_by": "agent",
  "target": "",
  "flags": [],
  "overlay": null,
  "precondition": "passed",
  "rejection_code": "",
  "would_fire": ["refund.created", "charge.refunded"],
  "currency": "usd",
  "started_at": 1792803261.73304,
  "ended_at": 1792803261.7348979,
  "request": {
    "method": "POST",
    "scheme": "http",
    "host": "api.stripe.com",
    "port": 58743,
    "path": "/v1/refunds",
    "query": "",
    "headers": [
      ["host", "api.stripe.com:58743"],
      ["authorization", "<redacted:7752e97fb967214a>"],
      ["idempotency-key", "da9e2887-d17c-a838-afa9-9276a8840df4"],
      ["content-type", "application/x-www-form-urlencoded"],
      ["stripe-version", "2026-08-26.dahlia"],
      ["content-length", "27"]
    ],
    "body": {
      "sha256": "5207a73704b05cf29e30a43d146e112f42a8c58573bd9807ca4242fbcde651ae",
      "size": 27,
      "truncated": false
    }
  },
  "response": {
    "status": 200,
    "headers": [["content-type", "application/json"], ["content-length", "495"]],
    "body": {
      "sha256": "758a4bdf6fd410843f6d53ebaded9d61003117ee5cb3dc6691d302bcf2dc3986",
      "size": 495,
      "truncated": false
    }
  },
  "stream_chunks": []
}
```

The write tool, whose fake ran in place of the agent's database write:

```json events.jsonl
{
  "seq": 6,
  "type": "tool_call",
  "tool_call_id": "e503095a612303bb",
  "run_id": "a0b7ded762e9ee2e",
  "name": "db.record_decision",
  "kind": "write",
  "ran": "shadow",
  "args": {"ticket_id": "T-1", "decision": {"refund_amount": 4900, "reply": "Refunded 4900."}},
  "result": {"recorded": false},
  "error": null,
  "started_at": 1792803261.735682,
  "ended_at": 1792803261.7357068,
  "awaited": false
}
```

The three blobs these events name:

| `blobs/…` | Bytes |
| --- | --- |
| `a6b0de1e…ee23c` | `{"id": "ch_demo1", "object": "charge", "amount": 4900, "amount_refunded": 0, "refunded": false, "currency": "usd", "customer": null, "status": "succeeded", "paid": true, "created": 1790000000, "metadata": {}}` |
| `5207a737…651ae` | `charge=ch_demo1&amount=4900` |
| `758a4bdf…c3986` | `{"amount": 4900, "balance_transaction": "txn_kYRDGB3FUDFCLA08rTInOvaw", "charge": "ch_demo1", "created": 1792803261, "currency": "usd", "customer": null, "customer_account": null, "destination_details": {"card": {"type": "reversal"}, "type": "card"}, "id": "re_a7CNxM1DbNFjcVBH0Or8Ihpu", "metadata": {}, "object": "refund", "payment_intent": null, "payment_method": null, "reason": null, "receipt_number": null, "source_transfer_reversal": null, "status": "succeeded", "transfer_reversal": null}` |
