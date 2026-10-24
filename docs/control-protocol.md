# The control protocol

For anyone writing an irimi SDK in another language, or labelling runs from an agent that cannot
use the Python one. An agent tells irimi two things the proxy cannot see on the wire: where each
run starts and ends, and the tool calls it made. It does so with plain HTTP to the control
endpoint on irimi's own listener, and by putting `Irimi-Run` on its requests. This page is the wire
format; the Python SDK ([sdk.md](sdk.md)) is one implementation of it.

## Where the endpoint is

The control endpoint is the path prefix `/_irimi/` on irimi's listener. `irimi shadow` and
`irimi replay` give their child its base URL as `IRIMI_CONTROL`, such as
`http://127.0.0.1:4000/_irimi`; under `irimi serve` you set it yourself
([serve-mode.md](serve-mode.md)). A request is a control request when it is addressed to the
listener itself (an address of this machine, on the listener's port) and its path starts with
`/_irimi/`. Send it directly, not through the proxy. A control request is never forwarded and never
recorded as an exchange.

The endpoint has no authentication: anything that can reach the listener can call it
([security.md](security.md#trust-boundaries)).

## Versioning

The protocol version is `1`.

- A client sends `Irimi-Protocol: 1` on every request. A request without the header is read as
  version 1.
- Every answer carries `Irimi-Protocol: 1`, so a client can tell which version irimi speaks without
  failing first.
- A request whose major version differs (`2`, `2.1`; `1.3` is major 1) is answered `426` and not
  acted on. The Python SDK then logs one warning naming both versions; nothing it reports is
  recorded until the two match.
- Every route ignores body fields it does not know, so a minor version can add fields without
  breaking either side. A change a version-1 reader would misread bumps the major version.

## Routes

| Method | Path under `/_irimi/` | Body | Answer |
| --- | --- | --- | --- |
| `POST` | `runs/{run_id}/start` | a run start | `204` |
| `POST` | `runs/{run_id}/tool-calls` | one tool call | `204` |
| `POST` | `runs/{run_id}/end` | a run end | `204` |
| `POST` | `runs/{run_id}/tool-calls/recorded` | a recorded-call lookup | `200` with the outcome, or `404` |
| `GET` | `health` | none | `200` with the engine's state |

Every answer carries `Irimi-Answered-By: control` and `Irimi-Protocol: 1`. Every body is a JSON
object, and so is every answer that has one.

### Run ids

A `run_id` is 1 to 64 characters of `A-Z`, `a-z`, `0-9`, `_` and `-`, chosen by the client. Use
something unique, such as 16 random hex characters. Two ids that differ only in case may share a
directory on a case-insensitive filesystem, and the second is then stored as unattributed.

- `unattributed` may receive tool calls and lookups, not a start or an end: it is where calls made
  outside any run go.
- The process run (`IRIMI_RUN` under `irimi shadow`) belongs to irimi: it may receive tool calls
  and lookups, and a start or end for it is refused `400`.
- A start for a run that has already ended is accepted (`204`) and dropped, counted in `health`'s
  `dropped`.

### `start`

```json
{
  "trigger": {
    "name": "handle_ticket",
    "entrypoint": "my_agent:handle_ticket",
    "args": {"ticket_id": "T-1"},
    "replayable": true
  },
  "agent_version": "1.4.0",
  "sdk_version": "0.1.0",
  "started_at": 1792803261.616826
}
```

Every field is required. `agent_version` may be `null`, and so may `trigger.entrypoint` (a block
with no function to call again). `args` is any JSON, captured as
[trace-format.md](trace-format.md#captured-arguments) describes; `replayable` says whether `args`
holds everything needed to call `entrypoint` again. `started_at` is seconds since the epoch. irimi
sets the run's `attribution` (`sdk`), its `engine_version` and, in a replay, `replay_of` itself.

Post the start before the run's first request, and the end after its last: irimi files requests by
their `Irimi-Run`, but a run with no start is stored as a `header` run with no trigger.

### `tool-calls`

```json
{
  "tool_call_id": "e503095a612303bb",
  "name": "db.record_decision",
  "kind": "write",
  "ran": "shadow",
  "args": {"ticket_id": "T-1"},
  "result": {"recorded": false},
  "error": null,
  "started_at": 1792803261.735682,
  "ended_at": 1792803261.7357068,
  "awaited": false
}
```

The fields are a stored tool call's ([trace-format.md](trace-format.md#tool_call)), less `run_id`,
which comes from the path; a body that names one is refused `400`. `ran` is `real` when the function
ran, `shadow` when its fake ran, and `recording` when a replay answered it from the recording.
`error` is `null` or `{"type": "module.QualName", "message": "..."}`.

### `end`

```json
{"ended_at": 1792803261.737567, "outcome": "error", "error": {"type": "builtins.ValueError", "message": "no such charge"}}
```

`ended_at` and `outcome` (`ok` or `error`) are required; `error` may be `null`.

### `tool-calls/recorded`

A replay's lookup of a read tool's recorded result, made before running the tool:

```json
{"name": "db.lookup_customer", "args": {"charge": "ch_demo1"}}
```

`args` must be captured exactly as a tool call's are, so it compares equal to the recording's. A hit
answers `200`:

```json
{"result": {"charge": "ch_demo1", "name": "Ada Lovelace"}, "error": null, "awaited": false}
```

Return `result` to the caller, raise `error` (a recorded exception), and, when `awaited` is true,
return them through an awaitable. A miss, or any lookup outside a replay, answers `404`: run the
tool for real. A lookup is not recorded as a tool call; post the call afterwards with
`ran: recording` or `ran: real`.

### `health`

```json
{"engine_version": "0.1.0", "serve": true, "store": {"queued": 0, "written": 42, "dropped": 0}}
```

`store` counts events waiting for the writer, written, and dropped since irimi started.

## Errors

A refusal or failure answers with irimi's JSON error:

```json
{"error": {"type": "irimi_control_refused", "message": "missing required field 'outcome'"}}
```

| Status | When |
| --- | --- |
| `400` | the body is not a JSON object, is not valid JSON, nests too deeply, holds `NaN` or `Infinity`, lacks a field or holds a wrong type; the run id cannot name a run; a start or end for the process run |
| `404` | no such route (`type` `irimi_control_refused`), or a recorded-call miss |
| `405` | the wrong method; the `Allow` header names the right one |
| `413` | the body is over 2 MiB, or decodes past irimi's limit |
| `426` | another major protocol version |
| `500` | irimi failed (`type` `irimi_internal_error`); the request may not have been recorded |

The message is at most 200 characters and is for a person to read; its wording is not part of the
protocol.

## Limits

| What | Limit |
| --- | --- |
| a request body | 2 MiB |
| a trigger's captured `args` | 1 MiB of JSON, so a start fits the body limit |
| a run's or tool's `name` | 1000 characters; cut it outside any secret's shape |
| an error's `type` and `message` | irimi stores 300 and 1000 characters, after redacting the whole; the Python SDK posts at most 1200 and 65,536 |
| an answer the client reads | the Python SDK reads at most 16 MiB |

Text the client cuts before sending is cut before irimi can redact it, so never cut through a
secret: move the cut to before any key-like token it would split.

## `Irimi-Run`

Every HTTP request a run sends through irimi carries `Irimi-Run: <run_id>`. irimi files the
exchange under that run, taking the first value that is a valid run id. A request without one (or
with no valid one) belongs to the engine's own run: the process run under `irimi shadow`,
`unattributed` under `irimi serve`. irimi removes the header before anything leaves it, so send it
only on connections to irimi's listener (through the proxy, or in base-URL mode), never directly to
a service. Control requests name their run in the path and need no header.

## Environment

These are set for a child of `irimi shadow` or `irimi replay`, and by you under `irimi serve`:

| Variable | Meaning |
| --- | --- |
| `IRIMI_ENGINE_ACTIVE` | `1` when irimi is in front of the process. An SDK does nothing without it. |
| `IRIMI_CONTROL` | the control endpoint's base URL, with no trailing slash |
| `IRIMI_RUN` | the process run's id: where tool calls made outside any run go. Unset under `irimi serve`, where such calls go to `unattributed`. |
| `IRIMI_REPLAY_RUN` | set only in a replay's child, to the replay's run id. A run started while it equals `IRIMI_RUN` adopts that run: post no start or end of its own, and label its requests with it. |
| `IRIMI_AGENT_VERSION` | the agent's own version, to send as `agent_version` |

The proxy variables and CA bundle an SDK must also honour are in
[how-it-works.md](how-it-works.md#what-the-child-gets).
