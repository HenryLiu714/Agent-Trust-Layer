# Trace format, version 1

This is the reference for what irimi writes about a run, and the contract between the phase that
records runs (Phase 3) and every phase that reads them: the Phase 4 report prints a stored run,
and Phase 5 replays one. The shape and its JSON codec live in `src/irimi/trace.py`, which does no
I/O. The store (#70) writes the files described here. `tests/test_trace.py` decodes the example at
the end of this page with those codecs, so the page and the code cannot drift apart.

## Layout on disk

```
<root>/                         default $IRIMI_HOME/store
  blobs/<sha256-hex>            a redacted body, written once
  runs/<run_id>/run.json        the RunRecord (trace.run_to_json), rewritten atomically
  runs/<run_id>/events.jsonl    one event per line, in seq order
  unattributed/events.jsonl     exchanges whose run_id is trace.UNATTRIBUTED or invalid
```

- A **run id** is also a directory name, so it must match `^[A-Za-z0-9_-]{1,64}$`
  (`trace.RUN_ID_PATTERN`, `trace.is_valid_run_id`). An `Irimi-Run` header whose value does not
  match is treated as absent, and the exchange falls back to the engine's own run id.
- A **body** is stored once, as `blobs/<sha256 of its bytes>`, and never inline in a record. An
  exchange names it with a body ref. An empty body stores nothing, and its ref is `null`.
- Everything is redacted before it reaches disk (#69). A credential in the example below
  appears as a `<redacted:…>` placeholder for that reason.

## Records

Every timestamp is wall-clock seconds since the epoch (`time.time()`), as a JSON number.

### `run.json`: the run record (`trace.RunRecord`)

| Field | Type | Meaning |
| --- | --- | --- |
| `schema_version` | integer | The format version this run was written in. `1` here. |
| `run_id` | string | The run's id; also its directory name. |
| `mode` | `"shadow"` | The only mode in Phase 3. `record` and `replay` come later. |
| `attribution` | `"process"`, `"sdk"`, `"header"` | How the run came to exist. `process` is the `irimi shadow -- <cmd>` run. `sdk` is a run the SDK started around a trigger. `header` is a run id that arrived on exchanges with no start event. |
| `trigger` | trigger or `null` | What started the run. `null` for a `header` run. |
| `agent_version` | string or `null` | The agent's own version, from `IRIMI_AGENT_VERSION`. |
| `engine_version` | string | The irimi that recorded the run. |
| `sdk_version` | string or `null` | The SDK that started the run. `null` for a run it did not start. |
| `started_at` | number or `null` | When the run started. |
| `ended_at` | number or `null` | When the run ended. |
| `outcome` | `"ok"`, `"error"` or `null` | `null` means the run is incomplete: it never ended, or irimi stopped first. |
| `error` | error or `null` | Why the run failed, when `outcome` is `"error"`. |
| `exit_code` | integer or `null` | The child's exit code, set only for the `process` run. |
| `dropped_events` | integer | Events the store could not write. `0` means the run is whole. |

A **trigger** (`trace.Trigger`):

| Field | Type | Meaning |
| --- | --- | --- |
| `name` | string | The display name. |
| `entrypoint` | string or `null` | `module:qualname` of the wrapped function. `null` for a context-manager run and for the process run. |
| `args` | any JSON | The captured arguments. How a value that is not JSON gets captured is #74's. |
| `replayable` | boolean | Whether `args` captured everything needed to call `entrypoint` again. |

An **error** (`trace.ErrorInfo`) is `{"type": string, "message": string}`. `type` is the
exception's `module.qualname`, and `message` is at most 1000 characters.

### `events.jsonl`: one event per line

Every line is a JSON object with `seq` and `type` first, then the fields of the record `type`
names:

```
{"seq": <integer>, "type": "exchange" | "tool_call" | "telemetry", ...fields}
```

**An `exchange`** (`irimi.exchange.Exchange`) is one HTTP exchange irimi saw or made.

| Field | Type | Meaning |
| --- | --- | --- |
| `run_id` | string | The run it belongs to. |
| `service` | string | The service its map names, or its host when no map claims it. |
| `operation` | string | The route's operation (`refunds.create`), or `METHOD /path` when no route matched. |
| `kind` | `read`, `write`, `llm`, `telemetry`, `unknown` | What the classifier said it is. |
| `answered_by` | `live`, `fake-L0`, `fake-L1`, `delegated`, `overlay` | Who produced the response the agent got. |
| `validation` | `validated`, `unvalidated` | Always `unvalidated` in Phase 3. |
| `door` | `forward`, `reverse` | Which door it came in by. |
| `issued_by` | `agent`, `engine` | `engine` is a read irimi made on its own account, the L3 precondition read. |
| `target` | string | The address of the answer target that answered a `delegated` exchange. `""` otherwise. |
| `flags` | list of strings | Facts about this exchange, each from the list in `exchange.py`. |
| `overlay` | `full`, `partial` or `null` | How much of the run's faked writes the overlay could show in this read. `null` when it did not consider it. |
| `precondition` | `passed`, `rejected`, `not_evaluable` or `null` | What L3 said about this write before it was faked. `null` when nothing was asked. |
| `rejection_code` | string | The service's code for an L3 rejection. `""` otherwise. |
| `would_fire` | list of strings | The webhooks an accepted, faked write would have made the service send. |
| `currency` | string | The currency the write's L3 read found. `""` when there was no such read. |
| `started_at` | number | When irimi first parsed the request. An engine-issued read starts when irimi dialled it. |
| `ended_at` | number | When irimi finished the exchange. `started_at <= ended_at`. |
| `request` | request | What was asked, as irimi forwarded and recorded it. |
| `response` | response or `null` | What came back. `null` when nothing did: a lost upstream, an unreachable target. |

A **request** is `method`, `scheme`, `host`, `port` (integer), `path`, `query` (without the `?`),
`headers` and `body`. A **response** is `status` (integer), `headers` and `body`.

- `headers` is a list of `[name, value]` pairs, not an object: order and repeats (`set-cookie`) are
  part of what was sent. A request's names are lower-case; a response's are as the service sent
  them.
- `body` is a **body ref**, `{"sha256": string, "size": integer, "truncated": boolean}`, or `null`
  for an empty body. `sha256` names the file under `blobs/` and is 64 lower-case hex digits.
  `size` is the stored length. `truncated` says the stored bytes are only the start of the body.

**A `tool_call`** (`trace.ToolCall`) is a tool call the proxy cannot see, reported by the SDK.

| Field | Type | Meaning |
| --- | --- | --- |
| `tool_call_id` | string | Its id, unique in the run. |
| `run_id` | string | The run it belongs to. |
| `name` | string | The tool's name. |
| `kind` | `read`, `write` | What the tool does. |
| `ran` | `real`, `shadow` | `real` ran the function; `shadow` ran its shadow stand-in. |
| `args` | any JSON | What it was called with. |
| `result` | any JSON | What it returned. `null` when it raised. |
| `error` | error or `null` | What it raised. |
| `started_at`, `ended_at` | number | The call's span. |

**A `telemetry`** event (`trace.TelemetrySeen`) records only that one telemetry exchange happened:
`run_id`, `host` and `started_at`. Telemetry is forwarded, and its requests and responses are never
stored.

## Ordering

Events in a run are ordered by `seq`, which the store assigns as it receives each finished event,
so `seq` is completion order. `started_at` recovers start order.

A write that waits on its own precondition read shows both. In the example below, the refund
starts first and finishes last, so its precondition read is `seq` 1 and the refund is `seq` 2.

## Versioning

`schema_version` is one integer for the whole format, carried by `run.json`. A run's events are
read under their run's version.

- A decoder refuses a `schema_version` greater than its own (`trace.SCHEMA_VERSION`) with
  `trace.TraceFormatError`.
- A decoder ignores a field it does not know. Adding a field is therefore not a version bump,
  but changing what an existing field means is.
- Every field this page lists is required. A record missing one, a value of the wrong type, or a
  value outside a field's vocabulary is a `TraceFormatError` too, and no decoder raises anything
  else.

## Example

One SDK run of a refund agent: the agent refunds a charge, irimi checks the charge before faking
the refund, the agent marks its order refunded through a wrapped tool call, and its error
reporter sends one event to Sentry.

```json run.json
{
  "schema_version": 1,
  "run_id": "4f1c9a2b7d3e8a60",
  "mode": "shadow",
  "attribution": "sdk",
  "trigger": {
    "name": "handle_refund_request",
    "entrypoint": "agent.handlers:handle_refund_request",
    "args": {"order_id": "ord_1042"},
    "replayable": true
  },
  "agent_version": "refund-agent@1.4.0",
  "engine_version": "0.0.1",
  "sdk_version": "0.0.1",
  "started_at": 1790600000.125,
  "ended_at": 1790600001.875,
  "outcome": "ok",
  "error": null,
  "exit_code": null,
  "dropped_events": 0
}
```

On disk each event is one line. They are shown here one per block, indented, to be readable.

The precondition read irimi issued before faking the refund. It started after the refund and
finished before it:

```json events.jsonl
{
  "seq": 1,
  "type": "exchange",
  "run_id": "4f1c9a2b7d3e8a60",
  "service": "stripe",
  "operation": "charges.retrieve",
  "kind": "read",
  "answered_by": "live",
  "validation": "unvalidated",
  "door": "forward",
  "issued_by": "engine",
  "target": "",
  "flags": [],
  "overlay": null,
  "precondition": null,
  "rejection_code": "",
  "would_fire": [],
  "currency": "",
  "started_at": 1790600001.3125,
  "ended_at": 1790600001.5,
  "request": {
    "method": "GET",
    "scheme": "https",
    "host": "api.stripe.com",
    "port": 443,
    "path": "/v1/charges/ch_3PqA1",
    "query": "",
    "headers": [
      ["authorization", "<redacted:9f2c4e1a7b3d5f60>"],
      ["accept", "application/json"]
    ],
    "body": null
  },
  "response": {
    "status": 200,
    "headers": [["Content-Type", "application/json"]],
    "body": {
      "sha256": "326207aa2b5e05d67ef8e21aaaacfe80cd30e6110917b3bd7c889977b8ac9194",
      "size": 103,
      "truncated": false
    }
  }
}
```

The agent's refund, faked at L1 after the check passed:

```json events.jsonl
{
  "seq": 2,
  "type": "exchange",
  "run_id": "4f1c9a2b7d3e8a60",
  "service": "stripe",
  "operation": "refunds.create",
  "kind": "write",
  "answered_by": "fake-L1",
  "validation": "unvalidated",
  "door": "reverse",
  "issued_by": "agent",
  "target": "",
  "flags": ["fidelity:L1"],
  "overlay": null,
  "precondition": "passed",
  "rejection_code": "",
  "would_fire": ["refund.created", "charge.refunded"],
  "currency": "usd",
  "started_at": 1790600001.25,
  "ended_at": 1790600001.5625,
  "request": {
    "method": "POST",
    "scheme": "https",
    "host": "api.stripe.com",
    "port": 443,
    "path": "/v1/refunds",
    "query": "",
    "headers": [
      ["host", "api.stripe.com"],
      ["authorization", "<redacted:9f2c4e1a7b3d5f60>"],
      ["content-type", "application/x-www-form-urlencoded"]
    ],
    "body": {
      "sha256": "865bcafa7aceba593770cf3674e74a400ee0b75dca5f375006e54460d008fe1e",
      "size": 27,
      "truncated": false
    }
  },
  "response": {
    "status": 200,
    "headers": [["content-type", "application/json"]],
    "body": {
      "sha256": "98408eaa40f18069664e1f12333713775747a7905f04257afa177cda48cdad4d",
      "size": 126,
      "truncated": false
    }
  }
}
```

The agent's own database write, which the proxy never sees, run as its shadow stand-in:

```json events.jsonl
{
  "seq": 3,
  "type": "tool_call",
  "tool_call_id": "tc_1",
  "run_id": "4f1c9a2b7d3e8a60",
  "name": "db.mark_refunded",
  "kind": "write",
  "ran": "shadow",
  "args": {"order_id": "ord_1042", "refund_id": "re_Kd82nQ4xT1bV9mZ3pL6wR0yS"},
  "result": {"updated": 1},
  "error": null,
  "started_at": 1790600001.625,
  "ended_at": 1790600001.6875
}
```

One Sentry envelope, counted and not stored:

```json events.jsonl
{
  "seq": 4,
  "type": "telemetry",
  "run_id": "4f1c9a2b7d3e8a60",
  "host": "o0.ingest.sentry.io",
  "started_at": 1790600001.75
}
```

The three blobs these events name:

| `blobs/…` | Bytes |
| --- | --- |
| `326207aa…c9194` | `{"id":"ch_3PqA1","object":"charge","amount":4900,"amount_refunded":0,"currency":"usd","refunded":false}` |
| `865bcafa…8fe1e` | `charge=ch_3PqA1&amount=4900` |
| `98408eaa…dad4d` | `{"id":"re_Kd82nQ4xT1bV9mZ3pL6wR0yS","object":"refund","amount":4900,"charge":"ch_3PqA1","currency":"usd","status":"succeeded"}` |
