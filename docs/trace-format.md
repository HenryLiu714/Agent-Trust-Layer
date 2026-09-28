# Trace format, version 1

This is the reference for what irimi writes about a run, and the contract between the phase that
records runs (Phase 3) and every phase that reads them: the Phase 4 report prints a stored run,
and Phase 5 replays one. The shape and its JSON codec live in `src/irimi/trace.py`, which does no
I/O. The store (#70) writes the files described here. `tests/test_trace.py` holds this page to the
code: the example at the end decodes with those codecs and re-encodes to itself, key for key, and
the first column of each field table below is exactly the keys its encoder writes, in order.

## Layout on disk

```
<root>/                         default $IRIMI_HOME/store
  blobs/<sha256-hex>            a redacted body, written once
  runs/<run_id>/run.json        the RunRecord (trace.run_to_json), rewritten atomically
  runs/<run_id>/events.jsonl    one event per line, in seq order
  unattributed/events.jsonl     events whose run_id is trace.UNATTRIBUTED or invalid
```

- A **run id** is also a directory name, so it must match `^[A-Za-z0-9_-]{1,64}$`
  (`trace.RUN_ID_PATTERN`, `trace.is_valid_run_id`). An `Irimi-Run` header whose value does not
  match is treated as absent, and the exchange falls back to the engine's own run id.
- A **body** is stored once, as `blobs/<sha256 of its bytes>`, and never inline in a record. An
  exchange names it with a body ref. An empty body stores nothing, and its ref is `null`.
- `unattributed/events.jsonl` holds every kind of event, tool calls included (#76), not only
  exchanges. It has no `run.json`: see Versioning for the version its lines are read under.
- Everything is redacted before it reaches disk (#69): see Redaction. A credential in the example
  below appears as a `<redacted:…>` placeholder for that reason.

## Redaction

The store writes `redact.redact_exchange(exchange, key)`, never the exchange the engine answered
with, and passes a trigger's args and a tool call's args and result through `redact.redact_json`.
Redaction is always on; there is no switch and no per-repo rule in Phase 3.

- **A placeholder** is `<redacted:` + the first 16 hex digits of HMAC-SHA256(key, value) + `>`.
  The key is `$IRIMI_HOME/redact.key`, 32 random bytes created 0600 on first use (in a home
  created 0700 if it is missing), so one secret is always one placeholder on one install, and a
  replay matches a secret against its own placeholder. A symlink to the key is followed, so two
  installs can be given one key (a mounted secret). A `redact.key` that does not lead to a regular
  file of 32 bytes - a directory, a dangling symlink, a short file - is refused, never replaced.
- **Headers.** A credential header's whole value is replaced (`redact.is_credential_header`, the
  rule an answer target's request is stripped by), and so is `Set-Cookie` on a response. A value
  that is an absolute `http(s)` URL (`Location`, `Referer`) is redacted as a URL: its query and
  fragment as a query string, its path by its own host's rules.
- **Secret keys.** A query parameter, a form field (each by its last non-empty bracket segment,
  so `card[token]` and `token[]`) or a JSON object key at any depth named `api_key`, `apikey`,
  `secret`, `client_secret`, `password`, `access_token`, `refresh_token`, `id_token` or `token` -
  compared whole and case-insensitively, so `max_tokens` is not one - has its value replaced. A
  value that is not a string is replaced by the placeholder of its JSON with sorted keys. `null`
  and `""` hide nothing and are left as they are.
- **Secret shapes.** Stripe, Slack, GitHub, AWS, Anthropic and OpenAI key shapes
  (`redact.SECRET_PATTERNS`) are replaced wherever they appear: header names and values, the
  path, the query string and a form body (names and values), JSON strings and keys, every line of
  a text body (SSE included), `target` and `operation`. A shape glued to a preceding letter or
  digit (`task_test_runner`) is part of a word and is left alone, unless that letter or digit ends
  an escape - `\n`, `\t`, `\r`, `\b`, `\f`, `\uXXXX` or `%XX` - which is how text stored raw
  spells a separator (`key:\nsk_live_…`, `/Bearer%20sk_live_…`).
- **Credential-path hosts.** On `hooks.slack.com` (compared without a root dot, so
  `hooks.slack.com.` too) the path is the secret, so the whole path is replaced: in the request,
  in a delegated exchange's `target`, in a header URL that points there, and in the `operation`
  of a request no route matched, which is named `METHOD /path`. Any other place the exchange
  carries that path - a body, binary or not, or a header value, the path as sent or as the shape
  rule left it - has it replaced too: irimi's own 502 for an unreachable answer target names the
  target's URL. A path of `/` alone is left alone there.
- **How a body is read.** A body that is one JSON document (after a byte-order mark, which is kept)
  is walked as JSON, whatever its content type claims, a form's included. Otherwise a form content
  type is read as a form. Otherwise the body is read line by line, a line ending at CRLF, LF or a
  lone CR as an SSE line does: a line that is one JSON document (NDJSON), or an SSE `data:` field
  whose value is one, is walked as JSON, and every other line is scanned for shapes.
- **Bytes are kept when nothing matched.** A body is re-serialized only when a rule changed it: a
  JSON document compactly, with `ensure_ascii=False` (a JSON line alone, its `data:` prefix and
  line ending kept), a form body pair by pair.
- **Failure is closed.** A part that cannot be redacted is stored as `<redaction-failed>` (a path as
  `/<redaction-failed>`), and the exchange gains the flag `redaction-failed`. That includes a
  JSON body or JSON line the walk cannot see whole: an object with a repeated key, or JSON
  `json.loads` refuses for a reason other than not being JSON. `redact_json` returns
  `<redaction-failed>` for a value that is not JSON, such as a tuple.

Known limitations:

- **A body that is not valid UTF-8 is stored unscanned.** A credential inside a binary body
  reaches disk as it was sent.
- **JSON that is not a whole document on one line is scanned for shapes only.** A secret key's
  value is replaced only if it also has a secret shape when its JSON spans several lines of a
  body that is not one document (a multi-line SSE `data:` field, a truncated document), or is
  carried inside a JSON string (a streamed tool call's `partial_json`).
- **The key is per install.** A secret recorded on a production box and on a CI runner gets two
  different placeholders, so recordings from two machines do not match each other's redacted
  values. Whether recordings move between machines is a Phase 6 decision (push and pull).

## Records

Every timestamp is wall-clock seconds since the epoch (`time.time()`), as a finite JSON number.
An exchange's `started_at` and `ended_at` are `0.0` only on an exchange built with no clock to read
(a test, or a caller with none); never on one the engine recorded.

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
| `operation` | string | The route's operation (`refunds.create`), or `METHOD /path` when no route matched. `""` when the classify/answer decision itself failed, flagged `decision-failed` (`pipeline.unclassified`). |
| `kind` | `read`, `write`, `llm`, `telemetry`, `unknown` | What the classifier said it is. |
| `answered_by` | `live`, `fake-L0`, `fake-L1`, `delegated`, `overlay` | Who produced the response the agent got. |
| `validation` | `validated`, `unvalidated` | Always `unvalidated` in Phase 3. |
| `door` | `forward`, `reverse` | Which door it came in by. |
| `issued_by` | `agent`, `engine` | `engine` is a read irimi made on its own account, the L3 precondition read. |
| `target` | string | The address of the answer target a `delegated` exchange was pointed at: the one that answered it, or the one that could not be reached or applied, flagged `target-failed`. `""` otherwise. |
| `flags` | list of strings | Facts about this exchange, each from the list in `exchange.py`. |
| `overlay` | `full`, `partial` or `null` | How much of the run's faked writes the overlay could show in this read. `null` when it did not consider it. |
| `precondition` | `passed`, `rejected`, `not_evaluable` or `null` | What L3 said about this write before it was faked. `null` when nothing was asked. |
| `rejection_code` | string | The service's code for an L3 rejection. `""` otherwise. |
| `would_fire` | list of strings | The webhooks an accepted, faked write would have made the service send. |
| `currency` | string | The currency the write's L3 read found. `""` when there was no such read. |
| `started_at` | number | When irimi first parsed the request. An engine-issued read spans its `Reader` call. |
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
- The decoder hands the whole ref to the store's `get_body`, but a decoded `Exchange` carries only
  the bytes it returns: `size` and `truncated` do not survive decoding. The store that truncates a
  body (#70) must therefore also say so in the exchange's `flags`, and declaring that flag is #70's.

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
| `started_at` | number | When the call began. |
| `ended_at` | number | When it returned or raised. |

**A `telemetry`** event (`trace.TelemetrySeen`) records only that one telemetry exchange happened:
`run_id`, `host` and `started_at`. Telemetry is forwarded, and its requests and responses are never
stored.

## Ordering

Events in a run are ordered by `seq`, which the store assigns as it receives each finished event,
so `seq` is completion order. `started_at` recovers start order.

A write that waits on its own precondition read shows both. In the example below, the refund
starts first and finishes last, so its precondition read is `seq` 1 and the refund is `seq` 2.

## Versioning

`schema_version` is one integer for the whole format, `trace.SCHEMA_VERSION`, carried by
`run.json`. Event lines carry none.

- A run's `events.jsonl` is read under its `run.json`'s version. A writer never appends events to
  a run whose `run.json` carries a `schema_version` other than its own.
- `unattributed/events.jsonl` has no `run.json`, and is read under the reader's own
  `trace.SCHEMA_VERSION`.
- A decoder refuses a `schema_version` greater than its own, or less than 1, with
  `trace.TraceFormatError`.
- A decoder ignores a field it does not know, in any record or nested object.
- **Every field version 1 shipped with is required**: every field on this page as #68 wrote it.
- **A field added later within version 1 is optional on decode.** When it is absent the decoder
  gives the field's dataclass default, so a recording made before the field existed still reads.
  Whoever adds the field adds that decoder path and its test, and marks the field on this page with
  the issue that added it; #71's `stream_chunks` is the first. Such a field must be one an older
  reader, which ignores it, still reads correctly without. Adding it is not a version bump.
- **These are version bumps:** removing or renaming a field, changing its type or what it means,
  and adding a value to a closed vocabulary: `mode`, `attribution`, `outcome`, `kind`, `ran`,
  `answered_by`, `validation`, `door`, `issued_by`, `overlay`, `precondition`, and an event's
  `type`. An older reader refuses a vocabulary value it does not know, rather than reading a new
  mode as an old one.
- `flags` is open: a reader accepts a flag it does not know. `would_fire` is open too, since its
  values are event names from a service map.
- A record that is not a JSON object, lacks a required field, or holds a value of the wrong type,
  outside a closed vocabulary or not finite is a `TraceFormatError`. No decoder raises anything
  else on a malformed record, with one exception: `trace.exchange_from_json` and
  `trace.event_from_json` hand every body ref to the caller's `get_body`, and whatever that raises
  (a blob missing from the caller's store, say) reaches the caller unchanged.

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
