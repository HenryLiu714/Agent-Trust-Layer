# The SDK

For developers whose agent serves many requests in one process, or does work the proxy cannot
see. `irimi shadow -- <cmd>` alone treats one process as one run and sees only HTTP. The SDK,
`irimi.sdk`, marks in your own code where each run starts and ends, and labels the database writes,
files and library calls irimi cannot see, so a fake runs in their place.

The SDK does nothing unless irimi is running the process (`IRIMI_ENGINE_ACTIVE=1`), so it is safe
to leave in production code, and it never raises into your agent (the SDK never raises).
`pip install irimi` installs the proxy's dependencies too; the SDK itself imports only the
standard library and irimi's own modules.

## Quick start

```python
import requests
from irimi import sdk


def no_db_write(order_id: str, refund_id: str) -> dict:
    return {"updated": 0}  # what runs instead under irimi


@sdk.write_tool(fake=no_db_write)
def mark_refunded(order_id: str, refund_id: str) -> dict:
    return db.execute("UPDATE orders SET refunded = 1 WHERE id = ?", (order_id,))


@sdk.trigger
def handle_ticket(ticket: Ticket) -> None:
    refund = requests.post(STRIPE_REFUNDS, data={"charge": ticket.charge}, auth=(KEY, ""))
    mark_refunded(ticket.order_id, refund.json()["id"])  # irimi faked the refund; the fake runs
```

`db`, `Ticket`, `KEY` and `STRIPE_REFUNDS` stand for your own code. Run it with
`irimi shadow -- python -m my_agent`. Each call of `handle_ticket` is one run, stored with its
arguments; the summary lists the refund, the tool write and how to replay the run.
`examples/replay/` is a complete agent built this way, and `examples/sidecar/` one beside
`irimi serve`.

## Serve mode

Under `irimi serve` the agent is not irimi's child, so give its environment what `irimi shadow`
would have set ([serve-mode.md](serve-mode.md) has the whole table):

| Variable | Value |
| --- | --- |
| `HTTPS_PROXY`, `HTTP_PROXY`, `https_proxy`, `http_proxy` | `http://irimi:4000` |
| `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE` | irimi's `ca.pem` |
| `IRIMI_ENGINE_ACTIVE` | `1` |
| `IRIMI_CONTROL` | `http://irimi:4000/_irimi` |

Leave `IRIMI_RUN` unset. Set `IRIMI_AGENT_VERSION` to record your agent's version on each run.

## Threads and asyncio

The current run lives in a context variable. `asyncio` tasks and `asyncio.to_thread` inherit it.
A `threading.Thread`, `ThreadPoolExecutor.submit` or `loop.run_in_executor` does not, unless the
callable goes through `sdk.propagate`:

```python
executor.submit(sdk.propagate(refund_one), charge_id)
```

A pool's worker thread keeps the context it started in, so use `sdk.propagate` there even on a
build where new threads copy their creator's context.

## Which HTTP clients carry the run

irimi files a request under its run by the `Irimi-Run` header, never by connection or timing. On
the first run, the SDK patches HTTP client classes so each request a run sends to irimi's listener
carries it:

| Client | Labelled |
| --- | --- |
| `urllib.request`, `http.client` | yes |
| `requests`, `urllib3`, stripe-python (sync), slack_sdk `WebClient` | yes |
| `httpx`, sync and async, and SDKs built on it | yes |
| `httpx2`, and SDKs built on it, such as the Anthropic SDK 2.x | no |
| `aiohttp` (slack_sdk `AsyncWebClient`), `pycurl`, raw sockets, subprocesses | no |

A request through a client that is not labelled belongs to the process run under `irimi shadow`,
or to `unattributed` under `irimi serve`. Such a client must send the header itself:

```python
run_id = sdk.current_run_id()
client.messages.create(..., extra_headers={"Irimi-Run": run_id} if run_id else None)
```

`examples/replay/agent.py` does this for the Anthropic SDK. The header goes only to irimi's
listener (the host and port `IRIMI_CONTROL` or a proxy variable names, as spelled there), and irimi
removes it before forwarding, so a run id never reaches a service.

## Reference

Everything public is in `irimi.sdk.__all__`; anything else under `irimi.sdk` is private.

### `@sdk.trigger`, `@sdk.trigger(name=...)`

Mark a function as the start of a run. Each call outside a run is one run, recorded with its
arguments; a call inside a run joins it. Works on sync and async functions and methods,
`staticmethod` and `classmethod` included.

- `name`: the run's display name; defaults to the function's `module:qualname`.
- Raises `TypeError` at decoration for a generator function, a callable with no importable
  `module:qualname`, or a `name` that is not a string or is passed positionally.

### `sdk.run(name, *, inputs=None)`

Mark a block as one run, with `with` or `async with`, where there is no single function to
decorate (one message in a worker loop). Entering gives a `Run`, or `None` while the SDK is
inactive or the block joins a run. `inputs` is recorded as the run's arguments; a replay of such a
run needs `--entrypoint`. Raises `TypeError` when `name` is not a string.

```python
with sdk.run("nightly", inputs={"date": "2026-09-29"}) as r:
    reconcile()
```

### `sdk.Run`

A frozen dataclass: `id`, the run's id as `irimi runs show` and `irimi replay` take it, and
`name`.

### `@sdk.read_tool`, `@sdk.read_tool(name=...)`

Label a function as a read the proxy cannot see, such as a database query. It runs for real; under
irimi each call and its result are recorded in the current run, and a replay answers the call from
the recording when it can. `name` defaults to `module:qualname`.

### `@sdk.write_tool(*, fake, name=None)`

Label a function as a write the proxy cannot see, and name the fake that replaces it. Under irimi
only `fake` runs, with the same arguments (the instance first for a method); without irimi the real
function runs. `fake` must be callable, sync or async as the real function is, not a generator
function and not the function itself; each mistake is a `TypeError` at decoration.

| `sdk.is_active()` | Tool | What runs | What irimi records |
| --- | --- | --- | --- |
| false | read | the real function | nothing |
| false | write | the real function | nothing; one warning if a proxy is set |
| true | read | the real function, or the recorded answer in a replay | a tool call, `ran: real` (or `recording`) |
| true | write | only the fake | a tool call, `ran: shadow` |

### `sdk.current_run_id()`

The id of the run this code is running in, or `None` outside a run or while the SDK is inactive.

### `sdk.propagate(fn)`

Return a callable with `fn`'s signature that runs it in the current run, on whichever thread calls
it.

### `sdk.is_active()`

Whether irimi is running this process: `IRIMI_ENGINE_ACTIVE=1`, read at each call.

### `sdk.instrument()`

Patch `http.client` and httpx, process-wide, to label each request a run sends to irimi. The SDK
calls it on the first run; call it at startup to cover clients that cache connections earlier. A
no-op unless irimi is active, and safe to call again.

### `sdk.RecordedToolError`

An exception a read tool raised in the recorded run, raised again in a replay. For a builtin such
as `KeyError` it also subclasses that builtin, so `except KeyError` still catches it. `type_name`
is the original's type as recorded (`builtins.KeyError`), `message` its `str`, and `args` is
`(message,)`: the original's other attributes are not stored.

### `irimi.testing`

Replay a stored run from a test. It starts the proxy in a subprocess of the test's interpreter, so
it needs irimi's full install; agent code never imports it.

- `testing.replay(run_id, call=None, *, args=(), kwargs=None, store=None)` replays the run in a
  fresh process and returns a `ReplayResult`. Without `call`, the recorded trigger runs with its
  recorded arguments; with it, `call` (importable as `module:qualname`) runs with `args` and
  `kwargs`, which must be recordable. `store` defaults to `$IRIMI_STORE`, else
  `$IRIMI_HOME/store`. A replay that differed is a result, not an error.
- `testing.ReplayError`: arguments without `call`, a `call` not importable by name, arguments that
  do not bind or record, or a replay that wrote no result.
- `testing.ReplayResult`: `source_run_id`, `replay_run_id` (None when refused before it ran),
  `exit_code` (as `irimi replay`'s), `diverged_at` (the first call the recording did not answer, or
  None) and `lines` (what `irimi replay` printed). `result.same` is true when the writes were the
  same; `str(result)` is the printed report.

```python
result = testing.replay(run_id, handle_ticket, args=(Ticket("T-1"),))
assert result.same, result
```

## What a run records

A trigger's run stores its name, its entrypoint (`module:qualname`), the call's arguments bound to
its parameters and captured as JSON, `IRIMI_AGENT_VERSION` if set, and how it ended: `ok`, or
`error` with the exception's type and message. The exception itself propagates unchanged. How
values are captured, and which are replayable, is in
[trace-format.md](trace-format.md#captured-arguments).

## Edge cases

- **Start the agent by module.** Under `python -m my_agent` a trigger records as
  `my_agent:handle_ticket`, which a replay can import. Under `python my_agent.py` it records as
  `__main__:handle_ticket`, which it cannot, and the SDK warns once.
- **Nesting.** A trigger or `sdk.run` inside a run joins it. One `sdk.run(...)` object may be
  entered again, nested, from several threads or tasks at once, or kept as a module constant; each
  entry outside a run is a run of its own.
- **Names** are strings, cut to 1000 characters outside any secret's shape; a secret across the
  cut is replaced by `<redacted>`.
- **A sync function that returns a coroutine** keeps its run, or its tool call open, until the
  coroutine finishes.
- **A task outliving its run.** A task created inside a run keeps the run's id after the trigger
  returns, so its requests are stored in the run after its end.
- **A request that names its own run.** An `http.client` request gets the SDK's header ahead of the
  agent's, and irimi takes the first; an httpx request that already has one keeps it.
- **Redirects.** The header is added only on a connection to irimi, so the next hop of a redirect
  away from irimi never carries it.
- **The real-write warning.** A write tool that runs for real while `HTTPS_PROXY` is set but
  `IRIMI_ENGINE_ACTIVE` is not logs one warning per process: it was probably meant to run under
  irimi.
- **HTTP inside a tool** is recorded as the run's own exchanges, with no link to the tool call.
- **No overlay for tools.** A read tool after a write tool's fake reads what was there before;
  nothing applies the fake's write to it.
- **Failures to report** (irimi unreachable, a slow response, another protocol version) are logged
  once per kind on the `irimi.sdk` logger, and the agent carries on. After a timeout or an
  unreachable endpoint the SDK stops posting for 30 seconds. The wire protocol is
  [control-protocol.md](control-protocol.md).
- **Tools in a replay** are covered in [replay.md](replay.md#tools-in-a-replay).
