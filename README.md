# irimi

*Shadow mode for AI agents: reads are real, writes are virtual, and reads see the writes.*

irimi is a local proxy you put between an agent and the services it calls. Reads pass through to
the real service. Writes are intercepted and answered with a realistic fake success, and the
agent's later reads are edited to show those writes, so its view of the world stays consistent.
At the end you get a list of every write the agent would have made, and which of them the real
service would have refused.

It is for teams that do not yet trust an agent with write access: run it against production reads
for real, and see what it would have done before anything happens.

## Status

What works today, all of it covered by tests that need no network and no keys:

- `irimi shadow -- <command>` runs a command with its HTTP(S) traffic in shadow mode and prints a
  summary; `irimi serve` runs the same proxy on its own.
- Service maps for Stripe, Slack, OpenAI, Anthropic and six telemetry backends decide what each
  request is.
- Writes are answered locally at four levels of fidelity (L0 to L3, below): an echo, a full
  response object, reads that show the write, and a check against the real service's state.
- Idempotency keys, would-have-fired webhooks, answer targets you control, and the reverse door for
  SDKs that ignore proxy variables.
- Eleven sample agent workflows run end to end, bare and under `irimi shadow`, on every test run.

Every `serve` and `shadow` run is recorded, redacted, in the trace store under `$IRIMI_HOME/store`
([`docs/trace-format.md`](docs/trace-format.md)), and `irimi runs list` / `runs show` read it
back. What does not exist yet: the SDK (`irimi.sdk`), `irimi shadow --serve`, replay and
`irimi compare`. See [Roadmap](#roadmap).

## Quickstart

Requirements: macOS or Linux, and [`uv`](https://docs.astral.sh/uv/) (the setup script installs it
if it is missing). `uv` fetches Python 3.14 for you; installed as a tool, irimi runs on 3.12 or
newer.

```
git clone https://github.com/HenryLiu714/Agent-Trust-Layer.git
cd Agent-Trust-Layer
scripts/setup.sh              # creates the venv, installs irimi, runs `irimi init`
uv run irimi --help
```

`irimi init` generates a local CA in `$IRIMI_HOME/ca/` (default `~/.irimi/ca/`): `ca.key` (0600)
and `ca.pem` (0644). Running it again is a no-op; `--force` regenerates it.

A first write, under shadow. It needs no network and no key, because the write never leaves your
machine:

```
uv run irimi shadow -- curl -s -X POST https://api.stripe.com/v1/customers/cus_REAL123 \
  -d 'metadata[order_id]=6735' -d email=a@example.com
```

curl gets back a whole Stripe customer object with `id: cus_REAL123`, your `email`, `metadata`
as an object, `livemode: false` and the header `irimi-answered-by: fake-L1`. irimi prints:

```
irimi shadow · run 2336 · listening on 127.0.0.1:4000 · ca /Users/you/.irimi/ca/ca.pem
hosts not routed through the proxy are NOT virtualized.
backstop: none (Phase 4)
fake-L1   write     POST api.stripe.com/v1/customers/cus_REAL123 -> 200  [fidelity:L1]
irimi shadow · run 2336 · 1 exchange · 0.0s · backstop: none (Phase 4)

  api.stripe.com  1 write intercepted

  ○ update customer cus_REAL123  unvalidated (L1)

  1 exchange · 0 live · 0 delegated · 1 virtualized
  These writes did not happen. Would have fired: customer.updated.
```

To see a whole agent, run a sample workflow. It needs no keys and no network either: a fake
internet answers for the real host names, so irimi's shipped maps decide every call.

```
uv run python -m examples.workflows                                 # list workflows and scenarios
uv run python -m examples.workflows w01_ticket_triage double_refund # bare, then under shadow
```

Your own agent runs the same way: `uv run irimi shadow -- python agent.py`.

## How it works

### Reads live, writes virtual

Every request is classified by the first of these rules that answers:

1. the route in a service map: `POST /v1/refunds` is a write because Stripe's map says so;
2. the service's `default_kind:`, for routes its map does not list;
3. RFC 9110: `GET`, `HEAD` and `OPTIONS` are reads;
4. `unknown`.

`read`, `llm` and `telemetry` are forwarded to the real service. `write` and `unknown` are
answered locally and never reach the network. An `unknown` exchange is flagged `unclassified`, so a
route nobody has mapped is faked rather than sent: a `POST` to a real Stripe route that no map
lists never reaches Stripe, and neither does a `DELETE` to a host no map claims.

### Fidelity: L0 to L3

| Level | What the agent gets | Where it applies |
| --- | --- | --- |
| L0 | An echo: a `200` whose body reflects the request's own fields and mints an id for each field the route names (`re_...`, `txn_...`). Slack gets its envelope; an incoming webhook gets the literal `ok`. | Every locally answered write with no fixture. It never fails. |
| L1 | A whole response object from `src/irimi/fixtures/<service>.json`, with the request's fields written over it, so the agent reads `status`, `currency` and the rest instead of `None`. | Stripe `refunds.create`, `customers.update`, `payment_intents.cancel`; Slack `chat.postMessage`. |
| L2 | The overlay: the agent's later reads of the same service show the run's writes. A re-read charge carries the new `amount_refunded`; the refunds list and the Slack channel history carry the new objects; a read of an object the run minted is answered with it instead of a 404. | Stripe and Slack. |
| L3 | A precondition: before faking a write, irimi makes one real read and checks the write against it, with the run's own writes applied. A refund of an already refunded charge comes back as Stripe's own `charge_already_refunded` error. | Stripe refunds (`charge_refundable`); Slack posts (`channel_postable`). |

Some details that keep an SDK from taking the wrong branch. A write that names its resource in the
path gets that id back (`cus_REAL123` above), not a fresh one. Bracketed form fields become nested
objects, repeated keys become lists, and numbers stay numbers at any depth, except under `metadata`,
whose values are always strings. `id`, `object`, `created` and `livemode` belong to the service and
no request can set them; `livemode` is always `false`. A fixture field is overwritten only when the
types agree, and an unknown parameter is dropped as the live API drops it. A fixture file that is
missing or damaged degrades the answer to L0 and flags the exchange `fixture-failed`. A minted
Slack `ts` always sorts after every real message the run has read.

Two more behaviours sit beside the levels. A retry with a Stripe `Idempotency-Key` the run has
already used is answered with the first answer and counted as one write; the same key with
different parameters gets Stripe's `idempotency_error`. And a write irimi accepted records the
webhooks the real service would have sent (a route's `fires:` list). Nothing is delivered.

### Irimi-Answered-By

Every response irimi decided carries `Irimi-Answered-By`: `fake-L0`, `fake-L1`, `delegated` (an
answer target answered it) or `overlay` (a live read whose body irimi edited to show the run's
writes). A response with no such header came from the real service, unchanged. Absence is the
signal: adding a header to a read irimi did not change would make it differ from what the service
sent.

irimi changes one kind of read on its way out. A later `GET /v1/refunds?starting_after=<minted id>`
would be an error on the real Stripe, so irimi drops that cursor before forwarding and marks the
request with `Irimi-Rewrote`, naming what it removed. `Irimi-Rewrote` and `Irimi-Run` are irimi's
own vocabulary: one the agent sends is stripped and never reaches a service.

### The summary

This is `w02_nightly_reconcile refund_then_page` under shadow: the agent refunds a charge, pages
the refunds list starting after the refund irimi minted, and posts to Slack.

```
irimi shadow · run 0f83 · 7 exchanges · 0.1s · backstop: none (Phase 4)

  api.stripe.com  3 reads (1 showing this run's writes)  1 engine read  1 write intercepted
  slack.com       1 engine read  1 write intercepted

  ○ refund $1.00 on ch_N000  unvalidated (L3 preconditions passed)
    ↳ GET /v1/refunds saw it  overlay
  ○ post to #C0RECON: "reconciled 0 for 2026-09-27"  unvalidated (L3 preconditions passed)

  7 exchanges · 5 live · 0 delegated · 2 virtualized
  These writes did not happen. Would have fired: refund.created, charge.refunded.
```

And a refund the real service would have refused, from `w01_ticket_triage double_refund`:

```
  ○ refund $49.00 on ch_TICKET1  unvalidated (L3 preconditions passed)
  ✗ refund $49.00 on ch_TICKET1  would fail: charge_already_refunded
```

- Each write gets one line, written from its route's `human:` template with this request's fields.
  The bracket says what irimi knew: `L3 preconditions passed`, `L2` when the check could not be
  made, `L1` or `L0` when the route has no check, and `unclassified` for a route no map names.
- An `engine read` is one irimi made itself for an L3 check. It is counted apart, so "reads are
  real" means the reads your agent made.
- A `↳` line is one of the agent's own later reads of that service. `saw it  overlay` is a read
  irimi edited to show the write. `saw it in part` and `did not show it  live (partial)` mean irimi
  knows the world it showed was incomplete.
- `live` means forwarded to the real service; `delegated` means an answer target answered;
  `virtualized` means irimi did.
- An amount is formatted from the currency the request carried, or the one the L3 read found.
  Otherwise it prints as sent: dividing by 100 without knowing the currency would print `¥49.00`
  for a 4900-yen refund.
- `Would have fired:` lists webhooks for accepted writes only. A refused write, an idempotent retry
  and a delegated write list none.

### The child process

`irimi shadow` gives the command `HTTP_PROXY`, `HTTPS_PROXY` (and their lowercase forms),
`NO_PROXY=localhost,127.0.0.1`, `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE`,
`NODE_EXTRA_CA_CERTS`, `NODE_USE_ENV_PROXY=1`, `IRIMI_ENGINE_ACTIVE=1`, `IRIMI_RUN` and
`IRIMI_CONTROL`. That covers requests, httpx, urllib, curl and Node's fetch with no code change.
The command's exit code is passed through. One process tree is one run; the subcommand is the only
thing that chooses the mode.

`IRIMI_CONTROL` is `http://127.0.0.1:<port>/_irimi`, irimi's control endpoint on the same listener.
The SDK posts a run's start, its end and the tool calls the proxy cannot see there
(`POST /_irimi/runs/<run_id>/start`, `/tool-calls`, `/end`), and `GET /_irimi/health` reports the
engine's version and the trace store's counters. Every answer carries
`Irimi-Answered-By: control`, and a control request is never forwarded or recorded as an exchange.
The process run (`IRIMI_RUN`) is irimi's: the SDK may post tool calls to it, but a start or an end
for it is refused.

The CA variables replace the child's trust store rather than adding to it. A TLS connection that
skips the proxy (anything on `localhost` or `127.0.0.1`) will fail to verify; point such a client at
plain HTTP or give it its own bundle.

### The reverse door

Some SDKs ignore proxy variables or ship their own CA bundle; stripe-python is the canonical case.
The same listener also accepts plain HTTP at `http://127.0.0.1:4000/<upstream-host>/<path>` and
handles it like any other request:

```python
stripe.api_base = "http://127.0.0.1:4000/api.stripe.com"
stripe.upload_api_base = "http://127.0.0.1:4000/files.stripe.com"
stripe.connect_api_base = "http://127.0.0.1:4000/connect.stripe.com"
stripe.meter_events_api_base = "http://127.0.0.1:4000/meter-events.stripe.com"
```

Write `127.0.0.1`, not `localhost`: irimi listens on IPv4 loopback only, and on macOS `localhost`
resolves to `::1` first. The door is not an open relay. It forwards only to the exact hosts in a
loaded map (`irimi maps list`) or named with `--allow-host`; anything else gets a `403`. A wildcard
host in a map is not an allow-list entry here. The scheme upstream is always https.

### Safety model

- **Fail closed.** Nothing in a proxy hook may raise, because a raised hook forwards the flow and a
  forwarded write escapes shadow mode. A decision that fails is answered with a `502` flagged
  `decision-failed`.
- **THE SCOPE RULE.** No classification that forwards live may apply to a method it did not name
  explicitly. The loader refuses a `default_kind` that forwards live, a live kind on a route with no
  `method:` (which means `*`, and `*` includes `DELETE`), and a live kind on `DELETE`, `PUT` or
  `PATCH` without `persists: false` and a `comment:` saying why nothing persists. The classifier
  enforces it again per request: a live kind reaching a method its route did not name is answered
  locally and flagged `kind-downgraded`. Each scope of this rule was once a real bug here;
  `src/irimi/servicemap/rules.py` tells the story.
- **Stamping.** Every response irimi decided carries `Irimi-Answered-By`; a live forward carries
  none.
- **Targets stay on the machine.** See [Answer targets](#answer-targets).
- **No backstop yet.** A client that ignores the proxy variables, or ships its own CA bundle, talks
  to the real service. The banner says `backstop: none (Phase 4)` for this reason, and
  `w11_leaky_agent` pins each known escape.
- **HTTP(S) only.** Database writes, files on disk, gRPC and WebSocket traffic are not virtualized
  and happen for real. Point database URLs at scratch data. Bytes sent through the proxy that are
  not HTTP, and a connection upgraded to anything but a WebSocket, are refused, never relayed, and
  leave no record (#75).

## Service maps

A service map is YAML that says what a route is: its service and hosts, the operation, its kind,
the one-line `human:` template the summary prints, and the ids a fake mints. The shipped maps live
in `src/irimi/maps/` and need no Python to contribute to. Stripe, Slack (including
`hooks.slack.com`), OpenAI, Anthropic, and six telemetry backends (LangSmith, Langfuse, Sentry,
Datadog, Honeycomb, PostHog) ship today:

```
$ uv run irimi maps list
irimi maps · 10 service(s) · 14 host(s) · 8 pattern(s) · 47 route(s)
  api.anthropic.com           anthropic    2 routes  target: self
  api.eu1.honeycomb.io        honeycomb    2 routes  target: self
  api.honeycomb.io            honeycomb    2 routes  target: self
  api.openai.com              openai       4 routes  target: self
  api.smith.langchain.com     langsmith    4 routes  target: self
  api.stripe.com              stripe      10 routes  target: self
  cloud.langfuse.com          langfuse     2 routes  target: self
  connect.stripe.com          stripe      10 routes  target: self
  eu.api.smith.langchain.com  langsmith    4 routes  target: self
  files.slack.com             slack       10 routes  target: self
  files.stripe.com            stripe      10 routes  target: self
  hooks.slack.com             slack       10 routes  target: self
  meter-events.stripe.com     stripe      10 routes  target: self
  slack.com                   slack       10 routes  target: self
  *.datadoghq.com             datadog      5 routes  target: self
  *.datadoghq.eu              datadog      5 routes  target: self
  *.ddog-gov.com              datadog      5 routes  target: self
  *.ingest.de.sentry.io       sentry       4 routes  target: self
  *.ingest.sentry.io          sentry       4 routes  target: self
  *.ingest.us.sentry.io       sentry       4 routes  target: self
  *.langfuse.com              langfuse     2 routes  target: self
  *.posthog.com               posthog      4 routes  target: self
```

One route looks like this:

```yaml
version: 1
service: stripe
verbs: honest # Stripe reads are GETs, so the HTTP method carries information here
hosts:
  - api.stripe.com
routes:
  - match:
      method: POST
      path: /v1/refunds
    operation: refunds.create
    kind: write # read | write | llm | telemetry | unknown
    human: refund {amount} on {charge}
    fixture: refund # optional: the object in fixtures/stripe.json an L1 answer starts from
    precondition: charge_refundable # optional: the L3 check, from src/irimi/services/
    fires: # optional: the webhooks this write would have caused; nothing is delivered
      - refund.created
      - charge.refunded
    ids:
      id: re_
    volatile:
      - idempotency_key
```

- `match.path` may carry `{name}` segments, each matching one path segment; a literal path beats a
  pattern. Write `match:` in block style: a YAML flow mapping cannot hold a `{`.
- A host may be a wildcard: one leading `*.` in front of two or more labels, so `*.posthog.com`
  matches `eu.i.posthog.com` but never `posthog.com`. Quote it in YAML. An exact host beats a
  pattern, and the longest pattern wins. Several vendors appear more than once because their
  regional intake hosts are separate names (`o<org>.ingest.us.sentry.io` does not end in
  `.ingest.sentry.io`); the shipped regional hosts are pinned by test.
- `fixture:`, `fires:` and `precondition:` are valid only where irimi answers the write itself.
  On a live route they would never be read, and the loader says so.
- Slack's map sets `verbs: post-only`, because its SDKs send every call as `POST`. Only the map
  makes `conversations.history` a read that forwards live.
- `llm` routes (OpenAI's chat completions, responses and embeddings; Anthropic's messages and
  token counting) are forwarded live and counted in their own bucket. Everything else on those
  hosts is unmapped on purpose, so `/v1/files` or `/v1/batches` is faked, not sent. A
  `text/event-stream` response is streamed through token by token and never rewritten.
- `telemetry` is forwarded live in every mode. The trace store keeps only that it happened and to
  which host, never its request or response: a recording of the agent's own tracing is noise, and
  replaying it would re-emit someone else's events.
- `default_kind:` may be `write` or `unknown` only. No shipped map sets it.

`CONTRIBUTING.md` has the checklist for adding a map.

## Answer targets

Every write is answered by a target, and the default target is irimi itself (`self`). A service or
a route can name an address you control instead; the agent gets whatever it answers, and the real
upstream still sees nothing.

Set targets in `./irimi.maps.yaml`, or in `$IRIMI_HOME/maps.yaml` if the working directory has
none. It is merged over the shipped maps, and it may set `target`, `target_reads` and
`forward_auth` and nothing else: an override that could change a route's `kind` would be a way to
turn a write into a read.

```yaml
service: stripe
target: http://127.0.0.1:3000 # answers the service's writes
target_reads: true # optional: its reads too, making it a delegated service
---
service: slack
routes:
  - match:
      method: POST
      path: /api/chat.postMessage
    target: http://127.0.0.1:3111
```

`--target '<host>[<path>]=<url>'` does the same for one run and beats the file;
`--target-reads <host>` adds the reads. A bare host targets the whole service, including routes its
map does not list. Naming a service, route or host no map has is an error, not a silent no-op.
Path handling follows nginx `proxy_pass`: a bare origin keeps the request's path, and a target with
a path replaces all of it, so a stub that needs the id in `/v1/customers/{customer}` should be given
a bare origin. The query, method, body and content type pass through.

The rules that keep a target from becoming a way out of the machine are checked when the maps load
and again when the answer is chosen:

- A target must be loopback (`127.0.0.1`, `::1`, `localhost`). `--allow-target-host <host>` is the
  deliberate exception, and it prints a warning.
- A target may not be irimi's own listener.
- Credential headers are stripped before forwarding unless the route sets `forward_auth: true`:
  `Authorization`, `Cookie`, `x-api-key`, and any header whose name contains `auth`, `api-key`,
  `token`, `secret`, `credential`, `password` or `signature`.
- `hooks.slack.com`, where the URL is the credential, may be targeted at loopback and never through
  `--allow-target-host`. The rule covers every target on the `slack` service.
- `target_reads: true` without a `target:` is refused.

A delegated service gets a banner line (`delegated: stripe → http://127.0.0.1:3000 (reads +
writes)`) and no overlay, because its target owns read-after-write consistency. An unreachable
target is a `502` flagged `target-failed`, never a silent fall back to the fake:

```
{"error": {"type": "irimi_target_failed", "message": "answer target 'http://127.0.0.1:3111/cust' could not be reached: [Errno 61] Connection refused"}}
```

and the summary says `unanswered (target unreachable)` on the write's line.

## Attributing runs with the SDK

`irimi shadow -- <cmd>` treats one process as one run. An agent that serves - a webhook handler,
a queue worker - handles many requests in one process, often at once, and the proxy alone sees
one interleaved stream. The SDK, `irimi.sdk`, marks in the agent's own code where each run starts
and ends:

```python
from irimi import sdk


@sdk.trigger  # or @sdk.trigger(name="refund")
def handle_ticket(ticket: Ticket) -> None: ...


@sdk.trigger  # async def works the same way
async def on_event(raw: bytes, signature: str) -> None: ...


with sdk.run(trigger={"date": "2026-09-29"}, name="nightly"):  # or `async with`
    ...
```

Each call of a trigger, and each `sdk.run` block, is one run. irimi stores it with
`attribution: sdk`, the function's `module:qualname` as its entrypoint (none for `sdk.run`), the
call's arguments captured as JSON
([Captured arguments](docs/trace-format.md#captured-arguments)), `IRIMI_AGENT_VERSION` if it is
set, and how the run ended: `ok`, or `error` with the exception's type and message. The exception
itself always propagates, unchanged. `sdk.current_run_id()` is the current run's id.

- **When it is active.** Only while `IRIMI_ENGINE_ACTIVE=1`, which `irimi shadow` sets for its
  child (`sdk.active()`). Otherwise a trigger or `sdk.run` calls straight through: no run id, no
  network call, no patching, so the SDK is safe to leave in production code. When active, it
  reports each run to `IRIMI_CONTROL` directly, never through the proxy. If it cannot, it logs
  one warning per kind of failure on the `irimi.sdk` logger and the agent carries on.
- **Threads and tasks.** The run's id lives in a context variable. `asyncio` tasks and
  `asyncio.to_thread` inherit the run; `threading.Thread`, `ThreadPoolExecutor.submit` and
  `loop.run_in_executor` do not, unless the callable goes through `sdk.propagate`:
  `executor.submit(sdk.propagate(work))`. (A free-threaded 3.14 build starts a thread in a copy of
  its creator's context by default, so there a bare thread does inherit the run. A pool's worker
  thread, though, keeps the context it was started in, and runs later work in the run that started
  it, not the one that submitted it: use `sdk.propagate` there too.)
- **What a trigger wraps.** A function, an `async def`, or a method, `staticmethod` and
  `classmethod` included. A function that is not `async def` but returns a coroutine (a decorator
  that does not mark itself async, or `return handle_async(x)`) keeps its run until the coroutine
  has been awaited to its end.
- **Nesting.** A trigger called inside a run joins it: one run, not two. So does a `sdk.run`
  block. One `sdk.run(...)` object may be entered again - nested in itself, from several threads
  or tasks at once, or as a module-level constant - and each entry is a run of its own unless it
  is nested in one.
- **Names.** A trigger's name is a string: `@sdk.trigger(name="refund")`. A name passed
  positionally, or one that is not a string, is a `TypeError` when the module loads, and so is a
  trigger on a generator function, a class, a `functools.partial` or an object with `__call__`.
  Names are cut to 1000 characters.
- **Start the agent by module name.** `python -m my_agent` and `python my_agent.py` run it as
  `__main__`, so its entrypoint is recorded as `__main__:handle_ticket`, which replay (#84) cannot
  import. Import the module by its name and call it, as the sample workflows'
  `examples/workflows/launch.py` does.

### Every request a run makes through irimi carries its run

The proxy attributes a request to a run by its `Irimi-Run` header, never by connection or timing,
which fail under connection pools and concurrency. On the first run it starts, the SDK patches the
HTTP clients' classes so each request a run sends **to irimi** carries the run's id, read from the
context variable as the request is sent. irimi strips the header before anything leaves it.

- **Covered.** Every client built on `http.client`: `urllib.request`, `requests` and `urllib3`,
  stripe-python's sync client, slack_sdk's `WebClient`. And `httpx`, sync and async, when it is
  installed: the OpenAI and Anthropic SDKs, and stripe-python's async client.
- **Not covered**: `aiohttp` (slack_sdk's `AsyncWebClient` among them), `pycurl`, gRPC,
  WebSockets, a client that writes HTTP on its own socket, and any subprocess the agent starts. A
  run's request through one of them carries no run unless the agent sets
  `Irimi-Run: <sdk.current_run_id()>` itself, as the sample workflows' raw asyncio client does
  (W3).
- **The fallback.** A request with no `Irimi-Run` belongs to the engine's own run: the process run
  under `irimi shadow -- <cmd>`, and `unattributed` under serve mode (#77).
- **Only through irimi.** irimi can strip only what passes through it, so a request is labelled
  only when its connection goes to irimi's listener: the host and port `IRIMI_CONTROL` or a proxy
  variable (`HTTP_PROXY`, `HTTPS_PROXY`, either case) names, as spelled there: `localhost` is not
  `127.0.0.1`. That covers the proxy, a CONNECT tunnel through it, and the reverse door at
  `http://127.0.0.1:4000/...`. A `NO_PROXY` host, a client built with no proxy or a sidecar on
  loopback gets no header, and neither does the next hop of a redirect away from irimi, so a run's
  id never reaches a service irimi does not stand in front of.
- **When.** The patches are on classes, not instances, so a client created before the first run
  is covered too. They are made on the first run's entry, never on import and never while the SDK
  is inactive; `sdk.instrument()` makes them at startup instead, and is safe to call again.
- **A request that names its own run.** An `http.client` request gets the SDK's header ahead of
  the agent's, and irimi takes the first. An httpx request that already has one keeps it.
- **A task outliving its run.** A sync trigger that returns an `asyncio.Task` or a `Future`, not
  a coroutine, ends its run when it returns, but a task it created inside the run keeps the run's
  id. That task's requests are labelled with the ended run and stored in it, after its end.

## CLI

```
irimi init [--force]              generate the local CA under $IRIMI_HOME/ca
irimi serve [options]             run the shadow proxy in the foreground on 127.0.0.1:4000
irimi shadow [options] -- CMD     run CMD with its HTTP(S) traffic in shadow mode, then summarize
irimi maps list                   print each mapped host, its service, route count and target
irimi runs list [--limit N]       print one line per stored run, newest first (default 20)
irimi runs show RUN_ID            print a stored run: its record, every event, its summary
```

`serve` and `shadow` take `--port`, `--store DIR`, `--allow-host HOST`, `--target SPEC`,
`--target-reads HOST` and `--allow-target-host HOST`; the last four repeat. Every run is recorded,
redacted, under `--store` (default `$IRIMI_HOME/store`). `shadow` stores its command as one
`process` run with the command's exit code, and the agent's version when `IRIMI_AGENT_VERSION` is
set. `serve` starts no run of its own: an exchange is stored under the run its `Irimi-Run` names, or
else under the id `serve` printed, and the store is closed on Ctrl-C but not yet on SIGTERM (#77).
`serve` prints one line per exchange and no summary. A map, overrides file or `--target` the loader
refuses stops `serve` and `shadow` before the proxy starts, with the rule it broke on stderr, and so
does a redaction key or `--store` irimi cannot use. `IRIMI_HOME` moves irimi's state (see
`.env.example`).

`runs list` and `runs show` take `--store DIR` too. `runs list` prints each run's id, start,
duration, status (`ok`, `error`, or `incomplete` for one that never ended), attribution, trigger,
and its exchange and write counts. `runs show` prints the summary `irimi shadow` printed for the
run, from what was stored; `runs show unattributed` shows the exchanges no run claimed. See
[Reading a run back](docs/trace-format.md#reading-a-run-back) for where the two summaries differ.

To install irimi as a standalone tool instead of using the venv, run `uv tool install .` or
`pipx install .` from the repo root.

## Sample workflows

`examples/workflows/` holds eleven small agents, each built to exercise part of irimi, and a harness
that runs every scenario bare and under `irimi shadow` against a fake internet. The agents call the
real host names, so irimi classifies them with its shipped maps; only name resolution is faked.
The bare run is the baseline, and its writes really land on stateful fake services. The shadow run
goes through `irimi shadow`, and nothing may land. `examples/workflows/README.md` explains the
harness and how to add a workflow.

```
uv run python -m examples.workflows                                 # list workflows and scenarios
uv run python -m examples.workflows w09_scope_gauntlet verbs        # one scenario, bare then shadow
uv run python -m examples.workflows w04_slack_ops_bot own_thread --mode shadow
uv run pytest -q tests/workflows                                    # the whole corpus
```

`tests/workflows/test_invariants.py` holds every scenario to five rules: no write reached a fake
service under shadow, no request carried `Irimi-Run` out, no write tool's real function ran, no
canary credential reached disk, and the agent exited the same way bare and under shadow. Each
`tests/workflows/test_wNN_*.py` then pins what irimi did. A behaviour that looks wrong is pinned
under a `LOOKS WRONG:` comment rather than worked around, so a fix shows up as a changed pin.

Where each feature is exercised end to end:

| Feature | Workflows |
| --- | --- |
| Classification, THE SCOPE RULE, the L0 floor, the reverse door, forged irimi headers | W9 `w09_scope_gauntlet` |
| L1 fixtures, L3 preconditions, idempotency, an LLM tool loop | W1 `w01_ticket_triage` |
| The overlay: paging past a minted refund, reads that see a write | W2 `w02_nightly_reconcile`, W3 `w03_queue_worker` |
| Slack: minted threads, names versus ids, `missing_scope`, archived channels, webhooks | W4 `w04_slack_ops_bot` |
| Would-have-fired webhooks, a signed inbound webhook | W5 `w05_dispute_responder` |
| SSE streaming, resets mid-stream, telemetry stored as a count | W7 `w07_streaming_assistant` |
| The trace store: one process run per scenario, one `sdk` run per trigger, redaction on disk | every workflow (`tests/workflows/test_stored_runs.py`), W2, W3; a credential in a live response, W5 |
| The SDK: triggers and `sdk.run`, nesting, threads and asyncio, errors, captured args | W1, W2, W3, W5, W7, W8, W10; every workflow (`tests/workflows/test_stored_runs.py`) |
| Unmapped internal services, a map the loader refuses, `Irimi-Run` across a hop | W8 `w08_orchestrator` |
| Upstream failures: 429, 500, timeouts, resets, a failed L3 read, a killed run | W10 `w10_flaky_upstream` |
| What irimi cannot see: clients that bypass the proxy, loopback services | W11 `w11_leaky_agent` |
| Tool calls the proxy cannot see | W6 `w06_crm_db_agent` (through a stand-in, below) |
| The control endpoint: runs started, given tool calls and ended over `/_irimi/`, health, every refusal | W9 `w09_scope_gauntlet`; every workflow (`tests/workflows/test_control_endpoint.py`) |

Answer targets, overrides, the telemetry maps beyond W7's one LangSmith trace, and the CLI's own
flags are not in the corpus; they are covered by the engine and CLI tests in `tests/`. Every new
feature is driven through the real `irimi shadow` in the workflows its issue names, by extending
their scenarios and pins, and a fix that flips a `LOOKS WRONG:` pin updates it in the same PR
(`CONTRIBUTING.md`).

The agents use the SDK API (`@sdk.trigger`, `sdk.run`, `@sdk.tool`) through
`examples/workflows/sdk.py`, which re-exports `irimi.sdk`. Its triggers and runs are irimi's own
(#74). Until `irimi.sdk` has `@sdk.tool` (#76), `sdk.py` stands in for it, so W6's tool stand-ins
test the stand-in's rules, not irimi's.

## The refund agent

`examples/refund_agent/agent.py` is the live check: it lists charges from Stripe test mode,
refunds one, lists refunds, re-reads the charge and retries the refund. It refuses any key that is
not `sk_test_`, and it needs the opt-in `examples` group:

```
uv sync --group examples
export STRIPE_API_KEY=sk_test_...
uv run python examples/refund_agent/seed.py              # once, outside shadow: seeds a charge
uv run irimi shadow -- python examples/refund_agent/agent.py
```

Bare, the refund is real. Under shadow, the read returns your real test-mode charges, the refund is
answered from the fixture, the list and the re-read show it through the overlay, and the retry is
refused with `charge_already_refunded`. Stripe itself never sees a refund.
`tests/test_phase_exit.py` runs the same flow hermetically on every test run, and live when
`STRIPE_API_KEY` is set; `CONTRIBUTING.md` has the details, including the optional Slack post.

## Roadmap

Work is tracked in the [issues](https://github.com/HenryLiu714/Agent-Trust-Layer/issues), labelled
by phase.

**Done**

- **Phase 1: the proxy.** `init`, `serve`, `shadow`, the reverse door, the service maps and
  classification, the L0 echo, answer targets, the summary.
- **Phase 2: believable writes.** L1 fixtures, the Stripe and Slack overlay, L3 preconditions, the
  idempotency store, would-have-fired webhooks, the fuller summary, and its follow-ups (#52, #53,
  #55, #60).
- **Phase 3, first chunk.** `Irimi-Run` is stripped before a request leaves irimi (#67). Trace
  format v1 and exchange timestamps (#68, [`docs/trace-format.md`](docs/trace-format.md)).
  Redaction before anything reaches disk (#69). The sample workflows (#89). The trace store on
  disk (#70). Streamed SSE bodies recorded chunk by chunk (#71). `irimi runs list` / `runs show`
  and a summary from a stored run (#72). The control endpoint, `/_irimi/` (#73). The SDK:
  `@sdk.trigger` and `sdk.run()` with run identity in a context variable (#74), and
  `Irimi-Run` on every request a run makes through irimi (#75).

**Next: the rest of Phase 3, the run**

- The rest of the SDK: `@sdk.tool` for calls the proxy cannot see (#76).
- `irimi shadow --serve` with per-run summaries (#77), a server agent example and the Phase 3 exit
  test (#78), `irimi compare` (#79), Slack channel names in the summary (#61).
- Replay: answer a run from its recording (#82), read and write tools in replay (#83),
  `irimi replay` and `sdk.replay()` (#84), and record, edit a prompt, replay and diff end to end
  (#85).

**Later.** A shadow-week report over many runs, with readiness checks for clients that bypass the
proxy (the Phase 4 backstop the banner names). Then diffs of an agent's behaviour on pull requests,
and a runner inside the customer's own network that shadows new versions against production
triggers.

## Development

```
uv sync            # install everything, including the dev tools
make check         # what CI runs: ruff check, ruff format --check, mypy, pytest
uv run pytest -q   # the tests alone: under a minute, no network, no keys
```

[`CONTRIBUTING.md`](CONTRIBUTING.md) has the dev loop, how to add a service map, and the
conventions. [`docs/architecture.md`](docs/architecture.md) has the module map, how one request
moves through the engine, and the rules the tests enforce. Branch per issue, named
`<issue-number>-<short-slug>`, and open a PR against `main` with `Closes #<n>`. CI runs on Ubuntu
and macOS, on Python 3.14 and 3.12.
