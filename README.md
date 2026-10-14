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
  summary; `irimi shadow --serve` runs the same proxy as a sidecar to an agent it does not launch,
  and prints each run's summary as the run ends ([Serve mode](#serve-mode)).
- Service maps for Stripe, Slack, OpenAI, Anthropic and six telemetry backends decide what each
  request is.
- Writes are answered locally at four levels of fidelity (L0 to L3, below): an echo, a full
  response object, reads that show the write, and a check against the real service's state.
- Idempotency keys, would-have-fired webhooks, answer targets you control, and the reverse door for
  SDKs that ignore proxy variables.
- Eleven sample agent workflows run end to end, bare and under `irimi shadow`, on every test run,
  and nine of their scenarios under `irimi shadow --serve` too.

Every `serve` and `shadow` run is recorded, redacted, in the trace store under `$IRIMI_HOME/store`
([`docs/trace-format.md`](docs/trace-format.md)), and `irimi runs list` / `runs show` read it
back. The SDK, `irimi.sdk`, marks where an agent's runs start and end and labels the tools the
proxy cannot see ([Attributing runs with the SDK](#attributing-runs-with-the-sdk)). What does not
exist yet: replay and `irimi compare`. See [Roadmap](#roadmap).

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
- A write tool the SDK ran as its stand-in gets a line after the HTTP writes:
  `○ tool db.mark_refunded  shadow stand-in ran`, or `raised <type>`, and it did not happen
  either: a run whose only writes are tool writes still closes with `These writes did not happen.`
  A read tool gets no line, and neither is counted as an exchange
  ([Tools the proxy cannot see](#tools-the-proxy-cannot-see)).

### The child process

`irimi shadow` gives the command `HTTP_PROXY`, `HTTPS_PROXY` (and their lowercase forms),
`NO_PROXY=localhost,127.0.0.1`, `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE`,
`NODE_EXTRA_CA_CERTS`, `NODE_USE_ENV_PROXY=1`, `IRIMI_ENGINE_ACTIVE=1`, `IRIMI_RUN` and
`IRIMI_CONTROL`. That covers requests, httpx, urllib, curl and Node's fetch with no code change.
The command's exit code is passed through. One process tree is one run; the subcommand is the only
thing that chooses the mode. SIGTERM to `irimi shadow`, as a supervisor or a container runtime
sends it, is passed to the command, and the command's exit then ends the run as any exit does:
the run is stored with its code, what is queued is written, and the summary is printed (#77).

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

Write `127.0.0.1`, not `localhost`: irimi listens on IPv4 loopback unless `--listen-host` says
otherwise, and on macOS `localhost` resolves to `::1` first. The door answers only a client on
loopback, whatever address irimi listens on, and is not an open relay. It forwards only to the
exact hosts in a loaded map (`irimi maps list`) or named with `--allow-host`; anything else gets a
`403`. A wildcard host in a map is not an allow-list entry here. The scheme upstream is always
https.

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
  and happen for real, unless the agent labels the function that makes them with `@sdk.tool`
  ([Tools the proxy cannot see](#tools-the-proxy-cannot-see)). Point database URLs at scratch
  data. Bytes sent through the proxy that are not HTTP, and a connection upgraded to anything but
  a WebSocket, are refused, never relayed, and leave no record (#75).

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

### Tools the proxy cannot see

The proxy sees only HTTP. A database write, a file written to disk or a call into another library
happens for real under `irimi shadow` unless the agent labels it. `@sdk.tool` labels a function
as a read or a write. A write names its stand-in: the function irimi runs in its place, with the
same arguments, while irimi is in front.

```python
from irimi import sdk


def mock_database_write(account: Account) -> dict:
    return {"id": account.id, "stood_in": True}


@sdk.tool(kind="write", shadow=mock_database_write)  # name= defaults to "module.qualname"
def database_write(account: Account) -> dict:
    with db.transaction() as tx:
        tx.upsert(account)
    return {"id": account.id}


@sdk.tool(kind="read")  # a read runs for real, and takes no stand-in
def find_accounts(segment: str) -> list[Account]: ...
```

| `sdk.active()` | kind | What runs | What irimi records |
| --- | --- | --- | --- |
| false | read | the real function | nothing |
| false | write | the real function | nothing; one warning, below |
| true | read | the real function | a tool call, `ran: real` |
| true | write | **only the stand-in**: the real function never runs | a tool call, `ran: shadow` |

- **Mistakes fail when the module loads**, with `TypeError`, never at the call: a `kind` that is
  not `"read"` or `"write"`, a write with no `shadow=`, a read with one, a stand-in that is not
  callable, is a generator or is the function itself, a stand-in that is sync where the function
  is `async def` or the reverse, a generator function, and what `@sdk.trigger` also refuses (a
  class, a `functools.partial`, a name that is not a string). Nothing unnamed runs for real under
  irimi. A `staticmethod` stand-in in the tool's own class body is taken as its function.
- **What is recorded.** Each call is stored in the run it was made in, or, with no run, in the
  process run `irimi shadow` names (`IRIMI_RUN`), or under serve mode in `unattributed`. It holds
  the tool's name, its kind, which body ran, its arguments bound against the real function's
  signature and its result, both captured as a trigger's are
  ([Captured arguments](docs/trace-format.md#captured-arguments)) and redacted on disk, and what it
  raised, if anything. A result that would take the call's args and result together past 1 MiB is
  stored as `{"__irimi_truncated__": true, "bytes": n}`.
- **Where it shows.** `irimi shadow` prints a line per call among the exchange lines
  (`shadow    write     tool db.write -> ok`), and the summary a line per write whose stand-in ran
  ([The summary](#the-summary)). `irimi runs show` prints both from the store.
- **An exception propagates unchanged**, the same object, from the function or the stand-in, and
  the call is recorded with what it raised. A failed post to irimi is a warning on the `irimi.sdk`
  logger, as a run's is, never an exception.
- **The warning.** A write tool that runs for real while `HTTPS_PROXY` or `https_proxy` is set
  but `IRIMI_ENGINE_ACTIVE` is not was probably meant to run under irimi, so the SDK logs one
  WARNING per process: `irimi: write tool <name> ran for real: HTTPS_PROXY is set but
  IRIMI_ENGINE_ACTIVE is not`.
- **What it wraps**, as `@sdk.trigger` does: a function, an `async def`, a method, `staticmethod`
  and `classmethod` in either decorator order. A method's stand-in is called as the method is,
  with the instance first. A function that is not `async def` but returns a coroutine is recorded
  when the coroutine ends.
- **HTTP inside a tool** is recorded as the run's own exchanges, with no link to the tool call.
- **No tool overlay.** A read tool after a stood-in write sees the world as it was: nothing
  shows it the write that did not happen, as the overlay does for HTTP.

## Serve mode

`irimi shadow -- <cmd>` launches the agent and ends the run when it exits. A production agent is a
server or a worker that irimi does not launch and that never exits. Serve mode runs irimi on its
own, as a sidecar, until it is stopped:

```
irimi shadow --serve --listen-host 0.0.0.0 --port 4000 --store /data/irimi
```

`irimi serve` is the same command under a shorter name. With a command after `--`, `--serve` is a
usage error.

**The agent's container needs four settings**, naming irimi exactly as the agent reaches it
(`http://irimi:4000` in compose, not an address the agent never dials), because the SDK labels a
request only on a connection to a listener these name, as spelled:

| Variables | Value | Why |
| --- | --- | --- |
| `HTTPS_PROXY`, `HTTP_PROXY`, `https_proxy`, `http_proxy` | `http://irimi:4000` | every request goes through irimi; curl ignores upper-case `HTTP_PROXY` |
| `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE` | irimi's `ca.pem` | so the agent trusts irimi's certificates |
| `IRIMI_ENGINE_ACTIVE` | `1` | the SDK is inert without it, and write tools run for real |
| `IRIMI_CONTROL` | `http://irimi:4000/_irimi` | where the SDK posts each run's start, tool calls and end |

Leave `IRIMI_RUN` unset. Copy `ca.pem` from irimi's `$IRIMI_HOME/ca/`, never the `mitm/` bundle,
which holds the key. `NO_PROXY` names only what must not go through irimi, such as the agent's
own sidecars on loopback, and never a service the agent writes to: what bypasses irimi is not
shadowed. Nothing that is not HTTP goes through the proxy: a database or another protocol
tunnelled through it fails closed (#75). Publish irimi's port unchanged (`4000:4000`): irimi knows
a request is addressed to itself by its own port, so one remapped on the way in would be
forwarded to irimi again.

**A sidecar in the same pod or network namespace** reaches irimi on `127.0.0.1` and needs no wider
bind. It also keeps the reverse door, which only a loopback client may use: from another
container, an SDK that ignores the proxy variables, such as stripe-python, cannot be shadowed
([The reverse door](#the-reverse-door)).

- **Runs come from the SDK.** No process run exists. Each run is a `@sdk.trigger` call or an
  `sdk.run` block, and its requests are stored in it by their `Irimi-Run`
  ([Attributing runs with the SDK](#attributing-runs-with-the-sdk)). A request no run names, and a
  tool called in no run, is stored in `unattributed/` and builds no run: `irimi runs show
  unattributed` lists them. An agent without the SDK, or whose posts never reach irimi, has no
  runs, only `unattributed`. A post that fails is a WARNING on the agent's own `irimi.sdk` logger,
  not in irimi's output; the run is then stored as `header` and lists as `incomplete`.
- **What it prints.** The banner, then one line per exchange and per tool call as each happens,
  and each run's summary when the SDK ends the run, under `--- run <id> ---`: the same summary
  `irimi runs show <id>` prints, built from the store once the run's end is on disk. A streamed
  answer is recorded when its stream closes. One the agent reads to its end is in its run's
  summary; one the agent leaves open after its run has ended is stored in the run, but is missing
  from the summary already printed.
- **One faked world.** What one run faked, a later run sees: the write log is irimi's, not the
  run's, as production has one Stripe. A refund an earlier run already made is answered as Stripe
  would answer it, `charge_already_refunded` or `amount_too_large`, and its summary says so.
- **What it holds.** Nothing per exchange for printing. It does hold that write log, every write
  it faked, for as long as it runs, and every run that has not ended, one whose end never reached
  irimi included. Both grow over a long serve.
- **Binding.** irimi listens on `127.0.0.1` unless `--listen-host` says otherwise. Any other
  address, `0.0.0.0` and `::` included, prints a banner line: `listening on 0.0.0.0: any machine
  that can reach this port can send traffic through irimi and call its control endpoint`. The
  control endpoint has no authentication, so bind wider only on a network that holds the agent and
  nothing else. The reverse door stays loopback-only. A request addressed to irimi by a name or an
  address it was reached at (`http://irimi:4000/...`) is irimi's own, never forwarded to itself.
- **Stopping.** SIGINT and SIGTERM stop the proxy, write what is queued for up to 10 seconds, and
  exit 0. A run whose end arrived before the signal still prints its summary; a run still open
  is left `incomplete`. A proxy that stops by itself exits 1.
- **One serve per store.** A serve holds `serve.lock` in its `--store` while it runs, and a
  second serve on the same store exits 1 at once. A supervisor's restart retries until the old
  process has exited.

## CLI

```
irimi init [--force]              generate the local CA under $IRIMI_HOME/ca
irimi shadow [options] --serve    run the shadow proxy as a sidecar until SIGINT or SIGTERM
irimi serve [options]             the same as `irimi shadow --serve`
irimi shadow [options] -- CMD     run CMD with its HTTP(S) traffic in shadow mode, then summarize
irimi maps list                   print each mapped host, its service, route count and target
irimi runs list [--limit N]       print one line per stored run, newest first (default 20)
irimi runs show RUN_ID            print a stored run: its record, every event, its summary
```

`serve` and `shadow` take `--port`, `--listen-host HOST`, `--store DIR`, `--allow-host HOST`,
`--target SPEC`, `--target-reads HOST` and `--allow-target-host HOST`; the last four repeat. Every
run is recorded, redacted, under `--store` (default `$IRIMI_HOME/store`). `shadow` stores its
command as one `process` run with the command's exit code, and the agent's version when
`IRIMI_AGENT_VERSION` is set. Serve mode starts no run of its own ([Serve mode](#serve-mode)). A
map, overrides file or `--target` the loader refuses stops `serve` and `shadow` before the proxy
starts, with the rule it broke on stderr, and so does a redaction key or `--store` irimi cannot
use. `IRIMI_HOME` moves irimi's state (see `.env.example`).

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
goes through `irimi shadow`, and nothing may land. Nine scenarios run a third time under
`irimi shadow --serve`, with the agent beside irimi rather than its child.
`examples/workflows/README.md` explains the harness and how to add a workflow.

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
| Tool calls the proxy cannot see: `@sdk.tool` stand-ins, recorded, printed and redacted | W6 `w06_crm_db_agent`, W1, W2 |
| The control endpoint: runs started, given tool calls and ended over `/_irimi/`, health, every refusal | W9 `w09_scope_gauntlet`; every workflow (`tests/workflows/test_control_endpoint.py`) |

Answer targets, overrides, the telemetry maps beyond W7's one LangSmith trace, and the CLI's own
flags are not in the corpus; they are covered by the engine and CLI tests in `tests/`. Every new
feature is driven through the real `irimi shadow` in the workflows its issue names, by extending
their scenarios and pins, and a fix that flips a `LOOKS WRONG:` pin updates it in the same PR
(`CONTRIBUTING.md`).

The agents use the SDK API (`@sdk.trigger`, `sdk.run`, `@sdk.tool`) through
`examples/workflows/sdk.py`, which re-exports `irimi.sdk`. Its triggers, runs (#74) and tools
(#76) are irimi's own; `sdk.py` only wraps them to log which function ran, for the invariants.

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
  `@sdk.trigger` and `sdk.run()` with run identity in a context variable (#74),
  `Irimi-Run` on every request a run makes through irimi (#75), and `@sdk.tool` for calls the
  proxy cannot see (#76). `irimi shadow --serve` with per-run summaries (#77).

**Next: the rest of Phase 3, the run**

- A server agent example and the Phase 3 exit test (#78), `irimi compare` (#79), Slack channel
  names in the summary (#61).
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
uv run pytest -q   # the tests alone: about three minutes, no network, no keys
```

[`CONTRIBUTING.md`](CONTRIBUTING.md) has the dev loop, how to add a service map, and the
conventions. [`docs/architecture.md`](docs/architecture.md) has the module map, how one request
moves through the engine, and the rules the tests enforce. Branch per issue, named
`<issue-number>-<short-slug>`, and open a PR against `main` with `Closes #<n>`. CI runs on Ubuntu
and macOS, on Python 3.14 and 3.12.
