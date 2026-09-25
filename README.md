# irimi

*Shadow mode for AI agents: reads are real, writes are virtual, and reads see the writes.*

## What it is

irimi will be a local proxy you put between an agent and the outside world. Reads pass through to
real services, writes are intercepted and answered with a realistic fake success, and reads after
writes are answered from irimi's own record so the agent's view stays consistent. The output is a
log of every write the agent would have made.

## Status

Phase 1 is complete: `irimi init`, `irimi serve`, `irimi shadow`, the reverse door, the service
maps, the L0 echo and answer targets all work and are covered by the tests. Phase 2 is under way:
a mapped Stripe write is now answered from a vendored response object (`fake-L1`), and the
overlay that makes reads see the run's own writes is next. Work is tracked in the issues at
https://github.com/HenryLiu714/Agent-Trust-Layer/issues.

## Requirements

- macOS or Linux.
- [`uv`](https://docs.astral.sh/uv/) (the setup script installs it if missing).
- Python 3.14 is fetched by `uv` automatically; you do not need to install it. Installing as a
  standalone tool works on Python 3.12 or newer.

## Quickstart

```
git clone https://github.com/HenryLiu714/Agent-Trust-Layer.git
cd Agent-Trust-Layer
scripts/setup.sh
```

This creates the venv, installs irimi in editable mode, and runs `irimi init`. Then:

```
uv run irimi --help
```

Contributors: see `CONTRIBUTING.md` for the dev loop and `docs/architecture.md` for the map of
the code.

## Try the proxy

`irimi serve` runs the shadow proxy in the foreground on `127.0.0.1:4000`. Reads are forwarded to
the real service; writes are answered locally with a `fake-L1` or `fake-L0` response and never
reach the network. Which is which comes from the service maps, and from the HTTP method for
anything the maps do not cover. Each exchange prints as one line.

A locally answered write comes back at one of two fidelities, and the `Irimi-Answered-By` header
names which.

A **`fake-L0`** answer is a `200` whose JSON body echoes the request's own fields, stamps
`created`, and mints an id for every field the matched route names: a Stripe refund comes back
with `id: re_...`, `balance_transaction: txn_...` and `object: refund`, so stripe-python parses
it. A Slack Web API call gets Slack's own envelope instead, because its SDK refuses anything
else, and each mapped Slack write gets the envelope its own method really sends: `chat.postMessage`
answers `{"ok": true, "channel": "...", "ts": "..."}`, `reactions.add` and `files.upload` answer
the bare `{"ok": true}` and nothing more, and a Slack write no shipped map names falls back to
`{"ok": true, "ts": "..."}`. An incoming webhook gets the literal `ok` as `text/plain`, which is
what the real one answers. L0 is the floor: it is what every route irimi has no fixture for is
answered with, and it never fails.

A **`fake-L1`** answer is what a route whose map names a `fixture:` gets. It starts from a whole
response object in `src/irimi/fixtures/<service>.json` — vendored from the service's own mock
where it publishes one (Stripe, from stripe-mock), hand-written against its published docs where
it does not (Slack) — and writes the request's own fields over it, so the agent receives the
fields it never sent as well as the ones it did — `status`, `currency`, `destination_details` —
instead of reading `None` off a body that does not have them and taking the wrong branch. Three
Stripe writes are L1 today — `refunds.create`, `customers.update` and `payment_intents.cancel`,
each answered with the object as the whole body — and so is Slack's `chat.postMessage`, whose
fixture is the `message` object that sits *inside* the envelope: `ok`, `channel` and `ts` stay
envelope-owned, and `ts` is minted per answer rather than taken from the fixture, because it is
the faked message's identity for the rest of the run.

L1 follows four rules. A request field is written over the fixture only when the fixture **names**
it and the types agree — an unknown parameter is dropped, as the live API drops it, and
`amount=not-a-number` leaves the fixture's own value alone. A fixture field holding `null` names
no type, so `reason`, `description` and `customer` take whatever was sent. `id`, `object`,
`created` and `livemode` belong to the service and no request can choose them: `livemode` is
always `false`, because nothing irimi answers happened — that stamping is Stripe's shape, and a
Slack `message` object, which carries neither field, gets none of it. And an install whose fixture
file is missing or damaged still answers the write, at L0, with the exchange flagged
`fixture-failed`; a Slack write degrading that way still answers the envelope, since `ok` is what
its SDK reads to decide the call succeeded and the envelope's to say rather than the fixture's.

Four things both fidelities are careful about, because an SDK has to be able to read the fields
it just sent. A write that **names its resource in the path** gets that id back rather than a
fresh one: `POST /v1/customers/cus_REAL123` echoes `cus_REAL123`, because the live API does and an
agent that logs the id or retrieves it again would otherwise be handed one for a resource that
never existed.
The segment is percent-decoded first, and a value that is not shaped like an id — a PaymentIntent
client secret, say — is not mistaken for one. A **bracket-nested form field** becomes a nested
object, so `metadata[order_id]=6735` comes back as `metadata`. A **repeated key** collects into a
list, spelled either way: `tags=a&tags=b` and Stripe's own `expand[]=a&expand[]=b`. And a **number
is a number at any depth**, so `line_items[0][quantity]=2` echoes `2` exactly as `amount=4900`
does — except under `metadata`, whose values are always strings on the live API.

One thing a fake learns from the reads around it. A Slack `ts` is both a message's id and its sort
key, so a minted one has to sort after every real message the run has already read — otherwise an
agent that orders a transcript by `ts`, which is how a Slack transcript is ordered, finds its own
faked message somewhere in the middle of the real ones. Every Slack read irimi forwards raises a
watermark for the run as its body goes past, and the next minted `ts` comes strictly above it.
Reads stay real; the only thing this changes is which number a fake picks.

    uv run irimi serve
    # in another terminal
    curl --proxy 127.0.0.1:4000 --cacert ~/.irimi/ca/ca.pem https://api.stripe.com/v1/charges

## Run an agent in shadow mode

`irimi shadow -- <command>` starts the proxy, runs your command with the proxy and CA environment
variables already set, and prints a summary when the command exits. The command's exit code is
passed through.

    uv run irimi shadow -- python agent.py

Every exchange prints as one line while it runs; at the end you get the run's summary:

    irimi shadow · run 7f3a · 10 exchanges · 2.3s · backstop: none (Phase 4)

      api.openai.com     1 llm
      api.stripe.com     2 reads  1 engine read  2 writes intercepted
      slack.com          1 write intercepted (1 delegated)
      telemetry          2 exchanges to 2 hosts, forwarded live

      ○ refund $49.00 on ch_3QabcXYZ  unvalidated (L3 preconditions passed)
      ○ post to #refunds: "Refunded $49.00" → http://127.0.0.1:3111/post  unvalidated (delegated)

      10 exchanges · 6 live · 1 delegated · 3 virtualized
      These writes did not reach slack, stripe.
      1 was delegated to http://127.0.0.1:3111/post.

An `engine read` is one irimi made itself, to check a write against the real service before faking
it: a refund against its charge, a Slack post against its channel. It is counted apart from your
agent's reads so that "reads are real" means the reads your agent made. The write line says what
the check found: `L3 preconditions passed`, `L2` when irimi could not find out, or `✗ ... would
fail: charge_already_refunded` for a write the real service would have refused.

`live` means forwarded to the real service — reads, inference and telemetry alike; `delegated`
means an answer target answered it; `virtualized` means irimi did. Each intercepted write gets a
line of its own, written from its route's `human:` template with this request's own fields in it.
Amounts are formatted from minor units using the currency **the request carried**; a body that
names no currency keeps the number it sent, because dividing by 100 without knowing the currency
would print `¥49.00` for a 4900-yen refund.

The child is given `HTTP_PROXY`, `HTTPS_PROXY`,
`NO_PROXY=localhost,127.0.0.1`, `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE`,
`NODE_EXTRA_CA_CERTS`, `NODE_USE_ENV_PROXY=1`, plus `IRIMI_ENGINE_ACTIVE=1` and `IRIMI_RUN` naming
the run. That covers requests, httpx, urllib, curl and Node's fetch without any code change.

One process tree is one run. There is no `IRIMI_MODE` variable and no config file: the subcommand
is the only thing that chooses the mode.

Every response irimi decided rather than forwarded carries `Irimi-Answered-By`, naming the answer:
`fake-L1` for a fixture answer, `fake-L0` for the local echo, `delegated` when an answer target
answered it, and `overlay` for a live read whose body irimi edited so that it shows the writes the
run faked — after a faked refund, a re-read of the charge carries the new `amount_refunded`, and
page one of the refunds list carries the refund. A response with no such header came from the real
service, unchanged — absence is the signal, because adding a header of ours to a read we did not
change would make it differ from what the service sent.

In one narrow case irimi also edits a read on its way *out*. A refund it faked has an id the real
Stripe has never seen, so a later `GET /v1/refunds?starting_after=<that id>` would come back an
error. irimi drops that cursor before forwarding — everything after the newest refund is the real
list from its top — and marks the request it sent with `Irimi-Rewrote`, naming the parameter it
removed, so the same header in your Stripe logs tells you which request was not quite the one your
agent made. Nothing else about a read is ever changed on the way out.

**Nothing stops an agent from bypassing the proxy yet** — a client that ignores these variables, or
ships its own CA bundle, talks to the real service. The banner says `backstop: none (Phase 4)` for
exactly this reason.

Those CA variables *replace* the child's trust store rather than adding to it, so while the command
runs it trusts the irimi CA and nothing else. Traffic through the proxy is fine, but a TLS
connection that skips the proxy — anything on `localhost` or `127.0.0.1`, which `NO_PROXY` excludes
— will fail to verify. Point such a client at plain HTTP, or give it its own CA bundle.

## The reverse door (for SDKs that ignore proxy variables)

Some SDKs ship their own CA bundle or ignore `HTTPS_PROXY`. stripe-python is the canonical case:
it verifies against its bundled CA and ignores `REQUESTS_CA_BUNDLE`. For those, the same listener
also accepts plain HTTP at

    http://127.0.0.1:4000/<upstream-host>/<path>

and forwards it to `https://<upstream-host>/<path>`, after which it is handled exactly like any
other request: reads go live, writes are answered locally. For stripe-python, point the base URLs
at the door and nothing else changes:

```python
stripe.api_base = "http://127.0.0.1:4000/api.stripe.com"
stripe.upload_api_base = "http://127.0.0.1:4000/files.stripe.com"
stripe.connect_api_base = "http://127.0.0.1:4000/connect.stripe.com"
stripe.meter_events_api_base = "http://127.0.0.1:4000/meter-events.stripe.com"
```

Write `127.0.0.1`, not `localhost`. irimi listens on IPv4 loopback only, and on macOS `localhost`
resolves to `::1` first, so if anything else is listening on `[::1]:4000` it would receive the
request, credentials included, and irimi would never see it.

Under `irimi shadow`, `NO_PROXY=localhost,127.0.0.1` is what keeps these requests from also being
sent through the forward proxy.

The door is loopback-only and is not an open relay. It forwards only to the *exact* hosts in a
loaded service map (`irimi maps list` prints them) or named with `--allow-host <host>` (repeatable,
on `serve` and `shadow`); anything else is answered `403` with a one-line explanation. A wildcard
host in a map (`*.ingest.sentry.io`) classifies traffic through the forward proxy but is **not** an
allow-list entry here, because the door relays one literal host at a time — name that host with
`--allow-host` to reach it. The upstream host may carry a port (`/127.0.0.1:8443/...`); the scheme
is always https. Each exchange records which door it came through.

## Service maps

A service map is the YAML that tells irimi what a route *is*: which service and hosts it belongs
to, the operation name, whether it is a read or a write, the one-line human template the summary
prints, and the id prefixes a fake response mints. The maps that ship with irimi live in
`src/irimi/maps/` and are contributable without touching Python:

    irimi maps list

```
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

Hosts and patterns are counted separately because only the exact hosts are the reverse door's
allow-list. Several vendors appear more than once because they split their intake by region, and a
region is not always a subdomain: a Sentry DSN issued since 2024 is `o<org>.ingest.us.sentry.io`,
which does not end in `.ingest.sentry.io`; Datadog's EU1 and US1-FED sites are the separate TLDs
`datadoghq.eu` and `ddog-gov.com`; Honeycomb's EU instance and LangSmith's EU tenant are each their
own host. A pattern that is accepted and then matches nothing looks exactly like a working one —
the traffic is simply faked instead of forwarded — so the shipped regional hosts are pinned by
test.

Stripe, Slack (including `hooks.slack.com`), OpenAI, Anthropic and six telemetry backends —
LangSmith, Langfuse, Sentry, Datadog, Honeycomb and PostHog — ship with a map today.

A host may be a **wildcard pattern**: one leading `*.` label in front of two or more labels, so
`*.ingest.sentry.io` matches `o1234.ingest.sentry.io` and `*.posthog.com` matches `eu.posthog.com`
and `eu.i.posthog.com` — but never the bare `posthog.com`. An exact host beats a pattern, and among
patterns the longest suffix wins. Quote it in YAML (`- "*.posthog.com"`): a bare `*` starts a YAML
alias and the file will not parse.

One route looks like this. `match.path` may carry `{name}` segments, each matching exactly one
path segment; a literal path wins over a pattern of the same shape:

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
    fixture: refund # optional: the object in irimi/fixtures/stripe.json an L1 answer starts from
    fires: # optional: the webhooks this write would have caused; nothing is delivered
      - refund.created
      - charge.refunded
    ids:
      id: re_
    volatile:
      - idempotency_key
```

A `fixture:` is valid on a `write` or `unknown` route only — a live route is answered by the real
service, so a fixture on one would never be read, and the loader says so rather than ignoring it.

A `fires:` is valid on the same routes and for the same reason: a live route is forwarded and the
real service sends its own webhooks, so ours would never be listed. The events land on the
exchange as `would_fire`, and only for a write irimi accepted and answered itself. A write the
service would have refused — by an L3 precondition or for a reused idempotency key — lists nothing,
because nothing would have fired; so does a retry answered out of the idempotency store, whose
events are already on the first write's exchange, and a write an answer target answered. irimi
records these events and delivers none of them.

Write `match:` in block style, as above. A YAML flow mapping cannot hold a plain scalar containing
`{`, so `match: {method: GET, path: /v1/charges/{charge}}` is a parse error.

Slack's map sets `verbs: post-only`, because its SDKs send every call as POST and the method
therefore says nothing about what a call does. For a service with honest verbs, a route that
declares `kind: read` on an unsafe method is a write in disguise, so the loader refuses it unless
it says `persists: false` and carries a `comment:` explaining why nothing persists.

The same bar applies to every kind irimi *forwards live* — `read`, `llm` and `telemetry` — because
for those the method is performed on the real service. Such a route may not match every method: a
`match:` with no `method:` means `*`, and `*` includes `DELETE`. And on `DELETE`, `PUT` or `PATCH`
it needs the same `persists: false` plus `comment:`. `POST` is left alone: it is what an LLM
completion and a telemetry batch are.

### Classification

What a request *is* comes from four rules, and the first one that answers wins:

1. the route rule in a service map — `POST /v1/refunds` is a write because Stripe's map says so;
2. the service's `default_kind:`, for the routes that map does not list;
3. RFC 9110: `GET`, `HEAD` and `OPTIONS` are reads;
4. `unknown`.

Anything `unknown` is answered locally and flagged `unclassified`, so a route nobody has classified
is faked rather than sent, and the line it prints says the classification was a guess. That is why a
`POST` to a real Stripe route no shipped map lists never reaches Stripe, even though the map claims
the host. A host no map claims at all is intercepted the same way: its reads forward live and
everything else is faked and flagged.

The order matters most where the verb lies. Slack's `conversations.history` is a `POST`, and only
its map entry makes it a read that forwards live — without it the agent would get an empty channel
back from a faked write.

These three rules — no live `default_kind`, no live kind on `*`, no live kind on a destructive
verb without a reason — are one rule at three scopes: **no classification that forwards live may
apply to a method it did not name explicitly.** Each has been a real bug here, one scope at a
time, so the classifier also enforces it per request: a live kind reaching a method its route did
not name is answered locally and flagged `kind-downgraded`, however it got there.

`default_kind:` may name any kind irimi answers locally — `write` or `unknown` — and no kind it
forwards live. `read`, `llm` and `telemetry` are all refused, each with its own reason: a `read`
default forwards every route the map does not list to the real service; `llm` is route-level, so
on an LLM host `/v1/files` and `/v1/batches` stay real billable writes; and a `telemetry` default
forwards live too, which matters because most telemetry vendors serve their REST control plane
from the same host as their intake — an early draft of the telemetry maps used one and sent
`DELETE api.datadoghq.com/api/v1/dashboard/{id}` to the real API. The list is derived from the set
of kinds shadow mode forwards, so a live kind added later is refused the day it is added. No
shipped map sets `default_kind`; the telemetry maps list their intake routes one by one instead.

### `llm` and `telemetry`

`llm` is a route kind in the OpenAI and Anthropic maps: `/v1/chat/completions`, `/v1/responses`,
`/v1/embeddings`, `/v1/messages` and `/v1/messages/count_tokens` are forwarded live and counted in
their own bucket in the run summary. `GET /v1/models` is a plain `read`: a listing is not
inference. Everything else on those hosts is deliberately unmapped, so an unlisted `POST` —
`/v1/files`, `/v1/batches`, `/v1/fine_tuning/jobs` — reaches the fallback, is answered locally and
is flagged `unclassified`. A `text/event-stream` response is streamed straight through to the
client rather than buffered, so a streamed completion still arrives token by token; the recorded
exchange then carries an empty body, and nothing downstream may rewrite such a response — its
headers are already on the wire and its body was never assembled.

`telemetry` is what the observability backends are classified as. It is forwarded live in every
mode and is never written to the trace store: a recording of the agent's own tracing traffic is
noise, and replaying it would re-emit someone else's events. It is counted as `telemetry` in the
summary, not as a read or a write.

### Answer targets

Every write is answered by a **target**, and the default target is irimi itself — `self`, the
local fake. A service or a route can name an address you control instead, and the agent receives
whatever that address answers; the real upstream still sees nothing. Set one in `./irimi.maps.yaml`
(or `$IRIMI_HOME/maps.yaml`), which is merged over the shipped maps:

```yaml
service: stripe
target: http://127.0.0.1:3000 # answers the writes
target_reads: true # optional: send this service's reads there too
```

The shipped maps never set a target, so a fresh install answers everything itself. An overrides
file may set `target`, `target_reads` and `forward_auth` and nothing else: an override able to
change a route's `kind` would be a way to turn a write into a read.

`--target` does the same thing for one run, and beats the file:

    irimi shadow --target 'api.stripe.com/v1/refunds=http://127.0.0.1:3000/refund' -- python agent.py

A bare host — `--target 'api.stripe.com=http://127.0.0.1:3000'` — targets the whole service,
including the routes its map does not list; naming a host or a path no map claims is an error, not
a silent no-op. Add `--target-reads api.stripe.com` and that service's *reads* go to the same
address, which makes it a **delegated service**: its target, not production, is the world the
agent sees. A targeted exchange is stamped `delegated` rather than `fake-L0`, and records the
address that answered it.

Path semantics follow nginx `proxy_pass`. A bare origin keeps the request's own path; a target
carrying a path replaces the part the route matched, which — because a route pattern always
matches the whole path — is all of it. The query string is always kept, and the method, body and
content type pass through unchanged.

That has a consequence worth knowing before you write a stub. A target with a path on a route with
`{…}` segments **discards the captures**: `--target
'api.stripe.com/v1/customers/{customer}=http://127.0.0.1:3000/cust'` sends every customer to
`/cust`, so the stub cannot tell one request from another. If your stub needs the id, give it a
**bare origin** — `--target 'api.stripe.com=http://127.0.0.1:3000'` — and it receives
`/v1/customers/cus_REAL123` with the path intact.

`http://` and `https://` targets are both accepted, but a `https://` target must present a
certificate mitmproxy trusts: it verifies an upstream chain against certifi's bundle, and there is
no option to relax that. A self-signed stub is reachable over `http://`.

Five rules keep a target from becoming a way out of the machine. Each is checked when the maps
load — so a shipped map, an overrides file and a `--target` flag are all covered by the same
check — and again at the moment the answer is chosen, so a target that arrives some other way is
refused rather than let through:

- A target must be **loopback** (`127.0.0.1`, `::1`, `localhost`). Anything else is refused when
  the maps load, naming the rule. `--allow-target-host <host>` is the deliberate way out, and it
  prints a warning saying what it allows.
- A target naming **irimi's own listener** is refused, or the proxy would dial itself in a loop.
- **Credential headers are stripped** before forwarding, unless the route sets
  `forward_auth: true`. A local stub does not need your real key. It is every header that carries
  one and not only `Authorization` — `Cookie`, `x-api-key`, `DD-API-KEY` and anything else whose
  name contains `auth`, `api-key`, `token`, `secret`, `credential`, `password` or `signature` —
  because the list of vendor spellings is never finished, and the one it is missing is the one
  that leaks. `forward_auth: true` keeps all of them, for a sandbox tenant or an internal
  simulator that really does need the key.
- A **credential-path host** — today `hooks.slack.com` — may be targeted at loopback and never
  through `--allow-target-host`, because there the URL *is* the credential. The rule is on the
  host, so it covers every path on it, not only the routes the map lists: `/services/...`,
  `/workflows/...` and `/triggers/...` are all real Slack webhook forms. In practice that means
  no target anywhere on the `slack` service may leave the machine — a route target can answer a
  request addressed to the webhook host, because routes are matched within a service.
- `target_reads: true` without a `target:` is refused — a service irimi answered end to end would
  be a twin, not a shadow.

An **unreachable target is a `502`** flagged `target-failed`, never a silent fall back to the fake:
that would hide a broken setup and look exactly like a working run. The body is JSON naming irimi
and the target —

```json
{"error": {"type": "irimi_target_failed", "message": "answer target 'http://127.0.0.1:3111/post' could not be reached: [Errno 61] Connection refused"}}
```

— so an SDK raises something that says what went wrong. A loopback target is probed before the
request is redirected, which costs microseconds and is what makes that body possible: irimi hands
the request to the proxy layer to forward, and once a dial fails there the error page is already
committed. Two cases still get mitmproxy's own HTML `502` instead: a target reached through
`--allow-target-host`, which is not probed because a remote connect would block the proxy on every
request, and a stub that dies between the probe and the dial. The flag, the absence of a fallback
and the summary line are the same either way. The summary never calls such a write delegated — a
stub that was not running answered nothing:

      api.stripe.com  1 write intercepted (1 target unreachable)

      ○ refund $49.00 on ch_3QabcXYZ → http://127.0.0.1:3111/post  unanswered (target unreachable)

      These writes did not reach stripe.
      1 was not answered at all: http://127.0.0.1:3111/post could not be reached, and the agent
      got a 502.

Loading happens before the proxy starts, and a map, overrides file or `--target` the loader refuses
stops `serve` and `shadow` with the rule it broke on stderr.

#### What a delegated service changes

The banner's promise is that hosts *not* routed through the proxy are not virtualized. A routed
host may not be live either, so every service or route with a target gets its own banner line, and
`irimi maps list` prints route-level targets under the host rather than only the service's:

    delegated: stripe → http://127.0.0.1:3000 (reads + writes)
    delegated: slack POST /api/chat.postMessage → http://127.0.0.1:3111 (writes)

With `--allow-target-host` the line says the target is not loopback, and is red on a terminal. The
exit summary counts a delegated exchange apart from a live one for the same reason: a delegated
read is not a real read, and "reads are real" is what that count means to whoever reads it.

Two consequences are Phase 2's and are written down in `src/irimi/overlay.py` so the issue that
builds the overlay inherits them: a delegated service gets **no overlay**, because its target owns
read-after-write consistency and layering irimi's minted objects over that state would corrupt it;
and L3 preconditions for a delegated service must read from the target, or they check the write
against a world the agent is not in.

## The example agent

`examples/refund_agent/agent.py` is the fixture the proxy is tested against: it lists charges from
Stripe (a real read) and refunds one of them (a write). It is **test mode only** and refuses any
key that is not `sk_test_`; the README GIF is recorded elsewhere, on a live account with a
restricted key.

Put a Stripe test-mode key in your environment, then seed one charge to refund:

    uv sync --group examples          # installs the stripe and slack_sdk SDKs
    export STRIPE_API_KEY=sk_test_...
    uv run python examples/refund_agent/seed.py

Run it bare and the refund is **real** (visible in the Stripe test dashboard):

    uv run python examples/refund_agent/agent.py

Run the same command under `irimi shadow` and the refund never leaves your machine:

    uv run irimi shadow -- python examples/refund_agent/agent.py

The read still goes to Stripe and returns your real test-mode charges; the POST to `/v1/refunds` is
answered locally from the refund fixture (`fake-L1`), so the agent prints a minted `re_...` id that
no refund on Stripe will ever have, on an object carrying every field a real refund does. Re-read
the charge afterwards and it carries no refund.

`tests/test_phase_exit.py` is that run, automated. Its first test needs no key and no network: it
drives both doors with the wire shapes the two SDKs produce, and asserts the refund was answered by
irimi, that the echo carries a minted `re_` id, that nothing on the other side was ever asked to do
anything, and that the summary says so. Its second test is the live version above — it runs the
agent itself and re-reads the charge from Stripe — and it skips unless you give it a key:

    STRIPE_API_KEY=sk_test_... uv run pytest -q -rs tests/test_phase_exit.py

The agent points `stripe.api_base` at the reverse door only when `IRIMI_ENGINE_ACTIVE=1`, and reads
the port from `HTTPS_PROXY`, so `--port` works and a bare run is unaffected. Set `SLACK_BOT_TOKEN`
and `SLACK_CHANNEL` to have it post the result to Slack as well; `slack_sdk` honors the proxy
variables, so that write goes through the forward proxy.

## Installing as a standalone tool

If you do not want a venv, run one of these from the repo root:

```
pipx install .
uv tool install .
```

After that, `irimi` is on your `PATH`.

## What `irimi init` does

`irimi init` generates a self-signed CA into `$IRIMI_HOME/ca/` (default `~/.irimi/ca/`):

- `ca.key` with mode 0600
- `ca.pem` with mode 0644

Running it again is a no-op. `--force` regenerates the CA. `IRIMI_HOME` overrides the location;
see `.env.example`.

## What the proxy does not cover

irimi only sees HTTP(S). Side effects that are not HTTP, such as database writes, files on disk,
gRPC and WebSocket traffic, are not virtualized and happen for real. Point database URLs at scratch
data when running an agent in shadow mode.

## Contributing

```
uv sync                     # install everything, including dev tools
make check                  # lint, format check, mypy, tests - what CI runs
```

`CONTRIBUTING.md` has the dev loop, how to add a service map and the conventions;
`docs/architecture.md` has the module map, the request flow and the rules the tests enforce.
Branch per issue, named `<issue-number>-<short-slug>`. Open a PR against `main` with `Closes #<n>`
in the body. CI runs on macOS and Ubuntu, on Python 3.14 and 3.12.
