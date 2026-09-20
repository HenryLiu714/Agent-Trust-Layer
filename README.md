# irimi

*Shadow mode for AI agents: reads are real, writes are virtual, and reads see the writes.*

## What it is

irimi will be a local proxy you put between an agent and the outside world. Reads pass through to
real services, writes are intercepted and answered with a realistic fake success, and reads after
writes are answered from irimi's own record so the agent's view stays consistent. The output is a
log of every write the agent would have made.

## Status

Early. `irimi init`, `irimi serve`, `irimi shadow` and the reverse door work; the rest is being
built in the Phase 1 issues at
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

## Try the proxy

`irimi serve` runs the shadow proxy in the foreground on `127.0.0.1:4000`. Reads are forwarded to
the real service; writes are answered locally with a `fake-L0` response and never reach
the network. Which is which comes from the service maps, and from the HTTP method for anything the
maps do not cover. Each exchange prints as one line.

A `fake-L0` answer is a `200` whose JSON body echoes the request's own fields, stamps `created`,
and mints an id for every field the matched route names: a Stripe refund comes back with
`id: re_...`, `balance_transaction: txn_...` and `object: refund`, so stripe-python parses it. A
Slack call gets Slack's own `{"ok": true, "ts": "..."}` envelope instead, because its SDK refuses
anything else.

    uv run irimi serve
    # in another terminal
    curl --proxy 127.0.0.1:4000 --cacert ~/.irimi/ca/ca.pem https://api.stripe.com/v1/charges

## Run an agent in shadow mode

`irimi shadow -- <command>` starts the proxy, runs your command with the proxy and CA environment
variables already set, and prints a summary when the command exits. The command's exit code is
passed through.

    uv run irimi shadow -- python agent.py

Every exchange prints as one line while it runs; at the end you get a count of what was forwarded
live and what was virtualized. The child is given `HTTP_PROXY`, `HTTPS_PROXY`,
`NO_PROXY=localhost,127.0.0.1`, `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE`,
`NODE_EXTRA_CA_CERTS`, `NODE_USE_ENV_PROXY=1`, plus `IRIMI_ENGINE_ACTIVE=1` and `IRIMI_RUN` naming
the run. That covers requests, httpx, urllib, curl and Node's fetch without any code change.

One process tree is one run. There is no `IRIMI_MODE` variable and no config file: the subcommand
is the only thing that chooses the mode.

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
irimi maps · 10 service(s) · 17 host(s) · 47 route(s)
  api.anthropic.com        anthropic    2 routes  target: self
  api.honeycomb.io         honeycomb    2 routes  target: self
  api.openai.com           openai       4 routes  target: self
  api.smith.langchain.com  langsmith    4 routes  target: self
  api.stripe.com           stripe      10 routes  target: self
  cloud.langfuse.com       langfuse     2 routes  target: self
  connect.stripe.com       stripe      10 routes  target: self
  files.slack.com          slack       10 routes  target: self
  files.stripe.com         stripe      10 routes  target: self
  hooks.slack.com          slack       10 routes  target: self
  meter-events.stripe.com  stripe      10 routes  target: self
  slack.com                slack       10 routes  target: self
  *.datadoghq.com          datadog      5 routes  target: self
  *.i.posthog.com          posthog      4 routes  target: self
  *.ingest.sentry.io       sentry       4 routes  target: self
  *.langfuse.com           langfuse     2 routes  target: self
  *.posthog.com            posthog      4 routes  target: self
```

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
    ids:
      id: re_
    volatile:
      - idempotency_key
```

Write `match:` in block style, as above. A YAML flow mapping cannot hold a plain scalar containing
`{`, so `match: {method: GET, path: /v1/charges/{charge}}` is a parse error.

Slack's map sets `verbs: post-only`, because its SDKs send every call as POST and the method
therefore says nothing about what a call does. For a service with honest verbs, a route that
declares `kind: read` on an unsafe method is a write in disguise, so the loader refuses it unless
it says `persists: false` and carries a `comment:` explaining why nothing persists.

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
`/v1/embeddings`, `/v1/models`, `/v1/messages` and `/v1/messages/count_tokens` are forwarded live
and counted in their own bucket in the run summary. Everything else on those hosts is deliberately
unmapped, so an unlisted `POST` — `/v1/files`, `/v1/batches`, `/v1/fine_tuning/jobs` — reaches the
fallback, is answered locally and is flagged `unclassified`. A `text/event-stream` response is
streamed straight through to the client rather than buffered, so a streamed completion still
arrives token by token; the recorded exchange then carries an empty body.

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
change a route's `kind` would be a way to turn a write into a read. Targets must be loopback for
now, and `target_reads: true` without a `target:` is refused — a service irimi answered end to end
would be a twin, not a shadow.

Loading happens before the proxy starts, and a map or overrides file the loader refuses stops
`serve` and `shadow` with the rule it broke on stderr. Forwarding to a target is not wired up yet;
this release loads, validates and reports it.

## The example agent

`examples/refund_agent/agent.py` is the fixture the proxy is tested against: it lists charges from
Stripe (a real read) and refunds one of them (a write). It is **test mode only** and refuses any
key that is not `sk_test_`; the README GIF is recorded elsewhere, on a live account with a
restricted key.

Put a Stripe test-mode key in your environment, then seed one charge to refund:

    export STRIPE_API_KEY=sk_test_...
    uv run --with stripe python examples/refund_agent/seed.py

Run it bare and the refund is **real** (visible in the Stripe test dashboard):

    uv run --with stripe python examples/refund_agent/agent.py

Run the same command under `irimi shadow` and the refund never leaves your machine:

    uv run --with stripe irimi shadow -- python examples/refund_agent/agent.py

The read still goes to Stripe and returns your real test-mode charges; the POST to `/v1/refunds` is
answered locally with the L0 echo, so the agent prints a minted `re_...` id that no refund on
Stripe will ever have. Re-read the charge afterwards and it carries no refund.

The agent points `stripe.api_base` at the reverse door only when `IRIMI_ENGINE_ACTIVE=1`, and reads
the port from `HTTPS_PROXY`, so `--port` works and a bare run is unaffected. Set `SLACK_BOT_TOKEN`
and `SLACK_CHANNEL` (and add `--with slack_sdk`) to have it post the result to Slack as well;
`slack_sdk` honors the proxy variables, so that write goes through the forward proxy.

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
uv run pytest               # tests
uv run ruff check .         # lint
uv run ruff format .        # format
```

Branch per issue, named `<issue-number>-<short-slug>`. Open a PR against `main` with `Closes #<n>`
in the body. CI runs lint, format check and tests on macOS and Ubuntu.
