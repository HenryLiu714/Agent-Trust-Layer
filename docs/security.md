# Security model

For security reviewers deciding whether irimi may sit between an agent and production, and for
contributors changing anything this page names. It says what irimi trusts, what it guarantees,
what it writes to disk, and which traffic it cannot see. Every other page links here rather than
restating these rules.

## What irimi is, to a reviewer

irimi is a TLS-intercepting HTTP proxy that runs on the agent's machine (or beside it, as a
sidecar). It forwards the agent's reads to the real services and answers its writes itself. It
holds a certificate authority the agent is told to trust, sees every credential the agent sends
through it, and stores a redacted copy of each request and response.

irimi sends nothing of its own anywhere: it has no telemetry, no update check and no hosted
service. The only requests it originates are irimi's check reads (one real read before faking a
write whose route has a precondition check, sent with the write's own credentials) and a probe of
a target you configured.

## Trust boundaries

- **The local CA.** `irimi init` writes an RSA-2048 CA to `$IRIMI_HOME/ca/` (default
  `~/.irimi/ca/`): `ca.key` mode `0600`, `ca.pem` mode `0644`, valid for 825 days. irimi starts
  warning 30 days before it expires; `irimi init --force` replaces it. irimi never adds it to an
  operating system or browser trust store: only the processes irimi starts are told to trust it,
  through `SSL_CERT_FILE` and its siblings ([how-it-works.md](how-it-works.md#what-the-child-gets)).
  Anyone holding `ca.key` can impersonate any host to a process that trusts `ca.pem`.
  `$IRIMI_HOME/mitm/mitmproxy-ca.pem` holds the key too; never hand it to an agent's container.
- **The listener.** irimi binds `127.0.0.1` unless `--bind` names another address. Any other
  address, `0.0.0.0` included, prints a warning in the banner: any machine that can reach the port
  can then send traffic through irimi and call its control endpoint.
- **The control endpoint.** `/_irimi/` on the listener has no authentication. Any client that can
  reach the listener can start, end and add tool calls to runs, and read the store's counters. It
  can never make irimi forward a request or read a stored body. See
  [control-protocol.md](control-protocol.md).
- **Base-URL mode** (`http://127.0.0.1:PORT/<host>/<path>`, for SDKs that ignore proxy settings)
  answers only clients on loopback, whatever `--bind` says, and relays only to the exact hosts of a
  loaded map or `--base-url-host`. Anything else gets a `403`. It is not an open relay.
- **Targets stay on the machine.** A target (a URL you name to answer a route instead of irimi) must
  be loopback (`127.0.0.1`, `::1`, `localhost`) unless `--allow-remote-target` names its host, and
  irimi's own listener is never a target. A service whose host carries the credential in its path
  (`hooks.slack.com`, a map's `credential_path_hosts:`) may only have loopback targets, with no
  escape. Before a request goes to a target, every credential header (the list under
  [Redaction](#redaction)) is removed unless the route sets `forward_auth: true`. These rules are
  checked when the maps load and again when the answer is chosen.
- **The store.** Its directories are `0700` and its files `0600`, as are `redact.key` and
  `ca.key`.

## Invariants

Five rules hold in every mode. Each is stated once, in the code that enforces it, and each has a
test in `tests/safety/` (its README maps rule to test).

- **The fail-closed hook rule** (`engine/mitm.py`). Nothing in a mitmproxy hook may raise: a raised
  hook forwards the flow, and a forwarded write escapes shadow mode. A request irimi cannot handle
  gets irimi's own answer instead (a `400` refusal, or a `502` flagged `decision-failed`), and once
  an answer is set, a failure costs only its record.
- **The scope rule** (`servicemaps/rules.py`). No classification that forwards live may apply to an
  unsafe method it did not name explicitly. The loader refuses a live `default_kind`, a live kind
  on a route with no `method:` (which matches every method, `DELETE` included), and a live kind on
  `DELETE`, `PUT` or `PATCH` without a `live_because:`. The classifier enforces it again on every
  request: a live kind on an unsafe method its route did not name is answered by irimi and flagged
  `kind-downgraded`. `GET`, `HEAD` and `OPTIONS` are safe methods and exempt.
- **The answered-by header** (`engine/wire.answered_by_header`). Every response irimi decided
  carries `Irimi-Answered-By`, naming who answered: `fake-L0`, `fake-L1`, `delegated` (a target),
  `overlay`, `recording` or `irimi`. A live forward carries none, because absence is how a client
  tells the real service's bytes from irimi's. Control endpoint answers carry `control`.
- **Nothing unredacted on disk** (`record/redact.py`, `record/store/__init__.py`). The store writes
  only the redacted copy of each event and run record. A part that cannot be redacted is stored as
  `<redaction-failed>` and flagged, never as it was. The live traffic is never redacted.
- **The SDK never raises** (`sdk/__init__.py`). The SDK's own failures are logged once and
  swallowed. An agent's exceptions propagate unchanged; irimi's never reach the agent.

Rules that follow from these, each also held by a test:

- **irimi's headers are irimi's.** `Irimi-Run` and `Irimi-Rewrote` are taken off every request
  before it is forwarded, sent to a target or used for a check read, so a run id never reaches a
  service. When irimi stamps an answer, it replaces any `Irimi-Answered-By` the target or service
  sent.
- **A store failure never affects traffic.** The store only enqueues; one writer thread does the
  I/O. A full queue or a failed write drops the event and counts it on the run
  (`dropped_events`), never blocking or failing the agent's request.
- **Telemetry is forwarded and never stored.** A request classified `telemetry` is recorded only as
  the fact that it happened, and to which host.
- **Nothing is answered from part of a body.** A write whose request body decodes past 64 MiB is
  refused with irimi's `502`, never faked from its start, and never forwarded.
- **Non-HTTP bytes are refused.** Raw TCP relay is off: bytes that are not HTTP, and a connection
  upgraded to anything but a WebSocket, are closed, never relayed.

## What the store keeps

Every `irimi shadow`, `serve` and `replay` records its runs under `--store` (default
`$IRIMI_STORE`, else `$IRIMI_HOME/store`). Redaction removes credentials; it does not remove
customers, amounts, messages or any other business data in the bodies. Treat a store as you treat
a production log. `irimi runs rm` and `irimi runs prune --older-than 30d` delete runs and the bodies
no other run uses ([cli.md](cli.md#irimi-runs-rm-and-runs-prune)).

## Redaction

The store writes a redacted copy of each exchange, tool call and run record. The engine and the
agent never see a placeholder.

- **Placeholder.** A secret becomes `<redacted:` + the first 16 hex digits of
  HMAC-SHA256(key, value) + `>`. The key is `$IRIMI_HOME/redact.key`, 32 random bytes created `0600`
  on first use, so one secret is always one placeholder on one install, and a replay can match a
  secret against its own recording. A symlinked key is followed, so two installs can share one
  (a mounted secret). A `redact.key` that is not a regular 32-byte file is refused, never replaced.
- **Credential headers.** A header's whole value is replaced when its name, lower-cased, contains
  any of `auth`, `api-key`, `api_key`, `apikey`, `token`, `secret`, `credential`, `password` or
  `signature`, or is one of `cookie`, `dd-application-key` or `x-honeycomb-team`. `Set-Cookie` on a
  response is replaced too. The same list decides which headers a target never receives. A header
  whose value is an absolute `http(s)` URL (`Location`, `Referer`) is redacted as a URL.
- **Secret keys.** A query parameter, a form field (by its last bracket segment, so `card[token]`)
  or a JSON key at any depth named `api_key`, `apikey`, `secret`, `client_secret`, `password`,
  `access_token`, `refresh_token`, `id_token` or `token` (whole and case-insensitive, so
  `max_tokens` is not one) has its value replaced. `null` and `""` are left alone.
- **Secret shapes.** Stripe (`sk_live_`, `rk_test_`, `whsec_`…), Slack (`xoxb-`, `xapp-`…), GitHub
  (`ghp_`, `github_pat_`…), AWS (`AKIA…`), Anthropic (`sk-ant-`) and OpenAI (`sk-`) key shapes are
  replaced wherever they appear: header names and values, the path and query, form fields, JSON
  strings and keys, every line of a text body (SSE included), a target URL and an operation name.
  A shape glued to a preceding letter or digit is part of a word and left alone, unless that
  character ends an escape such as `\n` or `%20`.
- **Credential-path hosts.** On `hooks.slack.com`, and any host a map lists under
  `credential_path_hosts:`, the URL path is the secret, so the whole path is replaced wherever the
  exchange carries it.
- **How a body is read.** A body that is one JSON document is walked as JSON, whatever its content
  type says. Otherwise a form content type is read as a form, and anything else line by line: a
  line that is one JSON document (NDJSON), or an SSE `data:` field holding one, is walked as JSON,
  and every other line is scanned for shapes. A body nothing matched keeps its bytes.
- **Streams.** A streamed body is redacted joined, never chunk by chunk, so a secret split across
  two chunks is still found on its line.
- **Cuts never split a secret.** A body is redacted whole and only then cut to its stored size.
  Text the SDK must shorten before irimi can redact it (trigger and tool names, captured `repr`s,
  error messages) is cut at its limit, or earlier where a secret's shape crosses the limit.
- **Failure is closed.** A part that cannot be redacted, including a JSON document with a repeated
  key, is stored as `<redaction-failed>` and the exchange is flagged `redaction-failed`.

What redaction does not catch is listed in [limitations.md](limitations.md#redaction).

## Known bypasses

irimi sees only traffic sent through its proxy. Each of these reaches the real service, and the
sample workflow `w11_leaky_agent` pins each one:

- a client that ignores `HTTPS_PROXY` (built with no proxy handler, or `trust_env=False`);
- a subprocess the agent starts with `NO_PROXY=*`, or a host in `NO_PROXY`;
- HTTP written on a raw socket;
- a redirect that leaves irimi for a host the client reaches directly;
- a service on loopback, which irimi's own `NO_PROXY=localhost,127.0.0.1` exempts (a write tool
  around the call closes this one: its fake runs instead);
- a WebSocket: its upgrade request is classified like any other, but once a service accepts it,
  the messages are relayed both ways and never decided;
- anything that is not HTTP(S): a database connection, files on disk, gRPC, a queue.

A client that does not trust irimi's CA fails its TLS handshake and never sends the request; the
summary counts those failures per host. To check a single response, look for
`Irimi-Answered-By`: a response without it came from the real service. The converse does not
hold. irimi stamps only the answers it decided and leaves a live answer's headers as the service
sent them, so a real service, or anything posing as one, that sends its own `Irimi-Answered-By`
reaches the agent with it. The stored exchange's `answered_by` is the record to trust.
