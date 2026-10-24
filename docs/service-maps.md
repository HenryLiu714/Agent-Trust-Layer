# Service maps

For anyone writing or reviewing a service map, and for users pointing a route at a target. A map
is YAML that says what each route of a service is: a read irimi forwards, a write it fakes, an LLM
call or telemetry. This page is the schema: every key, its default, where it is valid and the rule
the loader applies.

Maps ship with irimi, in `src/irimi/maps/`; add one by pull request
([adding-a-service.md](adding-a-service.md)). What you can change on your own machine is where
writes are answered: the [overrides file and `--target`](#targets). `irimi maps list` prints every
mapped host, its service, its route count and its target.

## An example

One route of `maps/stripe.yaml`:

```yaml
version: 1
service: stripe
style: rest
hosts:
  - api.stripe.com
routes:
  - match:
      method: POST
      path: /v1/refunds
    operation: refunds.create
    kind: write
    human: refund {amount:money} on {charge}
    fixture: refund
    precondition: charge_refundable
    fires:
      - refund.created
      - charge.refunded
    ids:
      id: re_
      balance_transaction: txn_
```

A `POST /v1/refunds` to `api.stripe.com` is a write. irimi fakes it from the `refund` object in
`fixtures/stripe.json`, after checking the charge with the `charge_refundable` precondition check.
It mints a `re_…` id and a `txn_…` balance transaction, lists the two webhooks Stripe would have
sent, and the summary prints it as `refund $49.00 on ch_1`.

## Service keys

One YAML document per service. A file may hold several, separated by `---`.

| Key | Type | Default | Meaning and loader rule |
| --- | --- | --- | --- |
| `version` | integer | required | The map format version. Must be `1`. |
| `service` | string | required | The service's name, as the summary and the trace record it. Unique across maps. |
| `hosts` | list of strings | required | Exact host names, or wildcards (below). A host may belong to one service only. |
| `style` | `rest` or `rpc` | `rest` | `rest`: the HTTP method carries meaning, so a `read` route on an unsafe method needs `live_because:`. `rpc`: every call is a `POST` (Slack's Web API), and the method says nothing. |
| `default_kind` | `write` or `unknown` | none | The kind for routes the map does not list. Never a live kind: that would forward every unlisted route to the real service. With none, unlisted routes follow RFC 9110. No shipped map sets it. |
| `credential_path_hosts` | list of strings | `[]` | Hosts whose URL path is the credential (incoming webhooks). Each must be one of this map's exact `hosts`. Such paths never reach disk, and every target of the service must be loopback. `hooks.slack.com` is always one. |
| `target` | `self` or URL | `self` | Who answers the service's `write` and `unknown` routes. Overrides only; shipped maps never set it. |
| `target_reads` | boolean | `false` | Send the service's reads to `target` too. Needs a `target`. Overrides only. |
| `routes` | list | required | The route rules, below. Not empty. |

### Hosts and wildcards

A host is a bare name, with no scheme, port or path. A wildcard is one leading `*.` in front of
two or more labels: `*.posthog.com` matches `eu.i.posthog.com`, never `posthog.com`. Quote it in
YAML. An exact host beats a pattern, and the longest pattern wins. A wildcard host classifies
proxied traffic only; base-URL mode reaches it only through `--base-url-host`.

## Route keys

| Key | Type | Valid on | Meaning and loader rule |
| --- | --- | --- | --- |
| `match.method` | string | all | An HTTP method, or `*` (the default) for every method. A live kind may not use `*`. |
| `match.path` | string | all | Starts with `/`. A `{name}` segment matches exactly one segment, and a name appears once per path. A literal path beats a pattern. Write `match:` in block style: a flow mapping cannot hold `{`. |
| `operation` | string | all | The route's name, such as `refunds.create`. Recorded on each exchange. |
| `kind` | `read`, `write`, `llm`, `telemetry`, `unknown` | all | What the route is ([concepts.md](concepts.md#terms)). |
| `human` | string | all | The summary's sentence for this write. `{name}` is a path parameter or a body field; `{name:money}` formats minor units in the write's currency. No other format is allowed. Quote a template containing `#`. |
| `ids` | mapping | `write`, `unknown` | Field name to how a faked write mints it: a prefix (`id: re_` gives `re_` plus 24 characters), `{format: int}` or `{format: uuid}`. A field the path names keeps the path's value. |
| `fixture` | string | `write`, `unknown` | The object in `fixtures/<service>.json` a faked write starts from. Must exist there. |
| `precondition` | string | `write`, `unknown` | The precondition check run before faking the write. Must be a check irimi has for this service. |
| `fires` | list of strings | `write`, `unknown` | Webhook events the real write would have sent. Each named once. Recorded and printed, never delivered. |
| `compare_ignores` | list of strings | `write`, `unknown`, `llm` | Top-level body fields that a retry check, a replay's matching and `irimi compare` ignore: values that differ on every call (a request id, a `stream` flag). |
| `live_because` | string | `read`, `llm`, `telemetry` | Why this live route persists nothing. Required for a live kind on `DELETE`, `PUT` or `PATCH`, and for a `read` on an unsafe method under `style: rest`. |
| `target` | `self` or URL | `write`, `unknown` | Who answers this route. Overrides only. |
| `forward_auth` | boolean | routes with a target | Send credential headers to the target instead of removing them. Overrides only. |

The loader refuses `fixture:`, `precondition:` and `fires:` on a live route (the real service
answers it, so they would never be used), a route listed twice, and any key it does not know.
These keys are retired, and the loader names the replacement: `volatile` (now
`compare_ignores`), `verbs` (now `style`; `honest` is `rest`, `post-only` is `rpc`), and `persists`
with `comment` (now `live_because`).

The live kinds' method rules are the scope rule ([security.md](security.md#invariants)).

## Targets

A target is a URL you control that answers a service's writes, or one route's, instead of irimi.
The agent gets whatever the target answers; the real service still sees nothing.

Set targets in an overrides file, `./irimi.maps.yaml`, or `$IRIMI_HOME/maps.yaml` when the working
directory has none:

```yaml
service: stripe
target: http://127.0.0.1:3000 # answers the service's writes
target_reads: true # optional: its reads too
---
service: slack
routes:
  - match:
      method: POST
      path: /api/chat.postMessage
    target: http://127.0.0.1:3111
```

The overrides file is merged over the shipped maps. It may set `target` and `target_reads` on a
service and `target` and `forward_auth` on a route that the shipped map already has, and nothing
else, so no override can turn a write into a read. Naming a service or route no map has is an
error.

On the command line, `--target '<host>[<path>]=<url>'` does the same for one run and beats the
file: a bare host sets the service's target, and a path sets it on each `write` or `unknown` route
that path names. `--target-reads <host>` adds the reads. `irimi maps list --target ...` previews the
result.

- **Paths** follow nginx `proxy_pass`: a target with no path keeps the request's path. A target
  with a path replaces the path of a request a route matched, and is prefixed to the path of one
  no route matched. So a stub that needs the id in `/v1/customers/{customer}` should be given a
  bare origin. The query, method, body and content type pass through.
- **Where a target may be:** loopback only, unless `--allow-remote-target <host>` names it; never
  irimi's own listener; and loopback only, with no escape, on a service with a credential-path host.
- **Credentials:** every credential header is removed before the request goes to a target, unless
  the route sets `forward_auth: true`.
- **No overlay.** A service whose reads go to a target gets no overlay: the target owns
  read-after-write consistency.
- **Failure:** an unreachable target is irimi's `502` flagged `target-failed`, never a silent fall
  back to a faked write. The summary says `could not reach target <host:port>`.

## Limits

- Routes match on method and path only. A GraphQL API is one route (`POST /graphql`); map it
  `write`, so every call is faked, or leave it unmapped.
- An effect can edit a read's body and status, not its headers, so a service whose pagination or
  caching lives in headers (`Link`, `ETag`, `X-Total-Count`) cannot be overlaid correctly.
- A map alone gives a service echo answers. Fixtures, the overlay, precondition checks and
  idempotency need Python ([adding-a-service.md](adding-a-service.md)).
- `fires:` names are free text; nothing checks them against the service's real events.
