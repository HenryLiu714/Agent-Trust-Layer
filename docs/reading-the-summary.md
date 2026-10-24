# Reading the summary

For anyone looking at what `irimi shadow`, `irimi serve` or `irimi runs show` printed and wondering
what a line means. It covers the banner, the line per exchange, and every line and status of the
summary. The terms are defined in [concepts.md](concepts.md).

## An example

This is the sample workflow `w01_ticket_triage double_refund`, run with
`uv run python scripts/demo.py w01`: an LLM agent refunds a $49.00 charge, then tries to refund it
again. Only the CA's temporary path is shortened.

```
irimi shadow · run b27b8bdc967bd7cf · listening on 127.0.0.1:62167 · ca /tmp/irimi-demo/home/ca/ca.pem
only traffic sent through this proxy is shadowed; a client that ignores HTTPS_PROXY talks to the real service
real          llm       POST api.anthropic.com/v1/messages -> 200
real          read      GET api.stripe.com/v1/charges/ch_TICKET1 -> 200
tool-real     read      tests.e2e.workflows.agents.w01_ticket_triage.agent:lookup_customer -> ok
real          llm       POST api.anthropic.com/v1/messages -> 200
irimi-check   read      GET api.stripe.com/v1/charges/ch_TICKET1 -> 200
faked         write     POST api.stripe.com/v1/refunds -> 200
real          llm       POST api.anthropic.com/v1/messages -> 200
irimi-check   read      GET api.stripe.com/v1/charges/ch_TICKET1 -> 200
faked         write     POST api.stripe.com/v1/refunds -> 400
real          llm       POST api.anthropic.com/v1/messages -> 200
tool-fake     write     tests.e2e.workflows.agents.w01_ticket_triage.agent:send_reply -> ok
real          llm       POST api.anthropic.com/v1/messages -> 200

irimi shadow · run b27b8bdc967bd7cf (whole process) · 10 exchanges · 0.3s

  None of these writes reached the real service:
  ○ stripe  refund $49.00 on ch_TICKET1  faked; checked against live state: would succeed
  ✗ stripe  refund $49.00 on ch_TICKET1  faked; would fail: charge_already_refunded
  ○ tool  tests.e2e.workflows.agents.w01_ticket_triage.agent:send_reply  fake ran instead
  money: $98.00 in faked writes; $49.00 of it would fail
  would have fired: refund.created, charge.refunded

  api.anthropic.com  5 llm
  api.stripe.com     1 read  2 irimi check reads  2 faked writes

  10 exchanges · 8 real · 2 answered by irimi (2 faked writes) · 0 sent to targets

  run 820db3b9  tests.e2e.workflows.agents.w01_ticket_triage.agent:handle_ticket(...)  10 exchanges · 2 writes
  replay with: irimi replay 820db3b9
```

The second refund got Stripe's own `charge_already_refunded` error, a `400`, because irimi's check
read found the charge already refunded by the first faked refund. Neither refund reached Stripe.

## The banner

```
irimi <command> · run <id> · listening on <host>:<port> · ca <path to ca.pem>
only traffic sent through this proxy is shadowed; a client that ignores HTTPS_PROXY talks to the real service
```

- `irimi serve` prints `run: per request (SDK), unattributed otherwise` in place of a run id.
- `irimi replay` adds `replaying <recorded run> as <new run>`.
- A listener bound off loopback adds `listening on <host>: any machine that can reach this port can
  send traffic through irimi and call its control endpoint`.
- Each service or route sent to a target adds
  `delegated: <service> -> <url> (writes)` (or `(reads + writes)`, or a route's
  `<service> <METHOD> <path>`), and a second line warning when the target is not on loopback.

## The line per exchange

One line per exchange and per tool call, as each finishes (`--quiet` skips them):

```
<who>         <kind>    <METHOD> <host><path> -> <status>  [<flags>]
```

The first column says who answered:

| Word | Meaning |
| --- | --- |
| `real` | the real service; with `[overlay]`, irimi applied the run's faked writes to its answer |
| `faked` | irimi faked this write |
| `target` | your target answered |
| `recorded` | a replay answered from the recording |
| `irimi` | irimi's own error answer |
| `irimi-check` | irimi's check read, made before faking a write; never the agent's request |
| `tool-real` | a read tool ran for real |
| `tool-fake` | a write tool's fake ran instead of it |
| `tool-recorded` | a replay answered a read tool from the recording |

A tool call's line ends `-> ok` or `-> raised <type>`. The flags are the exchange's
([trace-format.md](trace-format.md#flags)). Every line escapes control characters the agent sent,
so nothing it prints can break a line or drive the terminal.

## The summary

### Header

```
irimi shadow · run <id> · <N> exchanges · <seconds>s
```

`(whole process)` after the run id means the agent used the SDK: the block covers every exchange
the process made, whichever run it belongs to. A replay's stored run opens `irimi replay · run`.

### Writes

`None of these writes reached the real service:` heads one line per write the agent attempted,
in the order irimi answered them:

```
<marker> <service>  <what the write does>  <status>
```

- `○` is a write the real service would have accepted, or that a target answered; `✗` one it
  would have refused, or that irimi refused or could not deliver.
- The sentence comes from the route's `human:` template in its service map, filled with this
  request's fields. A field the request did not send prints `?`. A write with no template, or no
  route, is spelled as its request: `POST api.example.com/v1/things`.
- Amounts are formatted only where the template marks them (`{amount:money}`), in the currency the
  request named or, failing that, the one irimi's check read found. With neither, an amount prints
  as `4900 (minor units, currency unknown)`.

The status says what irimi did and what it knew:

| Status | Meaning |
| --- | --- |
| `faked; checked against live state: would succeed` | irimi's check read found the real service would accept it |
| `faked; would fail: <code>` | the check found it would refuse it; the agent got the service's own error with `<code>` |
| `faked; could not check: <why>` | the route has a check, and irimi could not complete it (below) |
| `faked; not checked` | the route has no precondition check |
| `faked; not checked (unclassified)` | no map names this route, so irimi faked it as a presumed write |
| `sent to target <host:port>` | a target you configured answered it |
| `could not reach target <host:port>` | the target was unreachable or refused; the agent got irimi's `502` |
| `refused by irimi` | irimi refused the request before deciding it |
| `refused by irimi: its body is too large to read whole` | the request body decodes past 64 MiB, so irimi would not fake it |
| `refused by irimi: irimi could not decide how to answer it` | the decision failed; the agent got irimi's `502` |

`could not check` names why: `<service> answered irimi's check read with <status>` (a `401` or
`403` means the agent's own credentials could not read the object), `irimi's check read of
<service> got no usable answer` (a network failure, or a body too large to read),
`irimi could not tell from its check read`, `irimi made no check read`, or, in a replay,
`the recorded run could not check it`.

Under a write, `↳` lines name the agent's later reads of that service that the overlay touched:

```
  ↳ GET /v1/refunds saw it  overlay
```

`saw it in part  overlay (partial)` and `did not show it  real (partial)` mean irimi knows the read
showed only part of the run's writes.

A write tool whose fake ran gets a line after the HTTP writes: `○ tool  <name>  fake ran instead`,
or `fake raised <type>`.

### Money and webhooks

```
money: $98.00 in faked writes; $49.00 of it would fail
would have fired: refund.created, charge.refunded
```

The money line sums the formatted amounts of faked writes per currency, and counts apart any
amount whose currency is unknown. `would have fired:` lists the webhooks of accepted faked writes
only: a rejected write, a retry, a write sent to a target, and a write whose check read was
refused with `401` or `403` list none. Nothing is delivered.

### Hosts

One line per host, saying what was real and what was not:

```
api.stripe.com     3 reads (1 showing this run's writes)  1 irimi check read  1 faked write
```

| Phrase | Meaning |
| --- | --- |
| `N reads` | the agent's reads the real service answered; qualified by `N showing this run's writes` (overlaid), `N from the recording` and `N failed: <statuses>` |
| `N irimi check reads` | reads irimi made itself, never counted as the agent's |
| `N llm` | LLM calls, forwarded live (`N from the recording` in a replay) |
| `N faked writes` | writes irimi answered, `(N unclassified)` when no map named them |
| `N reads from the target`, `N writes sent to the target` | answered by your target |
| `N reads refused by irimi`, `N writes refused by irimi` | irimi's own refusals |
| `... that could not reach the target` | the target failed |

All telemetry hosts share one line: `telemetry  N exchanges to N hosts, forwarded live`.

### Totals

```
10 exchanges · 8 real · 2 answered by irimi (2 faked writes) · 0 sent to targets
```

`real` counts every exchange the real service answered, irimi's check reads included. `answered by
irimi` counts every answer that did not come from the real service or a target, and the bracket how
many of those were faked writes. Tool calls are counted in none of these.

### Lines only a live summary prints

- `run <id>  <trigger>(...)  N exchanges · N writes`, one per SDK run the process made, then
  `replay with: irimi replay <id>`. Each SDK run is stored on its own and can be shown, compared or
  replayed by that id.
- `irimi saw no HTTP(S) traffic; if the agent made calls, they bypassed the proxy or failed TLS`,
  when the run recorded no exchange.
- `N connections to <host> failed TLS: the client did not trust irimi's CA; point it at <ca.pem>
  (some clients ignore SSL_CERT_FILE)`, per host whose client refused irimi's certificate.

Under `irimi serve`, each run's summary prints under `--- run <id> ---` when the SDK ends the run.
How a summary read back with `irimi runs show` can differ from the live one is in
[cli.md](cli.md#irimi-runs-show).
