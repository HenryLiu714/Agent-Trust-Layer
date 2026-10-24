# Limitations

For evaluators deciding whether irimi fits their agent, and for users puzzled by a result. What
irimi does not do, or does only in part, collected in one place. The traffic irimi cannot see at
all is listed under [known bypasses](security.md#known-bypasses).

## What irimi shadows

- **HTTP(S) through the proxy only.** Database writes, files on disk, gRPC, WebSocket traffic, queues
  and anything a client sends around the proxy happen for real. A write the proxy cannot see can be
  labelled with `@sdk.write_tool`, so its fake runs instead ([sdk.md](sdk.md)); point database URLs
  at scratch data otherwise.
- **Unmapped services are faked, not modelled.** A write to a host no map claims is answered with an
  echo of its own fields and flagged `unclassified`. Fixtures, the overlay and precondition checks
  exist only for services irimi has Python for (Stripe and Slack today).
- **Routes match on method and path.** A GraphQL API is one route; irimi cannot tell its queries
  from its mutations ([service-maps.md](service-maps.md#limits)).
- **The overlay edits bodies, not headers.** Pagination or caching carried in headers (`Link`,
  `ETag`) is left as the service sent it.
- **No overlay for tools.** A read tool after a write tool's fake reads what was there before;
  nothing applies the fake's write to it.
- **The overlay models what its effects name.** A read the overlay could express only in part is
  recorded `overlay: partial`, and the summary's `↳` lines say so; it is never silently wrong. A
  read of a faked object that the service answers with an error other than `404` is passed on as
  sent, so an agent that retries it sees what it would see in production.
- **Clients that ignore `SSL_CERT_FILE`** fail TLS under irimi rather than writing for real. The
  summary counts these failures per host and names the CA to point them at.

## The summary

- **Amounts need a currency.** An amount whose currency neither the request nor irimi's check read
  gave prints in minor units, `4900 (minor units, currency unknown)`, and is left out of the money
  total.
- **Precondition checks are one read.** A check irimi cannot finish (a `429`, a network failure, a
  body too large to read) records `could not check` and the write is faked anyway; irimi never
  guesses either way.
- **The sample workflows pin the summary's known gaps.** Each behaviour the corpus has found wrong
  is pinned under a `KNOWN BUG:` comment in `tests/e2e/workflows/test_w*.py`, so a fix shows up as a
  changed pin.

## Serve mode

- A stream the agent leaves open after its run ended is stored in the run but missing from the
  summary already printed.
- An agent whose control posts fail has its requests stored under a `header` run that lists as
  `incomplete`; the warning is on the agent's own logger, not irimi's.

## Redaction

- **A body that is not valid UTF-8 is stored unscanned**, so a credential inside a binary body
  reaches disk as it was sent.
- **JSON that is not one document on one line is scanned for shapes only.** A secret key's value is
  replaced only if it also has a secret's shape when its JSON spans several lines of a body that is
  not one document (a multi-line SSE `data:` field, a cut document), or sits inside a JSON string
  (a streamed tool call's `partial_json`).
- **A secret a stream sends in pieces is stored in pieces.** Each SSE event is its own line, so a
  secret whose characters arrive across several events is never whole on any line, and each piece
  reaches disk as it was sent.
- **Business data is not redacted.** Customers, amounts and messages are stored as sent; delete
  old runs with `irimi runs prune`.
- **The key is per install.** One secret gets different placeholders on two machines, so recordings
  from two machines do not match each other's redacted values.

## Replay

- **A trigger called alone runs without its process.** What the agent's `main()` set up (a database
  it seeded, a flag it parsed, a thread the trigger relied on) is not there. Replay such a run with
  `-- COMMAND` instead. A method's run cannot be called alone: a run does not record its instance.
- **A process run's replay reproduces its argv only**, not its environment, working directory or
  stdin.
- **A command is the whole process.** With `-- COMMAND`, every run the process starts is the one
  replay, answered from the one recorded run, so a process that held several runs shows the
  others' writes as added.
- **A divergence makes the rest live.** Past it, the agent's reads go to the real services and the
  model is asked again.
- **No provenance between tools and requests.** A stored tool call names none of the exchanges its
  body made, so a replay that answers the tool takes the exchanges recorded within its span as its
  body's.
- **Redacted values stay redacted.** A recorded secret comes back as its placeholder.
- **One run, one store.** The run replayed and its replay share one store, serve mode cannot
  replay, and each `irimi replay` replays one run.
- **Compare sees writes, not reads.** Two writes made at once can finish in either order, so a run
  that made the same two writes in the other order shows one of them as removed and added again.

## The SDK

- Requests from clients the SDK does not patch (`httpx2`, which the Anthropic SDK 2.x uses,
  `aiohttp`, raw sockets, subprocesses) carry no run unless the agent sets `Irimi-Run` itself
  ([sdk.md](sdk.md#which-http-clients-carry-the-run)).
- A trigger in a script run by path (`python agent.py`) records as `__main__:…`, which replay cannot
  import; start the agent with `python -m`.
- Captured values that are not plain data (a `Decimal`, a `datetime`, an object irimi cannot
  rebuild) are stored as descriptions, and a run holding one cannot be replayed by entrypoint
  without `--args`.
