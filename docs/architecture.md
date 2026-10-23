# Architecture

irimi is a local MITM proxy that sits between an agent and the services it talks to. Reads are
forwarded to the real service, writes are answered locally, and (from Phase 2) reads after writes
see those writes. This document is the map of the code: what each module is for, how a request
moves through them, and which rules the tests hold the layout to.

## One request, end to end

mitmproxy calls `IrimiAddon` (`src/irimi/engine/mitm.py`) once per flow. The addon only turns
flows into `Request`s and `Response`s and carries plans onto them; what irimi decides and records
is `engine.session.ShadowSession`'s, which every engine drives, in this order. Nothing
through the listener skips them: mitmproxy's raw TCP relay is off (`rawtcp=False`), so a tunnel
whose first bytes mitmproxy would not call HTTP (a method like `M-SEARCH`, or a first segment of
under three bytes) is parsed as HTTP and decided like any request, bytes that are not HTTP fail
there, and a connection a live `101` upgraded to anything but a WebSocket is closed. None of them
is relayed, and none leaves a record (#75).

1. **`request`** - `wire.parse` normalises the wire request, and `ingress.admit` says what it is
   to irimi: a request addressed to the listener itself whose path starts with `/_irimi/` is the
   control endpoint (#73), which `control.ControlEndpoint` answers in the hook, so it is never
   forwarded, decided or recorded as an exchange; a request on the listener otherwise came in as
   `/<host>/<path>` and is rewritten upstream; one irimi cannot route or rewrite is a `Refusal`,
   answered by `ShadowSession.refuse`. `admit` reads the run id from `Irimi-Run` and takes it and
   any agent-sent `Irimi-Rewrote` off the request, and the addon takes them off the flow, so
   neither leaves irimi (#67). Then `ShadowSession.plan`, on a worker thread, builds the `Plan`:
   `classify.classify` says what the request is (`read`, `write`, `llm`, `telemetry`,
   `unknown`), `targets.delegate` resolves its answer target once, a read the overlay can
   translate (a cursor naming a minted id, #43) is translated unless it is delegated, and is what
   the policy is asked and what the trace records, whoever answers it (#82). The `AnswerPolicy`
   decides `Forward`, `Delegate` or `Respond` (`services`: the route's L1 fixture when its map
   names one, the L0 echo otherwise); a `Respond` is stamped as it is planned, and a target is
   probed and refused on the worker thread. The addon carries the plan onto the flow and keeps it
   in the flow's metadata. **Nothing in this hook may raise**: an exception here makes mitmproxy
   forward the flow untouched, so the hook runs under one guard and fails closed with a `502`
   flagged `decision-failed`, and so does turning the answer into mitmproxy's, since a replay's
   answer was read off disk (#82); the precondition reads that answer made are still recorded.
2. **`responseheaders`** - stamps `Irimi-Answered-By` on a live or delegated answer before the
   headers go out, and streams a `text/event-stream` answer through a `StreamCapture`, which hands
   each chunk on unchanged and keeps a copy for the trace (#71).
3. **`response`** - `ShadowSession.complete`: every unstreamed read's body goes past
   `services.observe_read` first, which is how a service learns the real values its fakes have to
   sort against (Slack's `ts`); a service with no observer is a no-op. Then, for a live,
   unstreamed read, the `Overlay` gets a chance to rewrite the body from the write log. It builds
   the `Exchange`, writes join the write log, `wire.stamp` stamps a forwarded answer, and
   `ShadowSession.record` hands it to the `RunStore` - which keeps a telemetry exchange only as
   a count (#70) - and reports it to the CLI.
4. **`error`** - `ShadowSession.fail`: a lost upstream or an unreachable target is annotated with the right flag and
   recorded with no response, so a failed write is never silently dropped from the trace. A
   stream that fails after its headers went out keeps the complete lines its `StreamCapture` copied,
   flagged `stream-truncated` (#71).

Every body a hook records is decoded by `engine.decoding.decode_body`, which never raises and
never decodes past `limits.MAX_DECODED_BODY` (#94): each codec runs incrementally and stops at
the cap, so a small compressed bomb costs a hook at most twice the cap in memory. A body cut there
is never decoded or redacted whole, so it is recorded empty and flagged, as the store records any
body past the same bound; a stream keeps its first `limits.MAX_STORED_BODY` bytes of whole lines, as any stream that
is not whole does. Such a read is never overlaid, so the agent gets the service's bytes unchanged,
and one the run has a write for is `overlay: partial`, as a body the overlay cannot parse is.
Nothing is answered from the start of a body: a write whose request body was cut fails closed
like a decision that raised (`502`, `decision-failed`, `body-truncated`), a live kind is forwarded
with the agent's own bytes, and the control endpoint refuses a cut body `413`.

## Layers

The code is packaged by concept, and the packages sit in layers: a module may import any module
of a lower layer, and a module listed before it in its own package, and nothing else.
`tests/test_import_boundary.py` holds the code to this table, with each package's module order,
and this table to the test's; it also holds that `mitmproxy` is imported in `engine/mitm.py` and
nowhere else. Add a module and the test asks you to place it there and here. A package nested in
another (`services/stripe/`, `record/store/`) is one entry of its parent. Each module's own
docstring says what it holds.

| Layer | Packages | Modules, lowest first | Job |
| --- | --- | --- | --- |
| 0 | `core/` | `exchange`, `netaddr`, `secret_shapes`, `money`, `limits`, `opener`, `exitcodes`, `calls`, `bodies` | The vocabulary every other package speaks: requests, responses and exchanges with their kinds, flags and write-set predicates, addresses and the one loopback rule, credential shapes, size bounds, exit codes, a trigger's call bound to its parameters, and the never-raising body parser. |
| 1 | `protocol/` | `headers`, `env`, `trace`, `entrypoints`, `control`, `replay_files` | What crosses a process boundary or reaches disk as JSON: irimi's headers, the child's environment, the trace format's records and codecs, `module:qualname` entrypoints, the control endpoint's routes and bodies, and the files a replay's child and `irimi.testing` read. The engine and the SDK both import it and never each other. |
| 2 | `config/`, `sdk/` | `paths`, `files`, `ca`; `sdk/`'s private modules | Side by side: `config` is irimi's state on this machine (where it lives, owner-only files, the local CA); `sdk` is what an agent imports, whose public surface is `irimi.sdk.__all__` and whose imports `tests/test_sdk_surface.py` holds to `core` and `protocol`. |
| 3 | `servicemaps/` | `model`, `rules`, `loader`, `__init__`, `classify` | The service maps as code: the dataclasses and route matching, the validation (the scope rule lives in `rules`), the loader, and `classify`, what a request is by the maps. |
| 4 | `services/` | `model`, `generic`, `stripe/`, `slack/`, `__init__` | What irimi knows about each service: one `Service` spec per service in `REGISTRY`, the generic echo and fixture answers, and each service's answers, effects and preconditions. |
| 5 | `record/` | `redact`, `store/` | What a run leaves on disk: the redacted copy of each event, and the run store that writes and reads it. |
| 6 | `decide/` | `targets`, `reader`, `writelog`, `idempotency`, `preconditions`, `overlay`, `policy` | How irimi answers a request: which target answers it, irimi's check read, the run's faked writes, idempotent retries, the precondition check, the overlay and the policy. |
| 7 | `engine/` | `__init__`, `wire`, `ingress`, `decoding`, `capture`, `control`, `session`, `mitm` | The proxy: the `Engine` protocol, irimi's side of the wire, how a request reached irimi, bounded decoding, stream capture, the control endpoint, `ShadowSession` (the engine-agnostic core) and `MitmEngine`, the only module that imports mitmproxy. |
| 8 | `report/` | `terminal`, `model`, `summary`, `runs`, `maps` | Everything a run prints, as pure text: the banner and stream lines, the summary model and its rendering, the stored-run listings and `irimi maps list`. |
| 9 | `replay/` | `child`, `index`, `policy`, `compare`, `plan`, `report` | Re-running a stored run and judging it: the child that calls a trigger again, the recording a replay answers from, the replay policy, two stored runs compared write by write, what `irimi replay` runs and what it reports. |
| 10 | `cli/` | `options`, `runner`, `compose`, `init`, `maps`, `runs`, `shadow`, `serve`, `replay_cmd` | The `irimi` command: shared options, process plumbing, the composition root (`compose`), and one module per command. |
| 11 | `__main__`, `testing` | | The entry points: `python -m irimi`, and `irimi.testing`, the public test-time API, which starts the proxy and so needs the full install. |

`src/irimi/maps/*.yaml` are the shipped service maps. They are data, contributable without
touching Python, and `tests/test_servicemap.py` pins their contents.
`src/irimi/fixtures/*.json` are the response objects those maps' `fixture:` keys name - vendored
from the service's own mock where one exists (Stripe), hand-written against its published docs
where none does (Slack). Each file says which in its `_source` entry, and the same test proves
both directories are inside a built wheel.
A `write` or `unknown` route may also name a `fires:` list of the webhooks the real service would
have sent, which the exchange carries as `would_fire` for the writes irimi accepted and faked;
nothing is delivered (#47).

## The seams

Four protocols are where later phases plug in, and the reason the rest of the code can be tested
without a proxy:

- **`engine.Engine`** - run, shutdown, wait for the listener. `runner.start_engine` drives it on a
  background thread; `cli` never touches mitmproxy directly.
- **`policy.AnswerPolicy`** - given a `Request`, its `Classification`, its answer target and
  (defaulted, #45) the run's write log and id, return a `Decided`: `Forward`, `Delegate`, or
  `Respond` with this response. It stays synchronous; the session calls it on a worker thread.
  `ShadowPolicy` takes a
  `reader.Reader` for the L3 precondition read; built without one it does no L3 at all and records
  `precondition: None`. Replay is a new policy, not a new branch (#82): `ReplayPolicy` answers from
  a recording and hands the rest to a `ShadowPolicy` whose reader is that recording. A replay is
  one run, so only `irimi replay` replays, and serve mode cannot (#84).
- **`overlay.Overlay`** - given the write log, a read's classification and request and the upstream response, return
  an `Overlaid`: the response the agent should see, and how much of the write log that read could
  express. Its request side, `rewrite`, translates a read before it is forwarded. Both are pure
  functions of their arguments, and the engine asks `rewrite` of a read before the policy decides,
  so the policy and the trace see the read as irimi would ask it (#82).
- **`store.RunStore`** - where a run's record, its exchanges and its tool calls go (#70).
  `irimi shadow` starts its process run before spawning the child and ends it with the child's
  exit code; an exchange whose run has no start creates a `header` run. No method may raise or
  block: `record` is called from mitmproxy's hooks. `DirectoryStore` is the only real store, and
  a replay reads what it wrote with `StoreReader`; a store a replay writes keeps only starts whose
  record names that replay (`mode: replay`, `replay_of`, set by the record's constructor),
  makes each header run that replay's, and writes no run it did not make, the run it replays
  included. An SDK run's start, end
  and tool calls reach it through the control endpoint (#73), never from the agent's process
  directly.

## Rules the code holds itself to

Each of these has been a real bug here; each is now enforced by a test, and most are enforced
twice - once where a configuration is loaded and again at the decision it protects.

- **Never raise in the `request` hook.** A raised hook forwards the flow, and a forwarded write
  escapes shadow mode. The decision fails closed as a whole (`decision-failed`), and the L0 echo
  never raises on any body.
- **THE SCOPE RULE.** No classification that forwards live may apply to an unsafe method it did
  not name explicitly. The loader refuses a live `default_kind`, a live kind on `*`, and a live
  kind on `DELETE`/`PUT`/`PATCH` without a `live_because:`; the classifier
  downgrades anything that reaches it anyway (`kind-downgraded`). `servicemaps/rules.py` tells the
  story.
- **Stamping.** Every response irimi decided carries `Irimi-Answered-By` naming the answer;
  a live forward carries none, because absence is how a client tells the real service's response
  from ours. `tests/test_invariants.py` checks this against a real run and against policies
  built to break it. A control endpoint answer carries `control`, which is not an `answered_by`
  value because a control request is never an exchange (#73).
- **Irimi's own wire vocabulary is never the agent's.** The query pair irimi drops from a read it
  translates stays in the engine, which hands it back to the overlay with the response; it never
  goes on the wire. An agent-sent `Irimi-Rewrote` is stripped in the `request` hook all the same.
  `Irimi-Run` names the run a request belongs to; the `request` hook reads it and then
  strips it from the flow and the recorded request, so no live forward, answer target or L3
  read carries the agent's run id to a service, and `Exchange.run_id` is the one place it is
  kept (#67).
- **Targets stay on the machine.** A target must be loopback unless `--allow-remote-target` says
  otherwise; a credential-path host (`hooks.slack.com`) has no such escape; irimi's own listener
  is never a target; credential headers are stripped unless the route opts in. `netaddr` is the
  one spelling of "loopback" all of these share.
- **Nothing reaches disk unredacted, and the live flow is never redacted.** The store redacts its
  own copy of each exchange with `redact.redact_exchange`; the engine and the agent's answer never
  see a placeholder. A part that cannot be redacted is stored as `<redaction-failed>`, never as it
  was (#69).
- **A recording failure never affects traffic** (design §5.2). No `RunStore` method may raise
  or block: `DirectoryStore` only enqueues, and drops and counts what it cannot write. `ShadowSession.record`
  guards the call anyway, so a store that breaks the contract costs the exchange its record, never
  the hook, the terminal line or the summary (#70).
- **Telemetry is forwarded and its requests and responses are never stored.** One line per
  exchange records only that it happened and to which host, so a stored run's summary can count
  it. A trace of the agent's own observability traffic is noise, and replaying it would re-emit
  someone else's events (#70).
- **L0 is the floor.** A route with no `fixture:`, and a fixture this install cannot read, are
  both answered with the L0 echo rather than refused - the second carries `fixture-failed`, so
  the trace never claims a fidelity the answer did not have.
- **A delegated service gets no overlay, and a streamed response is never rewritten.** Both are
  written down at the top of `overlay.py` for Phase 2 to inherit.
- **A write L3 rejected never enters the write log, and shadow mode is never built without a
  reader.** The first keeps a write irimi says would have been refused from being replayed onto
  later reads as though it had happened; the second is what stops the whole L3 path from silently
  doing nothing in the product while every test that builds a policy bare still passes.
  `tests/test_invariants.py` holds both against a real run (#45).
- **A precondition says `not_evaluable` rather than guessing, in either direction.** A false
  rejection invents a refusal the service never made; a false pass claims a check that never ran,
  and the summary prints `L3 preconditions passed` off it. So a non-200, an unparseable body, an
  overlay that knows it is `partial`, and a document the check itself cannot read all record
  `not_evaluable` and fake the write at L2 (#45).
- **A read of an object this run minted is answered, not left as the service's refusal.** It is
  the one read the real service could not have answered: it has never heard of the id, so it
  refuses, and handing the agent that refusal for an object irimi told it exists is the
  contradiction the overlay exists to prevent. The read still goes out and still counts as a real
  read; the overlay replaces the body and, for Stripe's 404, the status. Only over the service's own
  not-found (`resource_missing`, `thread_not_found`): a rate limit or a bad key on the same read is
  left as sent and `partial`, because production would have sent it for the real object too. Every
  other effect leaves the status the service's. The alternative - answering such a read locally, off the write log,
  the way a faked write is - was weighed and refused, because it makes a read irimi never forwards
  (#52).
- **Only a write that would have happened lists its webhooks.** `would_fire` is set on the one
  path where irimi accepted and faked a write, so an L3 rejection, an idempotency conflict, a
  replay and a delegated write all list nothing by construction, not by a second check - the same
  set the write log holds. A replay listing its events again would promise one refund's
  `refund.created` twice. `tests/test_invariants.py` holds it against a real run (#47).

## State on disk

```
$IRIMI_HOME/            default ~/.irimi
  ca/ca.key             the local CA's private key (0600)
  ca/ca.pem             the CA certificate the child process is told to trust
  mitm/mitmproxy-ca.pem key + cert, the bundle mitmproxy mints leaf certificates from
  redact.key            the redaction HMAC key (0600), created on first use (#69)
  store/                the trace store, unless --store names another (#70):
    blobs/<sha256>        a redacted body, written once
    runs/<run_id>/        run.json and events.jsonl, one directory per run
    unattributed/         events.jsonl for events no usable run id claimed
    serve.lock            held by the one serve writing this store, while it runs (#77)
  maps.yaml             optional: the user's overrides file (targets only)
./irimi.maps.yaml       optional: a per-project overrides file; wins over the one above
```

No config file chooses the mode. The command is the mode. Under `irimi shadow -- <cmd>` one
process tree is one run, stored as the `process` run; the child learns the run id, 16 hex
characters, from `IRIMI_RUN`, and where the control endpoint is from `IRIMI_CONTROL` (#73). Under
`irimi serve` there is no child and no process run: the
engine's own run is `unattributed`, every run is one the SDK names on its requests, and the agent
is given `IRIMI_CONTROL` by whoever deploys it (#77).
