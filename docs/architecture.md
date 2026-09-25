# Architecture

irimi is a local MITM proxy that sits between an agent and the services it talks to. Reads are
forwarded to the real service, writes are answered locally, and (from Phase 2) reads after writes
see those writes. This document is the map of the code: what each module is for, how a request
moves through them, and which rules the tests hold the layout to.

## One request, end to end

Everything happens inside `IrimiAddon` (`src/irimi/engine/mitm.py`), which mitmproxy calls once
per flow. Each hook calls plain functions from the layers below it, in this order:

1. **`request`** - `pipeline.parse` normalises the wire request. `reverse_door.detect_door`
   decides whether it came in as `/<host>/<path>` on the listener itself, and if so
   `reverse_door.rewrite_reverse` points it upstream. Then `pipeline.classify` says what it is
   (`read`, `write`, `llm`, `telemetry`, `unknown`), `pipeline.attribute_run` picks the run id, and
   the `AnswerPolicy` decides: forward live, delegate to an answer target (`delegation.delegate`
   chose it; `_to_target` rewrites the flow), or answer it locally (`echo`: the route's L1
   fixture when its map names one, the L0 echo otherwise). The decision is stored on the flow
   as `_Pending`. **Nothing in this hook may raise**: an exception
   here makes mitmproxy forward the flow untouched, so the whole decision is wrapped and fails
   closed with a `502` flagged `decision-failed`.
2. **`responseheaders`** - stamps `Irimi-Answered-By` on a live or delegated answer before the
   headers go out, and turns on streaming for `text/event-stream`.
3. **`response`** - every unstreamed read's body goes past `echo.observe_read` first, which is
   how a service learns the real values its fakes have to sort against (Slack's `ts`); a service
   with no observer is a no-op. Then, for a live, unstreamed read, the `Overlay` gets a chance to
   rewrite the body from the write log. `pipeline.annotate` builds the `Exchange`, writes join
   the write log, `pipeline.respond` stamps the header, and `_finish` records it in the
   `TraceStore` (telemetry excepted) and reports it to the CLI.
4. **`error`** - a lost upstream or an unreachable target is annotated with the right flag and
   recorded with no response, so a failed write is never silently dropped from the trace.

## Layers

Every module lives in exactly one layer and may import only from layers above it in this table
(lower layers never import higher ones). `tests/test_import_boundary.py` enforces this, and also
that `mitmproxy` is imported in `engine/mitm.py` and nowhere else. Add a module and the test asks
you to place it.

| Layer | Modules | Responsibility |
| --- | --- | --- |
| 0 | `exchange` | The wire vocabulary: `Request`, `Response`, `Exchange`, the kinds, the `answered_by` values and every flag. |
| 0 | `paths` | Where irimi keeps state (`$IRIMI_HOME`, default `~/.irimi`) and the listener defaults. |
| 0 | `netaddr` | The one place that answers "is this address this machine?". Three safety rules depend on it agreeing with itself. |
| 1 | `ca` | Generate the local CA and write the bundle mitmproxy mints leaf certificates from. |
| 1 | `fixture` | The vendored response objects an L1 answer starts from. Reads `irimi/fixtures/<service>.json`, caches it, and never raises. |
| 1 | `servicemap/` | The service maps. `model` is the dataclasses, the host index, route matching and the target precedence; `rules` is the validation (THE SCOPE RULE lives here); `loader` turns YAML, the overrides file and the `--target` flags into a `MapIndex`. |
| 2 | `pipeline` | The pure request pipeline: parse, classify, attribute the run, annotate, respond. |
| 2 | `reverse_door` | The `/<host>/<path>` door for SDKs that ignore proxy variables. |
| 3 | `delegation` | Answer targets (design D20): which target answers a request, where it is sent, what it may never be, which headers it may not carry. |
| 3 | `echo` | The body a locally answered write gets. L0: form and JSON reflection, minted ids, Slack's envelope. L1: the route's fixture with the request's own fields written over it. `fake_rejection` carries an L3 rejection's modeled error body at the level the route's write would have been answered at - there is no `fake-L3` (#45). Also `observe_read`, the one seam where a forwarded read's body teaches the faker something (a Slack `ts` a minted one must sort after). |
| 3 | `services/` | What a faked write does to a later live read, per service, as plain functions over plain data (#43). `model` is the `Read` / `Write` / `Applied` / `Rewritten` vocabulary; `stripe` and `slack` are the effects tables (#44). Also the L3 preconditions (#45): `Proposal` / `Probe` / `Rejection` / `NotEvaluable` / `Check` in `model`, and `PRECONDITIONS`, the table a route's `precondition:` key names an entry in - each check says which one real read it needs and what the answer, with the run's writes applied, makes of the write. A verdict has three answers, not two: rejected, passed, and `NOT_EVALUABLE` for a document it cannot read, which is why a Slack `missing_scope` at HTTP 200 never prints as a check that passed. Pure: no clock, no minting, no I/O, so Phase 5 replay runs the same functions over a recording. Also `Idempotency` and the `IDEMPOTENCY` table (#46): the header a service keys a repeat on, and what it answers when that key comes back with different parameters. Slack has no entry, because it has no such mechanism. |
| 4 | `writelog` | The run's faked writes, decoded out of the trace into `services.Write`s, in the scope a read is asking about. Shared by the overlay, which applies them to a live read, and the policy, which checks a new write against them (#45). Pure. |
| 4 | `idempotency` | The run's answers to writes that carried an idempotency key, so an agent's retry with the same key is one write and not two (#46). A key held with different parameters is the service's own `idempotency_error`, and "the same write" is compared as the whole request - method, path, query and the posted fields the route does not call `volatile:` - because a key names one write and not one route, and `payment_intents.cancel` posts nothing at all. In memory, lock-guarded because the decision runs on a worker thread, and pure enough for Phase 5 replay to keep the same promise over a recording. |
| 5 | `policy` | `AnswerPolicy` and `ShadowPolicy`: the decision, and only the decision. Part of deciding a mapped write is L3 (#45): the `Reader` seam issues the precondition's one real read, `UpstreamReader` over stdlib urllib in shadow mode, and the policy returns that read, marked `issued_by: engine`, on the `Answer` for the engine to record. Also the idempotency lookup (#46), which sits between the live-kind check and L3: a retry with a key this run has already answered returns the first answer and issues no precondition read, and a key reused with different parameters is answered with the service's own `idempotency_error`. The accepted local fake, the one path left after all of those, is the only `Answer` that carries its route's `fires:` as `would_fire` (#47). |
| 5 | `overlay` | The `Overlay` seam and `ServiceOverlay`, which applies `services`' effect tables to a live read and translates a cursor naming a minted id before the read is forwarded. `NoOverlay` stays, for tests and for a mode with no overlay. The module's header lists the two hazards every overlay must respect. |
| 5 | `store` | The `TraceStore` seam. `NullStore` today; Phase 3 replaces it. |
| 6 | `engine` | The `Engine` protocol and `EngineConfig`. `engine/mitm.py` is the only mitmproxy-backed implementation and the only module that imports mitmproxy. |
| 7 | `report` | Everything a run prints: banner, per-exchange line, exit summary. Pure text. |
| 7 | `runner` | Process plumbing for `irimi shadow`: the child's environment and the engine thread. |
| 8 | `cli` | argparse and the composition root. `_build_engine` is the one place the concrete engine, policy, store and overlay meet. |

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
- **`policy.AnswerPolicy`** - given a `Request`, its `Classification` and (defaulted, #45) the
  run's write log and id, return an `Answer`: forward live, delegate, or send this response. It
  stays synchronous; the engine calls it on a worker thread. `ShadowPolicy` is the only one today,
  and takes a `policy.Reader` for the L3 precondition read; built without one it does no L3 at all
  and records `precondition: None`. Record and replay modes are new policies, not new branches.
- **`overlay.Overlay`** - given the write log, a read request and the upstream response, return
  an `Overlaid`: the response the agent should see, and how much of the write log that read could
  express. Its request side, `rewrite`, translates a read before it is forwarded. Both are pure
  functions of their arguments, so Phase 5 replay applies them over recorded reads.
- **`store.TraceStore`** - where finished exchanges go. Phase 3.

## Rules the code holds itself to

Each of these has been a real bug here; each is now enforced by a test, and most are enforced
twice - once where a configuration is loaded and again at the decision it protects.

- **Never raise in the `request` hook.** A raised hook forwards the flow, and a forwarded write
  escapes shadow mode. The decision fails closed as a whole (`decision-failed`), and the L0 echo
  never raises on any body.
- **THE SCOPE RULE.** No classification that forwards live may apply to an unsafe method it did
  not name explicitly. The loader refuses a live `default_kind`, a live kind on `*`, and a live
  kind on `DELETE`/`PUT`/`PATCH` without `persists: false` and a `comment:`; the classifier
  downgrades anything that reaches it anyway (`kind-downgraded`). `servicemap/rules.py` tells the
  story.
- **Stamping.** Every response irimi decided carries `Irimi-Answered-By` naming the answer;
  a live forward carries none, because absence is how a client tells the real service's response
  from ours. `tests/test_invariants.py` checks this against a real run and against policies
  built to break it.
- **Targets stay on the machine.** A target must be loopback unless `--allow-target-host` says
  otherwise; a credential-path host (`hooks.slack.com`) has no such escape; irimi's own listener
  is never a target; credential headers are stripped unless the route opts in. `netaddr` is the
  one spelling of "loopback" all of these share.
- **Telemetry is forwarded and never stored.** A trace of the agent's own observability traffic
  is noise, and replaying it would re-emit someone else's events.
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
  maps.yaml             optional: the user's overrides file (targets only)
./irimi.maps.yaml       optional: a per-project overrides file; wins over the one above
```

No config file chooses the mode. `irimi serve` and `irimi shadow` are the mode; one process tree
is one run; the child learns the run id from `IRIMI_RUN`.
