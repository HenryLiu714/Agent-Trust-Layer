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
3. **`response`** - for a live, unstreamed read, the `Overlay` gets a chance to rewrite the body
   from the write log. `pipeline.annotate` builds the `Exchange`, writes join the write log,
   `pipeline.respond` stamps the header, and `_finish` records it in the `TraceStore` (telemetry
   excepted) and reports it to the CLI.
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
| 3 | `echo` | The body a locally answered write gets. L0: form and JSON reflection, minted ids, Slack's envelope. L1: the route's fixture with the request's own fields written over it. |
| 4 | `policy` | `AnswerPolicy` and `ShadowPolicy`: the decision, and only the decision. |
| 4 | `overlay` | The `Overlay` seam. `NoOverlay` today; Phase 2 replaces it. The module's header lists the two hazards that overlay must respect. |
| 4 | `store` | The `TraceStore` seam. `NullStore` today; Phase 3 replaces it. |
| 5 | `engine` | The `Engine` protocol and `EngineConfig`. `engine/mitm.py` is the only mitmproxy-backed implementation and the only module that imports mitmproxy. |
| 6 | `report` | Everything a run prints: banner, per-exchange line, exit summary. Pure text. |
| 6 | `runner` | Process plumbing for `irimi shadow`: the child's environment and the engine thread. |
| 7 | `cli` | argparse and the composition root. `_build_engine` is the one place the concrete engine, policy, store and overlay meet. |

`src/irimi/maps/*.yaml` are the shipped service maps. They are data, contributable without
touching Python, and `tests/test_servicemap.py` pins their contents.
`src/irimi/fixtures/*.json` are the vendored response objects those maps' `fixture:` keys name;
each file says in its `_source` entry where it came from, and the same test proves both
directories are inside a built wheel.

## The seams

Four protocols are where later phases plug in, and the reason the rest of the code can be tested
without a proxy:

- **`engine.Engine`** - run, shutdown, wait for the listener. `runner.start_engine` drives it on a
  background thread; `cli` never touches mitmproxy directly.
- **`policy.AnswerPolicy`** - given a `Request` and its `Classification`, return an `Answer`:
  forward live, delegate, or send this response. `ShadowPolicy` is the only one today; record and
  replay modes are new policies, not new branches.
- **`overlay.Overlay`** - given the write log, a read request and the upstream response, return
  the response the agent should see. Phase 2.
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
