# Architecture

For contributors changing irimi's code. It maps the packages, follows a request through them,
says which thread runs what, and names the interfaces a test or another mode substitutes. The terms
are defined in [concepts.md](concepts.md), and the rules every change must keep are in
[security.md](security.md#invariants); this page refers to them by name. Each module's own
docstring says what it holds and guarantees.

## A request through the code

mitmproxy calls `IrimiAddon` (`engine/mitm.py`) once per flow. The addon only converts flows into
irimi's `Request` and `Response` and carries plans onto them. What irimi decides and records is
`engine.session.ShadowSession`'s, which knows nothing of mitmproxy.

1. **`request` hook.** `wire.parse` normalises the request. `ingress.admit` says what it is to
   irimi: a `ControlCall` (a request to the listener itself under `/_irimi/`, answered by
   `control.ControlEndpoint` in the hook), an `Admitted` request with its run id read from
   `Irimi-Run` and irimi's headers removed (a base-URL request is rewritten to its upstream here),
   or a `Refusal`, answered by `ShadowSession.refuse` with irimi's own `400` or `403`.
2. **`ShadowSession.plan`**, on a worker thread (`asyncio.to_thread`), builds a `Plan`:
   `classify.classify` gives the kind; `targets.delegate` resolves the target once; a read the
   overlay can translate (a cursor naming a minted id) is translated; the `AnswerPolicy` returns a
   `Decision`: `Forward`, `Delegate` or `Respond`. The policy runs the precondition check
   (`decide/preconditions.py`), whose check read is carried on the plan. A `Respond` is stamped
   `Irimi-Answered-By` as it is planned, and a target is probed and, if unreachable, refused here.
3. **Back in the hook**, the addon carries the plan onto the flow: a `Respond` becomes the flow's
   response, a `Delegate` points the flow at its target with credential headers removed, and a
   `Forward` goes upstream. The whole hook runs under one guard: if anything raises, the flow gets
   irimi's `502` flagged `decision-failed` instead (the fail-closed hook rule).
4. **`responseheaders` hook.** Stamps a target's answer before its headers go out (a live answer
   carries none), and tees a `text/event-stream` answer through a `StreamCapture`, which passes each
   chunk on unchanged and keeps a copy for the trace.
5. **`response` hook.** `ShadowSession.complete` lets the service observe a live read
   (`services.observe_read`: Slack learns the newest `ts`), applies the run's faked writes to an
   unstreamed live read through the `Overlay`, builds the `Exchange`, and adds a faked write to its
   run's write log. `ShadowSession.record` hands the exchange to the `RunStore` and then to the
   CLI's callback.
6. **`error` hook.** `ShadowSession.fail` records a lost connection or unreachable target with no
   response, flagged, so a failed write is never missing from the trace.

Every body a hook records is decoded by `engine.decoding.decode_body`, which never raises and stops
at `limits.MAX_DECODED_BODY`, so a small compressed bomb cannot exhaust memory. Raw TCP relay is
off: bytes that are not HTTP fail as HTTP, and are never relayed.

## Modes

The command is the mode; no config file chooses it. `cli/compose.py` is the composition root:
`prepare_run` loads the maps, the CA and the redaction key, opens the store and builds the write log
and idempotency store, and `build_engine` wires a `MitmEngine` with its policy and control
endpoint.

| | `irimi shadow -- cmd` | `irimi serve` | `irimi replay RUN` |
| --- | --- | --- | --- |
| `EngineConfig.mode` | `Shadow()` | `Serve()` | `Replay(source_run_id)` |
| engine's own run | the process run, given to the child as `IRIMI_RUN` | `unattributed` | the replay's run |
| policy | `ShadowPolicy` | `ShadowPolicy` | `ReplayPolicy`, falling back to a `ShadowPolicy` that reads the recording |
| child | the command | none | the command, or `python -m irimi.replay.child` |
| prints | stream, then one summary | stream, then a summary per SDK run as it ends | stream, then the replay report |

When the store reports a run ended, the composition root drops that run's write log and
idempotency entries, then (under serve) prints its summary. A run is never visible to another.

## Threads

| Thread | Runs | May not |
| --- | --- | --- |
| main | the CLI: parsing, the banner, starting and waiting for the child, signals, the summary | touch a flow |
| engine | mitmproxy's asyncio loop: every hook, the control endpoint, `ShadowSession.record` | block: no network or disk I/O |
| workers | `ShadowSession.plan` via `asyncio.to_thread`: the policy, irimi's check read, target probes | touch a flow or the store directly; the plan is carried back to the loop |
| store writer | `DirectoryStore`'s queue: redaction, then every file write | raise into a caller |

`runner.start_engine` runs the engine on its own thread and waits for its listener. Exchanges are
recorded on the engine thread, check reads included, so the store's queue has one producer per
engine; the SDK's posts reach the store through the control endpoint, on the same thread.

## Layers

The code is packaged by concept, and the packages sit in layers. A module may import any module of
a lower-numbered layer, and a module listed before it in its own package, and nothing else.
`tests/safety/test_import_boundary.py` holds the code to this table, with each package's module
order, and holds this table to its own. It also holds that `mitmproxy` is imported in
`engine/mitm.py` and nowhere else. A package nested in another (`services/stripe/`,
`record/store/`) is one entry of its parent. A new module goes in both places.

| Layer | Packages | Modules, lowest first | Job |
| --- | --- | --- | --- |
| 0 | `core/` | `exchange`, `netaddr`, `secret_shapes`, `money`, `limits`, `opener`, `exitcodes`, `calls`, `bodies` | The vocabulary every package speaks: requests, responses and exchanges with their kinds, flags and write-set predicates; `netaddr`, the one loopback rule; credential shapes; size bounds; exit codes; the never-raising body parser. |
| 1 | `protocol/` | `headers`, `env`, `trace`, `entrypoints`, `control`, `replay_files` | What crosses a process boundary or reaches disk: irimi's headers, the child's environment, the trace format's records and codecs, `module:qualname` entrypoints, the control endpoint's routes and bodies, the replay child's files. The engine and the SDK both import it and never each other. |
| 2 | `config/`, `sdk/` | `paths`, `files`, `ca`; `sdk/`'s private modules | Side by side: `config` is irimi's state on this machine (paths, owner-only files, the CA); `sdk` is what an agent imports, its surface `irimi.sdk.__all__`, its imports held by `tests/sdk/test_surface.py`. |
| 3 | `servicemaps/` | `model`, `rules`, `loader`, `__init__`, `classify` | The service maps as code: dataclasses and matching, load-time rules (the scope rule), the loader, and classification. |
| 4 | `services/` | `model`, `generic`, `stripe/`, `slack/`, `__init__` | What irimi knows about each service: one `Service` spec each in `REGISTRY`, the generic echo and fixture answers, each service's answers, effects and checks. |
| 5 | `record/` | `redact`, `store/` | What a run leaves on disk: the redacted copy of each event, and the store that writes and reads it. |
| 6 | `decide/` | `targets`, `reader`, `writelog`, `idempotency`, `preconditions`, `overlay`, `policy` | How irimi answers: targets, irimi's check read, each run's write log, retries, the precondition check, the overlay and the policy. |
| 7 | `engine/` | `__init__`, `wire`, `ingress`, `decoding`, `capture`, `control`, `session`, `mitm` | The proxy: the `Engine` protocol, stamping, admission, bounded decoding, stream capture, the control endpoint, `ShadowSession`, and `MitmEngine`. |
| 8 | `report/` | `terminal`, `model`, `summary`, `runs`, `maps` | Everything a run prints, as pure functions: banner and stream lines, the summary model and its rendering, the stored-run listings, `irimi maps list`. |
| 9 | `replay/` | `child`, `index`, `policy`, `compare`, `plan`, `report` | Re-running a stored run and judging it: the child that calls a trigger, the recording a replay answers from, the replay policy, run comparison, the replay plan and its report. |
| 10 | `cli/` | `options`, `runner`, `compose`, `init`, `maps`, `runs`, `shadow`, `serve`, `replay_cmd` | The `irimi` command: shared options, process plumbing, the composition root, and one module per command. |
| 11 | `__main__`, `testing` | | The entry points: `python -m irimi`, and `irimi.testing`, which starts the proxy and so needs the full install. |

`src/irimi/maps/*.yaml` are the shipped service maps and `src/irimi/fixtures/*.json` the objects
their `fixture:` keys name. Both are data, and `tests/test_servicemap.py` pins that they are in a
built wheel. Where a service's knowledge lives is in [adding-a-service.md](adding-a-service.md).

`netaddr` is shared by every rule that asks "is this address this machine?": admission (is a
request addressed to the listener?), the target rules (loopback only, never irimi itself) and the
credential-path rule. Changing it changes all of them.

## Seams

The interfaces a test or another mode substitutes. Each is a `Protocol`.

| Seam | What it does | Implementations |
| --- | --- | --- |
| `engine.Engine` | run, shut down, wait for the listener, report its port | `MitmEngine`; the CLI never touches mitmproxy |
| `decide.policy.AnswerPolicy` | decide one request: `Forward`, `Delegate` or `Respond`; synchronous, called on a worker thread | `ShadowPolicy`, `replay.policy.ReplayPolicy` |
| `decide.overlay.Overlay` | apply a run's faked writes to a live read, and translate a read before it is forwarded; pure | `ServiceOverlay`; `NoOverlay` for tests |
| `decide.reader.Reader` | make irimi's check read | `UpstreamReader`, `NoReader` (no check), `replay.index.RecordingReader` |
| `record.store.RunStore` | take run starts, exchanges, tool calls and run ends; never raises or blocks | `DirectoryStore`; `NullStore` in `tests/support` |
| `sdk._control_client.Reporter` | post the SDK's runs and tool calls, and look up recorded tool calls | `ControlClient` |

Commands that start a proxy also take an `engine_factory`, so the tests and the sample workflow
harness build their own engine without patching the composition root.

## State on disk

```
$IRIMI_HOME/            default ~/.irimi
  ca/ca.key             the local CA's private key (0600)
  ca/ca.pem             the CA certificate the child is told to trust
  mitm/mitmproxy-ca.pem key and certificate, which mitmproxy mints leaf certificates from
  redact.key            the redaction HMAC key (0600), created on first use
  store/                the trace store, unless --store or $IRIMI_STORE names another
  maps.yaml             optional: the overrides file, when the working directory has none
./irimi.maps.yaml       optional: the overrides file for this directory
```

The store's own layout is in [trace-format.md](trace-format.md#layout).

## Store internals

`record/store/writer.py`'s `DirectoryStore` is the only writer.

- **Never in the way.** Its public methods only enqueue; one daemon thread (`irimi-store`) redacts
  each event with `redact.for_disk` and writes it. The queue is capped at 10,000 items and 256 MiB
  of bodies, and a body over 64 MiB is dropped before queueing. A full queue or a failed write
  drops the event, counts it on the store and on its run, and logs on the first drop and every
  thousandth.
- **Appends.** `events.jsonl` is appended one whole line at a time, each in one `write` on an
  `O_APPEND` descriptor, so lines from several processes never interleave. A line the disk refused
  half-way is truncated away again, without touching a line another process appended after it.
- **Syncs.** Nothing is synced per line. The blobs and event files a writer touched are synced after
  1000 writes, after a second idle, when a run ends (its `events.jsonl`) and on close. A `run.json`
  is written to a temporary file, synced and renamed into place, so a crash leaves the old record
  or the new one.
- **Crashes.** A final line a crash cut off is skipped on read. A store that resumes the run (a
  restarted `irimi serve`) truncates it before appending and continues `seq` after the last line it
  can read. A blob a crash tore is rewritten by the next event that needs it.
- **Sharing a root.** One process writes a given run; several may share a root, since run
  directories never collide and a blob is the same bytes whoever writes it. `unattributed/` is the
  one shared file, so each writer numbers its own lines there. A serve fills it, so a root has at
  most one: it holds an exclusive `flock` on `serve.lock`, which the kernel releases however the
  process exits. Every store also holds a shared `flock` on the root directory while it writes,
  and `irimi runs rm` and `runs prune` take both locks exclusively, so they never delete a run in
  progress.
- **Bounded memory.** An ended run's state is forgotten, so a long serve does not grow with the
  runs it has finished. `close` waits up to 10 seconds for the queue to drain.
- **Reading back.** `record/store/reader.py` lists runs and loads one run's events in `seq` order,
  with bodies or without (`runs list` reads none, but checks each blob exists at its size).
  `retention.py` deletes runs and the blobs no remaining run refers to.
