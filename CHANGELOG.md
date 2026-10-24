# Changelog

Every user-visible change to irimi. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and irimi uses
[semantic versioning](https://semver.org/): while it is 0.x, a minor release may break the CLI,
the SDK or the map schema. Each breaking change is marked **Breaking** with how to migrate.

## [Unreleased]

### Changed

- **Breaking:** CLI flags are renamed, with no aliases. `--listen-host` is `--bind`,
  `--allow-host` is `--base-url-host`, `--allow-target-host` is `--allow-remote-target`, and
  `replay --call` is `replay --entrypoint`. Rename them in scripts and compose files.
- **Breaking:** abbreviated flags are refused (0.1.0 took `--allow-t` as
  `--allow-target-host`). Spell every flag in full.
- **Breaking:** `irimi shadow --serve` is removed. Use `irimi serve`.
- **Breaking:** `irimi shadow` and `irimi replay` listen on any free port by default; the command
  finds it in `HTTPS_PROXY`. Pass `--port 4000` where a fixed port was assumed (base-URL mode).
  `irimi serve` still defaults to 4000.
- **Breaking:** one table of exit codes for every command: 0 ok, 1 a write differs (`compare`,
  `replay`), 2 irimi could not do it or was invoked wrongly, 126 and 127 a command that could not
  run or was not found, 130 and 143 stopped by SIGINT or SIGTERM, 141 the reader of the output went
  away. Failures that exited 1 (`init`, `serve`, `runs show` of a missing run) exit 2, and so
  does a bare `irimi`. Check scripts that test for 1.
- **Breaking:** `@sdk.tool(kind=..., shadow=...)` is replaced by `@sdk.read_tool` and
  `@sdk.write_tool(fake=...)`, with no alias. Write `@sdk.read_tool` for `kind="read"`, and
  `@sdk.write_tool(fake=f)` for `kind="write", shadow=f`.
- **Breaking:** `sdk.run(name, *, inputs=None)` takes its name as the first argument, and
  `with sdk.run(...) as run` yields a `Run(id, name)`, or `None` when the SDK is inactive or the
  block joins an enclosing run. Replace `sdk.run(trigger=..., name=...)` with `sdk.run(name)`.
- **Breaking:** `sdk.replay` and `sdk.ReplayResult` move to `irimi.testing`, as `replay`,
  `ReplayError` and `ReplayResult`. The signature is
  `replay(run_id, call=None, *, args=(), kwargs=None, store=None)`, so a `store` meant for your
  function is no longer taken by irimi. `ReplayResult.same` says whether the writes matched.
- **Breaking:** `sdk.active()` is `sdk.is_active()`, and `RecordedToolError.type` is
  `RecordedToolError.type_name`.
- **Breaking:** the SDK's public surface is the ten names in `irimi.sdk.__all__`. Its modules are
  private, so imports such as `irimi.sdk.capture.revive` no longer work.
- **Breaking:** a tool's default name is `module:qualname`, as a trigger's is (it was
  `module.function`).
- **Breaking:** service map keys are renamed, and the loader refuses the old names, naming the new
  one: `volatile:` is `compare_ignores:`, `verbs: honest | post-only` is `style: rest | rpc`, and
  `persists: false` with `comment:` is one `live_because: "<why>"`. This matters for an overrides
  file only if it used these keys; maps ship with irimi.
- **Breaking:** a `human:` template formats an amount as money only where it says so,
  `{amount:money}`.
- **Breaking:** the end-of-run summary is written in plain words. It no longer prints
  `unvalidated`, `L0` to `L3` or a `backstop` line; each write says what irimi did and knew, as
  `faked; checked against live state: would succeed`. The totals line reads
  `N exchanges · N real · N answered by irimi (N faked writes) · N sent to targets`, and irimi's
  own check reads are counted apart from the agent's reads. Update anything that parses the
  summary.
- **Breaking:** irimi's own errors (a request it could not parse or route, a refused base-URL
  request, a failed decision, an unreachable target) are JSON,
  `{"error": {"type": ..., "message": ...}}`, carry `Irimi-Answered-By: irimi`, and are recorded
  in the run. They were plain text, unstamped and unrecorded, or stamped `delegated` or `fake-L0`.
- **Breaking:** the trace format is schema 3. It adds the `irimi` answered-by value and the
  `refused` flag, and drops the `validation` field and the `fidelity:*` flags. irimi still reads
  runs that 0.1.0 stored.
- **Breaking:** a control-protocol client posting a tool call must send `awaited`. The SDK does;
  a client in another language must add it.
- **Breaking:** the CA `irimi init` generates is valid for 825 days, not ten years. A CA already
  generated keeps its dates; `irimi init --force` replaces it.
- In serve mode, each run has its own write log, dropped when the run ends: one run's reads and
  precondition checks no longer see another run's faked writes.
- `irimi compare`, and so `irimi replay`'s exit code, compares writes by what they sent, not by
  the line the summary prints, and prints the fields that differ under a changed pair.
- `irimi runs list` counts writes as the summary does, prints a header row and aligned columns,
  and says how many older runs it left out.
- The generic echo for a service with no Python support reflects the request's fields and minted
  ids only; Stripe's answers keep `created`, `object` and `livemode`.
- `Irimi-Rewrote` is never sent to a real service.
- An answer target is probed off the event loop, and an unreachable remote target gets irimi's
  JSON 502 instead of mitmproxy's HTML page.
- An entrypoint started as `python -m pkg.agent` is recorded as `pkg.agent:function`, so
  `irimi replay` can import it.
- CLI messages read `error: ...`, `warning: ...` and `hint: ...`, and log lines start `irimi: `.
- `mitmproxy` is pinned below 13, since the engine depends on its hook internals.

### Added

- `irimi runs rm RUN...` and `irimi runs prune --older-than DURATION` (with `--dry-run`).
- `--quiet` on `shadow`, `serve` and `replay` prints only the banner and the summary.
- `irimi runs list --all`; `runs show`, `compare`, `replay` and `runs rm` take a unique run-id
  prefix or `last`, and `runs show` shows the newest run by default.
- `irimi runs` and `irimi maps` run `list` when given no subcommand, and `maps list` takes the map
  options to preview a `--target`.
- `IRIMI_STORE` sets the default `--store`.
- `replay --run=RUN` for a run id that starts with `-`.
- The summary lists each SDK run an `irimi shadow` process made, with the command that replays it.
- A summary names connections whose TLS handshake failed because the client did not trust irimi's
  CA, and says so when a run saw no traffic at all.
- A clear error when the port is in use, and a check that the command exists before the banner.
- A warning when irimi's CA expires within 30 days.
- The control protocol is versioned: the SDK sends `Irimi-Protocol: 1`, and irimi answers `426` to
  another major version. The SDK stops posting for 30 seconds after irimi times out or is
  unreachable.
- Service maps may declare `credential_path_hosts:`, and `ids:` may mint `{format: int}` or
  `{format: uuid}`. The maps are checked against irimi's service code when they load.
- `py.typed`, so type checkers read the SDK's annotations.
- `examples/hello_agent.py` and three rewritten examples (`quickstart`, `replay`, `sidecar`), and
  `make demo`, which runs one against fake services with no keys.
- `SECURITY.md`, a code of conduct, and issue and pull request templates.

### Removed

- `python -m irimi.cli`; use `python -m irimi`.
- The `charge` and `balance_transaction` objects in the Stripe fixture, which no route used.

### Fixed

- `irimi runs show unattributed` on a store with no unattributed events says so, instead of
  printing an invented run.
- A tool's name is cut outside any secret it contains.
- The Stripe customer fixture has no active discount, and every fixture id is clearly synthetic.
- A write refused because its body is too large keeps its service and operation in the run.
- irimi's banner and summary start on a fresh line after a command whose output did not end in one.

## [0.1.0] - 2026-10-20

The first release.

### Added

- `irimi shadow -- CMD`: run a command with its HTTP(S) traffic in shadow mode, and print a
  summary of every write it would have made.
- `irimi serve`: the same proxy as a sidecar to an agent irimi does not launch, with a summary per
  run.
- `irimi init`: a local CA, trusted only by the processes irimi launches.
- Service maps for Stripe, Slack, OpenAI, Anthropic, LangSmith, Langfuse, Sentry, Datadog,
  Honeycomb and PostHog, and classification of every request as a read, a write, an LLM call,
  telemetry or unknown.
- Faked writes: an echo of the request, whole Stripe and Slack objects from vendored fixtures,
  later reads edited to show the run's writes, and precondition checks against the real service's
  state.
- Idempotency keys answered from the first answer, and the webhooks a write would have fired.
- Answer targets: an address you control that answers instead of the service.
- Base-URL mode, for SDKs that ignore proxy settings.
- A trace store with credentials redacted before anything reaches disk, and `irimi runs list` and
  `irimi runs show` to read it back.
- The SDK, `irimi.sdk`: triggers and runs, tools the proxy cannot see, and run labels on every
  request a run sends through irimi.
- `irimi compare`: the writes of two stored runs, side by side.
- `irimi replay`: a stored run's trigger re-run against its recording, and what changed.
- Eleven sample agent workflows run end to end on every test run.

[Unreleased]: https://github.com/HenryLiu714/Irimi/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/HenryLiu714/Irimi/releases/tag/v0.1.0
