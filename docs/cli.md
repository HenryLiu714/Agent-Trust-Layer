# The `irimi` command

For users running irimi and for CI jobs that key on its exit codes. Every command, every flag and
every exit code, with what each read-back command prints. `irimi <command> --help` says the same in
short. The terms are defined in [concepts.md](concepts.md).

```
irimi init [--force]                    create the local CA in ~/.irimi (or $IRIMI_HOME)
irimi shadow [options] -- COMMAND       run a command with its HTTP(S) writes faked, then summarize
irimi serve [options]                   run the proxy for agents you start yourself, until stopped
irimi replay RUN [options] [-- COMMAND] re-run a stored run against its recorded responses
irimi runs [list]                       list stored runs, newest first
irimi runs show [RUN]                   show one stored run (default: the newest)
irimi runs rm RUN...                    delete stored runs
irimi runs prune --older-than 30d       delete stored runs older than a duration
irimi compare RUN_A RUN_B               show the writes two stored runs made, side by side
irimi maps [list]                       show the service maps irimi loads
```

`irimi --version` prints the version. Abbreviated flags are refused: `--allow-t` is an error, not
`--allow-remote-target`.

## Naming a run

Wherever a command takes `RUN`, it accepts a full run id from `irimi runs list`, a prefix only one
run id starts with (an ambiguous prefix is refused, listing the matches), `last` for the newest
run, or `unattributed` for the events no run claimed. `irimi replay` also takes `--run=RUN`, for a
run id that starts with `-`.

## `irimi init`

Create the local certificate authority irimi signs HTTPS traffic with, in `$IRIMI_HOME/ca`
(`~/.irimi/ca` by default). Running it again changes nothing and prints `hint: CA already exists
at <dir>; use --force to regenerate` to stderr; `--force` makes a new CA. Only the
processes irimi starts are told to trust it ([security.md](security.md#trust-boundaries)).

## `irimi shadow`

```
irimi shadow [options] -- COMMAND [ARGS...]
```

Run `COMMAND` with its HTTP(S) traffic sent through irimi's proxy: reads go to the real service,
writes are faked. When it exits, irimi prints what it would have written and stores the run.
`COMMAND` is everything after the first `--`; a command that does not resolve exits 127 (or 126)
before anything starts. irimi exits with the command's own exit code. What the command is given is
in [how-it-works.md](how-it-works.md#what-the-child-gets); how to read the output is in
[reading-the-summary.md](reading-the-summary.md).

## `irimi serve`

```
irimi serve [options]
```

Run the proxy as a sidecar until SIGINT or SIGTERM, for agents you start yourself. Each run the SDK
names prints its summary as it ends. See [serve-mode.md](serve-mode.md).

## `irimi replay`

```
irimi replay RUN [options] [-- COMMAND [ARGS...]]
```

Re-run a stored run's entrypoint, or its recorded command, with every request answered from the
run's recording, and compare the writes it makes with the recorded ones
([replay.md](replay.md)).

| Option | Meaning |
| --- | --- |
| `--run RUN` | the run to replay, for a run id that starts with `-`: `--run=RUN` |
| `--entrypoint MODULE:FUNC` | call this function instead of the run's recorded entrypoint |
| `--python PATH` | the agent's Python interpreter (it needs irimi installed) |
| `--args JSON` | call it with these arguments, a JSON object of `{parameter: value}`, instead of the recorded ones |
| `--args-file PATH` | read `--args` from a file |
| `--result-file PATH` | write the result as JSON to `PATH` |

It also takes the proxy, map and storage options below. `-- COMMAND` cannot be combined with
`--entrypoint`, `--args` or `--python`.

## Proxy, map and storage options

`shadow`, `serve` and `replay` take all of these; `maps list` takes the map options, and the
read-back commands take `--store`.

| Option | Meaning |
| --- | --- |
| `--port PORT` | port to listen on. `shadow` and `replay` default to `0` (any free port; the child is told which), `serve` to `4000` |
| `--bind HOST` | address to bind (default `127.0.0.1`); anything else exposes the proxy to the network |
| `--base-url-host HOST` | also allow `HOST` in base-URL mode, `http://127.0.0.1:PORT/HOST/PATH`, for SDKs that ignore proxy settings; mapped hosts are always allowed (repeatable) |
| `--quiet` | print only the banner and the summary, not a line per exchange and tool call |
| `--target SPEC` | answer a route from a URL you control instead of faking it: `'<host>[<path>]=<url>'`; a bare host sets the whole service's target (repeatable; overrides `./irimi.maps.yaml`) |
| `--target-reads HOST` | send `HOST`'s reads to its `--target` too, instead of the real service (repeatable) |
| `--allow-remote-target HOST` | allow a `--target` on `HOST`, off this machine (repeatable) |
| `--store DIR` | where to record runs, redacted (default `$IRIMI_STORE`, else `$IRIMI_HOME/store`) |

Targets are explained in [service-maps.md](service-maps.md#targets). A map, overrides file or
`--target` the loader refuses stops the command before the proxy starts, naming the rule it broke;
so does a redaction key or a `--store` irimi cannot use.

## `irimi runs list`

```
irimi runs list [--limit N] [--all] [--store DIR]
```

One line per stored run, newest first, under a header row. `--limit` defaults to 20, and a list
it cut ends with `(N older runs; use --limit or --all)`.

```
run               started              took  status  attribution                 trigger                                   exchanges     writes
61d24b6e2d3d3322  2026-10-23 20:55:05  0.2s  ok      replay of 0d19bf2b53a14efb  tests.e2e.workflows.agents.w01_ticket_t…  7 exchanges   1 write
0d19bf2b53a14efb  2026-10-23 20:55:05  0.1s  ok      sdk                         tests.e2e.workflows.agents.w01_ticket_t…  10 exchanges  2 writes
68711f898b451681  2026-10-23 20:55:05  0.3s  ok      process                     python -m tests.e2e.workflows.agents.w0…  0 exchanges   0 writes
```

- `status` is `ok`, `error`, `error (exit N)` for a process run, or `incomplete` for a run that
  never ended.
- `attribution` is `process`, `sdk` or `header`. A replay's run says `replay of <run id>`, and any
  other run a replay stored puts its own attribution first: `header, replay of <run id>`.
- `trigger` is a process run's command line without the program's directory, or the trigger's
  name.
- `writes` counts what the summary lists as writes.

A run irimi cannot read is skipped with a warning naming the file.

## `irimi runs show`

```
irimi runs show [RUN] [--store DIR]
```

One stored run (the newest by default): its record, every event in the order it was stored, in
the live stream's columns, and its summary. A replay's run also prints `mode: replay` and
`replay of: <run id>`, and its summary opens `irimi replay · run`. `irimi runs show unattributed`
shows the events no run claimed, or says there are none.

The summary is built by the same code as the live one, from what was stored. It differs from what
the command printed live in these places:

- **The elapsed time** is the run's `ended_at - started_at`, or `0.0` when either is unset.
- **Redacted fields** print as their placeholders, so a `human:` field that held a secret shows
  `<redacted:…>`.
- **A body the store cut** (past 8 MiB): a faked write's sentence may print `?` for a field past
  the cut, and a write irimi refused is spelled as its request, with
  `refused by irimi: its body is too large to read whole`.
- **Dropped events** are missing; the record's `dropped events:` line says how many.
- **Labelled runs.** Live, a shadow summary covers every exchange the process made. Stored, each run
  holds only its own events: the process run holds only requests that named no run, and each SDK
  run holds its own. All of a process's runs together, the unattributed events included, make the
  block `irimi shadow` printed.

Every stored text field is printed with control characters escaped (`\n`, `\x1b`), so an agent's
error message cannot break a line or drive the terminal. A run that cannot be read is named with
the file, and the line of `events.jsonl`, it failed on; a run irimi may not read gives the
operating system's reason. `irimi runs show` exits 2 for a run that is not stored.

## `irimi runs rm` and `runs prune`

```
irimi runs rm RUN [RUN ...] [--store DIR]
irimi runs prune --older-than DURATION [--dry-run] [--store DIR]
```

`runs rm` deletes the named runs; if any is not stored, it deletes nothing and exits 2.
`runs prune` deletes every run that ended longer ago than `DURATION`, a whole number of hours or
days (`12h`, `30d`); a run that has not ended is aged by when its files were last written, so a run
an `irimi shadow` is still writing is never pruned. `--dry-run` lists what it would delete. Both
delete the stored bodies no remaining run uses, and both are refused while an `irimi serve` holds
the store's lock. Neither checks for an `irimi shadow` writing the run you name.

## `irimi compare`

```
irimi compare RUN_A RUN_B [--store DIR]
```

Show the writes two stored runs made, side by side, and exit as `diff(1)` does. How writes are
matched is in [replay.md](replay.md#compare).

## `irimi maps list`

```
irimi maps list [--target SPEC] [--target-reads HOST] [--allow-remote-target HOST]
```

Each mapped host with its service, route count and target (`self` means irimi answers). The map
options preview what a `--target` would change. `irimi maps` alone does the same.

## Environment

| Variable | Meaning |
| --- | --- |
| `IRIMI_HOME` | irimi's state directory: the CA, the redaction key, the default store and the overrides file. Default `~/.irimi` |
| `IRIMI_STORE` | the default `--store` |
| `IRIMI_AGENT_VERSION` | set in the agent's environment: the version recorded on each of its runs |

The variables irimi sets for its child are in
[how-it-works.md](how-it-works.md#what-the-child-gets).

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | success; for `compare` and `replay`, every write is the same |
| 1 | `compare` and `replay`: a write differs |
| 2 | irimi could not do it, or was invoked wrongly (a bare `irimi` too) |
| 126 | `shadow`: the command was found but could not be run |
| 127 | `shadow`: the command was not found |
| 130 | stopped by SIGINT (Ctrl-C) |
| 141 | whatever read irimi's output went away (`irimi runs list \| head -1`) |
| 143 | stopped by SIGTERM |

`irimi shadow` otherwise exits with its command's own code, and `irimi serve` with 0 when stopped
by a signal.
