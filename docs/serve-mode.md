# Serve mode

For teams running an agent that irimi does not launch: a server or queue worker in production,
usually in a container. `irimi shadow -- <cmd>` starts the agent and ends the run when it exits;
`irimi serve` runs the proxy alone, as a sidecar, until it is stopped. The agent marks its runs
with the SDK ([sdk.md](sdk.md)).

```
irimi serve --bind 0.0.0.0 --port 4000 --store /data/irimi
```

`serve` takes the proxy, map and storage options `irimi shadow` takes ([cli.md](cli.md)); its port
defaults to `4000`.

## What the agent needs

Set these in the agent's environment, naming irimi exactly as the agent reaches it
(`http://irimi:4000` in compose), because the SDK labels a request only on a connection to a
listener these name, as spelled:

| Variables | Value | Why |
| --- | --- | --- |
| `HTTPS_PROXY`, `HTTP_PROXY`, `https_proxy`, `http_proxy` | `http://irimi:4000` | every request goes through irimi; curl ignores upper-case `HTTP_PROXY` |
| `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE` | irimi's `ca.pem` | so the agent trusts irimi's certificates |
| `IRIMI_ENGINE_ACTIVE` | `1` | the SDK is inert without it, and write tools would run for real |
| `IRIMI_CONTROL` | `http://irimi:4000/_irimi` | where the SDK posts each run's start, tool calls and end |

- Leave `IRIMI_RUN` unset: a request outside any run is then `unattributed`.
- Copy `ca.pem` from irimi's `$IRIMI_HOME/ca/`, never the `mitm/` bundle, which holds the key.
- `NO_PROXY` should name only what must not go through irimi, such as the agent's own sidecars on
  loopback, never a service the agent writes to: what bypasses irimi is not shadowed.
- If you publish irimi's port, publish it unchanged (`4000:4000`). irimi knows a request is
  addressed to itself by the port it arrived on, so a remapped one would be forwarded back to
  irimi.

A sidecar in the same pod or network namespace reaches irimi on `127.0.0.1` and needs no wider
bind. It also keeps base-URL mode, which answers only loopback clients: from another container, an
SDK that ignores proxy settings (stripe-python) cannot be shadowed unless it is given
`HTTPS_PROXY` and the CA explicitly.

## Runs

- **Runs come from the SDK.** There is no process run. Each run is a `@sdk.trigger` call or an
  `sdk.run` block, and its requests are stored in it by their `Irimi-Run`. A request no run names,
  and a tool called in no run, go to `unattributed`: `irimi runs show unattributed` lists them.
- **An agent without the SDK**, or whose posts never reach irimi, has no runs, only
  `unattributed`. A failed post is a warning on the agent's own `irimi.sdk` logger, not in irimi's
  output; the run is then stored as a `header` run and lists as `incomplete`.
- **Each run has its own write log.** One run never sees another's faked writes, even when two runs
  refund the same charge at once, and a run's write log is dropped when the run ends.

## What it prints

The banner, then one line per exchange and tool call as each finishes (`--quiet` skips these), and
each run's summary under `--- run <id> ---` when the SDK ends the run: the same block
`irimi runs show <id>` prints, built from the store once the run's end is on disk. A streamed answer
is recorded when its stream closes, so a stream the agent leaves open after its run ends is stored
in the run but missing from the summary already printed. On stop, if any requests named no run,
irimi prints `stopped · N exchanges not in any run: irimi runs show unattributed`.

## Binding, stopping and the store

- **Binding.** irimi listens on `127.0.0.1` unless `--bind` names another address. A wider bind
  prints a warning: the control endpoint has no authentication, so bind wider only on a network
  that holds the agent and nothing else ([security.md](security.md#trust-boundaries)).
- **Stopping.** SIGINT and SIGTERM stop the proxy, write what is queued (for up to 10 seconds) and
  exit 0. A run whose end arrived before the signal still prints its summary; a run still open is
  left `incomplete`. A proxy that stops on its own exits 2.
- **One serve per store.** A serve holds `serve.lock` in its store while it runs, and a second
  serve on the same store exits 2 at once. A supervisor's restart succeeds once the old process
  has exited. `irimi runs rm` and `runs prune` are refused while a serve holds the lock.
- **What it holds in memory**: every run that has not ended, one whose end never reached irimi
  included, and each open run's write log.

Serve mode cannot replay: `irimi replay` starts its own proxy for one run
([replay.md](replay.md)).

## A worked example

[`examples/sidecar/`](../examples/sidecar/README.md) is a refund agent on `ThreadingHTTPServer`
beside `irimi serve`, each request one `@sdk.trigger` run with a faked refund and a write tool,
and a `compose.yaml` that wires the two containers as above. `tests/e2e/test_sidecar.py` runs two
concurrent requests against it on every test run and checks that they are stored as two runs that
never mix.
