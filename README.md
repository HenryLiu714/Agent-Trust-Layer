# irimi

*Shadow mode for AI agents: reads are real, writes are virtual, and reads see the writes.*

## What it is

irimi will be a local proxy you put between an agent and the outside world. Reads pass through to
real services, writes are intercepted and answered with a realistic fake success, and reads after
writes are answered from irimi's own record so the agent's view stays consistent. The output is a
log of every write the agent would have made.

## Status

Early. `irimi init`, `irimi serve` and `irimi shadow` work; the rest is being built in the Phase 1
issues at
https://github.com/HenryLiu714/Agent-Trust-Layer/issues.

## Requirements

- macOS or Linux.
- [`uv`](https://docs.astral.sh/uv/) (the setup script installs it if missing).
- Python 3.14 is fetched by `uv` automatically; you do not need to install it. Installing as a
  standalone tool works on Python 3.12 or newer.

## Quickstart

```
git clone https://github.com/HenryLiu714/Agent-Trust-Layer.git
cd Agent-Trust-Layer
scripts/setup.sh
```

This creates the venv, installs irimi in editable mode, and runs `irimi init`. Then:

```
uv run irimi --help
```

## Try the proxy

`irimi serve` runs the shadow proxy in the foreground on `127.0.0.1:4000`. Reads are forwarded to
the real service; anything that is not a safe HTTP method is answered locally with a placeholder
`fake-L0` response and never reaches the network. Each exchange prints as one line.

    uv run irimi serve
    # in another terminal
    curl --proxy 127.0.0.1:4000 --cacert ~/.irimi/ca/ca.pem https://api.stripe.com/v1/charges

## Run an agent in shadow mode

`irimi shadow -- <command>` starts the proxy, runs your command with the proxy and CA environment
variables already set, and prints a summary when the command exits. The command's exit code is
passed through.

    uv run irimi shadow -- python agent.py

Every exchange prints as one line while it runs; at the end you get a count of what was forwarded
live and what was virtualized. The child is given `HTTP_PROXY`, `HTTPS_PROXY`,
`NO_PROXY=localhost,127.0.0.1`, `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE`,
`NODE_EXTRA_CA_CERTS`, `NODE_USE_ENV_PROXY=1`, plus `IRIMI_ENGINE_ACTIVE=1` and `IRIMI_RUN` naming
the run. That covers requests, httpx, urllib, curl and Node's fetch without any code change.

One process tree is one run. There is no `IRIMI_MODE` variable and no config file: the subcommand
is the only thing that chooses the mode.

**Nothing stops an agent from bypassing the proxy yet** — a client that ignores these variables, or
ships its own CA bundle, talks to the real service. The banner says `backstop: none (Phase 4)` for
exactly this reason.

Those CA variables *replace* the child's trust store rather than adding to it, so while the command
runs it trusts the irimi CA and nothing else. Traffic through the proxy is fine, but a TLS
connection that skips the proxy — anything on `localhost` or `127.0.0.1`, which `NO_PROXY` excludes
— will fail to verify. Point such a client at plain HTTP, or give it its own CA bundle.

## Installing as a standalone tool

If you do not want a venv, run one of these from the repo root:

```
pipx install .
uv tool install .
```

After that, `irimi` is on your `PATH`.

## What `irimi init` does

`irimi init` generates a self-signed CA into `$IRIMI_HOME/ca/` (default `~/.irimi/ca/`):

- `ca.key` with mode 0600
- `ca.pem` with mode 0644

Running it again is a no-op. `--force` regenerates the CA. `IRIMI_HOME` overrides the location;
see `.env.example`.

## What the proxy does not cover

irimi only sees HTTP(S). Side effects that are not HTTP, such as database writes, files on disk,
gRPC and WebSocket traffic, are not virtualized and happen for real. Point database URLs at scratch
data when running an agent in shadow mode.

## Contributing

```
uv sync                     # install everything, including dev tools
uv run pytest               # tests
uv run ruff check .         # lint
uv run ruff format .        # format
```

Branch per issue, named `<issue-number>-<short-slug>`. Open a PR against `main` with `Closes #<n>`
in the body. CI runs lint, format check and tests on macOS and Ubuntu.
