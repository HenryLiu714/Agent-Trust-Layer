# irimi

*Shadow mode for AI agents: reads are real, writes are virtual, and reads see the writes.*

## What it is

irimi will be a local proxy you put between an agent and the outside world. Reads pass through to
real services, writes are intercepted and answered with a realistic fake success, and reads after
writes are answered from irimi's own record so the agent's view stays consistent. The output is a
log of every write the agent would have made.

## Status

Early skeleton. Only `irimi init` works; the proxy is being built in the Phase 1 issues at
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
