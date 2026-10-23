"""`irimi serve`: the engine as a sidecar, with no child process."""

import argparse
import sys
import threading
from pathlib import Path
from typing import Any

from irimi import exitcodes, runner
from irimi.cli import CliError, session
from irimi.cli.options import DEFAULT_PORT, add_proxy_options, epilog, help_formatted
from irimi.exchange import Exchange
from irimi.report import terminal
from irimi.report.model import RunView
from irimi.report.summary import summary_lines
from irimi.servicemap import MapIndex
from irimi.trace import UNATTRIBUTED, ToolCall

# How often serve's main thread looks for a signal or a stop. A handler only records the
# signal, and the main thread acts on it within this long.
SERVE_POLL_S = 0.1


def register(subparsers: "argparse._SubParsersAction[Any]") -> None:
    serve = subparsers.add_parser(
        "serve",
        **help_formatted(
            help="run the proxy for agents you start yourself, until stopped",
            description="Run irimi's proxy as a sidecar until SIGINT or SIGTERM, for agents you "
            "start yourself\nand point at it with HTTPS_PROXY. Each run the SDK names prints its "
            "summary as it ends;\nrequests no run names are stored as unattributed.",
            epilog=epilog(
                [
                    "irimi serve",
                    "irimi serve --bind 0.0.0.0 --port 4000 --store /data/irimi",
                ]
            ),
        ),
    )
    add_proxy_options(serve, DEFAULT_PORT)
    serve.set_defaults(func=cmd_serve)


def cmd_serve(
    args: argparse.Namespace,
    stop: threading.Event | None = None,
    *,
    engine_factory: session.EngineFactory = session.build_engine,
) -> int:
    """`irimi serve`: the engine as a sidecar, with no child process, until SIGINT or SIGTERM, or
    until `stop` is set.

    Runs are named by their requests: each SDK run's `Irimi-Run`, and `unattributed` for a request
    that names none. Each exchange and tool call prints its line as it happens, and each run its
    summary once its end is on disk, under a `--- run <id> ---` line, built from the store as
    `irimi runs show` builds it. Nothing is kept per exchange here.

    Both signals stop the engine and close the store, which writes what is queued for up to
    `store.CLOSE_TIMEOUT_S`, release the store's `serve.lock`, and exit 0. A run still open stays
    incomplete: no end is written for it. A run whose end was queued before the signal still
    prints its summary as the store drains; nothing else is printed.

    `stop` is the way out that needs no signal, for a caller that runs serve on a thread of its
    own: the sample-workflow harness, whose fake internet resolves names only in its own process.
    The signal handlers are installed only on the main thread, where Python allows them, and first
    of all, so no signal finds serve without them.
    """
    with runner.stopping_on_signals() as signalled:
        return _run_serve(args, stop or threading.Event(), signalled, engine_factory)


class _ServeOutput:
    """Everything serve prints while it serves: a line per exchange and per tool call as
    each happens, from the engine's thread, and each run's summary once its end is on disk, from
    the store's writer thread. One lock covers every line, so two runs' summaries never
    interleave, nor does an exchange's line split one."""

    def __init__(self, store_root: Path, quiet: bool = False) -> None:
        self.store_root = store_root
        self.quiet = quiet
        self._printing = threading.Lock()
        # Exchanges no run named, which no run's summary prints: the stop line points at them.
        self.unattributed = 0

    def say(self, lines: list[str], *, err: bool = False) -> None:
        with self._printing:
            for line in lines:
                print(line, file=sys.stderr if err else sys.stdout, flush=True)

    def exchange(self, ex: Exchange) -> None:
        if ex.run_id == UNATTRIBUTED:
            self.unattributed += 1
        if not self.quiet:
            self.say([terminal.exchange_line(ex)])

    def tool_call(self, call: ToolCall) -> None:
        """The tool's name and what it raised, never its args or result, which are stored
        redacted and would print raw here."""
        if not self.quiet:
            self.say([terminal.tool_call_line(call)])

    def run_ended(self, index: MapIndex, run_id: str) -> None:
        """The run's summary, read back from the store as `irimi runs show` reads it, under
        `--- run <id> ---`. Called on the store's writer thread after it wrote the run's end, so
        the run is whole on disk. A run that cannot be read back is one warning naming the
        exception's type only, as the store logs its own: a message may quote what it read."""
        try:
            stored = session.load_stored_run(self.store_root, run_id)
        except CliError as exc:
            why = type(exc.__cause__).__name__
            self.say([f"warning: run {run_id} ended but could not be read back: {why}"], err=True)
            return
        summary = summary_lines(RunView(stored.record, stored.events), index)
        self.say([f"--- run {run_id} ---", *summary])


def _run_serve(
    args: argparse.Namespace,
    stop: threading.Event,
    signalled: list[int],
    engine_factory: session.EngineFactory,
) -> int:
    """`cmd_serve` inside its signal handlers: the engine's life, start to stop, with
    `_ServeOutput` saying what happened. `signalled` holds each signal that arrived."""
    output = _ServeOutput(session.store_root(args), args.quiet)
    died = threading.Event()
    run = session.prepare_run(args, serve=True, on_run_ended=output.run_ended)
    if run is None:
        return exitcodes.ERROR
    # The store is closed and its lock released on every way out from here, so a signal never
    # cuts serve short half-way through either.
    with run:
        engine = engine_factory(run, output.exchange, output.tool_call)
        if signalled:  # stopped before it served: no engine to start, no banner to print
            return 0
        handle = session.start_engine(engine)
        if handle is None:
            return exitcodes.ERROR
        try:
            if not signalled:  # a banner for a proxy already stopping would say it serves
                host, port = run.config.listen_host, handle.port()
                session.print_startup("serve", args, run.index, None, host, port, run.ca.cert)
            threading.Thread(
                target=runner.watch_engine, args=(handle.thread, stop, died), daemon=True
            ).start()
            while not signalled and not stop.wait(SERVE_POLL_S):
                pass
        finally:
            stop.set()  # before the engine stops, so `watch_engine` sees serve stopping it
            handle.stop()
    if output.unattributed:
        count = output.unattributed
        exchanges = "1 exchange" if count == 1 else f"{count} exchanges"
        output.say([f"stopped · {exchanges} not in any run: irimi runs show unattributed"])
    if died.is_set():
        print("error: the proxy stopped on its own", file=sys.stderr)
        return exitcodes.ERROR
    return exitcodes.OK
