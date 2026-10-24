"""`irimi serve`: the engine as a sidecar, with no child process."""

import argparse
import sys
import threading
from pathlib import Path
from typing import Any

from irimi.cli import CliError, compose, runner
from irimi.cli.options import DEFAULT_PORT, add_proxy_options, epilog, help_formatted
from irimi.core import exitcodes
from irimi.core.exchange import Exchange
from irimi.protocol.trace import UNATTRIBUTED, ToolCall
from irimi.report import terminal
from irimi.report.model import RunView
from irimi.report.summary import summary_lines, tls_failure_lines
from irimi.servicemaps import MapIndex

# How often the main thread checks for a recorded signal or a stop.
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
    engine_factory: compose.EngineFactory = compose.build_engine,
) -> int:
    """Run `irimi serve` until SIGINT, SIGTERM, or `stop` is set (for callers on another thread).

    Either signal drains the store, releases `serve.lock` and exits 0; a run still open gets no
    end. Signal handlers go in first, so no signal finds serve without them.
    """
    with runner.stopping_on_signals() as signalled:
        return _run_serve(args, stop or threading.Event(), signalled, engine_factory)


class _ServeOutput:
    """What serve prints: a line per exchange and tool call, and each run's summary when it ends.

    Called from the engine and store writer threads; one lock keeps lines from interleaving.
    """

    def __init__(self, store_root: Path, quiet: bool = False) -> None:
        self.store_root = store_root
        self.quiet = quiet
        self._printing = threading.Lock()
        self.unattributed = 0  # counted for the stop line, since no summary shows them
        # No run's summary shows a failed handshake, so serve warns once per host itself.
        self.ca_cert: Path | None = None
        self._tls_warned: set[str] = set()

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
        """Print the tool's name and error, never its args or result, which would print raw."""
        if not self.quiet:
            self.say([terminal.tool_call_line(call)])

    def tls_failure(self, host: str) -> None:
        """Warn, once per host, that a client refused irimi's certificate for `host`."""
        if host in self._tls_warned:
            return
        self._tls_warned.add(host)
        [line] = tls_failure_lines({host: 1}, self.ca_cert)
        self.say([f"warning: {line.strip()}"], err=True)

    def run_ended(self, index: MapIndex, run_id: str) -> None:
        """Print the run's summary from the store, on the writer thread after its end is written.

        A read failure warns with the exception type only: its message may quote unredacted data.
        """
        try:
            stored = compose.load_stored_run(self.store_root, run_id)
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
    engine_factory: compose.EngineFactory,
) -> int:
    """Run the engine from start to stop; `signalled` holds each signal that arrived."""
    output = _ServeOutput(compose.store_root(args), args.quiet)
    died = threading.Event()
    run = compose.prepare_run(args, serve=True, on_run_ended=output.run_ended)
    if run is None:
        return exitcodes.ERROR
    with run:
        output.ca_cert = run.ca.cert
        engine = engine_factory(run, output.exchange, output.tool_call, output.tls_failure)
        if signalled:  # stopped before it served: no engine to start, no banner to print
            return exitcodes.OK
        handle = compose.start_engine(engine)
        if handle is None:
            return exitcodes.ERROR
        try:
            if not signalled:  # a banner for a proxy already stopping would say it serves
                host, port = run.config.listen_host, handle.port()
                compose.print_startup("serve", args, run.index, None, host, port, run.ca.cert)
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
