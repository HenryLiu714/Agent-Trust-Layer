import argparse
import contextlib
import secrets
import sys
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from irimi import __version__, paths

if TYPE_CHECKING:  # the quoted annotations below; no runtime import
    import subprocess

    from irimi.ca import CAPaths
    from irimi.control import OnToolCall
    from irimi.engine import Engine, EngineConfig, OnExchange
    from irimi.exchange import Exchange
    from irimi.servicemap import MapIndex
    from irimi.store import ServeLock, TraceStore
    from irimi.trace import ToolCall

SIGINT_EXIT_CODE = 130  # 128 + SIGINT, the shell convention for a Ctrl-C'd command
DEFAULT_RUNS_LIMIT = 20  # `irimi runs list` without --limit (#72)

# Since #76 a non-HTTP write the agent labels with `@sdk.tool` runs only its stand-in under irimi,
# so the notice says the rest happen for real, not that all of them do.
NON_HTTP_NOTICE = (
    "Note: irimi only sees HTTP(S). Side effects that are not HTTP "
    "(database writes, files, gRPC, WebSockets) are not virtualized and happen for real, "
    "unless the agent labels the function that makes them with @sdk.tool."
)


def _host_arg(value: str) -> str:
    """argparse type for --allow-host: a bare host name, lower-cased. The port belongs on the
    request path (/127.0.0.1:8443/...), so a value with a scheme, port or path is a mistake that
    would otherwise never match anything."""
    host = value.strip().lower()
    if not host or "/" in host or ":" in host:
        raise argparse.ArgumentTypeError(
            f"{value!r}: give a bare host name with no scheme, port or path "
            "(the port goes on the request path, e.g. /127.0.0.1:8443/...)"
        )
    return host


def _target_arg(value: str) -> tuple[str, str, str]:
    """argparse type for --target '<host>[<path>]=<url>'.

    `api.stripe.com=http://127.0.0.1:3000` sets the service target;
    `api.stripe.com/v1/refunds=http://127.0.0.1:3000/refund` sets one route's. The URL is
    validated by the map loader, which owns every target rule.

    A trailing bare slash is the service too: `api.stripe.com/=<url>` is how a reader used to
    `proxy_pass` spells "the whole service", and it used to be taken as the route path `/`, which
    no map claims - so a spelling that means the right thing failed with "no route matches '/'"
    (#32).
    """
    spec, sep, url = value.partition("=")
    host, slash, path = spec.strip().partition("/")
    host = host.lower()
    if not sep or not url.strip() or not host or ":" in host:
        raise argparse.ArgumentTypeError(
            f"{value!r}: use --target '<host>[<path>]=<url>', e.g. "
            "--target 'api.stripe.com/v1/refunds=http://127.0.0.1:3000/refund' "
            "(a bare host sets the whole service's target)"
        )
    return host, f"{slash}{path}" if slash and path else "", url.strip()


def _add_engine_args(parser: argparse.ArgumentParser) -> None:
    """The options `serve` and `shadow` share."""
    parser.add_argument(
        "--port",
        type=int,
        default=paths.DEFAULT_PORT,
        help=f"port to listen on (default {paths.DEFAULT_PORT})",
    )
    parser.add_argument(
        "--listen-host",
        default=paths.LISTEN_HOST,
        metavar="HOST",
        help=f"address to listen on (default {paths.LISTEN_HOST}). Anything but loopback lets "
        "every machine that reaches the port send traffic through irimi and call its control "
        "endpoint; the reverse door stays loopback-only",
    )
    parser.add_argument(
        "--allow-host",
        action="append",
        default=[],
        type=_host_arg,
        metavar="HOST",
        help=f"also let the reverse door http://{paths.LISTEN_HOST}:<port>/<host>/<path> relay "
        "to HOST, a bare host name (repeatable; every host in a loaded map is already allowed)",
    )
    parser.add_argument(
        "--target",
        action="append",
        default=[],
        type=_target_arg,
        metavar="SPEC",
        dest="target",
        help="answer a route from an address you control instead of faking it: "
        "'<host>[<path>]=<url>', e.g. 'api.stripe.com/v1/refunds=http://127.0.0.1:3000/refund'. "
        "A bare host sets the whole service's target (repeatable; beats the overrides file)",
    )
    parser.add_argument(
        "--target-reads",
        action="append",
        default=[],
        type=_host_arg,
        metavar="HOST",
        help="also send this service's reads to its target, making it a delegated service - "
        "its reads are then answered by the target, not by the real service (repeatable)",
    )
    parser.add_argument(
        "--allow-target-host",
        action="append",
        default=[],
        type=_host_arg,
        metavar="HOST",
        help="let an answer target name HOST instead of loopback. This sends the agent's "
        "requests off this machine; targets are loopback-only without it (repeatable)",
    )
    _add_store_arg(parser, "where to record runs, redacted")


def _add_store_arg(parser: argparse.ArgumentParser, what: str = "the trace store to read") -> None:
    parser.add_argument(
        "--store",
        type=Path,
        default=None,
        metavar="DIR",
        help=f"{what} (default: $IRIMI_HOME/store)",
    )


def _positive_int(value: str) -> int:
    """argparse type for --limit: a whole number of runs, at least one."""
    try:
        number = int(value)
    except ValueError:
        number = 0
    if number < 1:
        raise argparse.ArgumentTypeError(f"{value!r}: give a whole number of runs, at least 1")
    return number


def _reverse_hosts(args: argparse.Namespace, index: "MapIndex") -> frozenset[str]:
    """Every host in a loaded service map, plus every --allow-host, lower-cased and stripped."""
    extra = frozenset(h.strip().lower() for h in args.allow_host if h.strip())
    return index.hosts | extra


def _engine_config(
    args: argparse.Namespace,
    index: "MapIndex",
    run_id: str,
    ca_paths: "CAPaths",
    *,
    serve: bool = False,
) -> "EngineConfig":
    """The EngineConfig `serve` and `shadow` share.

    `maps=index` is what makes the classifier use the maps: without it the engine falls back to the
    verb rule and every mapped write on a POST-only service reads as `unknown`.
    """
    from irimi.engine import EngineConfig

    return EngineConfig(
        run_id=run_id,
        ca=ca_paths,
        confdir=paths.mitm_dir(),
        listen_host=args.listen_host,
        listen_port=args.port,
        reverse_hosts=_reverse_hosts(args, index),
        maps=index,
        serve=serve,
    )


def _load_maps(args: argparse.Namespace | None = None) -> "MapIndex | None":
    """The loaded service maps, or None after printing why they were refused.

    Fail closed: a map, overrides file or `--target` the loader rejects must not start a proxy
    that would then classify and answer with half a policy. `args` is None for `maps list`, which
    has no engine flags of its own.
    """
    from irimi import delegation, servicemap

    try:
        index = servicemap.load(
            allow_target_hosts=frozenset(getattr(args, "allow_target_host", ()) or ()),
            targets=getattr(args, "target", ()) or (),
            target_reads=getattr(args, "target_reads", ()) or (),
        )
        # The loader keeps targets on loopback, where the only thing separating a stub from our
        # own listener is the port - and dialling ourselves is a loop, not a delegation. The
        # addon repeats this per request against the port it actually bound, which is the
        # authoritative check; this one is here so `--port 4000` with a target on 4000 fails
        # before anything starts rather than on the first request. A `--listen-host` address is
        # the listener too (#77).
        port = getattr(args, "port", 0) or 0
        listen_host = getattr(args, "listen_host", "") or ""
        if port:
            for sm in index.services:
                for target in (sm.target, *(r.target for r in sm.routes)):
                    if target != servicemap.SELF_TARGET:
                        delegation.refuse_self_target(target, port, listen_host)
        return index
    except (servicemap.MapError, delegation.TargetRefused) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return None


def _escape_hatch_warning(args: argparse.Namespace) -> str | None:
    """The one line `--allow-target-host` has to print, or None when it was not used.

    Targets are loopback-only so that a delegated request cannot leave the machine; this flag is
    the deliberate way out of that, so it says so out loud. The delegated-service banner and the
    summary's own accounting are issue #20, not this one.
    """
    hosts = getattr(args, "allow_target_host", None)
    if not hosts:
        return None
    return (
        f"WARNING: --allow-target-host {', '.join(sorted(hosts))} - an answer target may now "
        "leave this machine. Requests to a targeted route go to a host you named, which is "
        "neither the real service nor loopback."
    )


def print_startup(
    command: str,
    args: argparse.Namespace,
    index: "MapIndex",
    run_id: str | None,
    host: str,
    port: int,
    ca_cert: "Path",
) -> None:
    """Everything a run says before it starts: the escape-hatch warning, the banner, and one line
    per delegated service. `command` is "shadow" or "shadow --serve", whose `run_id` is None.

    Every line is flushed. Python block-buffers stdout when it is a pipe rather than a terminal,
    and `serve` is a foreground process that may then print nothing for minutes, so
    `irimi serve > log`, `irimi serve | tee` and any supervisor capturing the process showed none
    of this until an exchange came along and flushed it - and a `serve` nobody talks to showed
    nothing at all. What was withheld is the port it bound, the CA path a client needs,
    `backstop: none`, and the red `NOT loopback` line saying the agent's requests are leaving
    this machine. `shadow` flushed already; `serve` did not, and the two are one function now so
    they cannot drift again.
    """
    from irimi import report  # deferred like every other heavy import in this module

    warning = _escape_hatch_warning(args)
    if warning is not None:
        print(warning, file=sys.stderr, flush=True)
    for line in report.banner_lines(command, run_id, host, port, ca_cert):
        print(line, flush=True)
    for line in report.delegated_lines(index, color=sys.stdout.isatty()):
        print(line, flush=True)


def cmd_maps_list(args: argparse.Namespace) -> int:
    from irimi import servicemap

    index = _load_maps()
    if index is None:
        return 1
    # Exact hosts first, then the wildcard patterns. A pattern is printed because a map whose
    # hosts are all wildcards would otherwise not appear here at all; it is still not a
    # reverse-door allow-list entry (see MapIndex.hosts).
    hosts = sorted(index.hosts)
    patterns = index.patterns
    rows = [(host, index.by_host[host]) for host in hosts]
    rows += [(pattern, index.by_suffix[pattern[1:]]) for pattern in patterns]
    width = max((len(name) for name, _ in rows), default=0)
    service_width = max((len(sm.service) for _, sm in rows), default=0)
    routes = sum(len(sm.routes) for sm in index.services)
    # Hosts and patterns are counted separately: only the exact hosts are the reverse door's
    # allow-list, and one number covering both told an operator a pattern host was reachable
    # through the door when it 403s (#29).
    print(
        f"irimi maps · {len(index.services)} service(s) · {len(hosts)} host(s) · "
        f"{len(patterns)} pattern(s) · {routes} route(s)"
    )
    for name, sm in rows:
        reads = " + reads" if sm.target_reads else ""
        print(
            f"  {name:<{width}}  {sm.service:<{service_width}}  {len(sm.routes):>3} routes  "
            f"target: {sm.target}{reads}"
        )
        # The service target alone printed `target: self` for a service whose *routes* carry
        # targets, so a partly delegated service looked untouched in the listing and contradicted
        # the banner printed two seconds later (#20). Route targets get a line of their own under
        # the host, because the column above is the service's answer and this is the route's.
        for route in sm.routes:
            if route.target != servicemap.SELF_TARGET:
                print(f"  {'':<{width}}  route {route.method} {route.path} → {route.target}")
    override = servicemap.override_path()
    if override is not None:
        print(f"  overrides from {override}")
    return 0


def cmd_init(args: argparse.Namespace) -> int:
    from irimi import ca  # deferred: keeps --version and --help free of the cryptography import

    p = ca.ca_paths()
    if ca.ca_exists(p) and not args.force:
        print(f"CA already exists at {p.key.parent} (use --force to regenerate).")
        print(NON_HTTP_NOTICE)
        return 0
    if ca.ca_partial(p) and not args.force:
        print(
            f"error: CA directory {p.key.parent} is incomplete "
            f"(expected both {p.key.name} and {p.cert.name}). Run `irimi init --force`.",
            file=sys.stderr,
        )
        return 1
    try:
        ca.generate_ca(p)
    except OSError as exc:
        print(f"error: could not write CA to {p.key.parent}: {exc}", file=sys.stderr)
        return 1
    print("Generated CA:")
    print(f"  key:  {p.key}")
    print(f"  cert: {p.cert}")
    print(NON_HTTP_NOTICE)
    return 0


@dataclass(frozen=True)
class _Run:
    """Everything `serve` and `shadow` need before an engine exists."""

    index: "MapIndex"
    ca: "CAPaths"
    run_id: str
    config: "EngineConfig"
    store: "TraceStore"
    # The store's `serve.lock`, held, in serve mode only (#77).
    lock: "ServeLock | None" = None


# What serve mode calls when a run's end is on disk: the maps the run was decided with, and its id.
_OnRunEnded = Callable[["MapIndex", str], None]


def _prepare_run(
    args: argparse.Namespace, *, serve: bool = False, on_run_ended: _OnRunEnded | None = None
) -> _Run | None:
    """Load the maps, find the CA and open the trace store, or print why not and return None.

    Fail closed: without all three, no engine starts and no child is spawned. The store is opened
    last, so a run refused for its maps or its CA leaves nothing on disk.

    In serve mode (#77) the engine's own run is `trace.UNATTRIBUTED`, so a request no `Irimi-Run`
    names is stored there and builds no run, and the store's `serve.lock` is taken as soon as the
    store is open: a second serve on one root is refused before its engine starts, and writes
    nothing.
    """
    from irimi import ca, trace

    index = _load_maps(args)
    if index is None:
        return None
    p = ca.ca_paths()
    if not ca.ca_exists(p):
        print(f"error: no CA at {p.key.parent}. Run `irimi init` first.", file=sys.stderr)
        return None
    ended = None if on_run_ended is None else (lambda run_id: on_run_ended(index, run_id))
    store = _open_store(args, ended)
    if store is None:
        return None
    lock = _hold_serve_lock(args) if serve else None
    if serve and lock is None:
        _close_store(store)
        return None
    # 16 hex characters: the run id is a directory name in the store, and must not collide with
    # another run's across months of them (#70).
    run_id = trace.UNATTRIBUTED if serve else secrets.token_hex(8)
    config = _engine_config(args, index, run_id, p, serve=serve)
    return _Run(index, p, run_id, config, store, lock)


def _open_store(
    args: argparse.Namespace, on_run_ended: Callable[[str], None] | None = None
) -> "TraceStore | None":
    """The trace store under `--store`, default `$IRIMI_HOME/store`, or None after printing why
    this install's redaction key or the store's directory cannot be used (#69, #70). A run that
    could not redact what it stores, or would drop every event unseen, must not start."""
    from irimi import redact
    from irimi.store import DirectoryStore

    try:
        key = redact.load_key(paths.irimi_home())
        return DirectoryStore(_store_root(args), key, on_run_ended)
    except (redact.RedactKeyError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return None


def _hold_serve_lock(args: argparse.Namespace) -> "ServeLock | None":
    """The store's `serve.lock`, held, or None after printing why not: another serve writes this
    store, or the lock file cannot be opened (#77)."""
    from irimi.store import ServeLock

    try:
        return ServeLock.hold(_store_root(args))
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return None


def _build_engine(
    run: _Run, on_exchange: "OnExchange", on_tool_call: "OnToolCall | None" = None
) -> "Engine":
    """The composition root: the one place the concrete engine, policy, store and overlay meet.

    Phase 2 swapped `NoOverlay` for the real overlay here in #43, and Phase 3 swapped `NullStore`
    for the directory store `_prepare_run` opens (#70).
    """
    from irimi.engine.mitm import MitmEngine
    from irimi.overlay import ServiceOverlay
    from irimi.policy import ShadowPolicy
    from irimi.reader import UpstreamReader

    # The overlay is built with the same maps the engine classifies against, because it has to
    # ask the same question of a read - which service, which operation - and there is one place
    # that answers it (#43).
    return MitmEngine(
        run.config,
        # The reader is the real upstream, chosen here and nowhere else: Phase 5's replay policy
        # gets a reader over the recording at this same line (#45).
        ShadowPolicy(reader=UpstreamReader(), maps=run.config.maps),
        run.store,
        ServiceOverlay(run.config.maps),
        on_exchange=on_exchange,
        on_tool_call=on_tool_call,
    )


# The signals that stop a serve (#77): Ctrl-C, and what a container runtime or supervisor sends.
SERVE_STOP_SIGNALS = ("SIGINT", "SIGTERM")
# How often serve's main thread looks for a signal or a stop (#77). A handler only records the
# signal, and the main thread acts on it within this long.
SERVE_POLL_S = 0.1


def cmd_serve(args: argparse.Namespace, stop: threading.Event | None = None) -> int:
    """`irimi shadow --serve`, and `irimi serve`, its alias: the engine as a sidecar, with no child
    process, until SIGINT or SIGTERM, or until `stop` is set (#77).

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
    of all, before even serve's imports, so no signal finds serve without them.
    """
    with _stopping_on_signals() as signalled:
        return _serve(args, stop or threading.Event(), signalled)


class _ServeOutput:
    """Everything serve prints while it serves (#77): a line per exchange and per tool call as
    each happens, from the engine's thread, and each run's summary once its end is on disk, from
    the store's writer thread. One lock covers every line, so two runs' summaries never
    interleave, nor does an exchange's line split one."""

    def __init__(self, store_root: Path) -> None:
        self.store_root = store_root
        self._printing = threading.Lock()

    def say(self, lines: list[str], *, err: bool = False) -> None:
        with self._printing:
            for line in lines:
                print(line, file=sys.stderr if err else sys.stdout, flush=True)

    def exchange(self, ex: "Exchange") -> None:
        from irimi import report

        self.say([report.exchange_line(ex)])

    def tool_call(self, call: "ToolCall") -> None:
        """The tool's name and what it raised, never its args or result, which are stored
        redacted and would print raw here (#76)."""
        from irimi import report

        self.say([report.tool_call_line(call)])

    def run_ended(self, index: "MapIndex", run_id: str) -> None:
        """The run's summary, read back from the store as `irimi runs show` reads it, under
        `--- run <id> ---`. Called on the store's writer thread after it wrote the run's end, so
        the run is whole on disk. A run that cannot be read back is one warning naming the
        exception's type only, as the store logs its own: a message may quote what it read."""
        from irimi import report
        from irimi.store import RunNotFound, StoreReader
        from irimi.trace import TraceFormatError

        try:
            stored = StoreReader(self.store_root).load_run(run_id)
        except (TraceFormatError, OSError, RunNotFound) as exc:
            warning = (
                f"warning: run {run_id} ended but could not be read back: {type(exc).__name__}"
            )
            self.say([warning], err=True)
            return
        self.say([f"--- run {run_id} ---", *report.stored_summary_lines(stored, index)])


def _serve(args: argparse.Namespace, stop: threading.Event, signalled: list[int]) -> int:
    """`cmd_serve` inside its signal handlers: the engine's life, start to stop, with
    `_ServeOutput` saying what happened. `signalled` holds each signal that arrived."""
    from irimi import runner
    from irimi.engine import EngineStartError

    output = _ServeOutput(_store_root(args))
    died = threading.Event()
    run = _prepare_run(args, serve=True, on_run_ended=output.run_ended)
    if run is None:
        return 1
    # The store is closed and its lock released on every way out from here, so a signal never
    # cuts serve short half-way through either.
    try:
        engine = _build_engine(run, output.exchange, output.tool_call)
        if signalled:  # stopped before it served: no engine to start, no banner to print
            return 0
        try:
            handle = runner.start_engine(engine)
        except (EngineStartError, TimeoutError) as exc:
            # A timeout from the ready future carries no message of its own.
            reason = str(exc) or f"proxy did not start within {runner.READY_TIMEOUT_S:.0f}s"
            print(f"error: {reason}", file=sys.stderr)
            return 1
        try:
            if not signalled:  # a banner for a proxy already stopping would say it serves
                host, port = run.config.listen_host, handle.port()
                print_startup("shadow --serve", args, run.index, None, host, port, run.ca.cert)
            threading.Thread(
                target=_watch_engine, args=(handle.thread, stop, died), daemon=True
            ).start()
            while not signalled and not stop.wait(SERVE_POLL_S):
                pass
        finally:
            stop.set()  # before the engine stops, so `_watch_engine` sees serve stopping it
            handle.stop()
    finally:
        _close_store(run.store)
        if run.lock is not None:
            run.lock.release()
    if died.is_set():
        print("error: the proxy stopped on its own", file=sys.stderr)
        return 1
    return 0


def _watch_engine(thread: threading.Thread, stop: threading.Event, died: threading.Event) -> None:
    """Stop serve when the engine's thread ends, and say whether it ended first. An engine that
    stops on its own ends serve as a failure: a sidecar whose proxy is gone must not look alive
    to its supervisor (#77)."""
    thread.join()
    if not stop.is_set():
        died.set()
    stop.set()


@contextlib.contextmanager
def _stopping_on_signals() -> Iterator[list[int]]:
    """For the length of the block, SIGINT and SIGTERM append their number to the list it yields
    and do nothing else: no KeyboardInterrupt to unwind through the engine thread's join, and a
    second signal while the store closes is ignored rather than cutting its last writes short. A
    container runtime sends SIGTERM and waits, which is what serve exists for (#77).

    A handler takes no lock. It runs on the main thread between two bytecodes, which may be
    inside `Event.wait` holding the event's own lock, so a handler calling `Event.set` there
    waits on itself forever: the list is the whole of its work, and the main thread polls it.
    Off the main thread, where Python refuses a handler, nothing is installed.

    A signal ignored when serve started stays ignored: `irimi serve &` from a script inherits an
    ignored SIGINT, and a Ctrl-C meant for the script must not stop it. Once a signal has arrived,
    serve is stopping, so on the way out the signals are ignored rather than given back: a second
    one during the interpreter's exit would otherwise end it with the default action. With none,
    the handlers found are put back."""
    import signal

    signalled: list[int] = []
    if threading.current_thread() is not threading.main_thread():
        yield signalled
        return
    previous = {}
    for name in SERVE_STOP_SIGNALS:
        signum = getattr(signal, name)
        if signal.getsignal(signum) is signal.SIG_IGN:
            continue
        previous[signum] = signal.signal(signum, lambda got, _frame: signalled.append(got))
    try:
        yield signalled
    finally:
        for signum, handler in previous.items():
            # None: the handler was not installed from Python, which `signal` cannot put back.
            restored = signal.SIG_DFL if handler is None else handler
            signal.signal(signum, signal.SIG_IGN if signalled else restored)


def cmd_shadow(args: argparse.Namespace, stop: threading.Event | None = None) -> int:
    import os
    import subprocess
    import time

    from irimi import report, runner
    from irimi.engine import EngineStartError
    from irimi.exchange import Exchange
    from irimi.trace import ToolCall

    cmd = list(args.cmd)
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]
    if args.serve:
        # Serve mode launches nothing, so a command with it is a mistake in the invocation, and is
        # answered as argparse answers one: usage and exit 2 (#77). `stop` is serve's (#77).
        if cmd:
            args.parser.error("--serve runs no command: drop the `-- <command>`, or the --serve")
        return cmd_serve(args, stop)
    if not cmd:
        print("error: nothing to run. Usage: irimi shadow -- <command> [args...]", file=sys.stderr)
        return 1

    run = _prepare_run(args)
    if run is None:
        return 1

    exchanges: list[Exchange] = []
    tool_calls: list[ToolCall] = []
    elapsed = 0.0

    def on_exchange(ex: Exchange) -> None:
        exchanges.append(ex)
        print(report.exchange_line(ex), flush=True)

    def on_tool_call(call: ToolCall) -> None:
        # On the proxy's event loop, after the store took the call (#73): it only appends and
        # prints. The line names the tool and what it raised, never its args or result, which
        # are stored redacted and would be printed raw here.
        tool_calls.append(call)
        print(report.tool_call_line(call), flush=True)

    engine = _build_engine(run, on_exchange, on_tool_call)

    try:
        handle = runner.start_engine(engine)
    except (EngineStartError, TimeoutError) as exc:
        # A timeout from the ready future carries no message of its own.
        reason = str(exc) or f"proxy did not start within {runner.READY_TIMEOUT_S:.0f}s"
        print(f"error: {reason}", file=sys.stderr)
        _close_store(run.store)
        return 1  # fail closed: the child is never spawned without the proxy
    except KeyboardInterrupt:
        print("error: interrupted before the proxy was ready.", file=sys.stderr)
        _close_store(run.store)
        return SIGINT_EXIT_CODE

    started_run = False
    code = SIGINT_EXIT_CODE
    try:
        host, port = run.config.listen_host, handle.port()
        print_startup("shadow", args, run.index, run.run_id, host, port, run.ca.cert)
        env = runner.child_env(dict(os.environ), host, port, run.ca.cert, run.run_id)
        # The process run is stored before the child exists, so every exchange it makes lands in
        # a run that already has its record (#70).
        agent_version = os.environ.get(paths.AGENT_VERSION_ENV)
        run.store.start_run(runner.process_run(run.run_id, cmd, agent_version, time.time()))
        started_run = True
        try:
            proc = subprocess.Popen(cmd, env=env)
        except FileNotFoundError:
            print(f"error: command not found: {cmd[0]}", file=sys.stderr)
            code = 127
            return code
        except OSError as exc:
            print(f"error: could not run {cmd[0]}: {exc}", file=sys.stderr)
            code = 126
            return code
        # The summary's duration is the child's, not the proxy's: what the reader wants to know is
        # how long the agent ran. monotonic, so a clock change mid-run cannot make it negative.
        started = time.monotonic()
        code = runner.exit_code_for(_wait_for_child(proc))
        elapsed = time.monotonic() - started
    except KeyboardInterrupt:  # Ctrl-C outside _wait_for_child's own handling
        code = SIGINT_EXIT_CODE
    finally:
        # The run ends before the engine stops, because stopping the engine closes the store.
        # An exchange that finishes after this line is still recorded: a late event is not an
        # error (#70).
        if started_run:
            runner.end_process_run(run.store, run.run_id, code, time.time())
        # An impatient second Ctrl-C lands here, while the engine thread is being joined.
        try:
            handle.stop()
        except KeyboardInterrupt:
            pass
        _close_store(run.store)

    for line in report.summary_lines(run.run_id, exchanges, elapsed, run.index, tool_calls):
        print(line, flush=True)
    return code


def _store_root(args: argparse.Namespace) -> Path:
    return args.store or paths.store_dir()


def cmd_runs_list(args: argparse.Namespace) -> int:
    """`irimi runs list`: one line per stored run, newest first (#72)."""
    from irimi import report
    from irimi.store import RunNotFound, StoreReader
    from irimi.trace import TraceFormatError

    root = _store_root(args)
    reader = StoreReader(root)
    try:
        records = reader.list_runs()[: args.limit]
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if not records:
        print(f"no runs in {root}")
        return 0
    for record in records:
        # One damaged run is named and counted as `?`, never allowed to hide the others: the
        # store's own rule for `list_runs` (#70).
        # Counted, never read: no body is loaded, or a few runs of LLM streams cost hundreds of
        # MB to print two numbers each (#72).
        try:
            events = reader.load_run(record.run_id, bodies=False).events
        except (TraceFormatError, OSError, RunNotFound) as exc:
            print(f"warning: run {record.run_id} could not be read: {exc}", file=sys.stderr)
            events = None
        print(report.run_list_line(record, events))
    return 0


def cmd_runs_show(args: argparse.Namespace) -> int:
    """`irimi runs show RUN_ID`: a stored run's header, every event, and its summary (#72)."""
    from irimi import report
    from irimi.store import RunNotFound, StoreReader
    from irimi.trace import TraceFormatError

    root = _store_root(args)
    try:
        run = StoreReader(root).load_run(args.run_id)
    except RunNotFound:
        print(f"error: no run {args.run_id} in {root}", file=sys.stderr)
        return 1
    except (TraceFormatError, OSError) as exc:
        print(f"error: run {args.run_id} could not be read: {exc}", file=sys.stderr)
        return 1
    for line in report.run_show_lines(run, _maps_for_show()):
        print(line)
    return 0


def _maps_for_show() -> "MapIndex | None":
    """The maps `runs show` renders write lines with, loaded as `serve` and `shadow` load them.

    Only the `human:` templates are read from them, so maps that do not load cost the reader
    those sentences and nothing else: the writes print as the requests they were (#72). Unlike
    `serve` and `shadow`, which must not start with half a policy, this carries on."""
    from irimi import servicemap

    try:
        return servicemap.load()
    except servicemap.MapError:
        print("warning: maps did not load; writes are shown as requests", file=sys.stderr)
        return None


def _close_store(store: "TraceStore") -> None:
    """Close the store `_prepare_run` opened, on every path out of `serve` and `shadow` (#70).

    The engine closes it too, when it stops. This does not rely on that: an engine that never
    started, or whose thread outlived `handle.stop()`, has not, and a second close waits out the
    first. An impatient Ctrl-C here, as in `handle.stop()`, may cut the writer's last seconds
    short, and never costs the summary."""
    try:
        store.close()
    except KeyboardInterrupt:
        pass


def _wait_for_child(proc: "subprocess.Popen[bytes]") -> int:
    """Wait for the child, surviving Ctrl-C. Returns Popen's returncode (negative if signalled).

    Ctrl-C in a terminal reaches the whole foreground process group, so the child already got its
    own SIGINT; give it time to exit, then escalate.
    """
    import signal

    from irimi import runner

    try:
        return proc.wait()
    except KeyboardInterrupt:
        pass
    try:
        return proc.wait(timeout=runner.CHILD_GRACE_S)
    except KeyboardInterrupt:
        pass
    except Exception:
        pass
    proc.terminate()
    try:
        return proc.wait(timeout=runner.CHILD_GRACE_S)
    except Exception:
        pass
    proc.kill()
    try:
        return proc.wait(timeout=runner.CHILD_GRACE_S)
    except Exception:
        # SIGKILL did not reap it (uninterruptible I/O). Don't block the CLI forever.
        return -signal.SIGKILL


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="irimi",
        description="Shadow mode for AI agents: reads are real, writes are virtual.",
    )
    parser.add_argument("--version", action="version", version=f"irimi {__version__}")
    subparsers = parser.add_subparsers(dest="command", metavar="<command>")

    init = subparsers.add_parser("init", help="generate the local CA under $IRIMI_HOME/ca")
    init.add_argument(
        "--force", action="store_true", help="regenerate the CA even if one already exists"
    )
    init.set_defaults(func=cmd_init)

    # `irimi serve` is `irimi shadow --serve` under a shorter name: the same function, flags and
    # banner (#77).
    serve = subparsers.add_parser(
        "serve",
        help="the same as `irimi shadow --serve`: run the proxy with no command, until SIGINT or "
        "SIGTERM",
    )
    _add_engine_args(serve)
    serve.set_defaults(func=cmd_serve)

    shadow = subparsers.add_parser(
        "shadow", help="run a command with its HTTP(S) traffic in shadow mode"
    )
    _add_engine_args(shadow)
    shadow.add_argument(
        "--serve",
        action="store_true",
        help="run no command: serve until SIGINT or SIGTERM, as a sidecar to an agent pointed "
        "at irimi with HTTPS_PROXY, and print each run's summary as the SDK ends it",
    )
    shadow.add_argument(
        "cmd",
        nargs=argparse.REMAINDER,
        metavar="-- <command> [args...]",
        help="the command to run under the proxy",
    )
    shadow.set_defaults(func=cmd_shadow, parser=shadow)

    maps = subparsers.add_parser("maps", help="show the loaded service maps")
    maps_sub = maps.add_subparsers(dest="maps_command", metavar="<subcommand>", required=True)
    maps_list = maps_sub.add_parser(
        "list", help="print each mapped host with its service, route count and answer target"
    )
    maps_list.set_defaults(func=cmd_maps_list)

    runs = subparsers.add_parser("runs", help="read back the runs in the trace store")
    runs_sub = runs.add_subparsers(dest="runs_command", metavar="<subcommand>", required=True)
    runs_list = runs_sub.add_parser("list", help="print one line per stored run, newest first")
    _add_store_arg(runs_list)
    runs_list.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_RUNS_LIMIT,
        metavar="N",
        help=f"print at most N runs (default {DEFAULT_RUNS_LIMIT})",
    )
    runs_list.set_defaults(func=cmd_runs_list)
    runs_show = runs_sub.add_parser(
        "show", help="print a stored run's header, every event and its summary"
    )
    runs_show.add_argument("run_id", metavar="RUN_ID", help="the run's exact id, or unattributed")
    _add_store_arg(runs_show)
    runs_show.set_defaults(func=cmd_runs_show)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 1
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
