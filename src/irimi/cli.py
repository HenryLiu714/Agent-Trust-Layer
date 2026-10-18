import argparse
import contextlib
import secrets
import sys
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, TextIO

from irimi import __version__, paths

if TYPE_CHECKING:  # the quoted annotations below; no runtime import
    import subprocess

    from irimi.ca import CAPaths
    from irimi.control import OnToolCall
    from irimi.engine import Engine, EngineConfig, OnExchange
    from irimi.exchange import Exchange
    from irimi.recording import Recording
    from irimi.runner import ChildCall
    from irimi.sdk.replay_child import CallOutcome
    from irimi.sdk.replays import ReplayResult
    from irimi.servicemap import MapIndex
    from irimi.store import ServeLock, StoredRun, TraceStore
    from irimi.trace import JSONValue, RunRecord, ToolCall

SIGINT_EXIT_CODE = 130  # 128 + SIGINT, the shell convention for a Ctrl-C'd command
SIGTERM_EXIT_CODE = 143  # 128 + SIGTERM: `irimi shadow` terminated before its command started
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


def _call_arg(value: str) -> str:
    """argparse type for --call: a function's `module:qualname`, as a trigger records it (#84)."""
    module, sep, qualname = value.partition(":")
    if not sep or not module.strip() or not qualname.strip():
        raise argparse.ArgumentTypeError(f"{value!r}: give the function as module:qualname")
    return value


def _args_arg(value: str) -> "dict[str, JSONValue]":
    """argparse type for --args: a JSON object, `{parameter: captured value}`, the shape a trigger
    records its args in (#74, #84)."""
    import json

    try:
        parsed = json.loads(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"--args is not JSON: {exc}") from None
    except RecursionError:
        # Which argparse lets through, a traceback where a usage error belongs (#84).
        raise argparse.ArgumentTypeError("--args is nested deeper than it can be read") from None
    if not isinstance(parsed, dict):
        raise argparse.ArgumentTypeError("--args is a JSON object of {parameter: value}")
    return parsed


def _args_file_arg(value: str) -> "dict[str, JSONValue]":
    """argparse type for --args-file: `--args`, read from the file `value` names (#84)."""
    try:
        text = Path(value).read_text(encoding="utf-8")
    except OSError as exc:
        raise argparse.ArgumentTypeError(f"--args-file {value}: {exc}") from None
    return _args_arg(text)


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
    replay_of: str | None = None,
) -> "EngineConfig":
    """The EngineConfig `serve`, `shadow` and `replay` share. `replay_of` is the stored run a
    replay answers from (#82, #84).

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
        replay_of=replay_of,
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
    replay_of: str | None = None,
) -> None:
    """Everything a run says before it starts: the escape-hatch warning, the banner, and one line
    per delegated service. `command` is "shadow", "replay" or "shadow --serve", whose `run_id` is
    None. `replay_of` is the run a replay answers from (#84).

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
    for line in report.banner_lines(command, run_id, host, port, ca_cert, replay_of):
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
    # The run `config.replay_of` names, read back to answer from, in a replay only (#82, #84).
    recording: "Recording | None" = None


# What serve mode calls when a run's end is on disk: the maps the run was decided with, and its id.
_OnRunEnded = Callable[["MapIndex", str], None]


def _prepare_run(
    args: argparse.Namespace,
    *,
    serve: bool = False,
    on_run_ended: _OnRunEnded | None = None,
    source: "StoredRun | None" = None,
) -> _Run | None:
    """Load the maps, find the CA, key the recording of `source`, the run a replay answers from
    (#82, #84), and open the trace store, or print why not and return None.

    Fail closed: without all of them, no engine starts and no child is spawned. The store is opened
    last, so a run refused for its maps, its CA or its key leaves nothing on disk.

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
    # 16 hex characters: the run id is a directory name in the store, and must not collide with
    # another run's across months of them (#70).
    # A replay is one run against one engine, whose write log and recording are the engine's;
    # serve mode is many runs at once, and `irimi serve` has no run to replay (#82, #84).
    assert not (serve and source is not None)
    run_id = trace.UNATTRIBUTED if serve else secrets.token_hex(8)
    replay_of = None if source is None else source.record.run_id
    config = _engine_config(args, index, run_id, p, serve=serve, replay_of=replay_of)
    key = _redaction_key()
    if key is None:
        return None
    recording = None
    if source is not None:
        from irimi.recording import Recording

        # Keyed with this install's redaction key, which made the recording's placeholders (#82).
        recording = Recording.from_events(source.events, key, config.maps)
    ended = None if on_run_ended is None else (lambda run_id: on_run_ended(index, run_id))
    store = _open_store(args, key, ended, config.replay_of)
    if store is None:
        return None
    lock = _hold_serve_lock(args) if serve else None
    if serve and lock is None:
        _close_store(store)
        return None
    return _Run(index, p, run_id, config, store, lock, recording)


def _redaction_key() -> bytes | None:
    """This install's redaction key (#69), loaded once for the store that redacts with it and the
    recording a replay matches against it, or None after printing why it cannot be used."""
    from irimi import redact

    try:
        return redact.load_key(paths.irimi_home())
    except (redact.RedactKeyError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return None


def _load_source(args: argparse.Namespace) -> "StoredRun":
    """The run `irimi replay` answers from, in the store under `--store`, read whole before
    anything starts, or `replay.Refused` saying why it cannot be (#82, #84)."""
    from irimi import trace
    from irimi.replay import Refused
    from irimi.store import RunNotFound, StoreReader
    from irimi.trace import TraceFormatError

    root = _store_root(args)
    if args.run_id == trace.UNATTRIBUTED:
        # Every serve session's requests no run named, in one pool: not one run to answer from.
        raise Refused(f"{trace.UNATTRIBUTED} is not a run to replay")
    try:
        return StoreReader(root).load_run(args.run_id)
    except RunNotFound:
        raise Refused(f"no run {args.run_id} in {root}") from None
    except (TraceFormatError, OSError) as exc:
        raise Refused(f"run {args.run_id} could not be read: {exc}") from None


def _open_store(
    args: argparse.Namespace,
    key: bytes,
    on_run_ended: Callable[[str], None] | None = None,
    replay_of: str | None = None,
) -> "TraceStore | None":
    """The trace store under `--store`, default `$IRIMI_HOME/store`, redacting with `key`, or None
    after printing why its directory cannot be used (#69, #70): a run that would drop every event
    unseen must not start. `replay_of` is the run a replay answers from, which every header run
    the store makes names (#82)."""
    from irimi.store import DirectoryStore

    try:
        return DirectoryStore(_store_root(args), key, on_run_ended, replay_of)
    except OSError as exc:
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
    from irimi.policy import AnswerPolicy, ReplayPolicy, ShadowPolicy
    from irimi.reader import UpstreamReader

    # The overlay is built with the same maps the engine classifies against, because it has to
    # ask the same question of a read - which service, which operation - and there is one place
    # that answers it (#43).
    overlay = ServiceOverlay(run.config.maps)
    policy: AnswerPolicy
    if run.recording is None:
        # The reader is the real upstream, chosen here and nowhere else (#45).
        policy = ShadowPolicy(reader=UpstreamReader(), maps=run.config.maps)
    else:
        # A replay's reader is the recording, which the policy builds itself: it never reads live
        # state for a precondition (#82).
        policy = ReplayPolicy(run.recording, maps=run.config.maps)
    return MitmEngine(
        run.config,
        policy,
        run.store,
        overlay,
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
    with _passing_sigterm_to_child() as child:
        return _shadow(args, cmd, child)


class _Child:
    """The command `irimi shadow` runs, as SIGTERM to irimi sees it (#77).

    A supervisor or a container runtime stops `irimi shadow` with SIGTERM, which reaches irimi
    alone, not its command. Its default action ended irimi at once, with the command still running
    and its proxy gone, the run never ended and the store never closed (#70's note). So SIGTERM is
    passed to the command instead, and the command's exit ends the run as any exit does. One that
    arrives before the command exists is held, and the command is then never started.

    The handler takes no lock, as serve's does not: `os.kill` and two attribute reads. A command
    already reaped has its `returncode` and is never signalled, so a recycled pid is never hit."""

    def __init__(self) -> None:
        self.terminated = False
        # A Ctrl-C reached irimi while the command ran (`_wait_for_child`), which a replay that it
        # stopped says (#84).
        self.interrupted = False
        self._proc: subprocess.Popen[bytes] | None = None

    def on_sigterm(self, signum: int, _frame: object) -> None:
        self.terminated = True
        self._pass_on(signum)

    def adopt(self, proc: "subprocess.Popen[bytes]") -> None:
        """`proc` is the command now. A SIGTERM that came while it was being started, when there
        was no command to pass it to, is passed to it here."""
        import signal

        self._proc = proc
        if self.terminated:
            self._pass_on(signal.SIGTERM)

    def _pass_on(self, signum: int) -> None:
        import os

        proc = self._proc
        if proc is not None and proc.returncode is None:
            with contextlib.suppress(OSError):
                os.kill(proc.pid, signum)


@contextlib.contextmanager
def _passing_sigterm_to_child() -> Iterator[_Child]:
    """For the length of the block, SIGTERM is `_Child.on_sigterm` (#77), on the main thread only,
    where Python allows a handler; a SIGTERM ignored when `irimi shadow` started stays ignored, as
    under serve. The handler found is put back on the way out. SIGINT is untouched: Ctrl-C reaches
    the whole foreground process group, the command included, and `_wait_for_child` handles it."""
    import signal

    child = _Child()
    if (
        threading.current_thread() is not threading.main_thread()
        or signal.getsignal(signal.SIGTERM) is signal.SIG_IGN
    ):
        yield child
        return
    previous = signal.signal(signal.SIGTERM, child.on_sigterm)
    try:
        yield child
    finally:
        # None: the handler was not installed from Python, which `signal` cannot put back.
        signal.signal(signal.SIGTERM, signal.SIG_DFL if previous is None else previous)


def _shadow(args: argparse.Namespace, cmd: list[str], child: _Child) -> int:
    """`irimi shadow -- <cmd>` inside its SIGTERM handler: the run, start to summary."""
    import os

    from irimi import report, runner

    run = _prepare_run(args)
    if run is None:
        return 1
    agent_version = os.environ.get(paths.AGENT_VERSION_ENV)
    record = runner.process_run(run.run_id, cmd, agent_version, 0.0)
    ran = _under_the_engine(args, run, child, _Launch("shadow", cmd, record))
    if not ran.summarize:
        return ran.code
    for line in report.summary_lines(
        run.run_id, ran.exchanges, ran.elapsed, run.index, ran.tool_calls
    ):
        print(line, flush=True)
    return ran.code


@dataclass(frozen=True)
class _Launch:
    """What a run starts under its engine (#84): the banner's command, `shadow` or `replay`; the
    child's argv; the run's record, stamped with its start as the child is spawned; and the
    trigger a replay's child calls (`runner.child_env`)."""

    banner: str
    argv: list[str]
    record: "RunRecord"
    call: "ChildCall | None" = None


@dataclass
class _Ran:
    """A child run under the engine: its exit code, how long it ran, and every exchange and tool
    call the engine reported meanwhile. `summarize` is False when the child never ran and irimi
    printed why, the code then irimi's own (1, 126, 127, 130, 143), and `stored` whether the run's
    record was stored first. `stopped` is the exit code of a SIGINT or SIGTERM that reached irimi
    while the child ran, and `call` how a replay's call ended, as its child wrote it (#84)."""

    code: int
    elapsed: float = 0.0
    exchanges: "list[Exchange]" = field(default_factory=list)
    tool_calls: "list[ToolCall]" = field(default_factory=list)
    summarize: bool = True
    stored: bool = False
    stopped: int | None = None
    call: "CallOutcome | None" = None


def _under_the_engine(args: argparse.Namespace, run: _Run, child: _Child, launch: _Launch) -> _Ran:
    """`launch` run as `run`'s child, under its engine, as `irimi shadow` and `irimi replay` run one
    (#84): the engine started, the banner, the run's record stored before the child exists, the
    child waited for, the run ended, the engine stopped and the store closed. A replay's run ends
    with what its call raised, as `replay_child` wrote it, and otherwise with the exit code."""
    import os
    import subprocess
    import time
    from dataclasses import replace

    from irimi import report, runner
    from irimi.engine import EngineStartError
    from irimi.sdk.replay_child import CallOutcome

    ran = _Ran(SIGINT_EXIT_CODE)

    def on_exchange(ex: "Exchange") -> None:
        ran.exchanges.append(ex)
        print(report.exchange_line(ex), flush=True)

    def on_tool_call(call: "ToolCall") -> None:
        # On the proxy's event loop, after the store took the call (#73): it only appends and
        # prints. The line names the tool and what it raised, never its args or result, which
        # are stored redacted and would be printed raw here.
        ran.tool_calls.append(call)
        print(report.tool_call_line(call), flush=True)

    engine = _build_engine(run, on_exchange, on_tool_call)

    try:
        handle = runner.start_engine(engine)
    except (EngineStartError, TimeoutError) as exc:
        # A timeout from the ready future carries no message of its own.
        reason = str(exc) or f"proxy did not start within {runner.READY_TIMEOUT_S:.0f}s"
        print(f"error: {reason}", file=sys.stderr)
        _close_store(run.store)
        return _Ran(1, summarize=False)  # fail closed: the child is never spawned without the proxy
    except KeyboardInterrupt:
        print("error: interrupted before the proxy was ready.", file=sys.stderr)
        _close_store(run.store)
        return _Ran(SIGINT_EXIT_CODE, summarize=False)

    cmd = launch.argv
    try:
        host, port = run.config.listen_host, handle.port()
        replay_of = run.config.replay_of
        print_startup(
            launch.banner, args, run.index, run.run_id, host, port, run.ca.cert, replay_of
        )
        env = runner.child_env(
            dict(os.environ),
            host,
            port,
            run.ca.cert,
            run.run_id,
            replaying=replay_of is not None,
            call=launch.call,
        )
        if child.terminated:  # SIGTERM while irimi started: the command is never run (#77)
            print("error: terminated before the command started.", file=sys.stderr)
            ran.code, ran.summarize = SIGTERM_EXIT_CODE, False
            return ran
        # The process run is stored before the child exists, so every exchange it makes lands in
        # a run that already has its record (#70).
        run.store.start_run(replace(launch.record, started_at=time.time()))
        ran.stored = True
        try:
            proc = subprocess.Popen(cmd, env=env)
            child.adopt(proc)
        except FileNotFoundError:
            print(f"error: command not found: {cmd[0]}", file=sys.stderr)
            ran.code, ran.summarize = 127, False
            return ran
        except OSError as exc:
            print(f"error: could not run {cmd[0]}: {exc}", file=sys.stderr)
            ran.code, ran.summarize = 126, False
            return ran
        # The summary's duration is the child's, not the proxy's: what the reader wants to know is
        # how long the agent ran. monotonic, so a clock change mid-run cannot make it negative.
        started = time.monotonic()
        ran.code = runner.exit_code_for(_wait_for_child(proc, child))
        ran.elapsed = time.monotonic() - started
        if child.terminated or child.interrupted:
            ran.stopped = SIGTERM_EXIT_CODE if child.terminated else SIGINT_EXIT_CODE
    except KeyboardInterrupt:  # Ctrl-C outside _wait_for_child's own handling
        ran.code = ran.stopped = SIGINT_EXIT_CODE
    finally:
        # The run ends before the engine stops, because stopping the engine closes the store.
        # An exchange that finishes after this line is still recorded: a late event is not an
        # error (#70).
        if ran.stored:
            if launch.call is not None:
                ran.call = CallOutcome.read(launch.call.outcome_file)
            error = ran.call.error if ran.call is not None else None
            runner.end_process_run(run.store, run.run_id, ran.code, time.time(), error)
        # An impatient second Ctrl-C lands here, while the engine thread is being joined.
        try:
            handle.stop()
        except KeyboardInterrupt:
            pass
        _close_store(run.store)
    return ran


def cmd_replay(args: argparse.Namespace) -> int:
    """`irimi replay RUN_ID`: a stored run's trigger run again through irimi, answered from the
    run's recording, and what the agent did differently (#84). What it runs is `replay.plan`'s
    to say, and what it prints `replay.Outcome`'s. Exits as `irimi compare` does: 0 when the
    replay made the writes its recording did, 1 when it did not, 2 for a replay that could not
    run, or whose run could not be read back, and 130 or 143 for one SIGINT or SIGTERM stopped.
    `--result-file` writes the result as JSON, the one `sdk.replay()` reads
    (`sdk.replays.ReplayResult`), whether or not the replay ran, holding what irimi said on
    stderr of a replay that did not."""
    args.cmd = _replay_command(args)
    if args.cmd and (args.call is not None or args.args is not None):
        args.parser.error(
            "-- CMD runs a command, not a call: drop --call and --args, or the command"
        )
    with _passing_sigterm_to_child() as child, _keeping_stderr() as said:
        try:
            result = _replay(args, child, said)
        except KeyboardInterrupt:
            # Ctrl-C while irimi read a run or prepared one, before any was stored.
            result = _interrupted(args, said, None)
    if args.result_file is not None:
        import json

        try:
            args.result_file.write_text(json.dumps(result.to_json()), encoding="utf-8")
        except OSError as exc:
            print(f"error: could not write {args.result_file}: {exc}", file=sys.stderr)
            return 2
    return result.exit_code


def _replay(args: argparse.Namespace, child: _Child, said: list[str]) -> "ReplayResult":
    """`cmd_replay` inside its SIGTERM handler: refused with what irimi said on stderr, or run,
    start to result."""
    import json
    import os
    import tempfile
    from dataclasses import replace

    from irimi import replay, runner

    try:
        source = _load_source(args)
        plan = replay.plan(source.record, list(args.cmd), args.call, args.args)
    except replay.Refused as exc:
        print(f"error: {exc}", file=sys.stderr)
        return replay.refused(args.run_id, said)
    if source.record.outcome is None:
        print(
            f"warning: run {args.run_id} did not end; it is replayed as far as it was recorded",
            file=sys.stderr,
        )
    # Made before the store is opened, so nothing after it can leave the store unclosed. Only
    # this user can enter it: the args are the run's, and the outcome the child's.
    with tempfile.TemporaryDirectory(prefix="irimi-replay-") as tmp:
        argv, call = list(plan.argv), None
        if plan.call is not None:
            call = runner.ChildCall(
                plan.call.entrypoint, Path(tmp) / "args.json", Path(tmp) / "outcome.json"
            )
            call.args_file.write_text(json.dumps(plan.call.args), encoding="utf-8")
            argv = [sys.executable, "-m", replay.CHILD_MODULE]
        run = _prepare_run(args, source=source)
        if run is None:
            return replay.refused(args.run_id, said)
        agent_version = os.environ.get(paths.AGENT_VERSION_ENV)
        record = runner.process_run(run.run_id, argv, agent_version, 0.0, plan.trigger)
        ran = _under_the_engine(args, run, child, _Launch("replay", argv, record, call))
    if not ran.summarize or not ran.stored:
        # The agent never ran: refused, or stopped by a signal before its run was stored, which
        # exits as `irimi shadow` would.
        result = replay.refused(args.run_id, said, run.run_id if ran.stored else None)
        if ran.code in (SIGINT_EXIT_CODE, SIGTERM_EXIT_CODE):
            return replace(result, exit_code=ran.code)
        return result
    try:
        return _replayed(args, source, run.run_id, ran, said)
    except KeyboardInterrupt:
        # Ctrl-C while irimi read the runs back or compared them: the replay's run is stored and
        # ended, and its result names it (#84).
        return _interrupted(args, said, run.run_id)


def _replayed(
    args: argparse.Namespace, source: "StoredRun", run_id: str, ran: _Ran, said: list[str]
) -> "ReplayResult":
    """A replay whose run `run_id` was stored and ended: both runs read back, and what the
    replay did printed and returned (`replay.outcome`)."""
    from irimi import replay
    from irimi.store import RunNotFound, StoreReader
    from irimi.trace import TraceFormatError

    try:
        replayed = StoreReader(_store_root(args)).load_run(run_id)
    except (RunNotFound, TraceFormatError, OSError) as exc:
        print(f"error: the replay's run {run_id} could not be read back: {exc}", file=sys.stderr)
        return replay.refused(args.run_id, said, run_id)
    ended = replay.Ended(ran.code, ran.call, ran.stopped)
    outcome = replay.outcome(source, replayed, ended, _maps_for_stored_runs())
    for line in outcome.lines():
        print(line, flush=True)
    return outcome.result()


def _interrupted(args: argparse.Namespace, said: list[str], run_id: str | None) -> "ReplayResult":
    """A replay Ctrl-C stopped while no agent ran, so none was waited on: exit 130, as `irimi
    shadow` exits, its result naming the replay's run when one was stored (#84)."""
    from dataclasses import replace

    from irimi import replay

    print("error: interrupted.", file=sys.stderr)
    return replace(replay.refused(args.run_id, said, run_id), exit_code=SIGINT_EXIT_CODE)


def _replay_command(args: argparse.Namespace) -> list[str]:
    """`irimi replay`'s `-- CMD`. After `--` argparse takes every word as a positional, RUN_ID
    first, so `irimi replay -- python agent.py` read as a replay of a run named `python` (#84).
    `-- RUN_ID` is how a run id that starts with `-` is given (`sdk.replay` gives every one so),
    and a command may follow it only after a second `--`. `args.argv` is what was typed.

    The command is read from what was typed, never from `args.cmd`: an older argparse, which
    Python 3.12's earlier releases ship, strips every `--` from it, the command's own as well,
    where a newer one strips only the first (#84)."""
    typed = list(args.argv or ())
    if "--" not in typed:
        return list(args.cmd)
    first = typed.index("--")
    after = typed[first + 1 :]
    # The subcommand's own words before `--`, parsed again with a stand-in run id after it: the
    # stand-in is the run id only when none was typed before `--`.
    before = typed[typed.index("replay") + 1 : first]
    if args.parser.parse_known_args([*before, "--", _NO_RUN_ID])[0].run_id != _NO_RUN_ID:
        return after  # RUN_ID came before `--`, and every word after it is the command
    if after[1:] and after[1] != "--":
        args.parser.error(
            "RUN_ID comes before `-- CMD`: irimi replay RUN_ID -- CMD, or "
            "irimi replay -- RUN_ID -- CMD for a run id that starts with -"
        )
    return after[2:]


# What `_replay_command` puts where a run id typed after `--` would be: no word anyone types.
_NO_RUN_ID = "\0irimi: no run id\0"


class _Tee:
    """`stream`, but each whole line written to it is kept in `lines` too. Everything else - a
    flush, `isatty`, `fileno` - is the stream's own. Not an `io` class, whose finalizer would
    flush a stream the caller may since have closed."""

    def __init__(self, stream: "TextIO", lines: list[str]) -> None:
        self._stream = stream
        self._lines = lines
        self._partial = ""

    def write(self, text: str) -> int:
        self._stream.write(text)
        *whole, self._partial = (self._partial + text).split("\n")
        self._lines.extend(whole)
        return len(text)

    def __getattr__(self, name: str) -> object:
        return getattr(self._stream, name)


@contextlib.contextmanager
def _keeping_stderr() -> Iterator[list[str]]:
    """For the block, stderr as it is, and each line written to it kept in the list yielded: what
    irimi said of why a replay could not run, which its result holds (#84). Python's own writes
    to stderr only, not a child's, which writes to the same file descriptor directly."""
    lines: list[str] = []
    with contextlib.redirect_stderr(_Tee(sys.stderr, lines)):  # type: ignore[type-var]
        yield lines


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
    for line in report.run_show_lines(run, _maps_for_stored_runs()):
        print(line)
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    """`irimi compare RUN_A RUN_B`: two stored runs' writes side by side, and no verdict (#79).
    Exits as diff(1) does: 0 when every write is the same, 1 when any is not, 2 when a run cannot
    be read."""
    from irimi import compare
    from irimi.store import RunNotFound, StoreReader
    from irimi.trace import TraceFormatError

    root = _store_root(args)
    reader = StoreReader(root)
    runs = []
    for run_id in (args.run_a, args.run_b):
        try:
            runs.append(reader.load_run(run_id))
        except RunNotFound:
            print(f"error: no run {run_id} in {root}", file=sys.stderr)
            return compare.TROUBLE
        except (TraceFormatError, OSError) as exc:
            print(f"error: run {run_id} could not be read: {exc}", file=sys.stderr)
            return compare.TROUBLE
    a, b = runs
    comparison = compare.compare_runs(a, b, _maps_for_stored_runs())
    for line in [*compare.header_lines(a.record, b.record), "", *comparison.lines()]:
        print(line)
    return comparison.exit_code


def _maps_for_stored_runs() -> "MapIndex | None":
    """The maps `runs show` and `compare` render a stored run's write lines with, loaded as
    `serve` and `shadow` load them (#72, #79).

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


def _wait_for_child(proc: "subprocess.Popen[bytes]", child: _Child) -> int:
    """Wait for the child, surviving Ctrl-C, which `child.interrupted` then says. Returns Popen's
    returncode (negative if signalled).

    Ctrl-C in a terminal reaches the whole foreground process group, so the child already got its
    own SIGINT; give it time to exit, then escalate.
    """
    import signal

    from irimi import runner

    try:
        return proc.wait()
    except KeyboardInterrupt:
        child.interrupted = True
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

    compare_runs = subparsers.add_parser(
        "compare",
        help="print two stored runs' writes side by side: a comparison, not a verdict",
    )
    compare_runs.add_argument(
        "run_a", metavar="RUN_A", help="the first run's exact id, or unattributed"
    )
    compare_runs.add_argument(
        "run_b", metavar="RUN_B", help="the second run's exact id, or unattributed"
    )
    _add_store_arg(compare_runs)
    compare_runs.set_defaults(func=cmd_compare)

    replay = subparsers.add_parser(
        "replay",
        help="run a stored run's trigger again, answered from its recording, and print what the "
        "agent did differently",
    )
    replay.add_argument("run_id", metavar="RUN_ID", help="the stored run's exact id")
    _add_engine_args(replay)
    replay.add_argument(
        "--call",
        type=_call_arg,
        metavar="MODULE:QUALNAME",
        help="call this function instead of the run's recorded entrypoint",
    )
    given = replay.add_mutually_exclusive_group()
    given.add_argument(
        "--args",
        type=_args_arg,
        metavar="JSON",
        help="call it with these arguments, a JSON object of {parameter: value}, instead of the "
        "run's recorded ones",
    )
    given.add_argument(
        "--args-file",
        type=_args_file_arg,
        dest="args",
        metavar="PATH",
        help="the same, read from a file: args too long for a command line, or not for `ps`",
    )
    replay.add_argument(
        "--result-file",
        type=Path,
        metavar="PATH",
        help="also write the result as JSON, the one sdk.replay() returns",
    )
    # Not REMAINDER, as `shadow`'s is: that would take every flag after RUN_ID as the command.
    # After `--`, argparse takes everything as positional, flags included.
    replay.add_argument(
        "cmd",
        nargs="*",
        metavar="-- <command> [args...]",
        help="run this command instead of a call of the run's trigger",
    )
    # `argv` is what was typed, which `main` sets (`_replay_command`).
    replay.set_defaults(func=cmd_replay, parser=replay, argv=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    argv = sys.argv[1:] if argv is None else argv
    args = parser.parse_args(argv)
    # What was typed, for `irimi replay`, whose RUN_ID argparse cannot tell from a command's first
    # word once both follow `--` (`_replay_command`, #84).
    args.argv = argv
    if args.command is None:
        parser.print_help()
        return 1
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
