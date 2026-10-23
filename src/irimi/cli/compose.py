"""The composition root: what `shadow`, `serve` and `replay` need before and around an engine,
and how every command reads a stored run back.

`prepare_run` loads the maps, finds the CA and the redaction key, and opens the trace store;
`build_engine` is the one place the concrete engine, policy, store and overlay meet.
"""

import argparse
import contextlib
import datetime as dt
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Protocol

from irimi import servicemaps, services
from irimi.cli import CliError, os_error, runner
from irimi.cli.options import LAST_RUN
from irimi.config import ca, paths
from irimi.config.ca import CAPaths
from irimi.core import exitcodes
from irimi.decide import idempotency, reader, targets
from irimi.decide.overlay import ServiceOverlay
from irimi.decide.policy import AnswerPolicy, ShadowPolicy
from irimi.decide.writelog import WriteLog
from irimi.engine import (
    Engine,
    EngineConfig,
    EngineMode,
    EngineStartError,
    OnExchange,
    OnTlsFailure,
    Replay,
    Serve,
    Shadow,
)
from irimi.engine.control import ControlEndpoint, OnToolCall
from irimi.protocol import trace
from irimi.protocol.trace import StoredRun, TraceFormatError
from irimi.record import redact
from irimi.record.store import RunStore, writer
from irimi.record.store.lock import ServeLock
from irimi.record.store.reader import RunNotFound, StoreReader
from irimi.replay.index import ReplayIndex
from irimi.replay.policy import ReplayPolicy
from irimi.report import terminal
from irimi.servicemaps import MapIndex


@dataclass(frozen=True)
class Run:
    """Everything `serve`, `shadow` and `replay` need before an engine exists. Used as a context
    manager, it closes its store and releases its lock on the way out."""

    index: MapIndex
    ca: CAPaths
    run_id: str
    config: EngineConfig
    store: RunStore
    # The store's `serve.lock`, held, in serve mode only.
    lock: ServeLock | None = None
    # The run a `Replay` mode names, read back to answer from, in a replay only.
    recording: ReplayIndex | None = None
    # Each run's faked writes, dropped as the store reports the run ended. None: the engine
    # keeps its own, and nothing ends its runs.
    write_log: WriteLog | None = None
    # Each run's idempotency keys, dropped with its writes. None: the policy keeps its own.
    keys: idempotency.Store | None = None

    def __enter__(self) -> "Run":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """The engine never closes the store: the composition root opened it, so it closes it,
        once the engine has stopped."""
        try:
            _close_store(self.store)
        finally:
            if self.lock is not None:
                self.lock.release()


# What serve mode calls when a run's end is on disk, after the run's writes and idempotency keys
# have been dropped: the maps the run was decided with, and its id.
OnRunEnded = Callable[[MapIndex, str], None]


class EngineFactory(Protocol):
    """What builds a run's engine: `build_engine`, or a harness's own in its place (`cli.main`)."""

    def __call__(
        self,
        run: Run,
        on_exchange: OnExchange,
        on_tool_call: OnToolCall | None = None,
        on_tls_failure: OnTlsFailure | None = None,
    ) -> Engine: ...


def prepare_run(
    args: argparse.Namespace,
    *,
    serve: bool = False,
    on_run_ended: OnRunEnded | None = None,
    source: StoredRun | None = None,
) -> Run | None:
    """Load the maps, find the CA, key the recording of `source`, the run a replay answers from,
    and open the trace store, or print why not and return None.

    Fail closed: without all of them, no engine starts and no child is spawned. The store is opened
    last, so a run refused for its maps, its CA or its key leaves nothing on disk.

    In serve mode the engine's own run is `trace.UNATTRIBUTED`, so a request no `Irimi-Run`
    names is stored there and builds no run, and the store's `serve.lock` is taken as soon as the
    store is open: a second serve on one root is refused before its engine starts, and writes
    nothing.
    """
    index = load_maps(frozenset(args.allow_remote_target), args.target, args.target_reads)
    if index is None:
        return None
    try:
        # A `--bind` address is the listener too.
        targets.refuse_self_targets(index, args.port, args.bind)
    except targets.TargetRefused as exc:
        print(f"error: {exc}", file=sys.stderr)
        return None
    p = ca.ca_paths()
    if not ca.ca_exists(p):
        print(f"error: no CA at {p.key.parent}; run `irimi init` first", file=sys.stderr)
        return None
    # A replay is one run against one engine, whose recording is the engine's;
    # serve mode is many runs at once, and `irimi serve` has no run to replay.
    assert not (serve and source is not None)
    run_id = trace.UNATTRIBUTED if serve else trace.new_run_id()
    mode: EngineMode = Shadow()
    if serve:
        mode = Serve()
    elif source is not None:
        mode = Replay(source.record.run_id)
    config = _engine_config(args, index, run_id, p, mode)
    key = _redaction_key()
    if key is None:
        return None
    recording = None
    if source is not None:
        # Keyed with this install's redaction key, which made the recording's placeholders.
        recording = ReplayIndex.from_events(source.events, key, config.maps)
    write_log = WriteLog()
    keys = idempotency.Store()

    def ended(run_id: str) -> None:
        # A run that has ended makes no more requests, so nothing reads its writes or keys again.
        write_log.end_run(run_id)
        keys.end_run(run_id)
        if on_run_ended is not None:
            on_run_ended(index, run_id)

    store = _open_store(args, key, ended, replayed_run(config.mode))
    if store is None:
        return None
    lock = _hold_serve_lock(args) if serve else None
    if serve and lock is None:
        _close_store(store)
        return None
    return Run(index, p, run_id, config, store, lock, recording, write_log, keys)


def replayed_run(mode: EngineMode) -> str | None:
    """The stored run a replay answers from, or None when `mode` is no replay."""
    return mode.source_run_id if isinstance(mode, Replay) else None


def _engine_config(
    args: argparse.Namespace,
    index: MapIndex,
    run_id: str,
    ca_paths: CAPaths,
    mode: EngineMode | None = None,
) -> EngineConfig:
    """The EngineConfig `serve`, `shadow` and `replay` share; `mode` is `Shadow()` when None.

    `maps=index` is what makes the classifier use the maps: without it the engine falls back to the
    verb rule and every mapped write on a POST-only service reads as `unknown`. Base-URL mode
    relays to every host in a loaded map, plus each `--base-url-host`.
    """
    return EngineConfig(
        run_id=run_id,
        ca=ca_paths,
        confdir=paths.mitm_dir(),
        listen_host=args.bind,
        listen_port=args.port,
        reverse_hosts=index.hosts | frozenset(args.base_url_host),
        maps=index,
        mode=Shadow() if mode is None else mode,
    )


def load_maps(
    allow_target_hosts: frozenset[str] = frozenset(),
    targets: Sequence[tuple[str, str, str]] = (),
    target_reads: Sequence[str] = (),
) -> MapIndex | None:
    """The loaded service maps, with `--target`, `--target-reads` and `--allow-remote-target`'s
    values, or None after printing why they were refused.

    Fail closed: a map, overrides file or `--target` the loader rejects must not start a proxy
    that would then classify and answer with half a policy.
    """
    try:
        index = servicemaps.load(
            allow_target_hosts=allow_target_hosts,
            targets=targets,
            target_reads=target_reads,
        )
        # The Python tables key on the maps' names; a map they disagree with would switch an
        # effect, a check or a fixture off silently, so it is refused here instead.
        services.check(index)
        return index
    except servicemaps.MapError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return None


def _escape_hatch_warning(args: argparse.Namespace) -> str | None:
    """The one line `--allow-remote-target` has to print, or None when it was not used.

    Targets are loopback-only so that a delegated request cannot leave the machine; this flag is
    the deliberate way out of that, so it says so out loud. The delegated-service banner and the
    summary's accounting of delegated writes are printed elsewhere.
    """
    hosts = getattr(args, "allow_remote_target", None)
    if not hosts:
        return None
    named = ", ".join(sorted(hosts))
    return f"warning: targets may leave this machine (--allow-remote-target {named})"


def _ca_expiry_warning(ca_cert: Path) -> str | None:
    """The line a CA that expires within `ca.EXPIRY_WARNING` prints, or None: a client stops
    trusting the certificates irimi mints once it has expired."""
    expires = ca.expires_at(ca_cert)
    now = dt.datetime.now(dt.UTC)
    if expires is None or expires - now > ca.EXPIRY_WARNING:
        return None
    when = expires.date().isoformat()
    if expires <= now:
        return f"warning: irimi's CA expired on {when}; run irimi init --force for a new one"
    return f"warning: irimi's CA expires on {when}; run irimi init --force for a new one"


def print_startup(
    command: str,
    args: argparse.Namespace,
    index: MapIndex,
    run_id: str | None,
    host: str,
    port: int,
    ca_cert: Path,
    replay_of: str | None = None,
) -> None:
    """Everything a run says before it starts: the escape-hatch and CA-expiry warnings, the banner,
    and one line per delegated service. `command` is "shadow", "replay" or "serve", whose `run_id`
    is None. `replay_of` is the run a replay answers from.

    Every line is flushed. Under a pipe or a supervisor stdout is block-buffered, and `serve` may
    print nothing else for minutes, so without the flush the port, the CA path a client needs and
    the `NOT loopback` line would not appear until the first request.
    """
    for warning in (_escape_hatch_warning(args), _ca_expiry_warning(ca_cert)):
        if warning is not None:
            print(warning, file=sys.stderr, flush=True)
    for line in terminal.banner_lines(command, run_id, host, port, ca_cert, replay_of):
        print(line, flush=True)
    for line in terminal.delegated_lines(index, color=sys.stdout.isatty()):
        print(line, flush=True)


def _redaction_key() -> bytes | None:
    """The install's redaction key, loaded once for the store that redacts with it and the
    recording a replay matches against it, or None after printing why it cannot be used."""
    home = paths.irimi_home()
    try:
        return redact.load_key(home)
    except redact.RedactKeyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return None
    except OSError as exc:
        print(f"error: {os_error(f'the redaction key in {home}', exc)}", file=sys.stderr)
        return None


def store_root(args: argparse.Namespace) -> Path:
    return args.store or paths.store_dir()


def store_subject(args: argparse.Namespace) -> str:
    """The store as an error names it: the `--store` typed, or the default one."""
    return f"--store {args.store}" if args.store else f"the trace store {store_root(args)}"


def _open_store(
    args: argparse.Namespace,
    key: bytes,
    on_run_ended: Callable[[str], None] | None = None,
    replay_of: str | None = None,
) -> RunStore | None:
    """The trace store under `--store` (default `paths.store_dir()`), redacting with `key`, or None
    after printing why its directory cannot be used: a run that would drop every event
    unseen must not start. `replay_of` is the run a replay answers from, which every header run
    the store makes names."""
    try:
        return writer.DirectoryStore(store_root(args), key, on_run_ended, replay_of)
    except OSError as exc:
        print(f"error: {os_error(store_subject(args), exc)}", file=sys.stderr)
        return None


def _hold_serve_lock(args: argparse.Namespace) -> ServeLock | None:
    """The store's `serve.lock`, held, or None after printing why not: another serve writes this
    store, or the lock file cannot be opened."""
    try:
        return ServeLock.hold(store_root(args))
    except OSError as exc:
        print(f"error: {os_error(store_subject(args), exc)}", file=sys.stderr)
        return None


def _close_store(store: RunStore) -> None:
    """Close a store `prepare_run` opened. An impatient Ctrl-C here, as in `handle.stop()`, may
    cut the writer's last seconds short, and never costs the summary."""
    with contextlib.suppress(KeyboardInterrupt):
        store.close()


def build_engine(
    run: Run,
    on_exchange: OnExchange,
    on_tool_call: OnToolCall | None = None,
    on_tls_failure: OnTlsFailure | None = None,
) -> Engine:
    """Wire the engine, answer policy, overlay, control endpoint and store for one run. The store
    is the run's, and the caller closes it once the engine has stopped."""
    # Imported here alone: `runs` and `compare` share this module, start no proxy, and so never
    # load mitmproxy.
    from irimi.engine.mitm import MitmEngine  # noqa: PLC0415

    overlay = ServiceOverlay()
    policy: AnswerPolicy
    if run.recording is None:
        # The reader is the real upstream, chosen here and nowhere else.
        policy = ShadowPolicy(reader=reader.UpstreamReader(), maps=run.config.maps, keys=run.keys)
    else:
        # A replay's reader is the recording, which the policy builds itself: it never reads live
        # state for a precondition.
        policy = ReplayPolicy(run.recording, maps=run.config.maps, keys=run.keys)
    # The policy that decides the wire answers the SDK's read tools too: from a replay's
    # recording, or from nothing.
    control = ControlEndpoint(
        run.store,
        serve=isinstance(run.config.mode, Serve),
        process_run=run.config.run_id,
        on_tool_call=on_tool_call,
        policy=policy,
        replay_of=replayed_run(run.config.mode),
    )
    return MitmEngine(
        run.config,
        policy,
        run.store,
        overlay,
        on_exchange=on_exchange,
        control=control,
        write_log=run.write_log,
        on_tls_failure=on_tls_failure,
    )


def start_engine(engine: Engine) -> runner.EngineThread | None:
    """`engine` started on its own thread, or None after printing why it did not: the one place
    a command says so. A child is never spawned without the proxy."""
    try:
        return runner.start_engine(engine)
    except EngineStartError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return None


def resolve_run_id(root: Path, typed: str) -> str:
    """The stored run `typed` names: `unattributed`, `last` for the newest run, a whole run id,
    or a prefix only one run id starts with. A prefix no run has is returned as typed, so the
    caller's read says there is no such run, as it does for a damaged one `list_runs` skips.
    Raises LookupError, saying why, for `last` in an empty store and for a prefix more than one
    run id starts with."""
    # An empty prefix would name every run, so it names none.
    if typed in ("", trace.UNATTRIBUTED):
        return typed
    try:
        ids = [record.run_id for record in StoreReader(root).list_runs()]
    except OSError:
        return typed
    if typed in ids:
        return typed
    if typed == LAST_RUN:
        if not ids:
            raise LookupError(f"no runs in {root}")
        return ids[0]
    matches = [run_id for run_id in ids if run_id.startswith(typed)]
    if len(matches) > 1:
        raise LookupError(f"run id {typed} is ambiguous; it starts {', '.join(sorted(matches))}")
    return matches[0] if matches else typed


def load_stored_run(root: Path, run_id: str, *, bodies: bool = True) -> StoredRun:
    """The run `run_id` in the store at `root`, or CliError saying why not: no such run, or one
    that cannot be read. The error's cause is what the store raised, which a caller with wording
    of its own names instead."""
    try:
        return StoreReader(root).load_run(run_id, bodies=bodies)
    except RunNotFound as exc:
        raise CliError(f"no run {run_id} in {root}", exitcodes.ERROR) from exc
    except (TraceFormatError, OSError) as exc:
        raise CliError(f"run {run_id} could not be read: {exc}", exitcodes.ERROR) from exc


def maps_for_stored_runs() -> MapIndex | None:
    """The maps `runs show` and `compare` render a stored run's write lines with, loaded as
    `serve` and `shadow` load them.

    Only the `human:` templates are read from them, so maps that do not load cost the reader
    those sentences and nothing else: the writes print as the requests they were. Unlike
    `serve` and `shadow`, which must not start with half a policy, this carries on."""
    try:
        return servicemaps.load()
    except servicemaps.MapError:
        print("warning: maps did not load; writes are shown as requests", file=sys.stderr)
        return None
