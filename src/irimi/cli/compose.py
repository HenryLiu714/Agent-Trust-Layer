"""The composition root: setup around an engine for `shadow`, `serve` and `replay`.

`prepare_run` loads the maps, CA and redaction key and opens the store; `build_engine` wires the
concrete engine. Also how every command reads a stored run back.
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
    """What `serve`, `shadow` and `replay` need before an engine exists.

    As a context manager it closes its store and releases its lock on exit.
    """

    index: MapIndex
    ca: CAPaths
    run_id: str
    config: EngineConfig
    store: RunStore
    lock: ServeLock | None = None  # serve mode only
    recording: ReplayIndex | None = None  # replay only
    # Faked writes and idempotency keys per run, dropped when the run ends. None lets the engine
    # or policy keep its own.
    write_log: WriteLog | None = None
    keys: idempotency.Store | None = None

    def __enter__(self) -> "Run":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the store and release the lock; call only after the engine has stopped."""
        try:
            _close_store(self.store)
        finally:
            if self.lock is not None:
                self.lock.release()


# Called with the maps and run id once a run's end is on disk and its state dropped.
OnRunEnded = Callable[[MapIndex, str], None]


class EngineFactory(Protocol):
    """Builds a run's engine: `build_engine`, or a test harness's replacement."""

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
    """Load the maps, CA and key, index `source` for a replay, and open the store.

    Returns None after printing why when any step fails; no engine may start then. The store opens
    last, so a refused run leaves nothing on disk. In serve mode the run is `trace.UNATTRIBUTED`
    and `serve.lock` is taken, so a second serve on one store is refused.
    """
    index = load_maps(frozenset(args.allow_remote_target), args.target, args.target_reads)
    if index is None:
        return None
    try:
        targets.refuse_self_targets(index, args.port, args.bind)
    except targets.TargetRefused as exc:
        print(f"error: {exc}", file=sys.stderr)
        return None
    p = ca.ca_paths()
    if not ca.ca_exists(p):
        print(f"error: no CA at {p.key.parent}; run `irimi init` first", file=sys.stderr)
        return None
    # Serve mode is many runs at once, so it cannot replay one.
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
        # The recording's placeholders were made with this install's redaction key.
        recording = ReplayIndex.from_events(source.events, key, config.maps)
    write_log = WriteLog()
    keys = idempotency.Store()

    def ended(run_id: str) -> None:
        # An ended run makes no more requests, so its writes and keys are never read again.
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
    """Return the stored run a replay answers from, or None when `mode` is not a replay."""
    return mode.source_run_id if isinstance(mode, Replay) else None


def _engine_config(
    args: argparse.Namespace,
    index: MapIndex,
    run_id: str,
    ca_paths: CAPaths,
    mode: EngineMode | None = None,
) -> EngineConfig:
    """Return the EngineConfig `serve`, `shadow` and `replay` share; `mode` defaults to Shadow.

    Without `maps=index` the classifier falls back to the verb rule. Base-URL requests may go to
    every mapped host plus each `--base-url-host`.
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
    """Return the loaded service maps with the target options applied, or None after printing why.

    A rejected map must not start a proxy that answers with half a policy.
    """
    try:
        index = servicemaps.load(
            allow_target_hosts=allow_target_hosts,
            targets=targets,
            target_reads=target_reads,
        )
        # The Python tables key on map names; a mismatch would silently switch an effect, check or
        # fixture off.
        services.check(index)
        return index
    except servicemaps.MapError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return None


def _escape_hatch_warning(args: argparse.Namespace) -> str | None:
    """Return the warning `--allow-remote-target` prints, or None when it was not used.

    Targets are loopback-only by default; this flag lifts that, so it says so.
    """
    hosts = getattr(args, "allow_remote_target", None)
    if not hosts:
        return None
    named = ", ".join(sorted(hosts))
    return f"warning: targets may leave this machine (--allow-remote-target {named})"


def _ca_expiry_warning(ca_cert: Path) -> str | None:
    """Return a warning if the CA expires within `ca.EXPIRY_WARNING`, else None."""
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
    """Print the startup warnings, banner and target lines.

    Every line is flushed: under a pipe stdout is block-buffered, and `serve` may print nothing
    else for minutes.
    """
    for warning in (_escape_hatch_warning(args), _ca_expiry_warning(ca_cert)):
        if warning is not None:
            print(warning, file=sys.stderr, flush=True)
    for line in terminal.banner_lines(command, run_id, host, port, ca_cert, replay_of):
        print(line, flush=True)
    for line in terminal.delegated_lines(index, color=sys.stdout.isatty()):
        print(line, flush=True)


def _redaction_key() -> bytes | None:
    """Return the install's redaction key, or None after printing why it cannot be used."""
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
    """Return the store as an error names it: the `--store` typed, or the default."""
    return f"--store {args.store}" if args.store else f"the trace store {store_root(args)}"


def _open_store(
    args: argparse.Namespace,
    key: bytes,
    on_run_ended: Callable[[str], None] | None = None,
    replay_of: str | None = None,
) -> RunStore | None:
    """Return the trace store, redacting with `key`, or None after printing why it cannot open.

    `replay_of` is recorded on every header run the store creates.
    """
    try:
        return writer.DirectoryStore(store_root(args), key, on_run_ended, replay_of)
    except OSError as exc:
        print(f"error: {os_error(store_subject(args), exc)}", file=sys.stderr)
        return None


def _hold_serve_lock(args: argparse.Namespace) -> ServeLock | None:
    """Return the store's held `serve.lock`, or None after printing why not."""
    try:
        return ServeLock.hold(store_root(args))
    except OSError as exc:
        print(f"error: {os_error(store_subject(args), exc)}", file=sys.stderr)
        return None


def _close_store(store: RunStore) -> None:
    """Close a store `prepare_run` opened, swallowing a Ctrl-C that cuts the last writes short."""
    with contextlib.suppress(KeyboardInterrupt):
        store.close()


def build_engine(
    run: Run,
    on_exchange: OnExchange,
    on_tool_call: OnToolCall | None = None,
    on_tls_failure: OnTlsFailure | None = None,
) -> Engine:
    """Wire the engine, answer policy, overlay and store for one run."""
    # Imported here so `runs` and `compare`, which share this module, never load mitmproxy.
    from irimi.engine.mitm import MitmEngine  # noqa: PLC0415

    overlay = ServiceOverlay()
    policy: AnswerPolicy
    if run.recording is None:
        policy = ShadowPolicy(reader=reader.UpstreamReader(), maps=run.config.maps, keys=run.keys)
    else:
        # A replay reads from the recording and never from live state.
        policy = ReplayPolicy(run.recording, maps=run.config.maps, keys=run.keys)
    # The same policy answers the SDK's read tools.
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
    """Start `engine` on its own thread, or return None after printing why it did not."""
    try:
        return runner.start_engine(engine)
    except EngineStartError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return None


def resolve_run_id(root: Path, typed: str) -> str:
    """Return the run id `typed` names: `unattributed`, `last`, a full id, or a unique prefix.

    An unmatched prefix is returned as typed, for the caller's read to report. Raises LookupError
    for `last` in an empty store or an ambiguous prefix.
    """
    # An empty prefix would match every run, so it matches none.
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
    """Return the stored run `run_id`; raise CliError, chained to the store's error, if not."""
    try:
        return StoreReader(root).load_run(run_id, bodies=bodies)
    except RunNotFound as exc:
        raise CliError(f"no run {run_id} in {root}", exitcodes.ERROR) from exc
    except (TraceFormatError, OSError) as exc:
        raise CliError(f"run {run_id} could not be read: {exc}", exitcodes.ERROR) from exc


def maps_for_stored_runs() -> MapIndex | None:
    """Return the maps `runs show` and `compare` render write lines with, or None.

    Only the `human:` templates are used, so a load failure is a warning, not an error.
    """
    try:
        return servicemaps.load()
    except servicemaps.MapError:
        print("warning: maps did not load; writes are shown as requests", file=sys.stderr)
        return None
