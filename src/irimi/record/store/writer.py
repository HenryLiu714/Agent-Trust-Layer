"""`DirectoryStore`: the trace store's writer, which puts each run under the store's root.

NOTHING HERE MAY RAISE INTO ITS CALLER, AND NOTHING HERE MAY BLOCK IT. `record` is called from
mitmproxy's hooks, and a raised hook forwards the flow, so a write escapes shadow mode. So every
public method of `DirectoryStore` only puts one item on a bounded queue, with `put_nowait`, and
returns; one daemon writer thread does all the redaction, hashing and file I/O. A full queue drops
the event, and so does a writer that fails on it. Either way the drop is counted on the store and
on the event's run (`RunRecord.dropped_events`), and a WARNING is logged on the first drop and
every 1000th after it: a recording failure never affects traffic.

NOTHING REACHES DISK UNREDACTED. The writer thread runs `redact.for_disk` on every event and
`redact.run_for_disk` on every run's record before it hashes or writes anything. A body is
redacted whole and only then cut at MAX_STORED_BODY, so a secret straddling the cut is never stored
as half a secret. A streamed body's `stream_chunks` are cut with it, so they still split what is
stored. An error is redacted whole and only then cut to a label, for the same reason.

A valid run id can still collide: `Run1` and `run1` are both valid, and on a case-insensitive
filesystem (macOS APFS by default) they are one directory. The first to reach the store keeps it;
an event for the other is stored in `unattributed/` like an invalid id, and an exchange among them
is flagged `exchange.Flag.BAD_RUN_ID`. So is an event of a replay's that names a run the replay did
not make (`RunAttribution`). Two runs are never merged into one directory.

A LONG `irimi serve` STAYS BOUNDED. The queue is capped in items and in bytes - an exchange's
bodies, a tool call and a run's start, each whole as JSON - and a body over
MAX_REDACTED_BODY is dropped before it is queued, so the writer never redacts hundreds of MB while
holding the GIL the proxy needs. A telemetry exchange is reduced to its `TelemetrySeen` on the
caller's side, for the same reason. The files a writer touched are fsynced in batches, a run's
state is forgotten once it ends (a late event reopens it from disk), and resuming a run reads only
the tail of its file.

A FAILED WRITE LEAVES NO DAMAGE BEHIND. A line the disk refused half-way is truncated off again, a
blob whose size is wrong - torn by a crash before any fsync - is rewritten by the next event that
needs it rather than trusted because it exists, and a run.json is synced before it is renamed into
place, so a crash never leaves one empty.
"""

import json
import logging
import os
import queue
import threading
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from irimi.config.files import append_line, private_dir, write_private
from irimi.core.exchange import Exchange, Flag, clip_chunks
from irimi.core.limits import (
    MAX_QUEUED_BYTES,
    MAX_QUEUED_ERROR,
    MAX_REDACTED_BODY,
    MAX_STORED_BODY,
)
from irimi.core.secret_shapes import cut_outside_secrets
from irimi.protocol import trace
from irimi.protocol.trace import (
    SCHEMA_VERSION,
    UNATTRIBUTED,
    BodyRef,
    ErrorInfo,
    Event,
    RunEnd,
    RunRecord,
    RunStatus,
    TelemetrySeen,
    ToolCall,
    TraceFormatError,
)
from irimi.record import redact
from irimi.record.store import StoreStats
from irimi.record.store.layout import (
    EVENTS_FILE,
    RUN_FILE,
    StoreLayout,
    names_a_run_dir,
    read_run_json,
    resume_seq,
)

logger = logging.getLogger(__name__)

# How many items may wait for the writer. A full queue drops the event rather than block a hook.
QUEUE_SIZE = 10_000
# The writer fsyncs the files it has written once it has made this many writes (lines and blobs)
# since the last sync, whenever the queue has been idle for SYNC_IDLE_S, and on close. Counting
# writes, not files, bounds what a crash can lose: one long run appends to one file, which as a
# count of distinct files never reaches 1000. And a `serve` running for weeks neither remembers
# every path it wrote nor syncs them all at shutdown.
SYNC_EVERY = 1000
SYNC_IDLE_S = 1.0
# How long `close()` waits for the writer to drain before it counts what is left as dropped.
CLOSE_TIMEOUT_S = 10.0
# A WARNING on the first drop and on every DROP_WARNING_EVERY-th after it.
DROP_WARNING_EVERY = 1000

# Called by the writer thread with a run's id once it has written that run's end.
OnRunEnded = Callable[[str], None]


class RunAttribution:
    """Which run directories a store may write, and whose runs it makes.

    A REPLAY WRITES ONLY THE RUNS IT MAKES. An agent that names its runs itself - an `Irimi-Run`
    label, a run id its SDK reuses, a start it posts by hand - names the recorded runs again: the
    one replayed, and every other the recorded process held. Each directory is another run's, as a
    clash's is, and appending to one would merge two runs in it and rewrite its record as the
    replay's, so a later replay of it would read both. So a run directory a replay's store did not
    make - recorded, or another process's - is never written by it, whether or not it is the run
    replayed; nor is the run replayed, wherever it is stored. A store that is no replay's resumes an
    existing run. Writer thread only."""

    def __init__(self, replay_of: str | None) -> None:
        # The stored run the engine writing here answers from, if it is a replay.
        self.replay_of = replay_of
        # The runs a replay's store made. Kept past a run's end, so a late event reopens it.
        self._made: set[str] = set()

    def accepts_start(self, record: RunRecord) -> bool:
        """Every run a replay's store starts is that replay's, and every other store's is no
        replay's: the record's constructor says so."""
        return record.replay_of == self.replay_of

    def header(self, run_id: str, started_at: float | None = None) -> RunRecord:
        """The record of a run this store knows only from its events: this store's replay's."""
        return RunRecord.header(run_id, started_at, self.replay_of)

    def claims(self, run_id: str, exists: Callable[[], bool]) -> bool:
        """True when this store may write `run_id`'s directory, which `exists` says is already
        there or not. A replay's store remembers a run it claims as one it made, before the
        directory is opened: one that raises after making it has still made a run of this
        replay's, which the next event may write."""
        if self.replay_of is None:
            return True
        if run_id == self.replay_of:
            return False
        if run_id not in self._made and exists():
            return False
        self._made.add(run_id)
        return True


@dataclass(frozen=True)
class _StartRun:
    record: RunRecord
    # The record as JSON, in bytes, measured once as it is queued.
    json_bytes: int = 0


@dataclass(frozen=True)
class _EndRun:
    # Counts one item of QUEUE_SIZE and no bytes (`_item_bytes`): its error waits cut to
    # MAX_QUEUED_ERROR, so the byte budget never drops a run's end.
    run_id: str
    ended_at: float
    outcome: RunStatus
    error: ErrorInfo | None
    exit_code: int | None


@dataclass(frozen=True)
class _QueuedEvent:
    event: Exchange | ToolCall | TelemetrySeen
    # A tool call as JSON, in bytes, measured once as it is queued: the whole call, since a
    # posted name or id may be nearly all of its 2 MiB, as its args may.
    json_bytes: int = 0


_CLOSE = object()  # the sentinel `close()` enqueues last

_Item = _StartRun | _EndRun | _QueuedEvent


@dataclass
class _OpenRun:
    """What the writer knows about one run directory, or about `unattributed/` (dir None)."""

    events: Path
    dir: Path | None
    record: RunRecord | None
    next_seq: int
    writable: bool  # False: its run.json is another version or damaged, so nothing is appended
    # The `dropped_events` its run.json held when this process first touched it: drops of an
    # earlier process, which this one adds its own to.
    dropped_before: int = 0
    dropped_written: int = 0  # the `dropped_events` its run.json last said


class DirectoryStore:
    """A RunStore writing the layout `docs/trace-format.md` describes under `root`."""

    def __init__(
        self,
        root: Path,
        key: bytes,
        on_run_ended: OnRunEnded | None = None,
        replay_of: str | None = None,
    ) -> None:
        """Raises OSError when `root` cannot be made a writable directory: a store that would
        silently drop every event must not start a run (the one place this class raises).

        `on_run_ended` is called on the writer thread with a run's id once that run's end is
        written, so every event queued before the end is already on disk: `serve` prints the
        run's summary from it. What it raises is logged and swallowed. It runs before the
        writer takes its next item, so a slow one holds the queue back.

        `replay_of` is the stored run the engine writing here answers from, if it is a replay: each
        run this store starts or makes from a header is that replay's, and it writes no run it did
        not make, the run it replays included (`RunAttribution`)."""
        private_dir(root)
        if not root.is_dir() or not os.access(root, os.W_OK | os.X_OK):
            raise PermissionError(f"the trace store {root} is not a writable directory")
        self.layout = StoreLayout(root)
        self._key = key
        self._on_run_ended = on_run_ended
        self._attribution = RunAttribution(replay_of)  # writer thread only
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=QUEUE_SIZE)
        self._lock = threading.Lock()  # guards everything below it and `_closed`
        self._closed = False
        self._dropped = 0
        self._written = 0
        self._queued_bytes = 0  # body bytes of the items waiting for the writer
        # By the run id the event named, or `unattributed` for one that names no run directory.
        self._run_drops: Counter[str] = Counter()
        self._runs: dict[str, _OpenRun] = {}  # writer thread only
        self._touched: set[Path] = set()  # writer thread only: files not yet fsynced
        self._unsynced = 0  # writer thread only: writes into them since the last sync
        self._abandoned = threading.Event()  # set when `close()` gave up waiting on the writer
        self._thread = threading.Thread(target=self._write_loop, name="irimi-store", daemon=True)
        self._thread.start()

    # -- the public methods: each enqueues one item and returns ------------------------------

    def start_run(self, record: RunRecord) -> None:
        # A start counts against the byte budget as a tool call does, so one that cannot be
        # measured is a drop, as a tool call's is. A dropped start leaves its run to be made from
        # its first event as a `header` run, as every dropped start does (docs/trace-format.md).
        try:
            size = len(json.dumps(trace.run_to_json(record)))
        except Exception as exc:
            self._drop(
                _item_run_id(_StartRun(record)), f"it could not be queued: {type(exc).__name__}"
            )
            return
        self._put(_StartRun(record, size))

    def record(self, exchange: Exchange) -> None:
        try:
            event = _as_queued(exchange)
        except Exception as exc:
            self._drop(
                _item_run_id(_QueuedEvent(exchange)),
                f"it could not be queued: {type(exc).__name__}",
            )
            return
        self._put(_QueuedEvent(event))

    def record_tool_call(self, call: ToolCall) -> None:
        try:
            size = len(json.dumps(trace.tool_call_to_json(call)))
        except Exception as exc:
            self._drop(
                _item_run_id(_QueuedEvent(call)), f"it could not be queued: {type(exc).__name__}"
            )
            return
        self._put(_QueuedEvent(call, size))

    def end_run(
        self,
        run_id: str,
        ended_at: float,
        outcome: RunStatus,
        error: ErrorInfo | None = None,
        exit_code: int | None = None,
    ) -> None:
        # A posted error may be 2 MiB, and the writer cuts it only once it has redacted it,
        # so it waits cut outside any secret to MAX_QUEUED_ERROR, and an end is never counted
        # against MAX_QUEUED_BYTES: a run never loses its end to the byte budget. An error that is
        # not text is a drop, never raised into the caller.
        try:
            queued = None if error is None else _queued_error(error)
        except Exception as exc:
            self._drop(run_id, f"it could not be queued: {type(exc).__name__}")
            return
        self._put(_EndRun(run_id, ended_at, outcome, queued, exit_code))

    def stats(self) -> StoreStats:
        with self._lock:
            return StoreStats(self._queue.qsize(), self._written, self._dropped)

    def close(self) -> None:
        """Let the writer finish what is queued, for at most CLOSE_TIMEOUT_S, then fsync every
        file it touched. Whatever is still queued after that is counted as dropped and written
        into each affected run's `dropped_events`. Idempotent, and a second call waits as the
        first does; never raises."""
        try:
            with self._lock:
                first = not self._closed
                self._closed = True  # `_put` refuses from here on, so the sentinel is last
            deadline = time.monotonic() + CLOSE_TIMEOUT_S
            if first:
                try:
                    self._queue.put(_CLOSE, timeout=CLOSE_TIMEOUT_S)
                except queue.Full:
                    pass
            # A second caller waits for the first one's writer too, so whoever returns from
            # close() - the engine's shutdown or `irimi shadow` itself - finds the files whole.
            self._thread.join(max(0.0, deadline - time.monotonic()))
            if first and self._thread.is_alive():
                self._abandon()
        except Exception as exc:
            logger.warning("irimi: the trace store failed to close cleanly: %s", type(exc).__name__)

    def _put(self, item: _Item) -> None:
        try:
            size = _item_bytes(item)
            with self._lock:
                budget = self._queued_bytes + size <= MAX_QUEUED_BYTES or not self._queued_bytes
                if not self._closed and budget:
                    self._queue.put_nowait(item)
                    self._queued_bytes += size
                    return
        except queue.Full:
            pass
        except Exception as exc:
            self._drop(_item_run_id(item), f"it could not be queued: {type(exc).__name__}")
            return
        self._drop(_item_run_id(item), "the queue is full or the store is closed")

    def _taken(self, item: _Item) -> None:
        """`item` left the queue, written or not: its bytes no longer count against the budget."""
        with self._lock:
            self._queued_bytes -= _item_bytes(item)

    def _drop(self, run_id: str, why: str) -> None:
        # A run id that can name no directory is counted under `unattributed`, which has no
        # run.json to write it to: a stream of distinct bad ids must not grow the counter, and one
        # that is not even a string must not raise here, in the writer's own error path.
        key = run_id if names_a_run_dir(run_id) else UNATTRIBUTED
        with self._lock:
            self._dropped += 1
            self._run_drops[key] += 1
            total = self._dropped
        if (total - 1) % DROP_WARNING_EVERY == 0:
            logger.warning("irimi: the trace store dropped an event (%s); %d dropped", why, total)

    def _abandon(self) -> None:
        """The writer did not finish in time: count what it left as dropped, and write those
        counts from this thread. The writer stops writing once it sees `_abandoned`."""
        self._abandoned.set()
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            if item is not _CLOSE:
                self._taken(item)
                self._drop(_item_run_id(item), "the store closed before writing it")
        logger.warning(
            "irimi: the trace store's writer did not finish within %.0fs", CLOSE_TIMEOUT_S
        )
        self._write_drop_counts()

    # -- the writer thread -------------------------------------------------------------------

    def _write_loop(self) -> None:
        while True:
            try:
                item = self._queue.get(timeout=SYNC_IDLE_S)
            except queue.Empty:
                # `close()` gave up on this writer and drained the queue, its sentinel included:
                # nothing more will come, so the writer ends here rather than wake up every
                # SYNC_IDLE_S for the life of the process.
                if self._abandoned.is_set():
                    return
                self._sync()  # idle: make what was written durable before more arrives
                continue
            if item is _CLOSE:
                self._finish()
                return
            self._taken(item)
            if self._abandoned.is_set():
                self._drop(_item_run_id(item), "the store closed before writing it")
                continue
            try:
                self._handle(item)
                if self._unsynced >= SYNC_EVERY:
                    self._sync()
            except Exception as exc:
                # The type only, and through `_drop`'s rate limit: a full disk fails every event,
                # and an exception's message may quote the value it choked on.
                logger.debug("irimi: the trace store's writer failed", exc_info=True)
                self._drop(_item_run_id(item), f"the writer raised {type(exc).__name__}")

    def _handle(self, item: _Item) -> None:
        if isinstance(item, _StartRun):
            self._start(item.record)
        elif isinstance(item, _EndRun):
            self._end(item)
        else:
            self._append(item.event)

    def _start(self, record: RunRecord) -> None:
        if not self._attribution.accepts_start(record):
            # A start that disagrees is dropped and counted on the store alone, so it makes no
            # run directory of its own.
            self._drop(UNATTRIBUTED, "its start names another replay than this store's")
            return
        run = self._run_dir(record.run_id)
        # A RUN THAT HAS ENDED IS NOT STARTED AGAIN. A start re-posted after the run's end
        # would rewrite its run.json whole, losing its end, its outcome and its first trigger, so
        # it would list `incomplete` again. It is dropped, and counted on the store alone, in
        # health's `dropped` (the endpoint answered 204 when it queued it): the run lost nothing,
        # so its run.json is not rewritten, its `dropped_events` included. `_run` reopens a run
        # forgotten at its end from disk, so its stored outcome is seen here, and it is forgotten
        # again, so a serve holds only the runs still open. A `header` run has no outcome, so a
        # start still rewrites it.
        if run.record is not None and run.record.end is not None:
            self._drop(UNATTRIBUTED, "a start was posted for a run that has already ended")
            self._forget(record.run_id, run)
            return
        self._write_record(run, redact.run_for_disk(record, self._key))

    def _end(self, end: _EndRun) -> None:
        run = self._run_dir(end.run_id)
        record = run.record or self._attribution.header(end.run_id)
        error = redact.redact_error(end.error, self._key)
        self._write_record(
            run, replace(record, end=RunEnd(end.ended_at, end.outcome, error, end.exit_code))
        )
        self._forget(end.run_id, run)
        if self._on_run_ended is not None:
            try:
                self._on_run_ended(end.run_id)
            except Exception as exc:
                # The run is stored whatever its caller did with the news. The type only, as
                # every failure here is logged: a message may quote what it choked on.
                logger.warning("irimi: the end-of-run callback raised %s", type(exc).__name__)
                logger.debug("irimi: the end-of-run callback's traceback", exc_info=True)

    def _run_dir(self, run_id: str) -> _OpenRun:
        """The writer's state for a run directory a start or an end may write. Raises ValueError -
        a drop - for an id that names no directory, before anything is opened for it, so neither
        creates one, `unattributed/` included."""
        if not names_a_run_dir(run_id):
            raise ValueError("this run id names no run directory")
        run = self._run(run_id)
        if run.dir is None or not run.writable:
            raise ValueError("this run cannot be written by this store")
        return run

    def _forget(self, run_id: str, run: _OpenRun) -> None:
        """An ended run is fsynced and forgotten, so a `serve` of thousands of runs holds only the
        open ones. The drops its run.json now shows leave the counter; a late event reopens the
        run from disk (`_open`), which reads them back as `dropped_before`. Its run.json was
        synced as it was written (`_write_record`)."""
        if run.events in self._touched:
            self._touched.discard(run.events)
            _fsync(run.events)
        with self._lock:
            self._run_drops[run_id] -= run.dropped_written - run.dropped_before
            if self._run_drops[run_id] <= 0:
                del self._run_drops[run_id]
        del self._runs[run_id]

    def _append(self, event: Exchange | ToolCall | TelemetrySeen) -> None:
        run = self._run(event.run_id)
        if not run.writable:
            raise ValueError(f"run {event.run_id!r} is another schema version")
        if run.dir is not None and run.record is None:
            # `0.0` is an event built with no clock: no start at all (docs/trace-format.md).
            started_at = event.started_at or None
            self._write_record(run, self._attribution.header(event.run_id, started_at))
        stored = self._prepare(event, bad_run_id=run.dir is None)
        line = json.dumps(
            trace.event_to_json(run.next_seq, stored, self._put_body), allow_nan=False
        )
        append_line(run.events, (line + "\n").encode())
        self._wrote(run.events)
        run.next_seq += 1
        with self._lock:
            self._written += 1

    def _prepare(self, event: Exchange | ToolCall | TelemetrySeen, *, bad_run_id: bool) -> Event:
        """`event` as it may be written: redacted (`redact.for_disk`), and an exchange flagged for
        what the store does to it here."""
        stored = redact.for_disk(event, self._key)
        if not isinstance(stored, Exchange):
            return stored
        flags = list(stored.flags)
        # A decoded Exchange carries only its bytes, not `BodyRef.truncated`, so a cut body says
        # so in its flags or the fact is lost on read.
        bodies = (stored.request.body, b"" if stored.response is None else stored.response.body)
        if any(len(body) > MAX_STORED_BODY for body in bodies):
            flags.append(Flag.BODY_TRUNCATED)
        if bad_run_id and stored.run_id != UNATTRIBUTED:
            flags.append(Flag.BAD_RUN_ID)
        # `_put_body` keeps the first MAX_STORED_BODY bytes, so a streamed body's chunks are cut
        # there too, and still split the body that is stored.
        chunks = clip_chunks(stored.stream_chunks, MAX_STORED_BODY)
        return replace(stored, flags=tuple(dict.fromkeys(flags)), stream_chunks=chunks)

    def _put_body(self, body: bytes) -> BodyRef:
        data = body[:MAX_STORED_BODY]
        ref = trace.body_ref(data, truncated=len(body) > MAX_STORED_BODY)
        path = self.layout.blob(ref.sha256)
        # Content-addressed, so the same bytes are written once - unless a crash tore the file
        # before any fsync, which its size gives away, and then it is written again.
        if not path.exists() or path.stat().st_size != len(data):
            write_private(path, data, how="atomic")
            self._wrote(path)
        return ref

    def _run(self, run_id: str) -> _OpenRun:
        """The writer's state for `run_id`, made on first touch. A run id the store cannot use as
        a directory, and `trace.UNATTRIBUTED` itself, share `unattributed/`."""
        if not isinstance(run_id, str):
            # A line holding it would be one no reader decodes, damaging `unattributed/` for good.
            raise TypeError("a run id is a string")
        if not names_a_run_dir(run_id):
            # Not remembered under its own id: a stream of distinct bad ids must not grow the map.
            return self._unattributed()
        known = self._runs.get(run_id)
        if known is not None:
            return known
        directory = self.layout.run_dir(run_id)
        if self._clashes(run_id) or not self._attribution.claims(run_id, directory.exists):
            run = self._unattributed()
        else:
            run = self._open(directory / EVENTS_FILE, directory)
        self._runs[run_id] = run
        return run

    def _unattributed(self) -> _OpenRun:
        run = self._runs.get(UNATTRIBUTED)
        if run is None:
            run = self._runs[UNATTRIBUTED] = self._open(self.layout.unattributed_events, None)
        return run

    def _clashes(self, run_id: str) -> bool:
        """True when `run_id`'s directory already exists under another spelling: `run1` on a
        case-insensitive filesystem where `Run1` got there first."""
        directory = self.layout.run_dir(run_id)
        return directory.exists() and run_id not in os.listdir(self.layout.runs)

    def _open(self, events: Path, directory: Path | None) -> _OpenRun:
        """Start tracking a run. An existing directory - a serve-mode restart - resumes its `seq`
        from its last readable line, after truncating a half-written one, and is never appended
        to when its run.json is another schema version (docs/trace-format.md, "Versioning") or
        cannot be decoded: damage, which includes a `replay_of` that is no run id or
        disagrees with `mode`. Its events and end are dropped and counted, and the record is left
        as it was found, never rewritten as a header run's over the damage."""
        private_dir(events.parent)
        record: RunRecord | None = None
        writable = True
        if directory is not None and (directory / RUN_FILE).exists():
            # An OSError - too many open files, say - is left to drop this one event: refusing the
            # run for good would drop every later one of a run that is only unreadable for now.
            try:
                record = read_run_json(directory)
                writable = record.schema_version == SCHEMA_VERSION
            except TraceFormatError:  # a newer version, or one damage leaves no version to read
                writable = False
        # A run this store will not append to is not touched at all, its half line included.
        run = _OpenRun(events, directory, record, resume_seq(events) if writable else 1, writable)
        if record is not None:
            run.dropped_before = run.dropped_written = record.dropped_events
        return run

    def _write_record(self, run: _OpenRun, record: RunRecord) -> None:
        """`record` as `run`'s run.json. Its `schema_version` is this writer's, whatever the caller
        said, since this writer's lines follow it. It is synced before it replaces the last one,
        not batched: a run.json a crash left empty would read as no version at all, and a restart
        would never append to that run again (`_open`). A run writes it a handful of times. The
        rename is not synced, which would take an fsync of the directory: a crash may lose the
        newest run.json and keep the one before it, whole - a run that reads as not ended, never
        as damage."""
        assert run.dir is not None
        record = replace(
            record,
            schema_version=SCHEMA_VERSION,
            dropped_events=self._dropped_in(run, record.run_id),
        )
        data = json.dumps(trace.run_to_json(record), allow_nan=False).encode()
        write_private(run.dir / RUN_FILE, data, how="atomic", durable=True)
        run.record = record
        run.dropped_written = record.dropped_events

    def _finish(self) -> None:
        """On the sentinel: write any drop count a run.json does not show yet, then fsync."""
        self._write_drop_counts()
        self._sync()

    def _wrote(self, path: Path) -> None:
        self._touched.add(path)
        self._unsynced += 1

    def _sync(self) -> None:
        """fsync every file touched since the last sync, and forget them."""
        touched, self._touched, self._unsynced = self._touched, set(), 0
        for path in touched:
            _fsync(path)

    def _dropped_in(self, run: _OpenRun, run_id: str) -> int:
        with self._lock:
            return run.dropped_before + self._run_drops[run_id]

    def _write_drop_counts(self) -> None:
        with self._lock:
            dropped = [run_id for run_id in self._run_drops if run_id != UNATTRIBUTED]
        for run_id in dropped:
            try:
                run = self._run(run_id)
                if not (run.dir is not None and run.writable):
                    continue
                if self._dropped_in(run, run_id) != run.dropped_written:
                    self._write_record(run, run.record or self._attribution.header(run_id))
            except Exception as exc:
                logger.warning(
                    "irimi: the trace store could not record run %r's drops: %s",
                    run_id,
                    type(exc).__name__,
                )


def _as_queued(exchange: Exchange) -> Exchange | TelemetrySeen:
    """What `record` queues, at O(1) cost on the caller's thread. Telemetry is forwarded and its
    requests and responses are never stored: one line per exchange records only that it happened
    and to which host, so a stored run's summary can count it. A body over MAX_REDACTED_BODY
    is dropped here and its exchange flagged, so the queue never holds it and the writer never
    redacts it."""
    if exchange.kind == "telemetry":
        return TelemetrySeen(exchange.run_id, exchange.request.host, exchange.started_at)
    request, response = exchange.request, exchange.response
    if len(request.body) > MAX_REDACTED_BODY:
        request = replace(request, body=b"")
    stream_chunks = exchange.stream_chunks
    if response is not None and len(response.body) > MAX_REDACTED_BODY:
        response = replace(response, body=b"")
        stream_chunks = ()  # an empty body has nothing to split
    if request is exchange.request and response is exchange.response:
        return exchange
    flags = tuple(dict.fromkeys((*exchange.flags, Flag.BODY_TRUNCATED)))
    return replace(
        exchange, request=request, response=response, flags=flags, stream_chunks=stream_chunks
    )


def _fsync(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError as exc:
        logger.warning("irimi: the trace store could not fsync %s: %s", path, type(exc).__name__)


def _item_bytes(item: _Item) -> int:
    """The bytes `item` holds while it waits: an exchange's two bodies, and a tool call and a
    run's start as their whole JSON, which the control endpoint takes up to
    `limits.MAX_CONTROL_BODY` of. An end holds an error cut to MAX_QUEUED_ERROR."""
    if isinstance(item, _QueuedEvent) and isinstance(item.event, Exchange):
        response = item.event.response
        return len(item.event.request.body) + (0 if response is None else len(response.body))
    if isinstance(item, _QueuedEvent | _StartRun):
        return item.json_bytes
    return 0


def _queued_error(error: ErrorInfo) -> ErrorInfo:
    """`error` as it waits for the writer: its type and its message each cut to MAX_QUEUED_ERROR
    outside any secret's shape. Raises TypeError for one that is not text."""
    return ErrorInfo(
        cut_outside_secrets(error.type, MAX_QUEUED_ERROR),
        cut_outside_secrets(error.message, MAX_QUEUED_ERROR),
    )


def _item_run_id(item: _Item) -> str:
    """The run id `item` names, to count its drop under. `unattributed` when the caller handed the
    store something that names none: counting a drop is the error path, and must not raise."""
    if isinstance(item, _EndRun):
        return item.run_id
    named = item.record if isinstance(item, _StartRun) else item.event
    return getattr(named, "run_id", UNATTRIBUTED)
