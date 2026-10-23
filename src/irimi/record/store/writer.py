"""`DirectoryStore`: the trace store's writer (guarantees in `irimi.record.store`).

- Public methods only enqueue; a full queue or a failed write drops the event, counted and
  logged on the first drop and every DROP_WARNING_EVERY-th.
- A body is redacted whole and only then cut at MAX_STORED_BODY, so no half secret is stored.
- `Run1` and `run1` share a directory on a case-insensitive filesystem: the first keeps it and the
  other's events go to `unattributed/`, flagged `bad-run-id`. Two runs are never merged.
- A long `serve` stays bounded: the queue is capped in items and bytes, bodies over
  MAX_REDACTED_BODY are dropped before queueing, and an ended run's state is forgotten.
"""

import contextlib
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

QUEUE_SIZE = 10_000
# Touched files are fsynced after this many writes, after SYNC_IDLE_S idle, and on close.
# Counting writes rather than files bounds a crash's loss for one long run appending to one file.
SYNC_EVERY = 1000
SYNC_IDLE_S = 1.0
# How long `close()` waits for the writer to drain before counting what is left as dropped.
CLOSE_TIMEOUT_S = 10.0
DROP_WARNING_EVERY = 1000

# Called on the writer thread with a run's id once its end is written.
OnRunEnded = Callable[[str], None]


class RunAttribution:
    """Which run directories a store may write. Writer thread only.

    A replay's store writes only runs it made: an agent that names its own runs would otherwise
    append to the recorded runs, merging two runs in one directory. Other stores resume existing
    runs.
    """

    def __init__(self, replay_of: str | None) -> None:
        self.replay_of = replay_of
        # Kept past a run's end, so a late event reopens it.
        self._made: set[str] = set()

    def accepts_start(self, record: RunRecord) -> bool:
        """Return True when the record's `replay_of` matches this store's."""
        return record.replay_of == self.replay_of

    def header(self, run_id: str, started_at: float | None = None) -> RunRecord:
        """Return the record of a run this store knows only from its events."""
        return RunRecord.header(run_id, started_at, self.replay_of)

    def claims(self, run_id: str, exists: Callable[[], bool]) -> bool:
        """Return True when this store may write `run_id`'s directory; a replay records it."""
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
    json_bytes: int = 0


@dataclass(frozen=True)
class _EndRun:
    # Counts no bytes (`_item_bytes`), so the byte budget never drops a run's end; its error
    # waits cut to MAX_QUEUED_ERROR.
    run_id: str
    ended_at: float
    outcome: RunStatus
    error: ErrorInfo | None
    exit_code: int | None


@dataclass(frozen=True)
class _QueuedEvent:
    event: Exchange | ToolCall | TelemetrySeen
    # A tool call's whole JSON size, since any posted field may be nearly all of its 2 MiB.
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
    # Earlier processes' drops, from run.json when this process first touched it.
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
        """Open the store; raises OSError when `root` cannot be made a writable directory.

        `on_run_ended` runs on the writer thread after a run's end is written, so the run's
        events are on disk; it blocks the queue while it runs, and what it raises is logged.
        `replay_of` is the stored run a replay answers from (`RunAttribution`).
        """
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
        # Keyed by run id, or `unattributed` for an id that names no run directory.
        self._run_drops: Counter[str] = Counter()
        self._runs: dict[str, _OpenRun] = {}  # writer thread only
        self._touched: set[Path] = set()  # writer thread only: files not yet fsynced
        self._unsynced = 0  # writer thread only: writes into them since the last sync
        self._abandoned = threading.Event()  # set when `close()` gave up waiting on the writer
        self._thread = threading.Thread(target=self._write_loop, name="irimi-store", daemon=True)
        self._thread.start()

    # Each public method enqueues one item and returns.

    def start_run(self, record: RunRecord) -> None:
        # A dropped start's run is made from its first event as a `header` run.
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
        # A posted error may be 2 MiB, so it waits cut; one that is not text is a drop.
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
        """Drain up to CLOSE_TIMEOUT_S, fsync, count the rest dropped. Idempotent; never raises."""
        try:
            with self._lock:
                first = not self._closed
                self._closed = True  # `_put` refuses from here on, so the sentinel is last
            deadline = time.monotonic() + CLOSE_TIMEOUT_S
            if first:
                with contextlib.suppress(queue.Full):
                    self._queue.put(_CLOSE, timeout=CLOSE_TIMEOUT_S)
            # Every caller waits, so whoever returns from close() finds the files whole.
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
        """Release `item`'s bytes from the budget once it leaves the queue."""
        with self._lock:
            self._queued_bytes -= _item_bytes(item)

    def _drop(self, run_id: str, why: str) -> None:
        # Bad ids count under `unattributed`, so distinct ones do not grow the counter and a
        # non-string cannot raise in this error path.
        key = run_id if names_a_run_dir(run_id) else UNATTRIBUTED
        with self._lock:
            self._dropped += 1
            self._run_drops[key] += 1
            total = self._dropped
        if (total - 1) % DROP_WARNING_EVERY == 0:
            logger.warning("irimi: the trace store dropped an event (%s); %d dropped", why, total)

    def _abandon(self) -> None:
        """Count what a late writer left as dropped and write the counts; the writer then stops."""
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

    # Everything below runs on the writer thread.

    def _write_loop(self) -> None:
        while True:
            try:
                item = self._queue.get(timeout=SYNC_IDLE_S)
            except queue.Empty:
                # `close()` drained the queue, sentinel included, so nothing more will come.
                if self._abandoned.is_set():
                    return
                self._sync()
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
                # The type only, rate-limited: a full disk fails every event, and a message may
                # quote the value it choked on.
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
            # Counted on the store alone, so it makes no run directory.
            self._drop(UNATTRIBUTED, "its start names another replay than this store's")
            return
        run = self._run_dir(record.run_id)
        # A start after the run's end would overwrite its end, so it is dropped and counted on
        # the store alone; the run lost nothing. A `header` run has no end, so a start rewrites it.
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
                # The type only: a message may quote what it choked on.
                logger.warning("irimi: the end-of-run callback raised %s", type(exc).__name__)
                logger.debug("irimi: the end-of-run callback's traceback", exc_info=True)

    def _run_dir(self, run_id: str) -> _OpenRun:
        """Return a writable run directory's state, else raise ValueError (a drop)."""
        if not names_a_run_dir(run_id):
            raise ValueError("this run id names no run directory")
        run = self._run(run_id)
        if run.dir is None or not run.writable:
            raise ValueError("this run cannot be written by this store")
        return run

    def _forget(self, run_id: str, run: _OpenRun) -> None:
        """Fsync and forget an ended run; a late event reopens it from disk, drops included."""
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
            # `0.0` means the event had no clock, so no start time (docs/trace-format.md).
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
        """Return `event` redacted for disk, an exchange flagged for what the store does to it."""
        stored = redact.for_disk(event, self._key)
        if not isinstance(stored, Exchange):
            return stored
        flags = list(stored.flags)
        # A decoded Exchange loses `BodyRef.truncated`, so a cut body is flagged.
        bodies = (stored.request.body, b"" if stored.response is None else stored.response.body)
        if any(len(body) > MAX_STORED_BODY for body in bodies):
            flags.append(Flag.BODY_TRUNCATED)
        if bad_run_id and stored.run_id != UNATTRIBUTED:
            flags.append(Flag.BAD_RUN_ID)
        # Chunks are cut where `_put_body` cuts the body, so they still split what is stored.
        chunks = clip_chunks(stored.stream_chunks, MAX_STORED_BODY)
        return replace(stored, flags=tuple(dict.fromkeys(flags)), stream_chunks=chunks)

    def _put_body(self, body: bytes) -> BodyRef:
        data = body[:MAX_STORED_BODY]
        ref = trace.body_ref(data, truncated=len(body) > MAX_STORED_BODY)
        path = self.layout.blob(ref.sha256)
        # Content-addressed, so written once, unless a crash tore it (its size gives that away).
        if not path.exists() or path.stat().st_size != len(data):
            write_private(path, data, how="atomic")
            self._wrote(path)
        return ref

    def _run(self, run_id: str) -> _OpenRun:
        """Return the state for `run_id`; ids that name no directory share `unattributed/`."""
        if not isinstance(run_id, str):
            # A line holding it would be undecodable, damaging `unattributed/` for good.
            raise TypeError("a run id is a string")
        if not names_a_run_dir(run_id):
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
        """Return True when `run_id`'s directory exists under another case spelling."""
        directory = self.layout.run_dir(run_id)
        return directory.exists() and run_id not in os.listdir(self.layout.runs)

    def _open(self, events: Path, directory: Path | None) -> _OpenRun:
        """Start tracking a run, resuming an existing directory's `seq` (`resume_seq`).

        A run.json of another schema version, or damaged, makes the run unwritable: its events
        and end are dropped and counted, and the file is left as found.
        """
        private_dir(events.parent)
        record: RunRecord | None = None
        writable = True
        if directory is not None and (directory / RUN_FILE).exists():
            # An OSError (too many open files) drops only this event, not the run for good.
            try:
                record = read_run_json(directory)
                writable = record.schema_version == SCHEMA_VERSION
            except TraceFormatError:
                writable = False
        # An unwritable run is not touched at all, its half line included.
        run = _OpenRun(events, directory, record, resume_seq(events) if writable else 1, writable)
        if record is not None:
            run.dropped_before = run.dropped_written = record.dropped_events
        return run

    def _write_record(self, run: _OpenRun, record: RunRecord) -> None:
        """Write `record` as `run`'s run.json, with this writer's `schema_version`.

        Synced before the rename, since an empty run.json would make the run unwritable. The
        rename itself is not synced: a crash may keep the previous run.json, whole.
        """
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
        """Write drop counts a run.json does not show yet, then fsync."""
        self._write_drop_counts()
        self._sync()

    def _wrote(self, path: Path) -> None:
        self._touched.add(path)
        self._unsynced += 1

    def _sync(self) -> None:
        """Fsync every file touched since the last sync."""
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
    """Return what `record` queues, at O(1) cost on the caller's thread.

    Telemetry is reduced to its host and time. A body over MAX_REDACTED_BODY is emptied and the
    exchange flagged `body-truncated`, so the writer never redacts it.
    """
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
    """Return the bytes `item` counts against the budget: bodies, or whole JSON; 0 for an end."""
    if isinstance(item, _QueuedEvent) and isinstance(item.event, Exchange):
        response = item.event.response
        return len(item.event.request.body) + (0 if response is None else len(response.body))
    if isinstance(item, _QueuedEvent | _StartRun):
        return item.json_bytes
    return 0


def _queued_error(error: ErrorInfo) -> ErrorInfo:
    """Return `error` cut to MAX_QUEUED_ERROR outside any secret; TypeError if not text."""
    return ErrorInfo(
        cut_outside_secrets(error.type, MAX_QUEUED_ERROR),
        cut_outside_secrets(error.message, MAX_QUEUED_ERROR),
    )


def _item_run_id(item: _Item) -> str:
    """Return the run id to count `item`'s drop under, else `unattributed`. Never raises."""
    if isinstance(item, _EndRun):
        return item.run_id
    named = item.record if isinstance(item, _StartRun) else item.event
    return getattr(named, "run_id", UNATTRIBUTED)
