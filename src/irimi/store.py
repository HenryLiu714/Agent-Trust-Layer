"""The trace store: where a run's record, its exchanges and its tool calls go (#70).

`TraceStore` is the seam. `NullStore` throws everything away; `DirectoryStore` writes the layout
`docs/trace-format.md` describes, and `StoreReader` reads it back.

NOTHING HERE MAY RAISE INTO ITS CALLER, AND NOTHING HERE MAY BLOCK IT. `record` is called from
mitmproxy's hooks, and a raised hook forwards the flow (CLAUDE.md). So every public method of
`DirectoryStore` only puts one item on a bounded queue, with `put_nowait`, and returns; one daemon
writer thread does all the redaction, hashing and file I/O. A full queue drops the event, and so
does a writer that fails on it. Either way the drop is counted on the store and on the event's run
(`RunRecord.dropped_events`), and a WARNING is logged on the first drop and every 1000th after it:
a recording failure never affects traffic (design §5.2).

NOTHING REACHES DISK UNREDACTED (#69). The writer thread runs `redact.redact_exchange` on every
exchange, and `redact.redact_json` on every trigger's name and args, every tool call's args and
result, and every error's message, before it hashes or writes anything. A body is redacted whole
and only then cut at MAX_STORED_BODY, so a secret straddling the cut is never stored as half a
secret. A streamed body's `stream_chunks` are cut with it, so they still split what is stored
(#71).

A RUN ID NAMES A DIRECTORY ONLY WHEN THIS STORE CAN USE IT AS ONE. `trace.is_valid_run_id` is the
path-traversal guard, and it is checked again here rather than trusted (#68). A valid id can still
collide: `Run1` and `run1` are both valid, and on a case-insensitive filesystem (macOS APFS by
default) they are one directory. The first to reach the store keeps it; an event for the other is
stored in `unattributed/` like an invalid id, and an exchange among them is flagged
`exchange.BAD_RUN_ID_FLAG`. Two runs are never merged into one directory.

WHAT IS STORED IS STILL PRIVATE. Redaction hides credentials, not the business data around them -
customers, amounts, messages - so every directory the store creates is 0700 and every file 0600, as
the CA's key is.

ONE PROCESS WRITES A RUN; SEVERAL MAY SHARE A ROOT. Two `irimi shadow`s on the default root write
different run directories, and a blob is written once whatever its writer. What they can share is
`unattributed/events.jsonl`, so each line goes out as one `write` on an O_APPEND descriptor and two
processes never interleave inside a line. Each writer numbers its own lines, so that file may repeat
a `seq` across processes; a run's own file never does.

A LONG `irimi serve` STAYS BOUNDED. The queue is capped in items and in body bytes, and a body
over MAX_REDACTED_BODY is dropped before it is queued, so the writer never redacts hundreds of MB
while holding the GIL the proxy needs. A telemetry exchange is reduced to its `TelemetrySeen` on
the caller's side, for the same reason. The files a writer touched are fsynced in batches, a run's
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
import secrets
import threading
import time
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Protocol

from irimi import __version__, redact, trace
from irimi.ca import DIR_MODE, KEY_MODE
from irimi.exchange import BAD_RUN_ID_FLAG, BODY_TRUNCATED_FLAG, Exchange, clip_chunks
from irimi.trace import (
    SCHEMA_VERSION,
    UNATTRIBUTED,
    BodyRef,
    ErrorInfo,
    Event,
    Outcome,
    RunRecord,
    TelemetrySeen,
    ToolCall,
    TraceFormatError,
)

logger = logging.getLogger(__name__)

# The longest body stored. A longer one keeps its first MAX_STORED_BODY bytes, its ref says
# `truncated: true`, and its exchange carries BODY_TRUNCATED_FLAG (#70).
MAX_STORED_BODY = 8 * 1024 * 1024
# How many items may wait for the writer. A full queue drops the event rather than block a hook.
QUEUE_SIZE = 10_000
# How many body bytes may wait for the writer, whatever the item count: 10,000 exchanges of 8 MiB
# each would otherwise hold 80 GiB. An item that would pass it is dropped, unless the queue holds
# no bodies at all, so one large exchange still reaches an idle writer.
MAX_QUEUED_BYTES = 256 * 1024 * 1024
# The writer fsyncs the files it has written once it has made this many writes (lines and blobs)
# since the last sync, whenever the queue has been idle for SYNC_IDLE_S, and on close. Counting
# writes, not files, bounds what a crash can lose: one long run appends to one file, which as a
# count of distinct files never reached 1000. And a `serve` running for weeks neither remembers
# every path it wrote nor syncs them all at shutdown (#70).
SYNC_EVERY = 1000
SYNC_IDLE_S = 1.0
# A body larger than this is not stored at all: its exchange is stored with that body empty and
# flagged BODY_TRUNCATED_FLAG, and the bytes never reach the queue or the redactor.
MAX_REDACTED_BODY = 64 * 1024 * 1024
# How much of the end of an events.jsonl resuming a run reads to find its last `seq`.
RESUME_TAIL = 1024 * 1024
# A stored file's mode: the CA key's, because the store holds data redaction does not remove.
FILE_MODE = KEY_MODE
# How long `close()` waits for the writer to drain before it counts what is left as dropped.
CLOSE_TIMEOUT_S = 10.0
# A WARNING on the first drop and on every DROP_WARNING_EVERY-th after it.
DROP_WARNING_EVERY = 1000

BLOBS_DIR = "blobs"
RUNS_DIR = "runs"
RUN_FILE = "run.json"
EVENTS_FILE = "events.jsonl"


@dataclass(frozen=True)
class StoreStats:
    """What `TraceStore.stats()` reports; the control endpoint's `GET /_irimi/health` answers
    with it (#73)."""

    queued: int  # items waiting for the writer
    written: int  # event lines written
    dropped: int  # events dropped, for any reason, since the store was built


class TraceStore(Protocol):
    """Where a run's record, exchanges and tool calls go. No method may raise (see module doc)."""

    def start_run(self, record: RunRecord) -> None: ...

    def record(self, exchange: Exchange) -> None: ...

    def record_tool_call(self, call: ToolCall) -> None: ...

    def end_run(
        self,
        run_id: str,
        ended_at: float,
        outcome: Outcome,
        error: ErrorInfo | None = None,
        exit_code: int | None = None,
    ) -> None: ...

    def stats(self) -> StoreStats: ...

    def close(self) -> None: ...


class NullStore:
    """A TraceStore that keeps nothing."""

    def start_run(self, record: RunRecord) -> None:
        return None

    def record(self, exchange: Exchange) -> None:
        return None

    def record_tool_call(self, call: ToolCall) -> None:
        return None

    def end_run(
        self,
        run_id: str,
        ended_at: float,
        outcome: Outcome,
        error: ErrorInfo | None = None,
        exit_code: int | None = None,
    ) -> None:
        return None

    def stats(self) -> StoreStats:
        return StoreStats(queued=0, written=0, dropped=0)

    def close(self) -> None:
        return None


def header_record(run_id: str, started_at: float | None = None) -> RunRecord:
    """The record of a run id that arrived on an event with no start: `attribution: "header"`,
    every optional field None. The writer creates it on the first event of a run with no
    `run.json`, and `StoreReader` gives it to `unattributed/`, which has none (#70).

    The writer passes that first event's `started_at`, the earliest moment the run is known by.
    Without it a header run sorted after every run that has a start, so `irimi runs list` hid an
    agent's labelled runs behind its process runs as soon as there were more of those than its
    `--limit` (#72)."""
    return RunRecord(
        schema_version=SCHEMA_VERSION,
        run_id=run_id,
        mode="shadow",
        attribution="header",
        trigger=None,
        agent_version=None,
        engine_version=__version__,
        sdk_version=None,
        started_at=started_at,
        ended_at=None,
        outcome=None,
        error=None,
        exit_code=None,
    )


def _read_run_json(directory: Path) -> RunRecord:
    """A run directory's `run.json`, for the writer resuming a run and the reader alike. Raises
    OSError when the file cannot be read now, and TraceFormatError, and nothing else, for what it
    holds when this irimi cannot read that: a newer version, bytes that are not UTF-8 or JSON
    (ValueErrors), or JSON nested past the parser's depth (a RecursionError, which would otherwise
    escape `list_runs` and hide every other run) (#70)."""
    path = directory / RUN_FILE
    data = path.read_bytes()
    try:
        return trace.run_from_json(json.loads(data.decode("utf-8")))
    except TraceFormatError:
        raise
    except (ValueError, RecursionError) as exc:
        raise TraceFormatError(f"{path}: {exc}") from exc


class StoreLayout:
    """The one statement of where each file lives under a store's root."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.blobs = root / BLOBS_DIR
        self.runs = root / RUNS_DIR
        self.unattributed_events = root / UNATTRIBUTED / EVENTS_FILE

    def run_dir(self, run_id: str) -> Path:
        """The directory of `run_id`, which the caller has checked with `trace.is_valid_run_id`."""
        return self.runs / run_id

    def blob(self, sha256: str) -> Path:
        """The file of a body, whose digest the caller has checked against trace.SHA256_PATTERN."""
        return self.blobs / sha256


# ------------------------------------------------------------------------------------ the writer


@dataclass(frozen=True)
class _StartRun:
    record: RunRecord


@dataclass(frozen=True)
class _EndRun:
    run_id: str
    ended_at: float
    outcome: Outcome
    error: ErrorInfo | None
    exit_code: int | None


@dataclass(frozen=True)
class _Record:
    event: Exchange | ToolCall | TelemetrySeen


_CLOSE = object()  # the sentinel `close()` enqueues last

_Item = _StartRun | _EndRun | _Record


@dataclass
class _Run:
    """What the writer knows about one run directory, or about `unattributed/` (dir None)."""

    events: Path
    dir: Path | None
    record: RunRecord | None
    next_seq: int
    writable: bool  # False: its run.json is another schema version, so nothing is appended
    # The `dropped_events` its run.json held when this process first touched it: drops of an
    # earlier process, which this one adds its own to.
    dropped_before: int = 0
    dropped_written: int = 0  # the `dropped_events` its run.json last said


class DirectoryStore:
    """A TraceStore writing the layout `docs/trace-format.md` describes under `root`."""

    def __init__(self, root: Path, key: bytes) -> None:
        """Raises OSError when `root` cannot be made a writable directory: a store that would
        silently drop every event must not start a run (the one place this class raises)."""
        _mkdirs(root)
        if not root.is_dir() or not os.access(root, os.W_OK | os.X_OK):
            raise PermissionError(f"the trace store {root} is not a writable directory")
        self.layout = StoreLayout(root)
        self._key = key
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=QUEUE_SIZE)
        self._lock = threading.Lock()  # guards everything below it and `_closed`
        self._closed = False
        self._dropped = 0
        self._written = 0
        self._queued_bytes = 0  # body bytes of the items waiting for the writer
        # By the run id the event named, or `unattributed` for one that names no run directory.
        self._run_drops: Counter[str] = Counter()
        self._runs: dict[str, _Run] = {}  # writer thread only
        self._touched: set[Path] = set()  # writer thread only: files not yet fsynced
        self._unsynced = 0  # writer thread only: writes into them since the last sync
        self._abandoned = threading.Event()  # set when `close()` gave up waiting on the writer
        self._thread = threading.Thread(target=self._write_loop, name="irimi-store", daemon=True)
        self._thread.start()

    # -- the public methods: each enqueues one item and returns ------------------------------

    def start_run(self, record: RunRecord) -> None:
        self._put(_StartRun(record))

    def record(self, exchange: Exchange) -> None:
        try:
            event = _as_queued(exchange)
        except Exception as exc:
            self._drop(
                _item_run_id(_Record(exchange)), f"it could not be queued: {type(exc).__name__}"
            )
            return
        self._put(_Record(event))

    def record_tool_call(self, call: ToolCall) -> None:
        self._put(_Record(call))

    def end_run(
        self,
        run_id: str,
        ended_at: float,
        outcome: Outcome,
        error: ErrorInfo | None = None,
        exit_code: int | None = None,
    ) -> None:
        self._put(_EndRun(run_id, ended_at, outcome, error, exit_code))

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
        # that is not even a string must not raise here, in the writer's own error path (#70).
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
                # SYNC_IDLE_S for the life of the process (#70).
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
                # and an exception's message may quote the value it choked on (#69).
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
        run = self._run_dir(record.run_id)
        trigger = record.trigger
        if trigger is not None:
            # The name is the command's argv[0], which `args` repeats, so it is redacted as they
            # are, or it would store what they hide (#69).
            trigger = replace(
                trigger,
                name=self._redact_text(trigger.name),
                args=redact.redact_json(trigger.args, self._key),
            )
        self._write_record(
            run, replace(record, trigger=trigger, error=self._redact_error(record.error))
        )

    def _end(self, end: _EndRun) -> None:
        run = self._run_dir(end.run_id)
        record = run.record or header_record(end.run_id)
        self._write_record(
            run,
            replace(
                record,
                ended_at=end.ended_at,
                outcome=end.outcome,
                error=self._redact_error(end.error),
                exit_code=end.exit_code,
            ),
        )
        self._forget(end.run_id, run)

    def _run_dir(self, run_id: str) -> _Run:
        """The writer's state for a run directory a start or an end may write. Raises ValueError -
        a drop - for an id that names no directory, before anything is opened for it, so neither
        creates one, `unattributed/` included (#70)."""
        if not names_a_run_dir(run_id):
            raise ValueError("this run id names no run directory")
        run = self._run(run_id)
        if run.dir is None or not run.writable:
            raise ValueError("this run cannot be written by this store")
        return run

    def _redact_text(self, text: str) -> str:
        """`text` with every secret shape replaced, REDACTION_FAILED when that failed (#69)."""
        redacted = redact.redact_json(text, self._key)
        return redacted if isinstance(redacted, str) else redact.REDACTION_FAILED

    def _redact_error(self, error: ErrorInfo | None) -> ErrorInfo | None:
        """An error's message redacted as text: `ErrorInfo.from_exception` keeps `str(exc)`, and
        an exception's message may quote the key it refused (#69)."""
        if error is None:
            return None
        return replace(error, message=self._redact_text(error.message))

    def _forget(self, run_id: str, run: _Run) -> None:
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
            # `0.0` is an event built with no clock: no start at all (docs/trace-format.md, #72).
            self._write_record(run, header_record(event.run_id, event.started_at or None))
        stored = self._for_disk(event, bad_run_id=run.dir is None)
        line = json.dumps(
            trace.event_to_json(run.next_seq, stored, self._put_body), allow_nan=False
        )
        _append_line(run.events, (line + "\n").encode())
        self._wrote(run.events)
        run.next_seq += 1
        with self._lock:
            self._written += 1

    def _for_disk(self, event: Exchange | ToolCall | TelemetrySeen, *, bad_run_id: bool) -> Event:
        """`event` as it may be written: redacted, and flagged."""
        if isinstance(event, TelemetrySeen):
            return event
        if isinstance(event, ToolCall):
            return replace(
                event,
                args=redact.redact_json(event.args, self._key),
                result=redact.redact_json(event.result, self._key),
                error=self._redact_error(event.error),
            )
        ex = redact.redact_exchange(event, self._key)
        flags = list(ex.flags)
        # A decoded Exchange carries only its bytes, not `BodyRef.truncated`, so a cut body says
        # so in its flags or the fact is lost on read (#68).
        bodies = (ex.request.body, b"" if ex.response is None else ex.response.body)
        if any(len(body) > MAX_STORED_BODY for body in bodies):
            flags.append(BODY_TRUNCATED_FLAG)
        if bad_run_id and event.run_id != UNATTRIBUTED:
            flags.append(BAD_RUN_ID_FLAG)
        # `_put_body` keeps the first MAX_STORED_BODY bytes, so a streamed body's chunks are cut
        # there too, and still split the body that is stored (#71).
        chunks = clip_chunks(ex.stream_chunks, MAX_STORED_BODY)
        return replace(ex, flags=tuple(dict.fromkeys(flags)), stream_chunks=chunks)

    def _put_body(self, body: bytes) -> BodyRef:
        data = body[:MAX_STORED_BODY]
        ref = trace.body_ref(data, truncated=len(body) > MAX_STORED_BODY)
        path = self.layout.blob(ref.sha256)
        # Content-addressed, so the same bytes are written once - unless a crash tore the file
        # before any fsync, which its size gives away, and then it is written again.
        if not path.exists() or path.stat().st_size != len(data):
            _write_atomically(path, data)
            self._wrote(path)
        return ref

    def _run(self, run_id: str) -> _Run:
        """The writer's state for `run_id`, made on first touch. A run id the store cannot use as
        a directory, and `trace.UNATTRIBUTED` itself, share `unattributed/`."""
        if not isinstance(run_id, str):
            # A line holding it would be one no reader decodes, damaging `unattributed/` for good
            # (#70).
            raise TypeError("a run id is a string")
        if not names_a_run_dir(run_id):
            # Not remembered under its own id: a stream of distinct bad ids must not grow the map.
            return self._unattributed()
        known = self._runs.get(run_id)
        if known is not None:
            return known
        if self._clashes(run_id):
            run = self._unattributed()
        else:
            directory = self.layout.run_dir(run_id)
            run = self._open(directory / EVENTS_FILE, directory)
        self._runs[run_id] = run
        return run

    def _unattributed(self) -> _Run:
        run = self._runs.get(UNATTRIBUTED)
        if run is None:
            run = self._runs[UNATTRIBUTED] = self._open(self.layout.unattributed_events, None)
        return run

    def _clashes(self, run_id: str) -> bool:
        """True when `run_id`'s directory already exists under another spelling: `run1` on a
        case-insensitive filesystem where `Run1` got there first."""
        directory = self.layout.run_dir(run_id)
        return directory.exists() and run_id not in os.listdir(self.layout.runs)

    def _open(self, events: Path, directory: Path | None) -> _Run:
        """Start tracking a run. An existing directory - a serve-mode restart - resumes its `seq`
        from its last readable line, after truncating a half-written one, and is never appended
        to when its run.json is another schema version (docs/trace-format.md, "Versioning")."""
        _mkdirs(events.parent)
        record: RunRecord | None = None
        writable = True
        if directory is not None and (directory / RUN_FILE).exists():
            # An OSError - too many open files, say - is left to drop this one event: refusing the
            # run for good would drop every later one of a run that is only unreadable for now
            # (#70).
            try:
                record = _read_run_json(directory)
                writable = record.schema_version == SCHEMA_VERSION
            except TraceFormatError:  # a newer version, or one damage leaves no version to read
                writable = False
        # A run this store will not append to is not touched at all, its half line included.
        run = _Run(events, directory, record, _resume_seq(events) if writable else 1, writable)
        if record is not None:
            run.dropped_before = run.dropped_written = record.dropped_events
        return run

    def _write_record(self, run: _Run, record: RunRecord) -> None:
        """`record` as `run`'s run.json. Its `schema_version` is this writer's, whatever the caller
        said, since this writer's lines follow it. It is synced before it replaces the last one,
        not batched: a run.json a crash left empty would read as no version at all, and a restart
        would never append to that run again (`_open`). A run writes it a handful of times. The
        rename is not synced, which would take an fsync of the directory: a crash may lose the
        newest run.json and keep the one before it, whole - a run that reads as not ended, never
        as damage (#70)."""
        assert run.dir is not None
        record = replace(
            record,
            schema_version=SCHEMA_VERSION,
            dropped_events=self._dropped_in(run, record.run_id),
        )
        data = json.dumps(trace.run_to_json(record), allow_nan=False).encode()
        _write_atomically(run.dir / RUN_FILE, data, durable=True)
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

    def _dropped_in(self, run: _Run, run_id: str) -> int:
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
                    self._write_record(run, run.record or header_record(run_id))
            except Exception as exc:
                logger.warning(
                    "irimi: the trace store could not record run %r's drops: %s",
                    run_id,
                    type(exc).__name__,
                )


def _as_queued(exchange: Exchange) -> Exchange | TelemetrySeen:
    """What `record` queues, at O(1) cost on the caller's thread. Telemetry is forwarded and its
    requests and responses are never stored: one line per exchange records only that it happened
    and to which host, so a stored run's summary can count it (#70). A body over MAX_REDACTED_BODY
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
        stream_chunks = ()  # an empty body has nothing to split (#71)
    if request is exchange.request and response is exchange.response:
        return exchange
    flags = tuple(dict.fromkeys((*exchange.flags, BODY_TRUNCATED_FLAG)))
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
    """The body bytes `item` holds while it waits: an exchange's two bodies, nothing else."""
    if isinstance(item, _Record) and isinstance(item.event, Exchange):
        response = item.event.response
        return len(item.event.request.body) + (0 if response is None else len(response.body))
    return 0


def _item_run_id(item: _Item) -> str:
    """The run id `item` names, to count its drop under. `unattributed` when the caller handed the
    store something that names none: counting a drop is the error path, and must not raise (#70)."""
    if isinstance(item, _EndRun):
        return item.run_id
    named = item.record if isinstance(item, _StartRun) else item.event
    return getattr(named, "run_id", UNATTRIBUTED)


def _mkdirs(path: Path) -> None:
    """`path` and every missing parent, each DIR_MODE. `Path.mkdir(parents=True)` would give the
    parents the umask's mode instead."""
    missing = []
    while not path.exists():
        missing.append(path)
        path = path.parent
    for directory in reversed(missing):
        try:
            directory.mkdir(mode=DIR_MODE)
        except FileExistsError:
            pass  # another process made it first


def names_a_run_dir(run_id: object) -> bool:
    """True when `run_id` may be a directory under `runs/`: `trace.is_valid_run_id`, the
    path-traversal guard, checked again rather than trusted (#68), and not `unattributed`, which is
    the name of "no run". Anything else is stored in `unattributed/` and never opens a path.

    Public because the control endpoint refuses, with a 400, the run ids this store would only
    count as a drop: the agent is told, rather than its run silently going missing (#73)."""
    return isinstance(run_id, str) and run_id != UNATTRIBUTED and trace.is_valid_run_id(run_id)


def _append_line(path: Path, data: bytes) -> None:
    """`data`, one whole line, in one `write` on an O_APPEND descriptor, so two processes appending
    to one file never interleave inside a line."""
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, FILE_MODE)
    try:
        start = end = -1  # where this line's bytes begin and end, once the disk took any
        view = memoryview(data)
        try:
            while view:
                written = os.write(fd, view)
                end = os.lseek(fd, 0, os.SEEK_CUR)
                start = end - written if start < 0 else start
                view = view[written:]
        except OSError:
            # A line the disk refused half-way would glue itself onto the next one and damage
            # the run for every later read: cut it off again before reporting the drop. Only
            # this line's own bytes, and only while they still end the file: `unattributed/` is
            # shared, and a line another process appended meanwhile is not this one's to cut.
            # A `write` that raised wrote nothing, so a line the disk never took is left alone
            # (#70).
            if start >= 0 and end - start == len(data) - len(view) and os.fstat(fd).st_size == end:
                os.ftruncate(fd, start)
            raise
    finally:
        os.close(fd)


def _write_atomically(path: Path, data: bytes, *, durable: bool = False) -> None:
    """`data` at `path`, FILE_MODE, through a temp file in the same directory and `os.replace`, so
    a reader never sees half of it. `durable` syncs the temp file first, so a crash cannot leave
    `path` renamed into place but empty."""
    _mkdirs(path.parent)
    tmp = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    try:
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, FILE_MODE)
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            if durable:
                f.flush()
                os.fsync(fd)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _resume_seq(events: Path) -> int:
    """The `seq` after the last readable line of `events`, 1 for a new file. Only the last
    RESUME_TAIL bytes are read, unless no whole readable line is inside them. A final line with no
    newline was cut off by a crash: it is truncated away, so the next line starts clean."""
    if not events.exists():
        return 1
    size = events.stat().st_size
    start = max(0, size - RESUME_TAIL)
    with open(events, "rb") as f:
        f.seek(start)
        data = f.read()
    last = _last_seq(data)
    if last is None and start:  # the tail holds no whole line: read them all
        start, data = 0, events.read_bytes()
        last = _last_seq(data)
    complete = start + data.rfind(b"\n") + 1
    if complete < size:
        os.truncate(events, complete)
    return (last or 0) + 1


def _last_seq(data: bytes) -> int | None:
    """The `seq` of the last whole line in `data` that parses and holds an integer `seq`, or None
    when none does. A tail's first line may start mid-line, and a damaged line may hold anything:
    `{"seq": 1e400}` is infinity, which `int()` refuses with an OverflowError, and a line nested
    past the parser's depth is a RecursionError. Either one escaping would drop every later event
    of the run, on every restart (#70)."""
    for line in reversed(data[: data.rfind(b"\n") + 1].splitlines()):
        try:
            seq = json.loads(line)["seq"]
        except (ValueError, KeyError, TypeError, RecursionError):
            continue
        if isinstance(seq, int) and not isinstance(seq, bool):
            return seq
    return None


# ------------------------------------------------------------------------------------ the reader


class RunNotFound(KeyError):
    """No stored run has this id."""


@dataclass(frozen=True)
class StoredRun:
    record: RunRecord
    events: list[Event]  # in `seq` order, bodies loaded from blobs/, or empty: see `load_run`


class StoreReader:
    """Reads what a DirectoryStore wrote under `root`."""

    def __init__(self, root: Path) -> None:
        self.layout = StoreLayout(root)

    def list_runs(self) -> list[RunRecord]:
        """Every run, newest `started_at` first and a None `started_at` last. A run.json this
        irimi cannot read - a newer schema version - is skipped with a WARNING, so one run
        cannot hide the others. So is one that names another run than its directory does:
        `load_run` would refuse that id, so listing it would name a run that cannot be shown
        (#70)."""
        records = []
        runs = sorted(self.layout.runs.iterdir()) if self.layout.runs.is_dir() else []
        for directory in runs:
            if not (directory / RUN_FILE).is_file():
                continue
            try:
                record = self._record(directory)
            except TraceFormatError as exc:
                logger.warning("irimi: skipping stored run %s: %s", directory.name, exc)
                continue
            if record.run_id != directory.name:
                logger.warning(
                    "irimi: skipping stored run %s: its run.json names another run", directory.name
                )
                continue
            records.append(record)
        return sorted(
            records,
            key=lambda r: (r.started_at is None, -(r.started_at or 0.0), r.run_id),
        )

    def load_run(self, run_id: str, *, bodies: bool = True) -> StoredRun:
        """One run, its events in `seq` order. `UNATTRIBUTED` gives the unattributed events under
        `header_record`. Raises RunNotFound for an id with no run, and TraceFormatError for a
        damaged run: an unparseable line other than a crash's truncated last one, a missing blob.

        `bodies=False` leaves every body empty and reads no blob, though each is still checked to
        exist at its size, so a missing or resized blob is damage either way. It is for a caller
        that counts events and never reads a body: `irimi runs list` read every body of every run
        it listed, hundreds of MB for a few runs of LLM streams, to print two numbers (#72)."""
        get_body = self._get_body if bodies else self._check_body
        if run_id == UNATTRIBUTED:
            events = self.layout.unattributed_events
            return StoredRun(header_record(UNATTRIBUTED), self._events(events, get_body))
        if not trace.is_valid_run_id(run_id):
            raise RunNotFound(run_id)
        directory = self.layout.run_dir(run_id)
        if not (directory / RUN_FILE).is_file():
            raise RunNotFound(run_id)
        record = self._record(directory)
        # On a case-insensitive filesystem `run1` opens `Run1/`: that is another run, not this one.
        if record.run_id != run_id:
            raise RunNotFound(run_id)
        return StoredRun(record, self._events(directory / EVENTS_FILE, get_body))

    def _record(self, directory: Path) -> RunRecord:
        try:
            return _read_run_json(directory)
        except OSError as exc:
            raise TraceFormatError(f"{directory / RUN_FILE}: {exc}") from exc

    def _events(self, path: Path, get_body: trace.GetBody) -> list[Event]:
        numbered = [trace.event_from_json(d, get_body) for d in _lines(path)]
        return [event for _, event in sorted(numbered, key=lambda pair: pair[0])]

    def _get_body(self, ref: BodyRef) -> bytes:
        try:
            data = self.layout.blob(ref.sha256).read_bytes()
        except OSError as exc:
            raise TraceFormatError(f"blob {ref.sha256} is missing: {exc}") from exc
        if len(data) != ref.size:
            raise TraceFormatError(f"blob {ref.sha256} holds {len(data)} bytes, not {ref.size}")
        return data

    def _check_body(self, ref: BodyRef) -> bytes:
        """`_get_body`'s checks without reading the blob: it exists, at the size its ref says."""
        try:
            size = self.layout.blob(ref.sha256).stat().st_size
        except OSError as exc:
            raise TraceFormatError(f"blob {ref.sha256} is missing: {exc}") from exc
        if size != ref.size:
            raise TraceFormatError(f"blob {ref.sha256} holds {size} bytes, not {ref.size}")
        return b""


def _lines(path: Path) -> Iterator[Any]:
    """Each line of an `events.jsonl`, parsed. A final line with no newline that does not parse
    was cut off by a crash mid-write, and is skipped; any other that does not parse is damage."""
    if not path.exists():
        return
    data = path.read_bytes()
    lines = data.split(b"\n")
    for i, line in enumerate(lines):
        if not line:
            continue
        try:
            yield json.loads(line)
        except (ValueError, RecursionError) as exc:
            if i == len(lines) - 1:  # no newline after it: the crash's half line
                return
            raise TraceFormatError(f"{path}, line {i + 1}: {exc}") from exc
