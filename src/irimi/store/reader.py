"""Reads back what a `writer.DirectoryStore` wrote: the runs a root holds, and one run's events."""

import json
import logging
import stat
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from irimi import trace
from irimi.store.layout import EVENTS_FILE, RUN_FILE, StoreLayout, read_run_json
from irimi.trace import UNATTRIBUTED, BodyRef, Event, RunRecord, StoredRun, TraceFormatError

logger = logging.getLogger(__name__)


class RunNotFound(KeyError):
    """No stored run has this id."""


class StoreReader:
    """Reads what a DirectoryStore wrote under `root`."""

    def __init__(self, root: Path) -> None:
        self.layout = StoreLayout(root)

    def list_runs(self) -> list[RunRecord]:
        """Every run, newest `started_at` first and a None `started_at` last. A run.json this
        irimi cannot read - a newer schema version, damage, or one it may not open - is skipped
        with a WARNING, so one run cannot hide the others. So is one that names another run than
        its directory does: `load_run` would refuse that id, so listing it would name a run that
        cannot be shown. A store with no `runs/` holds no run; one irimi may not list
        raises its OSError."""
        records = []
        try:
            # Not `Path.is_dir`, which reads a store irimi may not enter as absent, so `runs list`
            # would say it holds no runs.
            runs = sorted(self.layout.runs.iterdir())
        except (FileNotFoundError, NotADirectoryError):
            runs = []
        for directory in runs:
            # One irimi may not read is skipped with a warning too, never silently.
            try:
                if not _holds_run_file(directory):
                    continue
                record = read_run_json(directory)
            except (TraceFormatError, OSError) as exc:
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
        `RunRecord.header`. Raises RunNotFound for an id with no run, TraceFormatError for a
        damaged run: an unparseable line other than a crash's truncated last one, a missing blob;
        and OSError for one irimi may not read, its directory or a file in it, which is not a
        missing run.

        `bodies=False` leaves every body empty and reads no blob, though each is still checked to
        exist at its size, so a missing or resized blob is damage either way. It is for a caller
        that counts events and never reads a body: `irimi runs list` would otherwise read every
        body of every run it lists, hundreds of MB for a few runs of LLM streams, to print two
        numbers."""
        get_body = self._get_body if bodies else self._check_body
        if run_id == UNATTRIBUTED:
            events = self.layout.unattributed_events
            return StoredRun(RunRecord.header(UNATTRIBUTED), self._events(events, get_body))
        if not trace.is_valid_run_id(run_id):
            raise RunNotFound(run_id)
        directory = self.layout.run_dir(run_id)
        if not _holds_run_file(directory):
            raise RunNotFound(run_id)
        record = read_run_json(directory)
        # On a case-insensitive filesystem `run1` opens `Run1/`: that is another run, not this one.
        if record.run_id != run_id:
            raise RunNotFound(run_id)
        return StoredRun(record, self._events(directory / EVENTS_FILE, get_body))

    def _events(self, path: Path, get_body: trace.GetBody) -> list[Event]:
        numbered = []
        for line, d in _lines(path):
            # Every refusal names its file and line, as a line that does not parse does: in a run
            # of thousands of events, `'name' is a list` alone cannot be found. A missing blob is
            # named by the line whose event refers to it.
            try:
                numbered.append(trace.event_from_json(d, get_body))
            except TraceFormatError as exc:
                raise TraceFormatError(f"{path}, line {line}: {exc}") from exc
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


def _holds_run_file(directory: Path) -> bool:
    """Whether a run directory holds a run.json. An OSError other than its absence is raised:
    `Path.is_file` reads one as False, which would report a run irimi may not read as missing."""
    try:
        return stat.S_ISREG((directory / RUN_FILE).stat().st_mode)
    except (FileNotFoundError, NotADirectoryError):
        return False


def _lines(path: Path) -> Iterator[tuple[int, Any]]:
    """Each line of an `events.jsonl`, parsed, after its line number from 1. A final line with no
    newline that does not parse was cut off by a crash mid-write, and is skipped; any other that
    does not parse is damage. A file that is not there holds no line; one irimi may not read
    raises its OSError, never reads as empty."""
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return
    lines = data.split(b"\n")
    for i, line in enumerate(lines):
        if not line:
            continue
        try:
            parsed = json.loads(line)
        except (ValueError, RecursionError) as exc:
            if i == len(lines) - 1:  # no newline after it: the crash's half line
                return
            raise TraceFormatError(f"{path}, line {i + 1}: {exc}") from exc
        yield i + 1, parsed
