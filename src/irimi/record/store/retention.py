"""Deleting stored runs, for `irimi runs rm` and `irimi runs prune`.

- Only blobs no remaining run refers to are removed; blobs are shared by content.
- Removal is refused while any irimi process writes the store: a serve holds `serve.lock`, and
  every store writer holds the root's `lock.WriterLock`.
- A run that has not ended and no process writes is a crashed one, aged by its files' mtime.
"""

import os
import re
import shutil
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from irimi.protocol.trace import UNATTRIBUTED, RunRecord
from irimi.record.store.layout import EVENTS_FILE, RUN_FILE, StoreLayout, names_a_run_dir
from irimi.record.store.lock import ServeLock, WriterLock
from irimi.record.store.reader import StoreReader

# Any digest-shaped string in an events file counts as a reference: reading the lines as text
# keeps a blob that a damaged line still names, where decoding the events would lose it.
_DIGEST = re.compile(rb"[0-9a-f]{64}")


@dataclass(frozen=True)
class Removal:
    """What removing some runs deletes: their directories, and the blobs only they refer to."""

    runs: tuple[str, ...]
    blobs: tuple[str, ...]


def stored_run_ids(root: Path, run_ids: Iterable[str]) -> tuple[list[str], list[str]]:
    """Split `run_ids` into those with a `runs/` directory of exactly that name, and the rest."""
    layout = StoreLayout(root)
    try:
        names = {entry.name for entry in layout.runs.iterdir() if entry.is_dir()}
    except (FileNotFoundError, NotADirectoryError):
        names = set()
    held: list[str] = []
    missing: list[str] = []
    for run_id in dict.fromkeys(run_ids):
        if run_id == UNATTRIBUTED:
            found = layout.unattributed_events.parent.is_dir()
        else:
            found = names_a_run_dir(run_id) and run_id in names
        (held if found else missing).append(run_id)
    return held, missing


def older_than(root: Path, cutoff: float) -> list[str]:
    """Return the readable runs last active before `cutoff`, newest first (see `_last_moment`)."""
    layout = StoreLayout(root)
    return [
        record.run_id
        for record in StoreReader(root).list_runs()
        if _last_moment(record, layout.run_dir(record.run_id)) < cutoff
    ]


def plan(root: Path, run_ids: Iterable[str]) -> Removal:
    """Return what removing `run_ids` (all held) would delete.

    Raises as `remove` would refuse, or OSError when an events file cannot be read, so no needed
    blob is counted as unused.
    """
    with _held(root):
        return _plan(root, run_ids)


def _plan(root: Path, run_ids: Iterable[str]) -> Removal:
    layout = StoreLayout(root)
    runs = tuple(dict.fromkeys(run_ids))
    removed = {_events_of(layout, run_id) for run_id in runs}
    try:
        others = [d / EVENTS_FILE for d in layout.runs.iterdir() if d.name not in runs]
    except (FileNotFoundError, NotADirectoryError):
        others = []
    if UNATTRIBUTED not in runs:
        others.append(layout.unattributed_events)
    kept = set().union(*(_digests(path) for path in others))
    candidates = set().union(*(_digests(path) for path in removed)) - kept
    blobs = tuple(sorted(d for d in candidates if layout.blob(d).is_file()))
    return Removal(runs, blobs)


def remove(root: Path, run_ids: Iterable[str]) -> Removal:
    """Delete `run_ids` (all held) and the blobs only they refer to, under the store's locks.

    Raises StoreLocked while a serve runs, StoreBusy while another irimi process writes the
    store, or OSError; what was deleted before stays deleted.
    """
    with _held(root):
        removal = _plan(root, run_ids)
        layout = StoreLayout(root)
        for run_id in removal.runs:
            if run_id == UNATTRIBUTED:
                shutil.rmtree(layout.unattributed_events.parent)
            else:
                shutil.rmtree(layout.run_dir(run_id))
        for digest in removal.blobs:
            layout.blob(digest).unlink(missing_ok=True)
        return removal


@contextmanager
def _held(root: Path) -> Iterator[None]:
    """Hold `serve.lock`, then the writer lock; serve's first, as a serve holds both."""
    serve = ServeLock.hold(root)
    try:
        writers = WriterLock.exclude(root)
        try:
            yield
        finally:
            writers.release()
    finally:
        serve.release()


def _events_of(layout: StoreLayout, run_id: str) -> Path:
    if run_id == UNATTRIBUTED:
        return layout.unattributed_events
    return layout.run_dir(run_id) / EVENTS_FILE


def _digests(events: Path) -> set[str]:
    try:
        data = events.read_bytes()
    except (FileNotFoundError, NotADirectoryError):
        return set()
    return {match.decode() for match in _DIGEST.findall(data)}


def _last_moment(record: RunRecord, directory: Path) -> float:
    """Return when the run ended, else when its files were last written: a crash's last event."""
    if record.end is not None:
        return record.end.ended_at
    written = []
    for name in (RUN_FILE, EVENTS_FILE):
        try:
            written.append(os.stat(directory / name).st_mtime)
        except OSError:
            continue
    return max(written, default=time.time())
