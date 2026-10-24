"""Deleting stored runs, for `irimi runs rm` and `irimi runs prune`.

A run's bodies are content-addressed blobs that other runs may share, so removing a run removes
only the blobs no remaining run refers to. Removal is refused while any irimi process writes the
store: a serve holds `serve.lock`, and every store writing under the root holds its
`lock.WriterLock`, so a run in progress is never deleted, nor a blob a writer is about to name
again. A run that has not ended and that no process writes is one whose irimi crashed; pruning
judges its age by when its files were last written.
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
    """`run_ids` split into those the store at `root` holds and those it does not. A run id is
    held when `runs/` has a directory of exactly that name, so a damaged run can still be removed,
    and `run1` never names `Run1/` on a case-insensitive filesystem. `unattributed` is held when
    any event went there."""
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
    """The runs whose last known moment is before `cutoff`, newest first: the end of an ended run,
    else when its run.json or events file was last written. A run.json irimi cannot read is
    skipped, as `StoreReader.list_runs` skips it; `irimi runs rm` still takes it by id."""
    layout = StoreLayout(root)
    return [
        record.run_id
        for record in StoreReader(root).list_runs()
        if _last_moment(record, layout.run_dir(record.run_id)) < cutoff
    ]


def plan(root: Path, run_ids: Iterable[str]) -> Removal:
    """What removing `run_ids`, each one the store holds, deletes, without deleting it. Raises
    StoreLocked when `remove` would be refused, and OSError when an events file cannot be read,
    so no blob another run needs is ever counted as unused."""
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
    """Delete `run_ids`, each one the store holds, and the blobs only they refer to, holding the
    store's locks throughout. Raises StoreLocked while a serve holds `serve.lock`, StoreBusy while
    any other irimi process writes the store, and OSError when a file cannot be read or deleted;
    whatever was deleted before that stays deleted."""
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
    """`root`'s `serve.lock`, then its writer lock exclusively, until the block ends: the serve's
    refusal first, since a serve holds both."""
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
    """When the run ended, or for one that has not, when any of its files was last written: a
    crashed run is as old as its last event, however long ago it started."""
    if record.end is not None:
        return record.end.ended_at
    written = []
    for name in (RUN_FILE, EVENTS_FILE):
        try:
            written.append(os.stat(directory / name).st_mtime)
        except OSError:
            continue
    return max(written, default=time.time())
