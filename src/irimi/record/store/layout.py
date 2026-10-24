"""Where each file lives under a store's root, and how a run directory's files are read back."""

import json
import os
from pathlib import Path

from irimi.protocol import trace
from irimi.protocol.trace import UNATTRIBUTED, RunRecord, TraceFormatError

BLOBS_DIR = "blobs"
RUNS_DIR = "runs"
RUN_FILE = "run.json"
EVENTS_FILE = "events.jsonl"
SERVE_LOCK_FILE = "serve.lock"

# How much of the end of an events.jsonl resuming a run reads to find its last `seq`.
RESUME_TAIL = 1024 * 1024


class StoreLayout:
    """Where each file lives under a store's root."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.blobs = root / BLOBS_DIR
        self.runs = root / RUNS_DIR
        self.unattributed_events = root / UNATTRIBUTED / EVENTS_FILE
        self.serve_lock = root / SERVE_LOCK_FILE

    def run_dir(self, run_id: str) -> Path:
        """Return `run_id`'s directory; the caller has checked it with `trace.is_valid_run_id`."""
        return self.runs / run_id

    def blob(self, sha256: str) -> Path:
        """Return a body's file; the caller has checked the digest against `SHA256_PATTERN`."""
        return self.blobs / sha256


def names_a_run_dir(run_id: object) -> bool:
    """Return True when `run_id` is a safe name for a directory in `runs/` (not `unattributed`)."""
    return isinstance(run_id, str) and run_id != UNATTRIBUTED and trace.is_valid_run_id(run_id)


def read_run_json(directory: Path) -> RunRecord:
    """Return a run directory's `run.json`.

    Raises OSError, or TraceFormatError naming the file for any content this irimi cannot read.
    """
    path = directory / RUN_FILE
    data = path.read_bytes()
    try:
        return trace.run_from_json(json.loads(data.decode("utf-8")))
    except (ValueError, RecursionError) as exc:  # TraceFormatError is a ValueError
        raise TraceFormatError(f"{path}: {exc}") from exc


def resume_seq(events: Path) -> int:
    """Return the next `seq` for `events` (1 if new), truncating a final line a crash cut off."""
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
    """Return the `seq` of the last whole line in `data` that parses, or None. Never raises."""
    for line in reversed(data[: data.rfind(b"\n") + 1].splitlines()):
        try:
            seq = json.loads(line)["seq"]
        except (ValueError, KeyError, TypeError, RecursionError):
            continue
        if isinstance(seq, int) and not isinstance(seq, bool):
            return seq
    return None
