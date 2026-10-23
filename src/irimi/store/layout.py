"""Where each file lives under a store's root, and how a run directory's files are read back.

A RUN ID NAMES A DIRECTORY ONLY WHEN THIS STORE CAN USE IT AS ONE. `trace.is_valid_run_id` is the
path-traversal guard, and it is checked again here (`names_a_run_dir`) rather than trusted.
"""

import json
import os
from pathlib import Path

from irimi import trace
from irimi.trace import UNATTRIBUTED, RunRecord, TraceFormatError

BLOBS_DIR = "blobs"
RUNS_DIR = "runs"
RUN_FILE = "run.json"
EVENTS_FILE = "events.jsonl"
# The file a `serve` holds an exclusive lock on for as long as it runs.
SERVE_LOCK_FILE = "serve.lock"

# How much of the end of an events.jsonl resuming a run reads to find its last `seq`.
RESUME_TAIL = 1024 * 1024


class StoreLayout:
    """The one statement of where each file lives under a store's root."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.blobs = root / BLOBS_DIR
        self.runs = root / RUNS_DIR
        self.unattributed_events = root / UNATTRIBUTED / EVENTS_FILE
        self.serve_lock = root / SERVE_LOCK_FILE

    def run_dir(self, run_id: str) -> Path:
        """The directory of `run_id`, which the caller has checked with `trace.is_valid_run_id`."""
        return self.runs / run_id

    def blob(self, sha256: str) -> Path:
        """The file of a body, whose digest the caller has checked against trace.SHA256_PATTERN."""
        return self.blobs / sha256


def names_a_run_dir(run_id: object) -> bool:
    """True when `run_id` may be a directory under `runs/`: `trace.is_valid_run_id`, the
    path-traversal guard, checked again rather than trusted, and not `unattributed`, which is
    the name of "no run". Anything else is stored in `unattributed/` and never opens a path.

    Public because the control endpoint refuses, with a 400, the run ids this store would only
    count as a drop: the agent is told, rather than its run silently going missing."""
    return isinstance(run_id, str) and run_id != UNATTRIBUTED and trace.is_valid_run_id(run_id)


def read_run_json(directory: Path) -> RunRecord:
    """A run directory's `run.json`, for the writer resuming a run and the reader alike. Raises
    OSError when the file cannot be read now, and TraceFormatError, and nothing else, for what it
    holds when this irimi cannot read that: a newer version, bytes that are not UTF-8 or JSON
    (ValueErrors), or JSON nested past the parser's depth (a RecursionError, which would otherwise
    escape `list_runs` and hide every other run). Every refusal names the file, a decoder's
    as well as the parser's: `'name' is a list` alone does not say which run.json it is."""
    path = directory / RUN_FILE
    data = path.read_bytes()
    try:
        return trace.run_from_json(json.loads(data.decode("utf-8")))
    except (ValueError, RecursionError) as exc:  # TraceFormatError is a ValueError
        raise TraceFormatError(f"{path}: {exc}") from exc


def resume_seq(events: Path) -> int:
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
    of the run, on every restart."""
    for line in reversed(data[: data.rfind(b"\n") + 1].splitlines()):
        try:
            seq = json.loads(line)["seq"]
        except (ValueError, KeyError, TypeError, RecursionError):
            continue
        if isinstance(seq, int) and not isinstance(seq, bool):
            return seq
    return None
