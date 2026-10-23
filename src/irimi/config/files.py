"""Owner-only files: the mode is set before any byte lands; a symlink there is never followed."""

import contextlib
import os
import secrets
from pathlib import Path
from typing import Literal

DIR_MODE = 0o700
FILE_MODE = 0o600

# How `write_private` puts `data` at `path`:
#   in_place  create or truncate `path` itself, refusing to follow a symlink there
#   atomic    write a temp file beside it and `os.replace` it in, so a reader never sees half
#   publish   hard-link a temp file to `path` unless it exists: the first racing process wins
How = Literal["in_place", "atomic", "publish"]


def private_dir(path: Path) -> None:
    """Create `path` and every missing parent with DIR_MODE, which `mkdir(parents=True)` skips."""
    missing = []
    while not path.exists():
        missing.append(path)
        path = path.parent
    for directory in reversed(missing):
        with contextlib.suppress(FileExistsError):  # another process made it first
            directory.mkdir(mode=DIR_MODE)


def write_private(
    path: Path,
    data: bytes,
    *,
    mode: int = FILE_MODE,
    how: How = "in_place",
    durable: bool = False,
) -> None:
    """Write `data` to `path` with `mode`, making its directory private first.

    `durable` fsyncs before publishing; `publish` raises OSError where hard links are unsupported.
    """
    private_dir(path.parent)
    if how == "in_place":
        _write(path, data, mode, os.O_TRUNC | os.O_NOFOLLOW, durable)
        return
    tmp = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    try:
        _write(tmp, data, mode, os.O_EXCL | os.O_NOFOLLOW, durable)
        if how == "atomic":
            os.replace(tmp, path)
        else:
            with contextlib.suppress(FileExistsError):
                os.link(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def append_line(path: Path, data: bytes) -> None:
    """Append `data`, one whole line, on an O_APPEND descriptor so processes never interleave."""
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
            # Cut off a half-written line so it cannot glue onto the next, but only while it still
            # ends the file: another process may have appended since.
            if start >= 0 and end - start == len(data) - len(view) and os.fstat(fd).st_size == end:
                os.ftruncate(fd, start)
            raise
    finally:
        os.close(fd)


def _write(path: Path, data: bytes, mode: int, flags: int, durable: bool) -> None:
    """Open with `flags` and set `mode` on the descriptor: O_CREAT's is umasked or ignored."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | flags, mode)
    with os.fdopen(fd, "wb") as f:
        os.fchmod(fd, mode)
        f.write(data)
        if durable:
            f.flush()
            os.fsync(fd)
