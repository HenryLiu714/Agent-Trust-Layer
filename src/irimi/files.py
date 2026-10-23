"""Owner-only files and directories: the CA, the redaction key and the trace store.

Every private file irimi writes goes through here, so each is created with its mode set before
any byte lands, never through a symlink at its own path, and in one of three ways (`write_private`).
"""

import os
import secrets
from pathlib import Path
from typing import Literal

DIR_MODE = 0o700
FILE_MODE = 0o600

# How `write_private` puts `data` at `path`:
#   in_place  create or truncate `path` itself, refusing to follow a symlink there;
#   atomic    write a temp file beside it and `os.replace` it in, so a reader never sees half;
#   publish   write a temp file and hard-link it to `path` unless `path` exists: the first of
#             several racing processes wins, and `path` is never seen half-written.
How = Literal["in_place", "atomic", "publish"]


def private_dir(path: Path) -> None:
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


def write_private(
    path: Path,
    data: bytes,
    *,
    mode: int = FILE_MODE,
    how: How = "in_place",
    durable: bool = False,
) -> None:
    """`data` at `path` with `mode`, its directory made private first. `durable` syncs the bytes
    before they are published, so a crash cannot leave `path` in place but empty.

    `publish` has no fallback for a filesystem without hard links: the OSError is raised, since
    `os.rename` replaces, and a loser would overwrite what the winner already read."""
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
            try:
                os.link(tmp, path)
            except FileExistsError:
                pass  # another process published first, and its file is the one
    finally:
        tmp.unlink(missing_ok=True)


def _write(path: Path, data: bytes, mode: int, flags: int, durable: bool) -> None:
    """Open with `flags` and `mode`, and fix the mode on the descriptor before writing: O_CREAT's
    mode is subject to the umask, and ignored when the file already exists."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | flags, mode)
    with os.fdopen(fd, "wb") as f:
        os.fchmod(fd, mode)
        f.write(data)
        if durable:
            f.flush()
            os.fsync(fd)
