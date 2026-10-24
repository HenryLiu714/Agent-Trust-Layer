"""The serve lock: one `serve` writes a store's root at a time.

ONE PROCESS WRITES A RUN; SEVERAL MAY SHARE A ROOT. Two `irimi shadow`s on the default root write
different run directories, and a blob is written once whatever its writer. What they can share is
`unattributed/events.jsonl`, so each line goes out as one `write` on an O_APPEND descriptor
(`files.append_line`) and two processes never interleave inside a line. Each writer numbers its own
lines, so that file may repeat a `seq` across processes; a run's own file never does. A `serve` is
the writer that fills that file, so a root has at most one: it holds `serve.lock` while it runs
(`ServeLock`), and a second one on the same root refuses to start.
"""

import fcntl
import os
from pathlib import Path

from irimi.config.files import FILE_MODE, private_dir
from irimi.record.store.layout import StoreLayout


class StoreLocked(OSError):
    """Another process holds this store's `serve.lock`: another `serve` writes it."""


class StoreBusy(StoreLocked):
    """Another irimi process is writing this store: a shadow, a replay or a serve."""


class ServeLock:
    """A root's `serve.lock`, held exclusively by this process until `release()`.

    `unattributed/events.jsonl` is where a serve files every request no run claims. Two serves
    appending to it would repeat each other's `seq`, and one resuming it would cut off the other's
    line mid-write as a crash's half line. So a root has one serve: a second one is refused at
    once, never made to wait, and a supervisor restarting serve retries until the old process has
    exited. The lock is the kernel's: it is released when the process exits, however it exits, so
    a crashed serve never leaves the root locked."""

    def __init__(self, fd: int) -> None:
        self._fd: int | None = fd

    @classmethod
    def hold(cls, root: Path) -> "ServeLock":
        """Take `root`'s `serve.lock`. Raises StoreLocked when another process holds it, and
        OSError when the file cannot be opened."""
        private_dir(root)
        fd = os.open(StoreLayout(root).serve_lock, os.O_RDWR | os.O_CREAT, FILE_MODE)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            raise StoreLocked(f"another irimi serve is writing the trace store {root}") from None
        except BaseException:
            os.close(fd)
            raise
        return cls(fd)

    def release(self) -> None:
        """Let go of the lock: closing the descriptor releases it. Idempotent."""
        fd, self._fd = self._fd, None
        if fd is not None:
            os.close(fd)


class WriterLock:
    """A lock on a root's directory, held shared by every store writing under it for as long as
    it writes, and exclusively by whatever deletes runs. An `irimi shadow` takes no `serve.lock`,
    and a run it is writing may not have ended or touched a file for hours, so this is how
    deleting runs knows that no run under the root is in progress, nor about to name a blob it
    deletes. Held by the kernel, so a crashed writer never leaves the root locked."""

    def __init__(self, fd: int) -> None:
        self._fd: int | None = fd

    @classmethod
    def share(cls, root: Path) -> "WriterLock":
        """Take `root`'s lock shared, waiting while runs are being deleted. Raises OSError when
        `root` cannot be opened."""
        return cls._take(root, fcntl.LOCK_SH)

    @classmethod
    def exclude(cls, root: Path) -> "WriterLock":
        """Take `root`'s lock exclusively. Raises StoreBusy at once when any store writes under
        it, and OSError when `root` cannot be opened."""
        try:
            return cls._take(root, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise StoreBusy(f"another irimi process is writing the trace store {root}") from None

    @classmethod
    def _take(cls, root: Path, how: int) -> "WriterLock":
        fd = os.open(root, os.O_RDONLY)
        try:
            fcntl.flock(fd, how)
        except BaseException:
            os.close(fd)
            raise
        return cls(fd)

    def release(self) -> None:
        """Let go of the lock. Idempotent."""
        fd, self._fd = self._fd, None
        if fd is not None:
            os.close(fd)
