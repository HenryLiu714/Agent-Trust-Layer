"""The store's locks: one `serve` per root, and no deleting runs while any store writes.

A second serve would repeat `seq`s in `unattributed/`. Other processes may share the root; their
O_APPEND lines never interleave.
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
    """A root's `serve.lock`, held until `release()`; the kernel frees it if the process dies."""

    def __init__(self, fd: int) -> None:
        self._fd: int | None = fd

    @classmethod
    def hold(cls, root: Path) -> "ServeLock":
        """Take `root`'s `serve.lock`, or raise StoreLocked at once if another process holds it."""
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
        """Release the lock by closing its descriptor. Idempotent."""
        fd, self._fd = self._fd, None
        if fd is not None:
            os.close(fd)


class WriterLock:
    """A lock on a store root, shared by every writing store and exclusive to deleting runs.

    A run in progress may not touch a file for hours, so this, not mtime, keeps deletion off it.
    The kernel holds it, so a crashed writer never leaves the root locked.
    """

    def __init__(self, fd: int) -> None:
        self._fd: int | None = fd

    @classmethod
    def share(cls, root: Path) -> "WriterLock":
        """Take `root`'s lock shared, waiting while runs are deleted; OSError if it cannot open."""
        return cls._take(root, fcntl.LOCK_SH)

    @classmethod
    def exclude(cls, root: Path) -> "WriterLock":
        """Take `root`'s lock exclusively: StoreBusy at once while a store writes, else OSError."""
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
