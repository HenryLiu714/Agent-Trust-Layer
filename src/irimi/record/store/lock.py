"""The serve lock: one `serve` per store root, as two would repeat `seq`s in `unattributed/`.

Other processes may share the root; their O_APPEND lines never interleave.
"""

import fcntl
import os
from pathlib import Path

from irimi.config.files import FILE_MODE, private_dir
from irimi.record.store.layout import StoreLayout


class StoreLocked(OSError):
    """Another process holds this store's `serve.lock`: another `serve` writes it."""


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
