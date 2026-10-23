"""Waiting for something another thread or process does, with a deadline instead of a sleep."""

import time
from collections.abc import Callable


def eventually(
    condition: Callable[[], object],
    timeout: float = 10.0,
    interval: float = 0.01,
    message: str = "timed out",
) -> None:
    """Return once `condition()` is true; fail with `message` if it is not by `timeout`."""
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(message)
        time.sleep(interval)
