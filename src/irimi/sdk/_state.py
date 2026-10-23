"""The SDK's process-wide state: the reporter every post goes through, and the warnings logged once.

One object holds both, so a test resets the SDK with one call (`reset_for_tests`) instead of
reaching into each module's globals.
"""

import logging
import threading

from irimi.sdk import _http_labels
from irimi.sdk._control_client import LOGGER_NAME, ControlClient, Reporter

logger = logging.getLogger(LOGGER_NAME)


class _SDKState:
    """The reporter, read at each post so a test can replace it, and the keys already warned of."""

    def __init__(self) -> None:
        self.reporter: Reporter = ControlClient()
        self.warned: set[str] = set()
        self._lock = threading.Lock()

    def warn_once(self, key: str, msg: str, *args: object, exc_info: bool = False) -> None:
        """Log `msg % args` at WARNING the first time `key` is warned of in this process."""
        with self._lock:
            if key in self.warned:
                return
            self.warned.add(key)
        logger.warning(msg, *args, exc_info=exc_info)


state = _SDKState()


def reset_for_tests() -> None:
    """A fresh reporter, nothing warned of yet, and every HTTP patch `instrument()` made undone."""
    state.reporter = ControlClient()
    with state._lock:
        state.warned.clear()
    _http_labels.uninstall()
