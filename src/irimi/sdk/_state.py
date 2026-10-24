"""Hold the SDK's process-wide state: the reporter for every post and the warnings logged once."""

import logging
import threading

from irimi.sdk import _http_labels
from irimi.sdk._control_client import LOGGER_NAME, ControlClient, Reporter

logger = logging.getLogger(LOGGER_NAME)


class _SDKState:
    """Hold the reporter (read at each post, so tests can replace it) and keys already warned of."""

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
    """Replace the reporter, forget past warnings and undo every HTTP patch `instrument()` made."""
    state.reporter = ControlClient()
    with state._lock:
        state.warned.clear()
    _http_labels.uninstall()
