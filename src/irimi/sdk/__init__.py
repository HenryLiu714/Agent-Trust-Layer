"""The irimi SDK: mark where each of an agent's runs starts and ends, and label the tools the
proxy cannot see.

    @sdk.trigger
    def handle(ticket: Ticket) -> None: ...

The SDK is inert without irimi: it acts only while `IRIMI_ENGINE_ACTIVE=1`, so it is safe to leave
in production code. It never raises into the agent. This module is its whole public surface;
replaying a stored run from a test is `irimi.testing`.
"""

from irimi.sdk._context import current_run_id, is_active, propagate
from irimi.sdk._errors import RecordedToolError
from irimi.sdk._http_labels import instrument
from irimi.sdk._runs import Run, run, trigger
from irimi.sdk._tools import read_tool, write_tool

__all__ = [
    "RecordedToolError",
    "Run",
    "current_run_id",
    "instrument",
    "is_active",
    "propagate",
    "read_tool",
    "run",
    "trigger",
    "write_tool",
]
