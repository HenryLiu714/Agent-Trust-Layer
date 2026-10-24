"""The irimi SDK: mark where an agent's runs start and end, and label tools the proxy cannot see.

    @sdk.trigger
    def handle(ticket: Ticket) -> None: ...

The SDK acts only while `IRIMI_ENGINE_ACTIVE=1`, so it is safe to leave in production code.
The SDK never raises: its own failures are logged and swallowed, never raised into the agent.
To replay a stored run from a test, use `irimi.testing`.
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
