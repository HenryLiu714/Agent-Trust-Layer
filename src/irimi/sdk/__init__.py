"""The irimi SDK: tell irimi where each of an agent's runs starts and ends.

`irimi shadow -- <cmd>` knows one process as one run. A server-style agent handles many requests
in one long-lived process, often at once, and the proxy alone sees one interleaved stream of HTTP
calls. The SDK marks each run in the agent's own code:

    from irimi import sdk

    @sdk.trigger
    def handle(ticket: Ticket) -> None: ...

    with sdk.run(trigger={"date": "2026-09-29"}, name="nightly"): ...

It reports each run's start (with what triggered it) and its end to irimi's control endpoint, and
keeps the run's id in a context variable that `instrument()` puts on every request the run makes.

A tool the proxy cannot see, because it is not HTTP, is labelled read or write; a write names the
stand-in that runs in its place under irimi, and each call is recorded in its run:

    @sdk.tool(kind="write", shadow=mock_database_write)
    def database_write(row: Row) -> None: ...

In a replay, a read tool is answered from the recording when it can be, and what it raised then
comes back as `sdk.RecordedToolError`. `sdk.replay(run_id)` replays a stored run from a test,
as `irimi replay` does, and returns a `ReplayResult`.

THE SDK IS INERT WITHOUT IRIMI. It is active only while `IRIMI_ENGINE_ACTIVE=1`, which `irimi
shadow` sets for its child; otherwise a trigger or a run calls straight through, so the SDK is
safe to leave in production code. And it NEVER RAISES INTO THE AGENT: a control endpoint it
cannot reach is a warning on the `irimi.sdk` logger, and the run goes on.

Imports only the stdlib and irimi's lowest layers, never mitmproxy, so it can ship on its own.
"""

from irimi.sdk.api import run, trigger
from irimi.sdk.context import active, current_run_id, propagate
from irimi.sdk.errors import RecordedToolError
from irimi.sdk.instrumentation import instrument
from irimi.sdk.replays import ReplayResult, replay
from irimi.sdk.tools import tool

__all__ = [
    "RecordedToolError",
    "ReplayResult",
    "active",
    "current_run_id",
    "instrument",
    "propagate",
    "replay",
    "run",
    "tool",
    "trigger",
]
