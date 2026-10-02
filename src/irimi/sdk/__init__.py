"""The irimi SDK: tell irimi where each of an agent's runs starts and ends (#74).

`irimi shadow -- <cmd>` knows one process as one run. A server-style agent handles many requests
in one long-lived process, often at once, and the proxy alone sees one interleaved stream of HTTP
calls. The SDK marks each run in the agent's own code:

    from irimi import sdk

    @sdk.trigger
    def handle(ticket: Ticket) -> None: ...

    with sdk.run(trigger={"date": "2026-09-29"}, name="nightly"): ...

It reports each run's start (with what triggered it) and its end to irimi's control endpoint, and
keeps the run's id in a context variable that #75 puts on every request the run makes.

THE SDK IS INERT WITHOUT IRIMI. It is active only while `IRIMI_ENGINE_ACTIVE=1`, which `irimi
shadow` sets for its child; otherwise a trigger or a run calls straight through, so the SDK is
safe to leave in production code. And it NEVER RAISES INTO THE AGENT: a control endpoint it
cannot reach is a warning on the `irimi.sdk` logger, and the run goes on.

Imports only the stdlib and irimi's lowest layers, never mitmproxy, so it can ship on its own.
"""

from irimi.sdk.api import run, trigger
from irimi.sdk.context import current_run_id, propagate
from irimi.sdk.instrumentation import instrument
from irimi.sdk.runs import active

__all__ = ["active", "current_run_id", "instrument", "propagate", "run", "trigger"]
