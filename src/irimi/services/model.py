"""What a per-service effect table is handed and what it hands back (#43).

Plain data, so the effects stay pure functions the Phase 5 replay engine can run over a
recording (use case 1). The `Exchange` is deliberately absent: an effect is a function of the
fields a write carried and the object irimi answered it with, not of the trace.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from irimi.exchange import Request


@dataclass(frozen=True)
class Write:
    """One faked write from the run's write log, decoded for the effects.

    `operation` is the map's own name for it (`refunds.create`). `posted` is the caller's own
    fields, as `echo.reflect` read them off the request. `answer` is the object irimi answered
    with - already carrying the minted ids, `created`, and every fixture field the caller never
    sent - which is why no effect ever mints anything of its own.
    """

    operation: str
    posted: dict[str, Any]
    answer: dict[str, Any]


@dataclass(frozen=True)
class Applied:
    """What an effect table did to one read's document.

    `changed` and `partial` are independent: a page the table cannot model is `partial` with
    nothing changed, and that is a fact the trace has to carry rather than a body edit.
    """

    document: Any
    changed: bool = False
    partial: bool = False


@dataclass(frozen=True)
class Rewritten:
    """A read request an effect table translated before it is forwarded."""

    query: str  # the new raw query string, without the leading "?"
    removed: str  # the `key=value` pair that was dropped, for the Irimi-Rewrote header


# `(operation, read request, parsed body, the run's writes for this service) -> Applied`.
ReadEffects = Callable[[str, Request, Any, Sequence[Write]], Applied]
# `(operation, read request, the run's writes for this service) -> Rewritten | None`.
QueryRewrite = Callable[[str, Request, Sequence[Write]], "Rewritten | None"]
