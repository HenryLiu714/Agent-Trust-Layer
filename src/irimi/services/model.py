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
class Read:
    """One live read the effects are asked about, decoded the way `Write` is (#44).

    `posted` is the read request's own body, as `echo.reflect` read it. Stripe's reads are GETs
    that carry their parameters in the query string and post nothing, so theirs is `{}`; Slack's
    are POSTs that carry every parameter in the body, and the effects should not each have to
    know which. Reflecting it here rather than in `slack.py` is what keeps the one never-raising
    parser the only one: `echo` is in this package's own layer and cannot be imported from it.
    """

    operation: str
    request: Request
    posted: dict[str, Any]


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


@dataclass(frozen=True)
class Proposal:
    """A write irimi is about to fake, before it has an answer (#45).

    `Write` is the same write once it is in the log, with the body irimi answered it with.
    A precondition runs before there is one, which is why it gets its own shape rather than a
    `Write` with an empty `answer` that every check would then have to remember not to read.
    """

    operation: str
    posted: dict[str, Any]


@dataclass(frozen=True)
class Probe:
    """The one real read a precondition needs. The policy turns it into a request and issues it."""

    operation: str  # what the map should classify it as; the policy refuses anything not a read
    method: str
    path: str
    query: str = ""
    posted: dict[str, Any] | None = None  # a JSON body for a read-like POST (Slack); None = none


@dataclass(frozen=True)
class Rejection:
    """What the real service would have answered instead of performing the write.

    `body` is the service's own error shape, so the SDK raises its ordinary error rather than
    choking on something of irimi's. `code` is the machine name the summary prints; it is the
    body's own code where the service sends one, and irimi's label where it does not - see
    `stripe.AMOUNT_TOO_LARGE`.
    """

    status: int
    body: dict[str, Any]
    code: str


@dataclass(frozen=True)
class NotEvaluable:
    """A verdict's third answer: the probe came back and says nothing this check can read (#45).

    `Rejection` is "the service would have refused this write"; None is "it would have taken it".
    This is neither, and it is the state #45 names for a missing scope: irimi put the question and
    did not find out. Recording that as passed would claim a check that never ran - the summary
    prints `L3 preconditions passed` off it - and rejecting would invent a refusal the service
    never made. It carries nothing, because the reason belongs in the log line, not in the trace.
    """


NOT_EVALUABLE = NotEvaluable()


@dataclass(frozen=True)
class Check:
    """A precondition: the read it needs, and what it makes of the answer.

    `probe` returns None when this write cannot be checked at all - the policy records
    `not_evaluable` and fakes the write at L2. `verdict` is handed the probe's document with the
    run's own writes already applied to it, and returns a `Rejection`, `NOT_EVALUABLE`, or None
    for passed.
    """

    probe: Callable[[Proposal], "Probe | None"]
    verdict: Callable[[Proposal, Any], "Rejection | NotEvaluable | None"]


# `(read, parsed body, the run's writes for this service) -> Applied`.
ReadEffects = Callable[[Read, Any, Sequence[Write]], Applied]
# `(read, the run's writes for this service) -> Rewritten | None`.
QueryRewrite = Callable[[Read, Sequence[Write]], "Rewritten | None"]
