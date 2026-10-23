"""What irimi knows about one service, and what its effects and checks are handed and hand back.

`Service` is the whole of it: one per service with Python knowledge of its own, registered in
`services.REGISTRY`. The rest is plain data, so the effects stay pure functions a replay can run
over a recording. The `Exchange` is deliberately absent: an effect is a function of the fields a
write carried and the object irimi answered it with, not of the trace.
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from irimi.exchange import Request
from irimi.servicemap import Route

# Fields of a fixture object the service owns, which the request may therefore never write over.
# `id` is here as well as in every route's `ids:`, because a body posting `object=charge` to a
# refund route would otherwise hand stripe-python the wrong class to build, and `created` and
# `livemode` are facts about this answer rather than values the caller gets to choose.
# The read side keeps the same rule: `services.stripe.effects._customer` may not move one.
SERVICE_OWNED: frozenset[str] = frozenset({"id", "object", "created", "livemode"})


@dataclass(frozen=True)
class Write:
    """One faked write from the run's write log, decoded for the effects.

    `operation` is the map's own name for it (`refunds.create`). `posted` is the caller's own
    fields, as `bodies.reflect` read them off the request. `answer` is the object irimi answered
    with - already carrying the minted ids and every fixture field the caller never sent - which
    is why no effect ever mints anything of its own.
    """

    operation: str
    posted: dict[str, Any]
    answer: dict[str, Any]


@dataclass(frozen=True)
class Read:
    """One live read the effects are asked about, decoded the way `Write` is.

    `posted` is the read request's own body, as `bodies.reflect` read it. Stripe's reads are GETs
    that carry their parameters in the query string and post nothing, so theirs is `{}`; Slack's
    are POSTs that carry every parameter in the body, and the effects should not each have to
    know which. The caller reflects it once, with `bodies.reflect`, so no effect parses a body of
    its own.

    `translated_from` is the `key=value` query pair irimi dropped from this read before forwarding
    it (`Rewritten.removed`), and None for a read irimi forwarded as the agent sent it.
    """

    operation: str
    request: Request
    posted: dict[str, Any]
    translated_from: str | None = None


@dataclass(frozen=True)
class Applied:
    """What an effect table did to one read's document.

    `changed` and `partial` are independent: a page the table cannot model is `partial` with
    nothing changed, and that is a fact the trace has to carry rather than a body edit.

    `status` is None for every effect that edits a live body: the service's own status stands. It
    is set only when the read targets an object this run minted and the service answered
    not-found, so the effect answers the whole read, status included. An effect that sets
    `status` sets `changed` too: the body and the status are one answer.
    """

    document: Any
    changed: bool = False
    partial: bool = False
    status: int | None = None


@dataclass(frozen=True)
class Rewritten:
    """A read request an effect table translated before it is forwarded."""

    query: str  # the new raw query string, without the leading "?"
    removed: str  # the `key=value` pair that was dropped, which `Read.translated_from` carries


@dataclass(frozen=True)
class Proposal:
    """A write irimi is about to fake, before it has an answer.

    `Write` is the same write once it is in the log, with the body irimi answered it with.
    A precondition runs before there is one, which is why it gets its own shape rather than a
    `Write` with an empty `answer` that every check would then have to remember not to read.
    """

    operation: str
    posted: dict[str, Any]


@dataclass(frozen=True)
class Probe:
    """The one real read a precondition needs, named by the map's operation and the values of its
    route's `{name}` segments. The policy builds the request from the map's route and issues it,
    and refuses anything the map does not call a read."""

    operation: str
    params: dict[str, str] = field(default_factory=dict)  # `{charge}` -> `ch_1`
    query: str = ""
    posted: dict[str, Any] | None = None  # a JSON body for a read-like POST (Slack); None = none


@dataclass(frozen=True)
class Rejection:
    """What the real service would have answered instead of performing the write.

    `body` is the service's own error shape, so the SDK raises its ordinary error rather than
    choking on something of irimi's. `code` is the machine name the summary prints; it is the
    body's own code where the service sends one, and irimi's label where it does not.
    """

    status: int
    body: dict[str, Any]
    code: str


@dataclass(frozen=True)
class Idempotency:
    """How a service lets a caller say "this is the same write I already sent".

    `header` is the request header the caller puts the key in, lower-case as `wire.parse`
    normalizes it. `conflict` is what the service answers when that key comes back with different
    parameters - a `Rejection` like any other, so the SDK raises its own error rather than choking
    on something of irimi's. `replayed_header` is the `(name, value)` the service puts on the
    answer to a retry (Stripe's `Idempotent-Replayed: true`), or None when it marks none. A
    service whose `Service.idempotency` is None has no such mechanism and no store.
    """

    header: str
    conflict: Callable[[str], Rejection]  # the key the caller reused -> the service's refusal
    replayed_header: tuple[str, str] | None = None


@dataclass(frozen=True)
class NotEvaluable:
    """A verdict's third answer: the probe came back and says nothing this check can read.

    `Rejection` is "the service would have refused this write"; None is "it would have taken it".
    This is neither: irimi put the question and did not find out, as when the read lacks the
    scope to see the object. Recording that as passed would claim a check that never ran - the
    summary prints `checked against live state` off it - and rejecting would invent a refusal the
    service never made. It carries nothing, because the reason belongs in the log line, not in the
    trace.
    """


NOT_EVALUABLE = NotEvaluable()


@dataclass(frozen=True)
class Check:
    """A precondition: the read it needs, and what it makes of the answer.

    `probe` returns None when this write cannot be checked at all - the policy records
    `not_evaluable` and fakes the write at L2. `verdict` is handed the probe's document with the
    run's own writes already applied to it, and returns a `Rejection`, `NOT_EVALUABLE`, or None
    for passed.

    `currency` is the one fact this read carries that is not a verdict: the currency the
    write's amounts are denominated in, read off the same document, so the summary can print
    `refund $49.00` for a `Refund.create(charge=, amount=)` that names no currency of its own. It
    returns "" for a document that does not name one, and a check with no such fact to offer
    leaves it None.

    It is asked of the check rather than read by the policy because which key holds the currency
    is service knowledge: a `document.get("currency")` in `policy` would be a guess made on every
    service's behalf, and Slack's `conversations.info` has no such key and never will. Same reason
    `probe` and `verdict` live here and not there.
    """

    probe: Callable[[Proposal], "Probe | None"]
    verdict: Callable[[Proposal, Any], "Rejection | NotEvaluable | None"]
    currency: Callable[[Any], str] | None = None


# `(read, parsed body, the run's writes for this service) -> Applied`.
ReadEffects = Callable[[Read, Any, Sequence[Write]], Applied]
# `(read, the run's writes for this service) -> Rewritten | None`.
QueryRewrite = Callable[[Read, Sequence[Write]], "Rewritten | None"]


class AnswerClock(Protocol):
    """The sequence a policy's faked answers take their identity from, such as Slack's `ts`.

    `mint` hands out the next value; `observe_history` moves the sequence past every value a live
    read's body carries, and never raises. One per policy, so two engines never share one.
    """

    def mint(self) -> str: ...

    def observe_history(self, body: bytes) -> None: ...


# `(request, its route or None, the route's fixture object or None, the policy's clock)
# -> the body a faked write is answered with`. The object is the caller's to mutate.
WriteBody = Callable[[Request, Route | None, dict[str, Any] | None, AnswerClock], dict[str, Any]]
# `(the policy's clock, a live read's body) -> None`. Never raises.
ReadObserver = Callable[[AnswerClock, bytes], None]


@dataclass(frozen=True)
class Answer:
    """How a service's faked writes are answered, where that differs from the generic echo.

    `body` builds the JSON body of every faked write on the service, mapped or not. `literal`
    names the operations whose real answer is not JSON at all, as `(body, content type)`; those
    are answered before `body` is asked.
    """

    body: WriteBody
    literal: Mapping[str, tuple[bytes, str]] = field(default_factory=dict)


@dataclass(frozen=True)
class Service:
    """Everything irimi's Python knows about one service, keyed on its map's `service:` name.

    `operations` is every operation name this code keys on; `services.check` refuses maps that
    do not name each one, so a rename on either side fails at load rather than switching an
    effect off. A field left at its default means the service has no such knowledge: `answer`
    None is the generic echo, `idempotency` None is no store, and so on.

    `scope_headers` are the request headers that scope the service's state: a write made against
    one Stripe connected account, or one API version, says nothing about a read made against
    another, so the overlay only applies writes whose values match the read's. `probe_headers`
    carry the credential a precondition read copies from the write it checks.
    """

    name: str
    operations: frozenset[str] = frozenset()
    answer: Answer | None = None
    observe_read: ReadObserver | None = None
    effects: ReadEffects | None = None
    rewrite: QueryRewrite | None = None
    preconditions: Mapping[str, Check] = field(default_factory=dict)  # by `precondition:` name
    idempotency: Idempotency | None = None
    scope_headers: tuple[str, ...] = ()
    probe_headers: tuple[str, ...] = ("authorization",)
