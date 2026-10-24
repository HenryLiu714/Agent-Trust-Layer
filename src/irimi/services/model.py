"""The `Service` spec and its plain data: effects take no `Exchange`, so a replay can run them."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from irimi.core.exchange import Request
from irimi.servicemaps import Route

# Fixture fields a posted field never overwrites: `object=charge` breaks stripe-python's class.
SERVICE_OWNED: frozenset[str] = frozenset({"id", "object", "created", "livemode"})


@dataclass(frozen=True)
class Write:
    """One faked write from the write log: `posted` fields and irimi's `answer`, ids included."""

    operation: str
    posted: dict[str, Any]
    answer: dict[str, Any]


@dataclass(frozen=True)
class Read:
    """One live read the effects are asked about; `posted` is its reflected body (`{}` for GET)."""

    operation: str
    request: Request
    posted: dict[str, Any]
    translated_from: str | None = None  # the `Rewritten.removed` pair, if rewritten


@dataclass(frozen=True)
class Applied:
    """What an effect table did to one read's document; `changed` and `partial` are independent."""

    document: Any
    changed: bool = False
    partial: bool = False
    status: int | None = None  # replaces a not-found for an object this run minted


@dataclass(frozen=True)
class Rewritten:
    """A read request an effect table translated before it is forwarded."""

    query: str  # the new raw query string, without the leading "?"
    removed: str  # the dropped `key=value` pair


@dataclass(frozen=True)
class Proposal:
    """A write irimi is about to fake, before it has an answer (then it becomes a `Write`)."""

    operation: str
    posted: dict[str, Any]


@dataclass(frozen=True)
class Probe:
    """Irimi's check read, by map operation and `{name}` values; only a `read` route is issued."""

    operation: str
    params: dict[str, str] = field(default_factory=dict)  # `{charge}` -> `ch_1`
    query: str = ""
    posted: dict[str, Any] | None = None  # a JSON body for a read-like POST (Slack); None = none


@dataclass(frozen=True)
class Rejection:
    """The real service's refusal: its own error `body`, and the `code` the summary prints."""

    status: int
    body: dict[str, Any]
    code: str


@dataclass(frozen=True)
class Idempotency:
    """How a caller marks a retry: the lower-case key `header`, and the service's answers."""

    header: str
    conflict: Callable[[str], Rejection]  # the reused key -> the service's refusal
    replayed_header: tuple[str, str] | None = None  # e.g. `Idempotent-Replayed: true`


@dataclass(frozen=True)
class NotEvaluable:
    """A verdict meaning irimi's check read could not tell: neither passed nor rejected."""


NOT_EVALUABLE = NotEvaluable()


@dataclass(frozen=True)
class Check:
    """A precondition check: the read it needs, and a verdict on it once the run's writes apply."""

    probe: Callable[[Proposal], "Probe | None"]  # None: the write cannot be checked
    verdict: Callable[[Proposal, Any], "Rejection | NotEvaluable | None"]  # None: passed
    currency: Callable[[Any], str] | None = None  # the write's currency, "" when absent


# Both take the run's writes for this service; `Any` is the read's parsed body.
ReadEffects = Callable[[Read, Any, Sequence[Write]], Applied]
QueryRewrite = Callable[[Read, Sequence[Write]], "Rewritten | None"]


class AnswerClock(Protocol):
    """A policy's sequence for faked answers' ids (Slack's `ts`); `observe_history` never raises."""

    def mint(self) -> str: ...

    def observe_history(self, body: bytes) -> None: ...


# `(request, route, fixture object (mutable) or None, clock) -> a faked write's body`.
WriteBody = Callable[[Request, Route | None, dict[str, Any] | None, AnswerClock], dict[str, Any]]
ReadObserver = Callable[[AnswerClock, bytes], None]  # never raises


@dataclass(frozen=True)
class Answer:
    """How a service's faked writes are answered; `literal` (non-JSON answers) is checked first."""

    body: WriteBody
    literal: Mapping[str, tuple[bytes, str]] = field(default_factory=dict)


@dataclass(frozen=True)
class Service:
    """Everything irimi's Python knows about one service. A default means no such knowledge.

    name            the map's `service:` name
    operations      names this code keys on; `services.check` requires each in the map
    answer          how faked writes are answered; None for the generic echo
    observe_read    learns from each live read's body
    effects         applies the run's writes to a live read's document
    rewrite         translates a read's query before it is forwarded
    preconditions   checks, by `precondition:` name
    idempotency     the retry mechanism; None for no store
    scope_headers   headers scoping state (Stripe account); writes apply to matching reads
    probe_headers   credential headers irimi's check read copies from the write
    """

    name: str
    operations: frozenset[str] = frozenset()
    answer: Answer | None = None
    observe_read: ReadObserver | None = None
    effects: ReadEffects | None = None
    rewrite: QueryRewrite | None = None
    preconditions: Mapping[str, Check] = field(default_factory=dict)
    idempotency: Idempotency | None = None
    scope_headers: tuple[str, ...] = ()
    probe_headers: tuple[str, ...] = ("authorization",)
