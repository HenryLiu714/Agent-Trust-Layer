"""Engine-independent record of one HTTP exchange."""

import json
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any, Literal, Protocol

Kind = Literal["read", "write", "llm", "telemetry", "unknown"]
# How a faked write's body was built:
#   fake-L0    echo: the request's own fields reflected back (the default for every route)
#   fake-L1    fixture: the route's map names a vendored response object to start from
FakeLevel = Literal["fake-L0", "fake-L1"]
# Who produced the response the agent received:
#   live       the real service, forwarded unchanged
#   fake-L0/1  irimi, a faked write (see FakeLevel)
#   delegated  sent to a target, which answered
#   overlay    a live read irimi edited to show the run's faked writes
#   recording  a replay, resending a stored live answer byte for byte
#   irimi      irimi's own error answer: a refusal, failed decision or unreachable target
# Other replayed answers keep their recorded value and carry `from-recording`.
AnsweredBy = Literal["live", "fake-L0", "fake-L1", "delegated", "overlay", "recording", "irimi"]
# How much of the write log the overlay could express in a read:
#   full       the whole modelled effect
#   partial    the read is incomplete and irimi could not fix it (set even on an unchanged body)
OverlayCoverage = Literal["full", "partial"]
# What the precondition check said before irimi faked a write (None: nothing was asked):
#   passed / rejected   the check's two real answers
#   not_evaluable       irimi could not find out (429, network, unparseable or partial state)
PreconditionOutcome = Literal["passed", "rejected", "not_evaluable"]
# Who asked for the exchange:
#   agent      the agent itself
#   engine     irimi's check read; counted apart so "reads are real" means the agent's reads
IssuedBy = Literal["agent", "engine"]
# How the request reached irimi: "forward" proxied, "reverse" a base-URL request.
Door = Literal["forward", "reverse"]

KINDS: tuple[Kind, ...] = ("read", "write", "llm", "telemetry", "unknown")
# Here, not in policy: the map loader refuses a live `default_kind` and may not import policy.
LIVE_KINDS: tuple[Kind, ...] = ("read", "llm", "telemetry")
# `unknown` is included because an unclassified DELETE is a presumed write.
WRITE_KINDS: frozenset[Kind] = frozenset(KINDS) - frozenset(LIVE_KINDS)
SAFE_METHODS: frozenset[str] = frozenset({"GET", "HEAD", "OPTIONS"})
# A route's `match.method` when it names none: it matches every verb, including destructive ones.
ANY_METHOD = "*"


class Flag(StrEnum):
    """Every value that may appear in `Exchange.flags`; each is added at most once per exchange."""

    # The classifier could not say what this is; `kind` is `unknown` and the answer is faked.
    UNCLASSIFIED = "unclassified"
    # The scope rule refused a live kind because the route did not name this method.
    DOWNGRADED = "kind-downgraded"
    # A target was chosen and could not be reached or applied; the agent got a 502.
    TARGET_FAILED = "target-failed"
    # The decision itself raised, so irimi answered with a 502 before choosing anything.
    DECISION_FAILED = "decision-failed"
    # The request hook refused it: unparseable, unroutable, unrelayed base-URL host, unrewritable.
    REFUSED = "refused"
    # A live forward lost the real service before it answered.
    UPSTREAM_ERROR = "upstream-error"
    # The route names a `fixture:` this install could not read, so it was answered with an echo.
    FIXTURE_FAILED = "fixture-failed"
    # A retry with a used idempotency key and the same parameters got the first answer's bytes.
    IDEMPOTENT_REPLAY = "idempotent-replay"
    # A used key with different parameters, answered with the service's `idempotency_error`.
    IDEMPOTENCY_CONFLICT = "idempotency-conflict"
    # Part of the stored copy could not be redacted; set only on the copy written to disk.
    REDACTION_FAILED = "redaction-failed"
    # Only the stored body is cut: at `MAX_STORED_BODY`, or emptied past `MAX_DECODED_BODY`.
    BODY_TRUNCATED = "body-truncated"
    # The run id is unusable as a directory name, so the store filed it under `unattributed/`.
    BAD_RUN_ID = "bad-run-id"
    # The stored stream is incomplete; the agent still got every chunk that arrived.
    STREAM_TRUNCATED = "stream-truncated"
    # A replay found no recorded answer and handled the request as shadow mode would.
    REPLAY_MISS = "replay-miss"
    # A replay answered from the recording, keeping the recorded `answered_by` (live -> recording).
    FROM_RECORDING = "from-recording"


IDEMPOTENCY_FLAGS: tuple[Flag, ...] = (Flag.IDEMPOTENT_REPLAY, Flag.IDEMPOTENCY_CONFLICT)

Headers = tuple[tuple[str, str], ...]


def header_value(headers: Headers, name: str) -> str | None:
    """Return the first value of header `name`, compared case-insensitively, or None."""
    name = name.lower()
    for key, value in headers:
        if key.lower() == name:
            return value
    return None


def without_header(headers: Headers, name: str) -> Headers:
    """Return `headers` minus every header called `name`, compared case-insensitively."""
    name = name.lower()
    return tuple((k, v) for k, v in headers if k.lower() != name)


def media_type(content_type: str) -> str:
    """Return a Content-Type value without its parameters, lower-cased."""
    return content_type.partition(";")[0].strip().lower()


@dataclass(frozen=True)
class Request:
    method: str  # upper-case
    scheme: str  # "http" | "https"
    host: str  # lower-case, no port
    port: int
    path: str  # path only, no query, always starts with "/"
    query: str  # raw query string without the leading "?", "" if none
    headers: Headers
    body: bytes

    @property
    def url(self) -> str:
        default = 443 if self.scheme == "https" else 80
        netloc = self.host if self.port == default else f"{self.host}:{self.port}"
        return f"{self.scheme}://{netloc}{self.path_and_query}"

    @property
    def path_and_query(self) -> str:
        """Return the path, then `?query` when there is one."""
        return f"{self.path}?{self.query}" if self.query else self.path

    def header(self, name: str) -> str | None:
        """See `header_value`."""
        return header_value(self.headers, name)

    def without_header(self, name: str) -> "Request":
        """Return the request minus every `name` header; the same object if it had none."""
        kept = without_header(self.headers, name)
        return self if len(kept) == len(self.headers) else replace(self, headers=kept)


@dataclass(frozen=True)
class Response:
    status: int
    headers: Headers
    body: bytes

    @classmethod
    def json(cls, status: int, doc: Mapping[str, Any], headers: Headers = ()) -> "Response":
        """Return `doc` as a JSON response. Raises ValueError for values like NaN."""
        body = json.dumps(doc, allow_nan=False).encode()
        return cls(status, (("content-type", "application/json"), *headers), body)

    def header(self, name: str) -> str | None:
        """See `header_value`."""
        return header_value(self.headers, name)

    def without_header(self, name: str) -> "Response":
        """Return the response minus every `name` header; the same object if it had none."""
        kept = without_header(self.headers, name)
        return self if len(kept) == len(self.headers) else replace(self, headers=kept)


@dataclass(frozen=True)
class Precondition:
    """What the precondition check said of one write."""

    outcome: PreconditionOutcome | None = None  # None: nothing was asked
    rejection_code: str = ""  # the summary's code for a write the real service would refuse
    currency: str = ""  # from irimi's check read, used when the request names none; never guessed


@dataclass(frozen=True)
class WriteOutcome:
    """The precondition check's result and the webhooks a faked write would have fired."""

    precondition: Precondition = Precondition()
    would_fire: tuple[str, ...] = ()  # accepted faked writes only, not rejections or retries


@dataclass(frozen=True)
class Exchange:
    request: Request
    response: Response | None
    service: str
    operation: str
    kind: Kind
    answered_by: AnsweredBy
    run_id: str
    door: Door = "forward"
    flags: tuple[Flag, ...] = field(default_factory=tuple)
    target: str = ""  # a `delegated` exchange's target address; `target: self` has none
    overlay: OverlayCoverage | None = None  # None: the overlay did not consider this exchange
    precondition: Precondition = Precondition()
    issued_by: IssuedBy = "agent"
    would_fire: tuple[str, ...] = field(default_factory=tuple)  # see WriteOutcome
    started_at: float = 0.0  # `time.time()`; 0.0 only when built outside the engine
    ended_at: float = 0.0
    stream_chunks: tuple[int, ...] = field(default_factory=tuple)  # sizes, summing to the body

    def __post_init__(self) -> None:
        # The wall clock can step back mid-exchange, and a stored run's reader subtracts the two.
        if self.ended_at < self.started_at:
            object.__setattr__(self, "ended_at", self.started_at)


def clip_chunks(chunks: tuple[int, ...], size: int) -> tuple[int, ...]:
    """Return the chunk lengths covering the first `size` bytes, for a body the store cut short."""
    if sum(chunks) <= size:
        return chunks
    kept: list[int] = []
    room = size
    for length in chunks:
        if room <= 0:
            break
        kept.append(min(length, room))
        room -= length
    return tuple(kept)


def is_authored_write(exchange: Exchange) -> bool:
    """Return True if irimi faked this write itself, so later reads may show its effect.

    A run's write log holds exactly these. Excluded: live kinds; live, target and irimi answers;
    `decision-failed` (schema 2 recorded that 502 as `fake-L0`); rejections; idempotent retries
    and conflicts, which would apply one write twice or a write that never happened.
    """
    return (
        exchange.kind not in LIVE_KINDS
        and exchange.answered_by not in ("live", "delegated", "irimi")
        and Flag.DECISION_FAILED not in exchange.flags
        and exchange.precondition.outcome != "rejected"
        and not any(flag in exchange.flags for flag in IDEMPOTENCY_FLAGS)
    )


def is_summary_write(exchange: Exchange) -> bool:
    """Return True for a write answered by irimi or a target, except a retry: one summary line."""
    return (
        exchange.kind in WRITE_KINDS
        and exchange.answered_by != "live"
        and Flag.IDEMPOTENT_REPLAY not in exchange.flags
    )


def failed_target(exchange: Exchange) -> bool:
    """Return True if the request's target was refused or unreachable and never answered."""
    return Flag.TARGET_FAILED in exchange.flags


def refused_by_irimi(exchange: Exchange) -> bool:
    """Return True if irimi refused the request or failed to decide, unlike `failed_target`."""
    return Flag.DECISION_FAILED in exchange.flags or Flag.REFUSED in exchange.flags


def is_faked(exchange: Exchange) -> bool:
    """Return True for a write irimi faked: not refused, not sent to a target."""
    return (
        not refused_by_irimi(exchange)
        and not failed_target(exchange)
        and exchange.answered_by != "delegated"
    )


class ToolRun(Protocol):
    """What `fake_ran` reads of a tool call, such as `trace.ToolCall`."""

    @property
    def kind(self) -> str: ...

    @property
    def ran(self) -> str: ...


def fake_ran(call: ToolRun) -> bool:
    """Return True for a write tool whose fake ran, which the summary owes a line."""
    return call.kind == "write" and call.ran == "shadow"
