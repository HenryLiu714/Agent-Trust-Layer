"""Engine-independent record of one HTTP exchange."""

import json
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any, Literal, Protocol

Kind = Literal["read", "write", "llm", "telemetry", "unknown"]
# How faithful a locally answered write is. `fake-L0` is the floor - the request's own fields
# reflected back - and `fake-L1` is a route whose map names a vendored response object to start
# from. A level is only ever added here: L0 stays the answer for every route without one.
FakeLevel = Literal["fake-L0", "fake-L1"]
# `overlay` is a LIVE read whose body irimi changed so that it shows the run's faked writes,
# or a replay's answer from such a read in the recording. It is not a `FakeLevel`: nothing was
# faked, a real response was edited, and the level vocabulary stays the one
# `services.fake_response` can return. `recording` is an answer the service gave a stored run,
# which a replay sent again from disk, byte for byte: a read, an llm call, or a precondition
# read. Anything else a replay answers from the recording keeps the `answered_by` it was
# recorded with - a write its fake level, a read the overlay edited `overlay` - and every such
# answer carries `from-recording`. `irimi`
# is an error irimi answered with itself: a refusal, a failed decision, an unreachable target.
# Its flags say which.
AnsweredBy = Literal["live", "fake-L0", "fake-L1", "delegated", "overlay", "recording", "irimi"]
# How much of the write log the overlay could express in the read it was given. `full` is
# the whole modeled effect. `partial` is irimi saying "this read is incomplete and I could not
# fix it" - a page the effects table does not model, a filter it cannot read, a body it cannot
# parse - and it is set even when the body was left alone, because a half-known world presented
# as the whole one is the untruth the overlay exists to prevent.
OverlayFidelity = Literal["full", "partial"]
# What L3 said about a write before irimi faked it. `passed` and `rejected` are the check's
# two real answers; `not_evaluable` is irimi saying it could not find out - a 429, a network
# failure, a body it could not parse, a world the overlay knows is incomplete - and it degrades the
# write to L2 and is shown rather than hidden. None is the fourth state and the commonest: this
# route declares no `precondition:`, or the policy has no reader, so nothing was asked. Same shape
# as `Exchange.overlay`.
PreconditionOutcome = Literal["passed", "rejected", "not_evaluable"]
# Who asked for this exchange. `engine` is a read irimi issued on its own account to decide about
# a write - a precondition read - and it is counted apart from the agent's own so that "reads are
# real" keeps meaning "the reads your agent made are real".
IssuedBy = Literal["agent", "engine"]
Door = Literal["forward", "reverse"]

KINDS: tuple[Kind, ...] = ("read", "write", "llm", "telemetry", "unknown")
# The kinds shadow mode forwards to the real service instead of answering locally. It lives here,
# beside KINDS, rather than beside the policy, because the map loader has to refuse a
# `default_kind` that names one and servicemap must not import policy.
LIVE_KINDS: tuple[Kind, ...] = ("read", "llm", "telemetry")
# The kinds a shadow run answers instead of forwarding: every kind that is not live. `unknown` is
# here because an unclassified DELETE is a presumed write, so the summary accounts for it as one.
INTERCEPTED_KINDS: frozenset[Kind] = frozenset(KINDS) - frozenset(LIVE_KINDS)
SAFE_METHODS: frozenset[str] = frozenset({"GET", "HEAD", "OPTIONS"})
# A map route's `match.method` when it names no method: it matches every verb, including the ones
# that delete things. It lives here, with the rest of the shared wire vocabulary, because both the
# map loader and the classifier have to reason about "this route named no method" and a second
# spelling of `"*"` in either of them is the drift `SAFE_METHODS` is here to avoid.
ANY_METHOD = "*"


class Flag(StrEnum):
    """Every value that may appear in `Exchange.flags`, so a reader of a trace has one list to
    check against. A flag is a fact about one exchange, never a counter: the engine adds each at
    most once."""

    # The classifier could not say what this request is: `kind` is `unknown`, whether a map
    # declared it or the RFC fallback reached it, and the answer is a local fake, not a forward.
    UNCLASSIFIED = "unclassified"
    # A live-forwarding kind was refused because its route did not name this method (the scope
    # rule's decision half, in `classify.classify`). The exchange says `unknown`; this says why.
    DOWNGRADED = "kind-downgraded"
    # An answer target was chosen and could not be reached or applied, and the agent got a 502.
    TARGET_FAILED = "target-failed"
    # The classify/answer decision itself raised, so the request was answered locally with a 502
    # (see `ShadowSession.plan`). Not `target-failed`: that one says a target was chosen and
    # could not be reached; this one says irimi never got as far as choosing.
    DECISION_FAILED = "decision-failed"
    # The request hook refused the request before any decision: it could not be parsed or routed,
    # or it came in by base URL for a host irimi does not relay, or it could not be rewritten.
    REFUSED = "refused"
    # A live forward that lost the real service before it answered.
    UPSTREAM_ERROR = "upstream-error"
    # A route whose map names a `fixture:` was answered at L0 anyway, because this install could
    # not read the object. The answer is still safe - L0 is the floor - but it is not the
    # fidelity the map promised, and a trace that did not say so would read as a working L1.
    FIXTURE_FAILED = "fixture-failed"
    # This write carried an idempotency key the run had already answered, with the same canonical
    # parameters, so the agent got the first answer's own bytes back. It is not in the write log,
    # and the summary counts the write once.
    IDEMPOTENT_REPLAY = "idempotent-replay"
    # The same key came back with different parameters, so the write was answered with the
    # service's own `idempotency_error` instead of being faked. Nothing was minted, so it is not
    # in the write log either, but it is a distinct write the real service would have refused.
    IDEMPOTENCY_CONFLICT = "idempotency-conflict"
    # Some part of this exchange could not be redacted, so the stored copy holds
    # `<redaction-failed>` in its place. Set only on the copy `redact.redact_exchange` makes for
    # disk, never on the exchange the engine answered with: the live flow is never redacted.
    REDACTION_FAILED = "redaction-failed"
    # This exchange's recorded request or response body is not all of it. The trace store kept
    # only its first `limits.MAX_STORED_BODY` bytes, or the body decodes past
    # `limits.MAX_DECODED_BODY` and is recorded empty. Whatever irimi forwarded of such a body,
    # it forwarded unchanged. The store sets it on the copy it writes, the engine on its own.
    BODY_TRUNCATED = "body-truncated"
    # The trace store could not use this exchange's run id as a directory name, so it is stored in
    # `unattributed/`: an id `trace.is_valid_run_id` refuses, or one that differs only in case
    # from a run already on a case-insensitive filesystem. Set only on the copy the store writes.
    BAD_RUN_ID = "bad-run-id"
    # The recorded body of this streamed response is not the whole stream: the engine stopped
    # copying it at `limits.MAX_STORED_BODY`, its copy raised, the stream ended in an error
    # before its last chunk, or its `Content-Encoding` could not be decoded, or decoded past
    # `limits.MAX_DECODED_BODY`. The agent got every chunk that arrived either way.
    STREAM_TRUNCATED = "stream-truncated"
    # A replay looked this request up in the run it replays and found no answer it could give, so
    # it was decided as shadow mode decides it: a read or an llm call forwarded live, a write
    # faked and never forwarded. The first one in a run is where the replay diverged.
    REPLAY_MISS = "replay-miss"
    # A replay answered this exchange from the recording, with the answer the recorded run got for
    # the same request and the `answered_by` it was recorded with (`live` becomes `recording`).
    # A write keeps its body, its level, its L3 outcome and its webhooks.
    FROM_RECORDING = "from-recording"


# The two halves of idempotency: neither is a write the overlay may apply to a later read.
IDEMPOTENCY_FLAGS: tuple[Flag, ...] = (Flag.IDEMPOTENT_REPLAY, Flag.IDEMPOTENCY_CONFLICT)

# The control endpoint: a request addressed to irimi's own listener whose path starts here is
# the SDK reporting a run's start, end or tool calls, and is answered by `irimi.control`. It is
# never forwarded and never an `Exchange`.
CONTROL_PREFIX = "/_irimi/"
# The `Irimi-Answered-By` value every control answer carries. Not an `AnsweredBy`: that vocabulary
# is the trace's, and a control request never appears in a trace.
CONTROL_ANSWER = "control"
# The control endpoint's run routes under CONTROL_PREFIX: `runs/{run_id}/{action}`. Here
# rather than in `control`, so the SDK, which posts to them from layer 2, spells them the way the
# endpoint reads them.
RUNS_ROUTE = "runs"
START_ACTION = "start"
TOOL_CALLS_ACTION = "tool-calls"
END_ACTION = "end"
RUN_ACTIONS = (START_ACTION, TOOL_CALLS_ACTION, END_ACTION)
# The one control route that answers with something: `runs/{run_id}/tool-calls/recorded`, where the
# SDK asks a replay for a read tool's recorded result. Not a run action: it reports nothing.
RECORDED_ACTION = f"{TOOL_CALLS_ACTION}/recorded"
# The routes about a tool call, which a run with no SDK start may use: the process run, or
# `unattributed` for a call made in no run.
TOOL_ACTIONS = frozenset({TOOL_CALLS_ACTION, RECORDED_ACTION})
# The header the SDK names its control protocol version in, on every post. The endpoint answers
# 426 to another major version, so an SDK and a proxy upgraded apart say so instead of losing runs.
PROTOCOL_HEADER = "irimi-protocol"
PROTOCOL_VERSION = "1"
# The header that names the run a request belongs to. irimi reads it and strips it; the SDK
# puts it on every request a run makes. Here, in layer 0, rather than in `wire`, so the
# SDK, which sits beside `wire` and may not import it, spells it the same way. Header
# names are compared case-insensitively, and irimi stores them lower-case.
RUN_HEADER = "irimi-run"

Headers = tuple[tuple[str, str], ...]


def protocol_major(version: str) -> str:
    """The major part of a control protocol version: `1` of `1.2`."""
    return version.strip().partition(".")[0]


def header_value(headers: Headers, name: str) -> str | None:
    """The first value of the header `name`, or None when there is none.

    Names are compared case-insensitively: a `Request`'s are already lower-case
    (`wire.parse`), a `Response`'s are as the service or the target sent them. `name` is
    given lower-case, as every header name irimi spells is. One spelling of the lookup for every
    call site.
    """
    for key, value in headers:
        if key.lower() == name:
            return value
    return None


def without_header(headers: Headers, name: str) -> Headers:
    """`headers` minus every header called `name` (compared as `header_value` compares)."""
    return tuple((k, v) for k, v in headers if k.lower() != name)


def media_type(content_type: str) -> str:
    """A Content-Type value without its parameters, lower-cased.

    `Application/JSON; charset=utf-8` is `application/json`. The engine asks it of a response
    to decide whether to stream it, and the faker of a request to decide how to parse it.
    """
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
        """The request target as a flow spells it: the path, then `?query` when there is one."""
        return f"{self.path}?{self.query}" if self.query else self.path

    def header(self, name: str) -> str | None:
        """See `header_value`."""
        return header_value(self.headers, name)

    def without_header(self, name: str) -> "Request":
        """This request minus every `name` header. The SAME object when it carried none, so a
        caller's identity check keeps meaning "nothing changed"."""
        kept = without_header(self.headers, name)
        return self if len(kept) == len(self.headers) else replace(self, headers=kept)


@dataclass(frozen=True)
class Response:
    status: int
    headers: Headers
    body: bytes

    @classmethod
    def json(cls, status: int, doc: Mapping[str, Any], headers: Headers = ()) -> "Response":
        """`doc` as a JSON answer: `content-type` first, then `headers`. Raises ValueError for a
        value strict JSON cannot hold, such as NaN."""
        body = json.dumps(doc, allow_nan=False).encode()
        return cls(status, (("content-type", "application/json"), *headers), body)

    def header(self, name: str) -> str | None:
        """See `header_value`."""
        return header_value(self.headers, name)

    def without_header(self, name: str) -> "Response":
        """This response minus every `name` header. The SAME object when it carried none, so a
        caller's identity check keeps meaning "nothing changed"."""
        kept = without_header(self.headers, name)
        return self if len(kept) == len(self.headers) else replace(self, headers=kept)


@dataclass(frozen=True)
class Precondition:
    """What the precondition check said of one write, as one value.

    `outcome` is None when nothing was asked (see PreconditionOutcome). `rejection_code` is the
    machine code the summary prints for a write the real service would have refused: an L3
    rejection's, or a reused idempotency key's, which no check asked. `currency` is the currency
    the check read off the object the write names, "" when it read none: the summary uses it only
    when the request named no currency, and it is never a default or a guess.
    """

    outcome: PreconditionOutcome | None = None
    rejection_code: str = ""
    currency: str = ""


@dataclass(frozen=True)
class WriteOutcome:
    """What irimi decided about a write it answers itself: the precondition check's result, and
    the webhook events the real service would have sent. `would_fire` is set only for a write
    irimi accepted and faked; a rejection, a reused key and a retry list nothing."""

    precondition: Precondition = Precondition()
    would_fire: tuple[str, ...] = ()


@dataclass(frozen=True)
class Exchange:
    request: Request
    response: Response | None
    service: str
    operation: str
    kind: Kind
    answered_by: AnsweredBy
    run_id: str
    door: Door = "forward"  # "reverse" = came in as /<host>/<path> on the listener itself
    flags: tuple[Flag, ...] = field(default_factory=tuple)
    # The answer target's address for a `delegated` exchange. "" for every other one:
    # `target: self` is not an address, and a live forward went to the real service.
    target: str = ""
    # How much of the run's faked writes the overlay could express in this read. None for
    # every exchange the overlay did not consider - a write, a read on a service with no effects,
    # a read taken before any write. `full` or `partial` says it did consider it, and `partial`
    # can sit on an unchanged body: see OverlayFidelity.
    overlay: OverlayFidelity | None = None
    # What the precondition check said of this write. Its outcome is None when nothing was asked:
    # a read, a route with no `precondition:`.
    precondition: Precondition = Precondition()
    # Who asked for this exchange: the agent, or irimi itself to decide about a write.
    issued_by: IssuedBy = "agent"
    # The webhook events this write would have caused the real service to send, off its route's
    # `fires:`. Set only for a write irimi ACCEPTED and faked: a write L3 rejected, a key
    # reused for a different write, and a retry the idempotency store replayed all list nothing,
    # because in the first two cases nothing would have happened and in the third the events are
    # already on the first write's own exchange. Empty for every read. Nothing is delivered.
    would_fire: tuple[str, ...] = field(default_factory=tuple)
    # When this exchange began and ended, in wall-clock `time.time()` seconds. An agent's
    # exchange starts when the request hook first parses the flow and ends when the hook that
    # finishes it builds this record; an engine-issued read spans its `Reader` call. 0.0 only for
    # an Exchange built outside the engine - a test, or a caller that has no clock to read.
    started_at: float = 0.0
    ended_at: float = 0.0
    # The byte length of each chunk of a streamed response, in arrival order. They sum to
    # `len(response.body)`, so they split the recorded body back into the chunks the agent was
    # sent, and none is 0. Empty for a response that did not stream - and for a stream that sent
    # no body at all, which has nothing to split.
    stream_chunks: tuple[int, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        # The wall clock can step back mid-exchange, and a stored run's reader subtracts the two.
        if self.ended_at < self.started_at:
            object.__setattr__(self, "ended_at", self.started_at)


def clip_chunks(chunks: tuple[int, ...], size: int) -> tuple[int, ...]:
    """The chunk lengths of the first `size` bytes of a body `chunks` splits: the chunks that fit
    whole, then what fits of the next one. `chunks` itself when they already fit.

    For whoever keeps only the start of a streamed body - the store's cut at MAX_STORED_BODY - so
    that `stream_chunks` still sums to the body it describes."""
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


# ------------------------------------------------------------------------------ which writes count
#
# Two sets of writes, and the facts that split them. The write log holds the authored writes; the
# summary prints a line for each intercepted write and each tool whose fake ran, and says of each
# whether it was faked, refused by irimi, or sent to a target that failed.


def is_authored_write(exchange: Exchange) -> bool:
    """A write irimi itself authored: it changed state somewhere the real service does not know
    about. A run's write log holds exactly these, and they are what the overlay applies to the
    run's live reads - so they are also the only writes a later read can have been shown. Each
    clause keeps one thing out that is not that.

    `kind not in LIVE_KINDS` keeps reads out. A delegated read is the one non-live read
    (`target_reads`), and a GET replayed onto later reads is not a write by any reading; a
    delegated service gets no overlay, and neither does a streamed read. `answered_by not in
    ("live", "delegated", "irimi")` keeps out what irimi did not author: a live forward, a
    DELEGATED WRITE - the target performed it, or did something else with it, or nothing, and
    irimi cannot replay an effect it does not know - and irimi's own error answer, for which
    nothing was faked.

    A WRITE IRIMI COULD NOT DECIDE ABOUT IS NOT A WRITE. A run stored under schema 2 records that
    502 as `fake-L0` flagged `decision-failed`, so the flag keeps it out too. Counted here, a
    replay of that run would wait for a recorded write no replay can make, and diverge at the
    next read.

    A REJECTED WRITE IS NOT A WRITE. L3 said the real service would have refused it, and the
    agent got that refusal. Replaying it onto later reads would show the agent a refund that
    neither Stripe nor irimi ever made.

    NEITHER HALF OF IDEMPOTENCY IS A WRITE TO REPLAY. A replay is the SAME write: the store
    answered the agent's retry with the first write's own bytes, and a second entry would have the
    overlay decode one refund twice - a charge showing 200 refunded for a single 100 refund, and
    page 1 of the refunds list carrying the same minted id twice. A conflict is not a write at
    all: the real service would have refused it, nothing was minted, and there is no effect.
    """
    return (
        exchange.kind not in LIVE_KINDS
        and exchange.answered_by not in ("live", "delegated", "irimi")
        and Flag.DECISION_FAILED not in exchange.flags
        and exchange.precondition.outcome != "rejected"
        and not any(flag in exchange.flags for flag in IDEMPOTENCY_FLAGS)
    )


def is_intercepted(exchange: Exchange) -> bool:
    """A write this run answered instead of performing - locally, or at an answer target: the
    writes the summary prints a line for, and the ones `irimi compare` compares. Unlike
    `is_authored_write`, it keeps a rejected write, a conflict, a target's and irimi's own refusal.

    A REPLAYED write is not one of them. The agent retried with a key it had already used,
    the store answered with the first write's own bytes, and it is the same write: counting it
    again would make "nine would have been rejected" count retries instead of intentions. It is
    still in the trace and still carries `idempotent-replay` on its own line. A CONFLICT stays,
    because it is a different write the real service would have refused - which is exactly the
    line the baseline report exists to print.
    """
    return (
        exchange.kind in INTERCEPTED_KINDS
        and exchange.answered_by != "live"
        and Flag.IDEMPOTENT_REPLAY not in exchange.flags
    )


def failed_target(exchange: Exchange) -> bool:
    """A request whose target irimi refused or could not reach, answered `irimi`, or one
    mitmproxy could not finish dialling, which stays `delegated`. Unlike a delegated write, the
    target never answered it."""
    return Flag.TARGET_FAILED in exchange.flags


def refused_by_irimi(exchange: Exchange) -> bool:
    """A request irimi answered with its own error because it refused it or could not decide
    about it, unlike a failed target, which irimi did decide to send. Asked of the flags, which a
    run stored under schema 2 carries too."""
    return Flag.DECISION_FAILED in exchange.flags or Flag.REFUSED in exchange.flags


def is_faked(exchange: Exchange) -> bool:
    """An intercepted write irimi answered with a fake of its own: not refused by irimi, and sent
    to no target."""
    return (
        not refused_by_irimi(exchange)
        and not failed_target(exchange)
        and exchange.answered_by != "delegated"
    )


class ToolRun(Protocol):
    """What `fake_ran` reads of a tool call: `trace.ToolCall` is one."""

    @property
    def kind(self) -> str: ...

    @property
    def ran(self) -> str: ...


def fake_ran(call: ToolRun) -> bool:
    """A write tool whose fake ran in its place, which the summary owes a line, as it owes one to
    an intercepted HTTP write. The SDK runs a write tool's real function only while it is
    inactive, and then records nothing, so no write tool call it posts ran for real."""
    return call.kind == "write" and call.ran == "shadow"
