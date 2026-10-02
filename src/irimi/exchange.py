"""Engine-independent record of one HTTP exchange (design doc §2)."""

from dataclasses import dataclass, field, replace
from typing import Literal

Kind = Literal["read", "write", "llm", "telemetry", "unknown"]
# How faithful a locally answered write is. `fake-L0` is the floor - the request's own fields
# reflected back - and `fake-L1` is a route whose map names a vendored response object to start
# from (#41). A level is only ever added here: L0 stays the answer for every route without one.
FakeLevel = Literal["fake-L0", "fake-L1"]
# `overlay` is a LIVE read whose body irimi changed so that it shows the run's faked writes
# (#43). It is not a `FakeLevel`: nothing was faked, a real response was edited, and the level
# vocabulary stays the one `echo.fake_response` can return.
AnsweredBy = Literal["live", "fake-L0", "fake-L1", "delegated", "overlay"]
# How much of the write log the overlay could express in the read it was given (#43). `full` is
# the whole modeled effect. `partial` is irimi saying "this read is incomplete and I could not
# fix it" - a page the effects table does not model, a filter it cannot read, a body it cannot
# parse - and it is set even when the body was left alone, because a half-known world presented
# as the whole one is the untruth the overlay exists to prevent.
OverlayFidelity = Literal["full", "partial"]
# What L3 said about a write before irimi faked it (#45). `passed` and `rejected` are the check's
# two real answers; `not_evaluable` is irimi saying it could not find out - a 429, a network
# failure, a body it could not parse, a world the overlay knows is incomplete - and it degrades the
# write to L2 and is shown rather than hidden. None is the fourth state and the commonest: this
# route declares no `precondition:`, or the policy has no reader, so nothing was asked. Same shape
# as `Exchange.overlay` (#43).
PreconditionOutcome = Literal["passed", "rejected", "not_evaluable"]
# Who asked for this exchange. `engine` is a read irimi issued on its own account to decide about
# a write - a precondition read - and it is counted apart from the agent's own so that "reads are
# real" keeps meaning "the reads your agent made are real" (#45).
IssuedBy = Literal["agent", "engine"]
Validation = Literal["validated", "unvalidated"]
Door = Literal["forward", "reverse"]

KINDS: tuple[Kind, ...] = ("read", "write", "llm", "telemetry", "unknown")
# The kinds shadow mode forwards to the real service instead of answering locally. It lives here,
# beside KINDS, rather than beside the policy, because the map loader has to refuse a
# `default_kind` that names one (#30) and servicemap must not import policy.
LIVE_KINDS: tuple[Kind, ...] = ("read", "llm", "telemetry")
SAFE_METHODS: frozenset[str] = frozenset({"GET", "HEAD", "OPTIONS"})
# A map route's `match.method` when it names no method: it matches every verb, including the ones
# that delete things. It lives here, with the rest of the shared wire vocabulary, because both the
# map loader and the classifier have to reason about "this route named no method" and a second
# spelling of `"*"` in either of them is the drift `SAFE_METHODS` is here to avoid.
ANY_METHOD = "*"

# ------------------------------------------------------------------------------ exchange flags
#
# Every value that may appear in `Exchange.flags`, so a reader of a trace has one list to check
# against. A flag is a fact about one exchange, never a counter: the engine adds each at most once.

# The classifier could not say what this request is: `kind` is `unknown`, whether a map declared
# it or the RFC fallback reached it, and the answer is a local fake rather than a forward.
UNCLASSIFIED_FLAG = "unclassified"
# A live-forwarding kind was refused because its route did not name this method (THE SCOPE RULE,
# decision half, in `pipeline.classify`). The exchange says `unknown`, and this says why.
DOWNGRADED_FLAG = "kind-downgraded"
# An answer target was chosen and could not be reached or applied, and the agent got a 502.
TARGET_FAILED_FLAG = "target-failed"
# The classify/answer decision itself raised, so the request was answered locally with a 502 (see
# `IrimiAddon.request`). Not `target-failed`: that one says a target was chosen and could not be
# reached; this one says we never got as far as choosing.
DECISION_FAILED_FLAG = "decision-failed"
# A live forward that lost the real service before it answered.
UPSTREAM_ERROR_FLAG = "upstream-error"
# How faithful a locally decided answer is: the L0 echo, the L1 fixture, or whatever an answer
# target said. Exactly one of these is on every exchange irimi answered itself - `FIDELITY_FLAGS`
# is the mapping, so a new `answered_by` value cannot ship without one.
FIDELITY_L0_FLAG = "fidelity:L0"
FIDELITY_L1_FLAG = "fidelity:L1"
FIDELITY_DELEGATED_FLAG = "fidelity:delegated"
FIDELITY_OVERLAY_FLAG = "fidelity:overlay"
# A route whose map names a `fixture:` was answered at L0 anyway, because this install could not
# read the object. The answer is still safe - L0 is the floor - but it is not the fidelity the
# map promised, and a trace that did not say so would read as a working L1 (#41).
FIXTURE_FAILED_FLAG = "fixture-failed"
# This write carried an idempotency key the run had already answered, with the same canonical
# parameters, so the agent got the FIRST answer's own bytes back (#46). The write is in the trace
# and this says why it is not a second write: it is not in the write log, and the summary counts
# the write once.
IDEMPOTENT_REPLAY_FLAG = "idempotent-replay"
# The same key came back with DIFFERENT parameters, so the write was answered with the service's
# own `idempotency_error` instead of being faked (#46). Nothing was performed and nothing was
# minted, so it is not in the write log either - but it IS a distinct write the real service would
# have refused, and the summary prints it as one.
IDEMPOTENCY_CONFLICT_FLAG = "idempotency-conflict"
# The two together: neither is a write the overlay may replay onto a later read. One tuple rather
# than two clauses at each of the places that has to keep them out.
IDEMPOTENCY_FLAGS: tuple[str, ...] = (IDEMPOTENT_REPLAY_FLAG, IDEMPOTENCY_CONFLICT_FLAG)
# Some part of this exchange could not be redacted, so the stored copy holds `<redaction-failed>`
# in its place (#69). Set only on the copy `redact.redact_exchange` makes for disk, never on the
# exchange the engine answered with: the live flow is never redacted.
REDACTION_FAILED_FLAG = "redaction-failed"
# The trace store kept only the first `store.MAX_STORED_BODY` bytes of this exchange's request or
# response body (#70). A decoded Exchange carries a body's bytes and not its ref's `truncated`, so
# without this flag a stored run could not tell a cut body from a whole one. Set only on the copy
# the store writes, never on the exchange the engine answered with.
BODY_TRUNCATED_FLAG = "body-truncated"
# The trace store could not use this exchange's run id as a directory name, so it is stored in
# `unattributed/` (#70): an id `trace.is_valid_run_id` refuses, or one that differs only in case
# from a run already on a case-insensitive filesystem. Set only on the copy the store writes.
BAD_RUN_ID_FLAG = "bad-run-id"
# The recorded body of this streamed response is not the whole stream (#71): the engine stopped
# copying it at `store.MAX_STORED_BODY`, its copy raised, the stream ended in an error - an
# upstream reset, an agent that hung up - before its last chunk, or its `Content-Encoding` could
# not be decoded. The agent got every chunk that arrived either way; this is a fact about the
# recording only.
STREAM_TRUNCATED_FLAG = "stream-truncated"

# The control endpoint (#73): a request addressed to irimi's own listener whose path starts here is
# the SDK reporting a run's start, end or tool calls, and is answered by `irimi.control`. It is
# never forwarded and never an `Exchange`.
CONTROL_PREFIX = "/_irimi/"
# The `Irimi-Answered-By` value every control answer carries. Not an `AnsweredBy`: that vocabulary
# is the trace's, and a control request never appears in a trace.
CONTROL_ANSWER = "control"
# The header that names the run a request belongs to. irimi reads it and strips it (#67); the SDK
# puts it on every request a run makes (#75). Here, in layer 0, rather than in `pipeline`, so the
# SDK, which sits beside `pipeline` and may not import it, spells it the same way (#74). Header
# names are compared case-insensitively, and irimi stores them lower-case.
RUN_HEADER = "irimi-run"

# The fidelity flag each way of answering carries. One mapping rather than a branch per caller:
# the policy reads it, and `tests/test_exchange.py` holds it exhaustive over the non-live values.
FIDELITY_FLAGS: dict[AnsweredBy, str] = {
    "fake-L0": FIDELITY_L0_FLAG,
    "fake-L1": FIDELITY_L1_FLAG,
    "delegated": FIDELITY_DELEGATED_FLAG,
    "overlay": FIDELITY_OVERLAY_FLAG,
}

Headers = tuple[tuple[str, str], ...]


def header_value(headers: Headers, name: str) -> str | None:
    """The first value of the header `name`, or None when there is none.

    Names are compared case-insensitively: a `Request`'s are already lower-case
    (`pipeline.parse`), a `Response`'s are as the service or the target sent them. `name` is
    given lower-case, as every header name irimi spells is. One spelling of the lookup, because
    eight call sites had each grown a `next(...)` of their own.
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

    def header(self, name: str) -> str | None:
        """See `header_value`."""
        return header_value(self.headers, name)

    def without_header(self, name: str) -> "Response":
        """This response minus every `name` header. The SAME object when it carried none, so a
        caller's identity check keeps meaning "nothing changed"."""
        kept = without_header(self.headers, name)
        return self if len(kept) == len(self.headers) else replace(self, headers=kept)


@dataclass
class Exchange:
    request: Request
    response: Response | None
    service: str
    operation: str
    kind: Kind
    answered_by: AnsweredBy
    validation: Validation
    run_id: str
    door: Door = "forward"  # "reverse" = came in as /<host>/<path> on the listener itself
    flags: tuple[str, ...] = field(default_factory=tuple)
    # The address that answered a `delegated` exchange (design D20). "" for every other one:
    # `target: self` is not an address, and a live forward went to the real service.
    target: str = ""
    # How much of the run's faked writes the overlay could express in this read (#43). None for
    # every exchange the overlay did not consider - a write, a read on a service with no effects,
    # a read taken before any write. `full` or `partial` says it did consider it, and `partial`
    # can sit on an unchanged body: see OverlayFidelity.
    overlay: OverlayFidelity | None = None
    # What L3 decided about this write before it was faked (#45). None when nothing was asked -
    # a read, a route with no `precondition:` - see PreconditionOutcome.
    precondition: PreconditionOutcome | None = None
    # The machine code of an L3 rejection, for the summary line (#45). "" for everything else,
    # including a rejection's own modeled body when the service sends no code of its own - see
    # `services.Rejection`.
    rejection_code: str = ""
    # Who asked for this exchange: the agent, or irimi itself to decide about a write (#45).
    issued_by: IssuedBy = "agent"
    # The webhook events this write would have caused the real service to send, off its route's
    # `fires:` (#47). Set only for a write irimi ACCEPTED and faked: a write L3 rejected, a key
    # reused for a different write, and a retry the idempotency store replayed all list nothing,
    # because in the first two cases nothing would have happened and in the third the events are
    # already on the first write's own exchange. Empty for every read. Nothing is delivered.
    would_fire: tuple[str, ...] = field(default_factory=tuple)
    # The currency this write's amounts are denominated in, read off the document irimi itself
    # read before faking it: the L3 precondition read (#60). "" when no such read happened, or
    # when its document named none. The summary uses it only when the request named no currency,
    # as `stripe.Refund.create(charge=, amount=)` does not. Never a default and never a guess -
    # see `services.Check.currency`.
    currency: str = ""
    # When this exchange began and ended, in wall-clock `time.time()` seconds (#68). An agent's
    # exchange starts when the request hook first parses the flow and ends when the hook that
    # finishes it builds this record; an engine-issued read spans its `Reader` call. 0.0 only for
    # an Exchange built outside the engine - a test, or a caller that has no clock to read.
    started_at: float = 0.0
    ended_at: float = 0.0
    # The byte length of each chunk of a streamed response, in arrival order (#71). They sum to
    # `len(response.body)`, so they split the recorded body back into the chunks the agent was
    # sent, and none is 0. Empty for a response that did not stream - and for a stream that sent
    # no body at all, which has nothing to split.
    stream_chunks: tuple[int, ...] = field(default_factory=tuple)


def clip_chunks(chunks: tuple[int, ...], size: int) -> tuple[int, ...]:
    """The chunk lengths of the first `size` bytes of a body `chunks` splits: the chunks that fit
    whole, then what fits of the next one. `chunks` itself when they already fit (#71).

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


def is_authored_write(exchange: Exchange) -> bool:
    """A write irimi itself authored: it changed state somewhere the real service does not know
    about. The engine's write log holds exactly these, and they are what the overlay replays onto
    live reads - so they are also the only writes a later read can have been shown (#48). Each
    clause keeps one thing out that is not that.

    `kind not in LIVE_KINDS` keeps reads out. A delegated read is the one non-live read
    (`target_reads`), and a GET replayed onto later reads is not a write by any reading - #20 owns
    "no overlay for a delegated service" and #28 is the streaming twin. `answered_by not in
    ("live", "delegated")` keeps out what irimi did not author: a live forward, and a DELEGATED
    WRITE (#43) - the target performed it, or did something else with it, or nothing, and irimi
    cannot replay an effect it does not know. `TARGET_FAILED_FLAG` keeps out a write performed
    nowhere: a target irimi *refused* is answered in the engine's `request()` hook and recorded
    like any other answer, and without this clause the overlay replayed a write that never
    happened.

    A REJECTED WRITE IS NOT A WRITE (#45). L3 said the real service would have refused it, and the
    agent got that refusal. Replaying it onto later reads would show the agent a refund that
    neither Stripe nor irimi ever made.

    NEITHER HALF OF IDEMPOTENCY IS A WRITE TO REPLAY (#46). A replay is the SAME write: the store
    answered the agent's retry with the first write's own bytes, and a second entry would have the
    overlay decode one refund twice - a charge showing 200 refunded for a single 100 refund, and
    page 1 of the refunds list carrying the same minted id twice. A conflict is not a write at
    all: the real service would have refused it, nothing was minted, and there is no effect.
    """
    return (
        exchange.kind not in LIVE_KINDS
        and exchange.answered_by not in ("live", "delegated")
        and TARGET_FAILED_FLAG not in exchange.flags
        and exchange.precondition != "rejected"
        and not any(flag in exchange.flags for flag in IDEMPOTENCY_FLAGS)
    )
