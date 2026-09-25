"""Engine-independent record of one HTTP exchange (design doc §2)."""

from dataclasses import dataclass, field
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

# The fidelity flag each way of answering carries. One mapping rather than a branch per caller:
# the policy reads it, and `tests/test_exchange.py` holds it exhaustive over the non-live values.
FIDELITY_FLAGS: dict[AnsweredBy, str] = {
    "fake-L0": FIDELITY_L0_FLAG,
    "fake-L1": FIDELITY_L1_FLAG,
    "delegated": FIDELITY_DELEGATED_FLAG,
    "overlay": FIDELITY_OVERLAY_FLAG,
}

Headers = tuple[tuple[str, str], ...]


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
        q = f"?{self.query}" if self.query else ""
        return f"{self.scheme}://{netloc}{self.path}{q}"


@dataclass(frozen=True)
class Response:
    status: int
    headers: Headers
    body: bytes


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
