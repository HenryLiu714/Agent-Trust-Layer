"""AnswerPolicy: decides whether a request is forwarded live, sent to its target, or answered by
irimi, and whether a read tool the proxy cannot see is answered from a replay's recording.

A decision is one of three values: `Forward`, `Delegate` or `Respond`, each carrying only its own
data. What a faked answer *contains* is `irimi.services`; which target a request goes to is
`irimi.decide.targets`, resolved by the caller; the precondition check is
`irimi.decide.preconditions`; a replay's policy is `irimi.replay.policy`.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from irimi import services
from irimi.core import bodies
from irimi.core.exchange import (
    LIVE_KINDS,
    AnsweredBy,
    Exchange,
    Flag,
    OverlayCoverage,
    Precondition,
    Request,
    Response,
    WriteOutcome,
)
from irimi.decide import idempotency, preconditions
from irimi.decide.reader import NoReader, Reader
from irimi.decide.targets import ForwardTo
from irimi.decide.writelog import Translation
from irimi.protocol.trace import JSONValue, ToolCall
from irimi.servicemaps import MapIndex, Route
from irimi.servicemaps.classify import Classification

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Forward:
    """Send the request to the real service. `translation` is set when the overlay translated a
    read before it was asked: its query goes onto the wire, and the pair it dropped goes back to
    the overlay with the response."""

    translation: Translation | None = None

    @property
    def answered_by(self) -> AnsweredBy:
        return "live"


@dataclass(frozen=True)
class Delegate:
    """Send the request to its answer target instead of the real service. `precondition` is
    `not_evaluable` for a checked route: irimi checks no write a target performs."""

    target: ForwardTo
    precondition: Precondition = Precondition()

    @property
    def answered_by(self) -> AnsweredBy:
        return "delegated"


@dataclass(frozen=True)
class Respond:
    """Answer with `response`, which never leaves the machine. `outcome` is what irimi decided
    about a write; `overlay` is how much of the run's writes a read a replay answers from the
    recording shows, which the overlay decided in the recorded run."""

    response: Response
    answered_by: AnsweredBy
    outcome: WriteOutcome = WriteOutcome()
    overlay: OverlayCoverage | None = None


Decision = Forward | Delegate | Respond


@dataclass(frozen=True)
class Decided:
    """A policy's decision, the flags it adds to the exchange, and the reads it issued on its own
    account, already recorded as exchanges. The caller records those reads; they never reach the
    agent."""

    decision: Decision
    flags: tuple[Flag, ...] = ()
    issued: tuple[Exchange, ...] = ()


class AnswerPolicy(Protocol):
    name: str

    def decide(
        self,
        request: Request,
        classification: Classification,
        write_log: Sequence[Exchange] = (),
        run_id: str = "",
        target: ForwardTo | None = None,
    ) -> Decided:
        """How this request is answered. `target` is its answer target, which the caller
        resolves once (`targets.delegate`), or None when it has none."""
        ...

    def recorded_tool_call(self, name: str, args: JSONValue) -> ToolCall | None:
        """The recorded read tool call that answers this one, which the SDK hands back in place
        of running the tool, or None: the tool runs for real. Never raises."""
        ...

    def observe_read(self, service: str, body: bytes) -> None:
        """Learn from a read the engine forwarded what later fakes have to sort after. Never
        raises."""
        ...


class ShadowPolicy:
    """Reads, llm and telemetry go live. write and unknown are answered by irimi.

    | request | decision |
    | --- | --- |
    | has an answer target | `Delegate` |
    | read, llm, telemetry | `Forward` |
    | a retry of a key this run answered | the first answer again |
    | the key again with other parameters | the service's idempotency error |
    | the precondition check rejects it | the service's own error body |
    | anything else | the fixture (L1) or the echo (L0) |

    `services.fake_response` decides the level of a faked write and says so; the level it reports
    is what the Exchange and the `Irimi-Answered-By` header carry.
    """

    name: str = "shadow"

    def __init__(
        self,
        reader: Reader | None = None,
        maps: MapIndex | None = None,
        keys: idempotency.Store | None = None,
    ) -> None:
        self.reader: Reader = reader if reader is not None else NoReader()
        self.maps = maps if maps is not None else MapIndex()
        # Per-instance, never a class attribute: two policies in one test process must not answer
        # each other's keys. `keys` is the composition root's, which drops a run's as it ends.
        self.idempotency = keys if keys is not None else idempotency.Store()
        # Per-instance for the same reason: one engine's faked Slack messages never take their
        # `ts` from another's sequence.
        self.slack_ts = services.TsClock()

    def recorded_tool_call(self, name: str, args: JSONValue) -> ToolCall | None:
        """None: shadow mode answers from no recording, so every read tool runs for real."""
        return None

    def observe_read(self, service: str, body: bytes) -> None:
        services.observe_read(service, body, self.slack_ts)

    def decide(
        self,
        request: Request,
        classification: Classification,
        write_log: Sequence[Exchange] = (),
        run_id: str = "",
        target: ForwardTo | None = None,
    ) -> Decided:
        route = classification.route
        if target is not None:
            return Decided(self._delegated(route, target))
        if classification.kind in LIVE_KINDS:
            return Decided(Forward())
        slot, known = _recall(self.idempotency, request, classification, run_id)
        if known is not None:
            return known
        checked = preconditions.check(
            request, classification, write_log, run_id, self.reader, self.maps, self.observe_read
        )
        if checked.rejection is not None:
            fake = services.fake_rejection(classification, checked.rejection.body)
            outcome = WriteOutcome(checked.precondition)
            decided = _local(fake, checked.rejection.status, outcome, issued=checked.issued)
            _remember(self.idempotency, slot, decided)
            return decided
        try:
            fake = services.fake_response(request, classification, self.slack_ts)
        except Exception:
            # The belt to reflect()'s braces, and to the fixture's. Raising here would make
            # mitmproxy forward the flow, and a forwarded write escapes shadow mode - an empty
            # object is far better. L0 is the floor whatever failed above it.
            logger.exception("irimi: the local answer failed; answering with an empty object")
            fake = services.Fake(b"{}", bodies.JSON_CT)
        fires = route.fires if route is not None else ()
        outcome = WriteOutcome(checked.precondition, fires)
        decided = _local(fake, 200, outcome, issued=checked.issued)
        _remember(self.idempotency, slot, decided)
        return decided

    def _delegated(self, route: Route | None, target: ForwardTo) -> Delegate:
        """A delegated write is one irimi did not author and cannot check: no precondition reads
        from a target. A policy with no reader asked nothing, delegated or not."""
        declared = route is not None and bool(route.precondition)
        asked = declared and not isinstance(self.reader, NoReader)
        return Delegate(target, preconditions.NOT_EVALUABLE if asked else Precondition())


def _local(
    fake: services.Fake,
    status: int,
    outcome: WriteOutcome,
    extra_flags: tuple[Flag, ...] = (),
    *,
    issued: tuple[Exchange, ...] = (),
) -> Decided:
    """An answer irimi built itself, from the body `services` built: the fake's level is the
    answer's, and its flags are the fake's own, then the caller's."""
    response = Response(
        status=status, headers=(("content-type", fake.content_type),), body=fake.body
    )
    return Decided(
        Respond(response, fake.answered_by, outcome),
        flags=(*fake.flags, *extra_flags),
        issued=issued,
    )


# ------------------------------------------------------------------------- the idempotency store
#
# One `idempotency.Store` per engine, shared by a replay's policy and the shadow policy it holds:
# a write a replay answered from the recording is remembered under its key as shadow mode's own
# answers are, so the agent's retry meets it.


def _recall(
    store: idempotency.Store, request: Request, classification: Classification, run_id: str
) -> tuple[idempotency.Slot | None, Decided | None]:
    """`(the slot this write occupies, the answer the run already gave it)`.

    Before L3, and before anything is minted. A retry with a key this run has already answered
    is the SAME write: it gets the first answer's own bytes, issues no precondition read, and
    appends nothing to the write log. Its own guard, because a store that failed must fall
    through to the ordinary answer rather than 502 a perfectly fakeable write.
    """
    slot: idempotency.Slot | None = None
    try:
        slot = idempotency.slot_for(request, classification, run_id)
        if slot is not None:
            first, conflicted = store.get(slot.id, slot.params)
            if conflicted:
                refusal = idempotency.conflict(classification.service, slot.idempotency_key)
                if refusal is not None:
                    return slot, _conflict_answer(classification, refusal)
            elif first is not None:
                return slot, _retry_answer(first, classification.service)
    except Exception:
        logger.exception("irimi: the idempotency store failed; answering this write afresh")
        slot = None
    return slot, None


def idempotency_standing(
    store: idempotency.Store, request: Request, classification: Classification, run_id: str
) -> str:
    """The idempotency flag shadow mode's answer to this write would carry:
    `idempotent-replay` when the run has answered its key with the same parameters,
    `idempotency-conflict` when with others, and "" when it has not, or there is no key. A
    replay takes a recorded write only from one that carried the same: the service
    answers a retry, a reused key and a fresh write three ways whatever their bodies say. Never
    raises: `_recall` is guarded."""
    _, known = _recall(store, request, classification, run_id)
    if known is None:
        return ""
    if Flag.IDEMPOTENCY_CONFLICT in known.flags:
        return Flag.IDEMPOTENCY_CONFLICT
    return Flag.IDEMPOTENT_REPLAY


def remember_answer(
    store: idempotency.Store,
    request: Request,
    classification: Classification,
    run_id: str,
    decided: Decided,
) -> None:
    """Keep `decided` as this write's, under its idempotency key. Never raises: a key that cannot
    be read costs that retry its first answer, and nothing else."""
    try:
        slot = idempotency.slot_for(request, classification, run_id)
    except Exception:
        logger.exception("irimi: this write's idempotency key could not be read")
        return
    _remember(store, slot, decided)


def _retry_answer(first: idempotency.FirstAnswer, service: str) -> Decided:
    """The first answer to this key, again, marked the way `service` marks a retry's answer.

    Every field is the first answer's. `answered_by` in particular is not re-derived: a fixture
    that has become unreadable since the first call would make the retry `fake-L0` over an
    original stamped `fake-L1`, and the trace would disagree with itself about one write. There is
    no `issued` because a retry makes no precondition read - the write was checked once, when it
    was made. The precondition is the first one's too, its currency included, so a retried
    refund's exchange and the first one's agree about what that single write was.
    """
    headers: tuple[tuple[str, str], ...] = (("content-type", first.content_type),)
    marker = idempotency.replayed_header(service)
    if marker is not None:
        headers = (*headers, marker)
    return Decided(
        Respond(
            Response(status=first.status, headers=headers, body=first.body),
            first.answered_by,
            WriteOutcome(first.precondition),
        ),
        flags=(*first.flags, Flag.IDEMPOTENT_REPLAY),
    )


def _conflict_answer(classification: Classification, refusal: services.Rejection) -> Decided:
    """The service's own `idempotency_error`: this key was used for a different write.

    NOT `precondition: rejected` - the outcome says what L3 decided, and L3 was never asked.
    `rejection_code` is still set, because the summary's `would fail:` line is about what the
    real service would have answered, and this is one of those. The body is carried at the level
    this route's write would have been faked at, the way `fake_rejection` carries an L3 one.
    """
    fake = services.fake_rejection(classification, refusal.body)
    outcome = WriteOutcome(Precondition(rejection_code=refusal.code))
    return _local(fake, refusal.status, outcome, (Flag.IDEMPOTENCY_CONFLICT,))


def _remember(store: idempotency.Store, slot: idempotency.Slot | None, decided: Decided) -> None:
    """Keep this answer under its key, so the agent's retry gets it back.

    A service stores the first response under a key whether it was accepted or REJECTED, so this
    is called on both of `decide`'s faking paths. Guarded like the lookup: a store that cannot
    remember costs the next retry its first answer, and nothing else - raising here would forward
    a write that has already been answered.
    """
    decision = decided.decision
    if slot is None or not isinstance(decision, Respond):
        return
    try:
        content_type = decision.response.header("content-type") or bodies.JSON_CT
        store.put(
            slot.id,
            slot.params,
            idempotency.FirstAnswer(
                status=decision.response.status,
                body=decision.response.body,
                content_type=content_type,
                answered_by=decision.answered_by,
                flags=decided.flags,
                precondition=decision.outcome.precondition,
            ),
        )
    except Exception:
        logger.exception("irimi: this write's answer could not be stored for a retry")
