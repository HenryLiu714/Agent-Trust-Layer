"""AnswerPolicy: whether a request goes live, goes to its target, or is answered by irimi.

A replay's policy is `irimi.replay.policy`.
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
    """Send the request to the real service, rewritten by `translation` if the overlay set one."""

    translation: Translation | None = None

    @property
    def answered_by(self) -> AnsweredBy:
        return "live"


@dataclass(frozen=True)
class Delegate:
    """Send the request to its target (recorded `delegated`); irimi checks no write it performs."""

    target: ForwardTo
    precondition: Precondition = Precondition()

    @property
    def answered_by(self) -> AnsweredBy:
        return "delegated"


@dataclass(frozen=True)
class Respond:
    """Answer with `response`; `overlay` is the fidelity of a read answered from a recording."""

    response: Response
    answered_by: AnsweredBy
    outcome: WriteOutcome = WriteOutcome()
    overlay: OverlayCoverage | None = None


Decision = Forward | Delegate | Respond


@dataclass(frozen=True)
class Decided:
    """A decision, the flags it adds, and irimi's own reads for the caller to record."""

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
        """Return how this request is answered; `target` comes from `targets.resolve`."""
        ...

    def recorded_tool_call(self, name: str, args: JSONValue) -> ToolCall | None:
        """Return the recorded read tool call to hand back, or None to run it. Never raises."""
        ...

    def observe_read(self, service: str, body: bytes) -> None:
        """Learn from a forwarded read what later fakes must sort after. Never raises."""
        ...


class ShadowPolicy:
    """Shadow mode's policy; the first matching row decides.

    | request | decision |
    | --- | --- |
    | has a target | `Delegate` |
    | read, llm, telemetry | `Forward` |
    | a retry of a key this run answered | the first answer again |
    | the key again with other parameters | the service's idempotency error |
    | the precondition check rejects it | the service's own error body |
    | anything else (write, unknown) | the fixture, else the echo |
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
        # Per instance, so two policies in one process share neither keys nor Slack `ts` sequence.
        self.idempotency = keys if keys is not None else idempotency.Store()
        self.slack_ts = services.TsClock()

    def recorded_tool_call(self, name: str, args: JSONValue) -> ToolCall | None:
        """Return None: shadow mode has no recording, so every read tool runs for real."""
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
            # Fail-closed hook rule: a fake that fails is answered with an empty object.
            logger.exception("irimi: the local answer failed; answering with an empty object")
            fake = services.Fake(b"{}", bodies.JSON_CT)
        fires = route.fires if route is not None else ()
        outcome = WriteOutcome(checked.precondition, fires)
        decided = _local(fake, 200, outcome, issued=checked.issued)
        _remember(self.idempotency, slot, decided)
        return decided

    def _delegated(self, route: Route | None, target: ForwardTo) -> Delegate:
        """Return the `Delegate`; `not_evaluable` if a check was declared and a reader exists."""
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
    """Return an answer by irimi from `fake`, with the fake's flags then the caller's."""
    response = Response(
        status=status, headers=(("content-type", fake.content_type),), body=fake.body
    )
    return Decided(
        Respond(response, fake.answered_by, outcome),
        flags=(*fake.flags, *extra_flags),
        issued=issued,
    )


# The idempotency store is one per engine, shared by a replay's policy and the shadow policy it
# holds, so a retry also gets a write a replay answered from the recording.


def _recall(
    store: idempotency.Store, request: Request, classification: Classification, run_id: str
) -> tuple[idempotency.Slot | None, Decided | None]:
    """Return `(slot, answer already given)` for this write, before any check or mint.

    A retry gets the first answer's bytes, no precondition read and no write-log entry. Never
    raises: a failed store falls through to the ordinary answer.
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
    """Return the idempotency flag shadow mode's answer would carry, or "" for a fresh write.

    `idempotent-replay` for a retry, `idempotency-conflict` for a reused key. A replay matches a
    recorded write only with the same flag. Never raises.
    """
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
    """Keep `decided` under this write's idempotency key. Never raises."""
    try:
        slot = idempotency.slot_for(request, classification, run_id)
    except Exception:
        logger.exception("irimi: this write's idempotency key could not be read")
        return
    _remember(store, slot, decided)


def _retry_answer(first: idempotency.FirstAnswer, service: str) -> Decided:
    """Return the key's first answer again (see `FirstAnswer`), marked as a retry for `service`."""
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
    """Return the service's idempotency error for a reused key; no check ran, so no verdict."""
    fake = services.fake_rejection(classification, refusal.body)
    outcome = WriteOutcome(Precondition(rejection_code=refusal.code))
    return _local(fake, refusal.status, outcome, (Flag.IDEMPOTENCY_CONFLICT,))


def _remember(store: idempotency.Store, slot: idempotency.Slot | None, decided: Decided) -> None:
    """Keep this answer, accepted or rejected, under its key for a retry. Never raises."""
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
