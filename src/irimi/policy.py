"""AnswerPolicy: decides whether a request is forwarded live, sent to its target, or answered by
irimi, and whether a read tool the proxy cannot see is answered from a replay's recording.

A decision is one of three values: `Forward`, `Delegate` or `Respond`, each carrying only its own
data. What a faked answer *contains* is `irimi.services`; which target a request goes to is
`irimi.delegation`, resolved by the caller; the precondition check is `irimi.preconditions`; what
a replay answers from is `irimi.recording`.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Protocol

from irimi import bodies, idempotency, preconditions, services, wire
from irimi.classify import Classification
from irimi.delegation import ForwardTo
from irimi.exchange import (
    LIVE_KINDS,
    AnsweredBy,
    Exchange,
    Flag,
    OverlayFidelity,
    Precondition,
    Request,
    Response,
    WriteOutcome,
)
from irimi.reader import NoReader, Reader
from irimi.recording import Recording, RecordingReader
from irimi.servicemap import MapIndex, Route
from irimi.trace import JSONValue, ToolCall
from irimi.writelog import Translation

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
    overlay: OverlayFidelity | None = None


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
        resolves once (`delegation.delegate`), or None when it has none."""
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


# The flags a recorded ANSWER carried besides its level: a fixture that did not load, and the two
# halves of idempotency. A replay that answers again carries these and `from-recording`, and no
# other: the classifier's own flags come back from this request's classification, and the
# engine's (a body cut at the cap, a lost upstream) are facts about the recorded exchange, never
# about this one.
_ANSWER_FLAGS = frozenset({Flag.FIXTURE_FAILED, Flag.IDEMPOTENT_REPLAY, Flag.IDEMPOTENCY_CONFLICT})
# Headers a recorded response carries that describe bytes it no longer has. The body was stored
# decoded and whole, so its `Content-Encoding` and framing are not the body served; mitmproxy sets
# the length of what is. And the recorded run's own stamp, which `wire.stamp` replaces.
_RECORDED_ONLY_HEADERS = frozenset(
    {"content-encoding", "content-length", "transfer-encoding", wire.ANSWERED_BY_HEADER}
)


class ReplayPolicy:
    """Answers a run from a stored one where it can, decides the rest as shadow mode would, and
    never forwards a write.

    | request | in the recording (`from-recording`) | not in it (`replay-miss`) |
    | --- | --- | --- |
    | llm; read before the replay diverged | the recorded response | live |
    | read, after the replay diverged | never taken | live, with the overlay |
    | write, unknown | the recorded ANSWER, at its recorded level | faked; the replay diverges |
    | telemetry, delegated | not looked up, no flag | as in shadow mode |

    A write is looked up with its standing in the run's idempotency store, which this policy and
    the `ShadowPolicy` it holds share, and one L3 checked is taken only while the replay has not
    diverged, as a read is (`Recording.take`).

    AN ANSWER FROM THE RECORDING IS WHAT IT WAS. It keeps the recorded `answered_by` and the
    recorded read's overlay fidelity: a write is the same fake irimi built in the recorded run,
    so `is_authored_write` puts it in the write log, the summary prints it as it did, and its minted
    ids are the recorded ones, which every later read or llm call naming one then matches without
    any mapping of ids; a read the overlay edited is still the overlay's, and its `↳` line comes
    back. One exception: `live` becomes `recording`, because irimi answered it, from disk, and
    `Irimi-Answered-By` must say so. Every such answer carries `from-recording`, which is how a
    reader of the trace tells it from the decision the run would have made itself.

    ONCE A WRITE HAS MISSED, THE REPLAY HAS DIVERGED (`Recording.diverge`), AND READS GO LIVE. A
    recorded read after that point shows the recorded write and not the new one, so from then on
    every read is answered as shadow mode answers it: live, with the overlay applying every write
    in the write log, recorded and new alike. A write L3 checked is checked again, for the same
    reason. An llm call that misses changes nothing about the world irimi shows, so it does not end
    serving reads. The engine translates a read before it asks, as it translated the one it
    recorded, and a recorded read is not overlaid again: a matched write's effect is already in
    those bytes.

    Everything it cannot answer goes to the `ShadowPolicy` it holds, whose reader is the recording
    (`RecordingReader`): a replay never checks a precondition against live state.

    THE NEVER-RAISE RULE covers matching too. An exception while looking a request up is logged, and
    the request is decided as a miss, so a write that broke the matcher is still faked and never
    forwarded.
    """

    name: str = "replay"

    def __init__(
        self,
        recording: Recording,
        maps: MapIndex | None = None,
        keys: idempotency.Store | None = None,
    ) -> None:
        self.recording = recording
        self.maps = maps if maps is not None else MapIndex()
        self.idempotency = keys if keys is not None else idempotency.Store()
        self.shadow = ShadowPolicy(
            reader=RecordingReader(recording), maps=self.maps, keys=self.idempotency
        )

    def recorded_tool_call(self, name: str, args: JSONValue) -> ToolCall | None:
        """The recording's answer to a read tool call (`Recording.take_tool_call`). A lookup
        that raises is logged and is a miss, as a request's is: the tool runs for real."""
        try:
            return self.recording.take_tool_call(name, args)
        except Exception:
            logger.exception("irimi: the recording could not be searched; the tool runs for real")
            return None

    def observe_read(self, service: str, body: bytes) -> None:
        self.shadow.observe_read(service, body)

    def decide(
        self,
        request: Request,
        classification: Classification,
        write_log: Sequence[Exchange] = (),
        run_id: str = "",
        target: ForwardTo | None = None,
    ) -> Decided:
        kind = classification.kind
        try:
            if kind == "telemetry" or target is not None:
                return self.shadow.decide(request, classification, write_log, run_id, target)
            found = self._from_recording(request, classification, run_id)
        except Exception:
            logger.exception("irimi: the recording could not be searched; deciding this as a miss")
            found = None
        if found is not None:
            return found
        if kind not in LIVE_KINDS:
            self.recording.diverge()
        decided = self.shadow.decide(request, classification, write_log, run_id, target)
        return replace(decided, flags=(*decided.flags, Flag.REPLAY_MISS))

    def _from_recording(
        self, request: Request, classification: Classification, run_id: str
    ) -> Decided | None:
        """The recorded answer to this request, or None when the recording has none to give."""
        kind = classification.kind
        held = (
            ""
            if kind in LIVE_KINDS
            else _standing(self.idempotency, request, classification, run_id)
        )
        recorded = self.recording.take(request, kind, held)
        if recorded is None or recorded.response is None:
            return None
        answered_by: AnsweredBy = (
            "recording" if recorded.answered_by == "live" else recorded.answered_by
        )
        answer = Decided(
            Respond(
                _as_served(recorded.response),
                answered_by,
                WriteOutcome(recorded.precondition, recorded.would_fire),
                overlay=recorded.overlay if kind == "read" else None,
            ),
            flags=tuple(flag for flag in recorded.flags if flag in _ANSWER_FLAGS),
        )
        if kind not in LIVE_KINDS:
            # Shadow mode keeps the answer under its idempotency key, so a retry that misses later
            # with the same key and other parameters is refused as the service would refuse it.
            # Kept without `from-recording`: shadow mode answers that retry.
            _remember_request(self.idempotency, request, classification, run_id, answer)
        return replace(answer, flags=(*answer.flags, Flag.FROM_RECORDING))


def _as_served(recorded: Response) -> Response:
    """A recorded response as a replay sends it: the recorded status and body, and the recorded
    headers less the ones that describe how the body travelled (`_RECORDED_ONLY_HEADERS`)."""
    headers = tuple((k, v) for k, v in recorded.headers if k.lower() not in _RECORDED_ONLY_HEADERS)
    return Response(status=recorded.status, headers=headers, body=recorded.body)


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


def _standing(
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


def _remember_request(
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
