"""AnswerPolicy: decides whether an exchange is forwarded live, delegated, or answered locally,
and whether a read tool the proxy cannot see is answered from a replay's recording.

The decision is all that lives here. What a local answer *contains* is `irimi.echo`; where a
delegated one goes is `irimi.delegation`; the read L3 issues goes through `irimi.reader`; what a
replay answers from is `irimi.recording`.
"""

import json
import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Any, Protocol

from irimi import bodies, echo, idempotency, pipeline, services, writelog
from irimi.delegation import ForwardTo, delegate
from irimi.exchange import (
    FIDELITY_DELEGATED_FLAG,
    FIDELITY_FLAGS,
    FIXTURE_FAILED_FLAG,
    FROM_RECORDING_FLAG,
    IDEMPOTENCY_CONFLICT_FLAG,
    IDEMPOTENT_REPLAY_FLAG,
    LIVE_KINDS,
    REPLAY_MISS_FLAG,
    AnsweredBy,
    Exchange,
    OverlayFidelity,
    PreconditionOutcome,
    Request,
    Response,
)
from irimi.pipeline import Classification
from irimi.reader import NoReader, Reader, answered_as
from irimi.recording import Recording, RecordingReader
from irimi.servicemap import MapIndex, Route
from irimi.trace import JSONValue, ToolCall

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Answer:
    answered_by: AnsweredBy
    response: Response | None  # None means "forward live"; set means "send this, do not forward"
    flags: tuple[str, ...] = ()  # merged into the Exchange by the engine
    # Set only on a `delegated` answer, and then `response` is None: the engine forwards there
    # instead of to the real service, and what comes back is what the agent receives.
    forward_to: ForwardTo | None = None
    # What L3 decided about this write before it was faked. None when nothing was asked.
    precondition: PreconditionOutcome | None = None
    # The machine code of the rejection, for the summary line. "" for every other answer.
    rejection_code: str = ""
    # The reads this decision issued on its own account, already annotated. The engine records
    # them; they never reach the agent.
    issued: tuple[Exchange, ...] = ()
    # The webhook events this write would have made the real service send. Set on ONE path:
    # the accepted local fake below. Delegation, live kinds, an idempotency replay, an idempotency
    # conflict and an L3 rejection all return before it, so "only an accepted write lists its
    # webhooks" holds by construction rather than by a second check, as "a replayed write issues
    # no precondition read" does.
    would_fire: tuple[str, ...] = ()
    # The currency this write's L3 precondition read found on the object the write names.
    # "" when there was no such read, or when its document named none. Set on the two paths that
    # HAD a document to read - the accepted fake and the L3 rejection - because both print an
    # amount: `report.write_sentence` renders the map's `human:` sentence before it branches on
    # the rejection, so the `✗` line carries the amount too.
    currency: str = ""
    # How much of the run's writes this answer's body shows, for an answer the engine does not
    # put past the overlay itself: a read a replay answered from the recording, whose bytes the
    # overlay edited in the run it replays. None for every other answer.
    overlay: OverlayFidelity | None = None


@dataclass(frozen=True)
class _Checked:
    """What L3 made of one write: the outcome, the read it issued, the rejection when the
    service would have refused it, and the currency that read found. The default is "nothing
    was asked" - a route with no `precondition:`, or a policy with no reader."""

    outcome: PreconditionOutcome | None = None
    issued: tuple[Exchange, ...] = ()
    rejection: services.Rejection | None = None
    currency: str = ""


class AnswerPolicy(Protocol):
    name: str

    def answer(
        self,
        request: Request,
        classification: Classification,
        write_log: Sequence[Exchange] = (),
        run_id: str = "",
    ) -> Answer: ...

    def recorded_tool_call(self, name: str, args: JSONValue) -> ToolCall | None:
        """The recorded read tool call that answers this one, which the SDK hands back in place
        of running the tool, or None: the tool runs for real. Never raises."""
        ...


class ShadowPolicy:
    """Reads, llm and telemetry go live. write and unknown are answered locally.

    A locally answered write is the L1 fixture when its route names one and the fixture can be
    read, and the L0 echo otherwise. `echo.fake_response` decides which and says so; the level it
    reports is what the Exchange and the `Irimi-Answered-By` header carry.

    Before that, a write whose route declares a `precondition:` is checked against real state plus
    the run's overlay. A write the check rejects is answered with the service's own error
    body, at the level `echo.fake_rejection` says, instead of the fake.
    """

    name: str = "shadow"

    def __init__(self, reader: Reader | None = None, maps: MapIndex | None = None) -> None:
        self.reader: Reader = reader if reader is not None else NoReader()
        self.maps = maps if maps is not None else MapIndex()
        # Per-instance, never a class attribute: one store per run, and two policies in one test
        # process must not answer each other's keys.
        self.idempotency = idempotency.Store()

    def recorded_tool_call(self, name: str, args: JSONValue) -> ToolCall | None:
        """None: shadow mode answers from no recording, so every read tool runs for real."""
        return None

    def answer(
        self,
        request: Request,
        classification: Classification,
        write_log: Sequence[Exchange] = (),
        run_id: str = "",
    ) -> Answer:
        route = classification.route
        forward = delegate(request, classification)
        if forward is not None:
            return self._delegated(route, forward)
        if classification.kind in LIVE_KINDS:
            return Answer(answered_by="live", response=None)
        slot, known = self._recall(request, classification, run_id)
        if known is not None:
            return known
        checked = _Checked()
        if route is not None and route.precondition:
            checked = self._precondition(request, classification, route, write_log, run_id)
            if checked.rejection is not None:
                fake = echo.fake_rejection(request, classification, checked.rejection.body)
                answer = _local(
                    fake,
                    checked.rejection.status,
                    precondition="rejected",
                    rejection_code=checked.rejection.code,
                    issued=checked.issued,
                    currency=checked.currency,
                )
                _remember(self.idempotency, slot, answer)
                return answer
        try:
            fake = echo.fake_response(request, classification)
        except Exception:
            # The belt to reflect()'s braces, and to the fixture's. Raising here would make
            # mitmproxy forward the flow, and a forwarded write escapes shadow mode - an empty
            # object is far better. L0 is the floor whatever failed above it.
            logger.exception("irimi: the local answer failed; answering with an empty object")
            fake = echo.Fake(b"{}", bodies.JSON_CT)
        answer = _local(
            fake,
            200,
            precondition=checked.outcome,
            issued=checked.issued,
            would_fire=route.fires if route is not None else (),
            currency=checked.currency,
        )
        _remember(self.idempotency, slot, answer)
        return answer

    def remember(
        self, request: Request, classification: Classification, run_id: str, answer: Answer
    ) -> None:
        """Keep `answer` as this write's, under its idempotency key, as `answer` keeps its own: a
        replay calls it for a write it answered from the recording, so the agent's retry
        meets it. Never raises: a key that cannot be read costs that retry its replay, and nothing
        else."""
        try:
            slot = idempotency.slot_for(request, classification, run_id)
        except Exception:
            logger.exception("irimi: this write's idempotency key could not be read")
            return
        _remember(self.idempotency, slot, answer)

    def idempotency_flag(
        self, request: Request, classification: Classification, run_id: str
    ) -> str:
        """The idempotency flag this policy's answer to this write would carry:
        `idempotent-replay` when the run has answered its key with the same parameters,
        `idempotency-conflict` when with others, and "" when it has not, or there is no key. A
        replay takes a recorded write only from one that carried the same: the service
        answers a retry, a reused key and a fresh write three ways whatever their bodies say. Never
        raises: `_recall` is guarded."""
        _, known = self._recall(request, classification, run_id)
        if known is None:
            return ""
        if IDEMPOTENCY_CONFLICT_FLAG in known.flags:
            return IDEMPOTENCY_CONFLICT_FLAG
        return IDEMPOTENT_REPLAY_FLAG

    def _delegated(self, route: Route | None, forward: ForwardTo) -> Answer:
        """A delegated write is one irimi did not author and cannot check: no precondition reads
        from a target. A policy with no reader asked nothing, delegated or not."""
        declared = route is not None and bool(route.precondition)
        asked = declared and not isinstance(self.reader, NoReader)
        return Answer(
            answered_by="delegated",
            response=None,
            flags=(FIDELITY_DELEGATED_FLAG,),
            forward_to=forward,
            precondition="not_evaluable" if asked else None,
        )

    def _recall(
        self, request: Request, classification: Classification, run_id: str
    ) -> tuple[idempotency.Slot | None, Answer | None]:
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
                stored, conflicted = self.idempotency.get(slot.key, slot.params)
                if conflicted:
                    refusal = idempotency.conflict(classification.service, slot.sent)
                    if refusal is not None:
                        return slot, _conflict_answer(request, classification, refusal)
                elif stored is not None:
                    return slot, _replayed_answer(stored)
        except Exception:
            logger.exception("irimi: the idempotency store failed; answering this write afresh")
            slot = None
        return slot, None

    def _precondition(
        self,
        request: Request,
        classification: Classification,
        route: Route,
        write_log: Sequence[Exchange],
        run_id: str,
    ) -> _Checked:
        """Check the write against real state plus this run's overlay, before faking it.

        Its own guard, not the one `answer` already has: that one degrades a *faker* failure to
        the L0 echo, and a precondition that could not be evaluated is a different thing - the
        write is still faked at its ordinary level, and the exchange says the check did not happen.
        Collapsing them would answer a perfectly fakeable write with `{}`.

        `currency` is the currency: the code the check read off the same document the
        verdict saw, or "" from every path that never got a document. A `NOT_EVALUABLE` verdict
        keeps it - irimi did read the charge, it just could not decide whether the write would
        have been taken, and what a write is denominated in is a different question from whether
        it could be checked.
        """
        # This policy does not do L3. Nothing was asked, so nothing is claimed: `None`, the state
        # of a route with no `precondition:`, and not `not_evaluable`, which is for a question
        # irimi put and could not get an answer to.
        if isinstance(self.reader, NoReader):
            return _Checked()
        issued: tuple[Exchange, ...] = ()
        try:
            service = classification.service
            # A map naming a check nothing implements is the same class of thing as a fixture that
            # will not load: honest degradation, said out loud.
            check = services.PRECONDITIONS.get((service, route.precondition))
            if check is None:
                logger.warning(
                    "irimi: no precondition named %r for %s", route.precondition, service
                )
                return _Checked("not_evaluable", issued)
            proposal = services.Proposal(classification.operation, bodies.reflect(request))
            probe = check.probe(proposal)
            if probe is None:
                return _Checked("not_evaluable", issued)
            probe_request = _probe_request(request, service, probe)
            # "Read operations only, never a read-like POST", in the one form that can be enforced:
            # ask the maps, which are what says a Slack `conversations.info` POST is a read, and
            # refuse whatever they do not call one.
            probe_cls = pipeline.classify(probe_request, self.maps)
            if probe_cls.kind != "read":
                logger.warning(
                    "irimi: refusing the precondition read %s, which the maps call %s",
                    probe.operation,
                    probe_cls.kind,
                )
                return _Checked("not_evaluable", issued)
            # The read's own span, not the write's: the engine records it as an exchange of its
            # own, and its timestamps are the `Reader` call's.
            started_at = time.time()
            try:
                response = self.reader(probe_request)
            except Exception as exc:
                # The `Reader` contract: raising is the same answer as None.
                logger.warning("irimi: the precondition read %s raised: %s", probe.operation, exc)
                response = None
            ended_at = time.time()
            read_answered_by, read_flags = answered_as(self.reader, response is not None)
            # Recorded before any verdict, and with no response when the reader got none, so a
            # network failure, a 429 and a body irimi cannot use all leave a trace of the read
            # irimi attempted on the agent's behalf.
            issued = (
                pipeline.annotate(
                    probe_request,
                    response,
                    probe_cls,
                    read_answered_by,
                    run_id,
                    extra_flags=read_flags,
                    issued_by="engine",
                    started_at=started_at,
                    ended_at=ended_at,
                ),
            )
            if response is None:
                return _Checked("not_evaluable", issued)
            # Only a 200 is evaluable. A 429, a 404 or a 5xx says nothing about the write.
            if response.status != 200:
                return _Checked("not_evaluable", issued)
            # The reader's body never passes the engine's `response` hook, so this is the only
            # place Slack's `ts` watermark can learn from the one read that exists to make the
            # fake honest.
            echo.observe_read(service, response.body)
            document: Any = bodies.json_object(response.body)
            if document is None:
                return _Checked("not_evaluable", issued)
            found = writelog.scoped_writes(services.EFFECTS, service, probe_request, write_log)
            if found is not None:
                effects, writes = found
                # The same effects the overlay applies to a live read, so the check sees the
                # world the agent would see. For Slack's `conversations.info` they hand the
                # document back untouched, and that is right rather than a gap: no write irimi
                # models changes whether a channel exists, is archived or has the bot in it.
                # One path for both services is one place to read and one shape to replay.
                applied = effects(
                    writelog.read_of(probe_cls.operation, probe_request), document, writes
                )
                if applied.partial:
                    # The overlay is saying the world it can show is incomplete, and a write
                    # checked against a world known to be incomplete was not checked. Passing
                    # it could bless a write production would refuse; rejecting it could
                    # invent a refusal that never would have happened.
                    return _Checked("not_evaluable", issued)
                document = applied.document
            # Off the same document the verdict sees, and before it, so a `NOT_EVALUABLE` verdict
            # still carries it - see the docstring.
            currency = check.currency(document) if check.currency is not None else ""
            verdict = check.verdict(proposal, document)
            if isinstance(verdict, services.NotEvaluable):
                # The check read the document and could not tell - a Slack `missing_scope`, a
                # charge whose own numbers will not parse. Distinct from a pass, which claims
                # the write was checked and would have been taken.
                return _Checked("not_evaluable", issued, currency=currency)
            return _Checked(
                "rejected" if verdict is not None else "passed", issued, verdict, currency
            )
        except Exception:
            # An exception means irimi did put the question and could not answer it, so this is
            # `not_evaluable` and never None.
            logger.exception("irimi: the precondition check failed; faking the write at L2")
            return _Checked("not_evaluable", issued)


# The flags a recorded ANSWER carried besides its level: a fixture that did not load, and the two
# halves of idempotency. A replay that answers again carries its level's fidelity flag, these, and
# `from-recording`, and no other: the classifier's own flags come back from this request's
# classification, and the engine's (a body cut at the cap, a lost upstream) are facts about the
# recorded exchange, never about this one.
_ANSWER_FLAGS = frozenset({FIXTURE_FAILED_FLAG, IDEMPOTENT_REPLAY_FLAG, IDEMPOTENCY_CONFLICT_FLAG})
# Headers a recorded response carries that describe bytes it no longer has. The body was stored
# decoded and whole, so its `Content-Encoding` and framing are not the body served; mitmproxy sets
# the length of what is. And the recorded run's own stamp, which `pipeline.respond` replaces.
_RECORDED_ONLY_HEADERS = frozenset(
    {"content-encoding", "content-length", "transfer-encoding", pipeline.ANSWERED_BY_HEADER}
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

    A write is looked up with its standing in the run's idempotency store
    (`ShadowPolicy.idempotency_flag`), and one L3 checked is taken only while the replay has not
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

    def __init__(self, recording: Recording, maps: MapIndex | None = None) -> None:
        self.recording = recording
        self.maps = maps if maps is not None else MapIndex()
        self.shadow = ShadowPolicy(reader=RecordingReader(recording), maps=self.maps)

    def recorded_tool_call(self, name: str, args: JSONValue) -> ToolCall | None:
        """The recording's answer to a read tool call (`Recording.take_tool_call`). A lookup
        that raises is logged and is a miss, as a request's is: the tool runs for real."""
        try:
            return self.recording.take_tool_call(name, args)
        except Exception:
            logger.exception("irimi: the recording could not be searched; the tool runs for real")
            return None

    def answer(
        self,
        request: Request,
        classification: Classification,
        write_log: Sequence[Exchange] = (),
        run_id: str = "",
    ) -> Answer:
        kind = classification.kind
        try:
            if kind == "telemetry" or delegate(request, classification) is not None:
                return self.shadow.answer(request, classification, write_log, run_id)
            found = self._from_recording(request, classification, run_id)
        except Exception:
            logger.exception("irimi: the recording could not be searched; deciding this as a miss")
            found = None
        if found is not None:
            return found
        if kind not in LIVE_KINDS:
            self.recording.diverge()
        answer = self.shadow.answer(request, classification, write_log, run_id)
        return replace(answer, flags=(*answer.flags, REPLAY_MISS_FLAG))

    def _from_recording(
        self, request: Request, classification: Classification, run_id: str
    ) -> Answer | None:
        """The recorded answer to this request, or None when the recording has none to give."""
        kind = classification.kind
        held = (
            ""
            if kind in LIVE_KINDS
            else self.shadow.idempotency_flag(request, classification, run_id)
        )
        recorded = self.recording.take(request, kind, held)
        if recorded is None or recorded.response is None:
            return None
        answered_by: AnsweredBy = (
            "recording" if recorded.answered_by == "live" else recorded.answered_by
        )
        kept = tuple(flag for flag in recorded.flags if flag in _ANSWER_FLAGS)
        answer = Answer(
            answered_by=answered_by,
            response=_as_served(recorded.response),
            flags=(FIDELITY_FLAGS[answered_by], *kept, FROM_RECORDING_FLAG),
            precondition=recorded.precondition,
            rejection_code=recorded.rejection_code,
            would_fire=recorded.would_fire,
            currency=recorded.currency,
            overlay=recorded.overlay if kind == "read" else None,
        )
        if kind not in LIVE_KINDS:
            # Shadow mode keeps the answer under its idempotency key, so a retry that misses later
            # with the same key and other parameters is refused as the service would refuse it.
            # Kept without `from-recording`: shadow mode answers that retry.
            self.shadow.remember(
                request, classification, run_id, replace(answer, flags=answer.flags[:-1])
            )
        return answer


def _as_served(recorded: Response) -> Response:
    """A recorded response as a replay sends it: the recorded status and body, and the recorded
    headers less the ones that describe how the body travelled (`_RECORDED_ONLY_HEADERS`)."""
    headers = tuple((k, v) for k, v in recorded.headers if k.lower() not in _RECORDED_ONLY_HEADERS)
    return Response(status=recorded.status, headers=headers, body=recorded.body)


def _probe_request(request: Request, service: str, probe: services.Probe) -> Request:
    """The precondition read, addressed where the write was and carrying its credentials.

    The agent's credentials and its account and version scope are exactly what make this read see
    the world the write would have been made in. Nothing else of the agent's is copied: its other
    headers would send an `Idempotency-Key` on a GET.
    """
    wanted = ("authorization", *services.SCOPE_HEADERS.get(service, ()))
    headers = [(k, v) for k, v in request.headers if k in wanted]
    headers.append(("accept", "application/json"))
    body = b""
    if probe.posted is not None:
        body = json.dumps(probe.posted).encode()
        headers.append(("content-type", "application/json; charset=utf-8"))
    return Request(
        method=probe.method,
        scheme=request.scheme,
        host=request.host,
        port=request.port,
        path=probe.path,
        query=probe.query,
        headers=tuple(headers),
        body=body,
    )


def _local(
    fake: echo.Fake,
    status: int,
    extra_flags: tuple[str, ...] = (),
    *,
    precondition: PreconditionOutcome | None = None,
    rejection_code: str = "",
    issued: tuple[Exchange, ...] = (),
    would_fire: tuple[str, ...] = (),
    currency: str = "",
) -> Answer:
    """An answer irimi built itself, from the body `echo` built: the fake's level is the answer's,
    and its fidelity flag comes first, then the fake's own flags, then the caller's."""
    return Answer(
        answered_by=fake.answered_by,
        response=Response(
            status=status, headers=(("content-type", fake.content_type),), body=fake.body
        ),
        flags=(FIDELITY_FLAGS[fake.answered_by], *fake.flags, *extra_flags),
        precondition=precondition,
        rejection_code=rejection_code,
        issued=issued,
        would_fire=would_fire,
        currency=currency,
    )


def _replayed_answer(stored: idempotency.Stored) -> Answer:
    """The first answer to this key, again.

    Every field is the stored one. `answered_by` in particular is not re-derived: a fixture that
    has become unreadable since the first call would make the replay `fake-L0` over an original
    stamped `fake-L1`, and the trace would disagree with itself about one write. `issued` is `()`
    because a replay makes no precondition read - the write was checked once, when it was made.
    `currency` is the stored one too, so a replayed refund's exchange and the first one's agree
    about what that single write was denominated in.
    """
    return Answer(
        answered_by=stored.answered_by,
        response=Response(
            status=stored.status,
            headers=(
                ("content-type", stored.content_type),
                (idempotency.REPLAYED_HEADER, idempotency.REPLAYED_VALUE),
            ),
            body=stored.body,
        ),
        flags=(*stored.flags, IDEMPOTENT_REPLAY_FLAG),
        precondition=stored.precondition,
        rejection_code=stored.rejection_code,
        currency=stored.currency,
    )


def _conflict_answer(
    request: Request, classification: Classification, refusal: services.Rejection
) -> Answer:
    """The service's own `idempotency_error`: this key was used for a different write.

    NOT `precondition: rejected` - `precondition` says what L3 decided, and L3 was never asked.
    `rejection_code` is still set, because the summary's `would fail:` line is about what the
    real service would have answered, and this is one of those. The body is carried at the level
    this route's write would have been faked at, the way `fake_rejection` carries an L3 one.
    """
    fake = echo.fake_rejection(request, classification, refusal.body)
    return _local(fake, refusal.status, (IDEMPOTENCY_CONFLICT_FLAG,), rejection_code=refusal.code)


def _remember(store: idempotency.Store, slot: idempotency.Slot | None, answer: Answer) -> None:
    """Keep this answer under its key, so the agent's retry gets it back.

    Stripe stores the first response under a key whether it was accepted or REJECTED, so this is
    called on both of `answer`'s local paths. Guarded like the lookup: a store that cannot
    remember costs the next retry its replay, and nothing else - raising here would forward a
    write that has already been answered.
    """
    if slot is None or answer.response is None:
        return
    try:
        content_type = answer.response.header("content-type") or bodies.JSON_CT
        store.put(
            slot.key,
            slot.params,
            idempotency.Stored(
                status=answer.response.status,
                body=answer.response.body,
                content_type=content_type,
                answered_by=answer.answered_by,
                flags=answer.flags,
                precondition=answer.precondition,
                rejection_code=answer.rejection_code,
                currency=answer.currency,
            ),
        )
    except Exception:
        logger.exception("irimi: this write's answer could not be stored for a retry")
