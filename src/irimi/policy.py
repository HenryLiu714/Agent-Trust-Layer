"""AnswerPolicy: decides whether an exchange is forwarded live, delegated, or answered locally.

The decision is all that lives here. What a local answer *contains* is `irimi.echo`; where a
delegated one goes is `irimi.delegation`; the read L3 issues goes through `irimi.reader`.
"""

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from irimi import bodies, echo, idempotency, pipeline, services, writelog
from irimi.delegation import ForwardTo, delegate
from irimi.exchange import (
    FIDELITY_DELEGATED_FLAG,
    FIDELITY_FLAGS,
    IDEMPOTENCY_CONFLICT_FLAG,
    IDEMPOTENT_REPLAY_FLAG,
    LIVE_KINDS,
    AnsweredBy,
    Exchange,
    PreconditionOutcome,
    Request,
    Response,
)
from irimi.pipeline import Classification
from irimi.reader import NoReader, Reader
from irimi.servicemap import MapIndex, Route

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Answer:
    answered_by: AnsweredBy
    response: Response | None  # None means "forward live"; set means "send this, do not forward"
    flags: tuple[str, ...] = ()  # merged into the Exchange by the engine
    # Set only on a `delegated` answer, and then `response` is None: the engine forwards there
    # instead of to the real service, and what comes back is what the agent receives.
    forward_to: ForwardTo | None = None
    # What L3 decided about this write before it was faked (#45). None when nothing was asked.
    precondition: PreconditionOutcome | None = None
    # The machine code of the rejection, for the summary line. "" for every other answer.
    rejection_code: str = ""
    # The reads this decision issued on its own account, already annotated. The engine records
    # them; they never reach the agent.
    issued: tuple[Exchange, ...] = ()
    # The webhook events this write would have made the real service send (#47). Set on ONE path:
    # the accepted local fake below. Delegation, live kinds, an idempotency replay, an idempotency
    # conflict and an L3 rejection all return before it, so "only an accepted write lists its
    # webhooks" holds by construction rather than by a second check - the same move #46 made for
    # "a replayed write issues no precondition read".
    would_fire: tuple[str, ...] = ()
    # The currency this write's L3 precondition read found on the object the write names (#60).
    # "" when there was no such read, or when its document named none. Set on the two paths that
    # HAD a document to read - the accepted fake and the L3 rejection - because both print an
    # amount: `report._write_line` renders the map's `human:` sentence before it branches on the
    # rejection, so the `✗` line carries the amount too.
    currency: str = ""


@dataclass(frozen=True)
class _Checked:
    """What L3 made of one write (#45): the outcome, the read it issued, the rejection when the
    service would have refused it, and the currency that read found (#60). The default is "nothing
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


class ShadowPolicy:
    """Reads, llm and telemetry go live. write and unknown are answered locally.

    A locally answered write is the L1 fixture when its route names one and the fixture can be
    read, and the L0 echo otherwise. `echo.fake_response` decides which and says so; the level it
    reports is what the Exchange and the `Irimi-Answered-By` header carry.

    Before that, a write whose route declares a `precondition:` is checked against real state plus
    the run's overlay (#45). A write the check rejects is answered with the service's own error
    body, at the level `echo.fake_rejection` says, instead of the fake.
    """

    name: str = "shadow"

    def __init__(self, reader: Reader | None = None, maps: MapIndex | None = None) -> None:
        self.reader: Reader = reader if reader is not None else NoReader()
        self.maps = maps if maps is not None else MapIndex()
        # Per-instance, never a class attribute: one store per run, and two policies in one test
        # process must not answer each other's keys (#46).
        self.idempotency = idempotency.Store()

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
            # The belt to reflect()'s braces, and now to the fixture's. Raising here would make
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

    def _delegated(self, route: Route | None, forward: ForwardTo) -> Answer:
        """A delegated write is one irimi did not author and cannot check, and nothing in Phase 2
        reads from a target (#45). A policy with no reader asked nothing, delegated or not."""
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
        """`(the slot this write occupies, the answer the run already gave it)` (#46).

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
        """Check the write against real state plus this run's overlay, before faking it (#45).

        Its own guard, not the one `answer` already has: that one degrades a *faker* failure to
        the L0 echo, and a precondition that could not be evaluated is a different thing - the
        write is still faked at its ordinary level, and the exchange says the check did not happen.
        Collapsing them would answer a perfectly fakeable write with `{}`.

        `currency` is the currency (#60): the code the check read off the same document the
        verdict saw, or "" from every path that never got a document. A `NOT_EVALUABLE` verdict
        keeps it - irimi did read the charge, it just could not decide whether the write would
        have been taken, and what a write is denominated in is a different question from whether
        it could be checked.
        """
        # This policy does not do L3. Nothing was asked, so nothing is claimed: `None`, the state
        # of a route with no `precondition:`, and not `not_evaluable`, which is for a question
        # irimi put and could not get an answer to (#45).
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
            # refuse whatever they do not call one (#45).
            probe_cls = pipeline.classify(probe_request, self.maps)
            if probe_cls.kind != "read":
                logger.warning(
                    "irimi: refusing the precondition read %s, which the maps call %s",
                    probe.operation,
                    probe_cls.kind,
                )
                return _Checked("not_evaluable", issued)
            try:
                response = self.reader(probe_request)
            except Exception as exc:
                # The `Reader` contract: raising is the same answer as None.
                logger.warning("irimi: the precondition read %s raised: %s", probe.operation, exc)
                response = None
            # Recorded before any verdict, and with no response when the reader got none, so a
            # network failure, a 429 and a body irimi cannot use all leave a trace of the read
            # irimi attempted on the agent's behalf (#45).
            issued = (
                pipeline.annotate(
                    probe_request, response, probe_cls, "live", run_id, issued_by="engine"
                ),
            )
            if response is None:
                return _Checked("not_evaluable", issued)
            # Only a 200 is evaluable. A 429, a 404 or a 5xx says nothing about the write (#45).
            if response.status != 200:
                return _Checked("not_evaluable", issued)
            # The reader's body never passes the engine's `response` hook, so this is the only
            # place Slack's `ts` watermark can learn from the one read that exists to make the
            # fake honest (#42).
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
                    # invent a refusal that never would have happened (#45).
                    return _Checked("not_evaluable", issued)
                document = applied.document
            # Off the same document the verdict sees, and before it, so a `NOT_EVALUABLE` verdict
            # still carries it - see the docstring (#60).
            currency = check.currency(document) if check.currency is not None else ""
            verdict = check.verdict(proposal, document)
            if isinstance(verdict, services.NotEvaluable):
                # The check read the document and could not tell - a Slack `missing_scope`, a
                # charge whose own numbers will not parse. Distinct from a pass, which claims
                # the write was checked and would have been taken (#45).
                return _Checked("not_evaluable", issued, currency=currency)
            return _Checked(
                "rejected" if verdict is not None else "passed", issued, verdict, currency
            )
        except Exception:
            # An exception means irimi did put the question and could not answer it, so this is
            # `not_evaluable` and never None.
            logger.exception("irimi: the precondition check failed; faking the write at L2")
            return _Checked("not_evaluable", issued)


def _probe_request(request: Request, service: str, probe: services.Probe) -> Request:
    """The precondition read, addressed where the write was and carrying its credentials.

    The agent's credentials and its account and version scope are exactly what make this read see
    the world the write would have been made in. Nothing else of the agent's is copied: its other
    headers would send an `Idempotency-Key` on a GET (#45).
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
    """The first answer to this key, again (#46).

    Every field is the stored one. `answered_by` in particular is not re-derived: a fixture that
    has become unreadable since the first call would make the replay `fake-L0` over an original
    stamped `fake-L1`, and the trace would disagree with itself about one write. `issued` is `()`
    because a replay makes no precondition read - the write was checked once, when it was made.
    `currency` is the stored one too, so a replayed refund's exchange and the first one's agree
    about what that single write was denominated in (#60).
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
    """The service's own `idempotency_error`: this key was used for a different write (#46).

    NOT `precondition: rejected` - `precondition` says what L3 decided, and L3 was never asked.
    `rejection_code` is still set, because the summary's `would fail:` line is about what the
    real service would have answered, and this is one of those. The body is carried at the level
    this route's write would have been faked at, the way `fake_rejection` carries an L3 one.
    """
    fake = echo.fake_rejection(request, classification, refusal.body)
    return _local(fake, refusal.status, (IDEMPOTENCY_CONFLICT_FLAG,), rejection_code=refusal.code)


def _remember(store: idempotency.Store, slot: idempotency.Slot | None, answer: Answer) -> None:
    """Keep this answer under its key, so the agent's retry gets it back (#46).

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
