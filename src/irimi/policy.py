"""AnswerPolicy: decides whether an exchange is forwarded live, delegated, or answered locally.

The decision is all that lives here. What a local answer *contains* is `irimi.echo`; where a
delegated one goes is `irimi.delegation`. The one exception is `UpstreamReader`, the real read L3
needs to decide about a write (#45); it lives beside the `Reader` seam it fills, and
`cli._build_engine` is the only place in the product that builds one.
"""

import json
import logging
import ssl
import urllib.error
import urllib.request
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from irimi import echo, idempotency, pipeline, services, writelog
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


class AnswerPolicy(Protocol):
    name: str

    def answer(
        self,
        request: Request,
        classification: Classification,
        write_log: Sequence[Exchange] = (),
        run_id: str = "",
    ) -> Answer: ...


# How long the engine waits for its own precondition read. The request hook runs the decision on a
# worker thread as of #45, so this delays one write's answer and nothing else - but a write whose
# answer never comes is a hung agent, so it is bounded, once, with no retry: a retried precondition
# read is a second real request made on the agent's behalf that the agent did not make.
PRECONDITION_TIMEOUT_S = 5.0


class Reader(Protocol):
    """One real read, issued by the engine rather than by the agent (#45).

    Returning None, or raising, means `not_evaluable`: irimi could not find out, says so on the
    exchange, and falls back to the L2 answer. Shadow mode's reader dials the real upstream;
    Phase 5's replay policy gets one over the recording, which is why this is a seam and not a
    call inside the engine.
    """

    def __call__(self, request: Request) -> Response | None: ...


class NoReader:
    """The default: a policy that does not do L3 at all.

    Distinct from a reader that tried and could not tell. A precondition on a policy holding this
    is recorded `precondition: None` - the same state as a route that declares no check - because
    nothing was asked, and `not_evaluable` is reserved for a question irimi put and could not get
    an answer to. Shadow mode never holds one: `cli._build_engine` wires `UpstreamReader`, and
    `tests/test_invariants.py` pins that it does.
    """

    def __call__(self, request: Request) -> Response | None:
        return None


class UpstreamReader:
    """`Reader` over the real service, on stdlib urllib - no new dependency, no proxy.

    It dials `request.host:request.port` exactly as the classifier saw them, so a test map
    claiming a loopback stub is dialled at the stub and the real Stripe is never touched. It does
    not go through irimi's own listener: the agent's request has already been rewritten to its
    upstream by the time the policy sees it.
    """

    def __call__(self, request: Request) -> Response | None:
        req = urllib.request.Request(
            request.url,
            data=request.body or None,
            headers={k: v for k, v in request.headers if k not in ("host", "content-length")},
            method=request.method,
        )
        try:
            # A context on an `http://` URL is accepted and ignored, so there is no scheme branch.
            with urllib.request.urlopen(
                req, timeout=PRECONDITION_TIMEOUT_S, context=ssl.create_default_context()
            ) as resp:
                return _capped(resp.status, resp.headers.items(), resp)
        except urllib.error.HTTPError as err:
            # A 429 or a 404 is information, not a failure to read: the exchange records it before
            # the policy calls it `not_evaluable` (#45).
            with err:
                return _capped(err.code, err.headers.items(), err)
        except Exception as exc:
            # Not `logger.exception`: an unreachable upstream is a normal outcome here, not a bug.
            logger.warning("irimi: the precondition read of %s failed: %s", request.url, exc)
            return None


def _capped(status: int, headers: Any, body: Any) -> Response | None:
    """The response, or None when its body is past `writelog.MAX_BODY_BYTES`.

    Read with a cap rather than whole, so a huge body cannot be pulled into memory on the answer
    path only to be refused by `writelog.json_object` afterwards (#45).
    """
    data = body.read(writelog.MAX_BODY_BYTES + 1)
    if len(data) > writelog.MAX_BODY_BYTES:
        logger.warning(
            "irimi: a precondition read answered more than %d bytes", writelog.MAX_BODY_BYTES
        )
        return None
    return Response(status=status, headers=tuple(headers), body=data)


def _slot(
    request: Request, classification: Classification, route: Route | None, run_id: str
) -> tuple[tuple[str, ...], idempotency.Canonical, str] | None:
    """`(slot, canonical params, the key the caller sent)` for a write the store covers, else None.

    Three conditions, each the literal reading of "mapped Stripe writes" (#46). The route must be
    matched, because `volatile:` is what makes two sendings comparable and an unmapped POST has
    none. The kind must be `write`, so a route THE SCOPE RULE downgraded to `unknown` is answered
    the way an unclassified request is and not out of a store. And the service must be one that
    HAS an idempotency mechanism, which is `key_of` returning something.
    """
    if route is None or classification.kind != "write":
        return None
    service = classification.service
    key = idempotency.key_of(service, request)
    if not key:
        return None
    scope = writelog.scope(service, request)
    return idempotency.key(run_id, service, scope, key), idempotency.canonical(request, route), key


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
        route = classification.matched[1] if classification.matched is not None else None
        forward = delegate(request, classification)
        if forward is not None:
            # A delegated write is one irimi did not author and cannot check, and nothing in Phase
            # 2 reads from a target (#45). A policy with no reader asked nothing, delegated or not.
            declared = route is not None and bool(route.precondition)
            asked = declared and not isinstance(self.reader, NoReader)
            return Answer(
                answered_by="delegated",
                response=None,
                flags=(FIDELITY_DELEGATED_FLAG,),
                forward_to=forward,
                precondition="not_evaluable" if asked else None,
            )
        if classification.kind in LIVE_KINDS:
            return Answer(answered_by="live", response=None)
        # Before L3, and before anything is minted (#46). A retry with a key this run has already
        # answered is the SAME write: it gets the first answer's own bytes, issues no precondition
        # read, and appends nothing to the write log. Its own guard, because a store that failed
        # must fall through to the ordinary answer rather than 502 a perfectly fakeable write.
        slot: tuple[tuple[str, ...], idempotency.Canonical, str] | None = None
        try:
            slot = _slot(request, classification, route, run_id)
            if slot is not None:
                stored, conflicted = self.idempotency.get(slot[0], slot[1])
                if conflicted:
                    refusal = idempotency.conflict(classification.service, slot[2])
                    if refusal is not None:
                        return _conflict_answer(request, classification, refusal)
                elif stored is not None:
                    return _replayed_answer(stored)
        except Exception:
            logger.exception("irimi: the idempotency store failed; answering this write afresh")
            slot = None
        precondition: PreconditionOutcome | None = None
        issued: tuple[Exchange, ...] = ()
        if route is not None and route.precondition:
            precondition, issued, rejection = self._precondition(
                request, classification, route, write_log, run_id
            )
            if rejection is not None:
                fake = echo.fake_rejection(request, classification, rejection.body)
                answer = Answer(
                    answered_by=fake.answered_by,
                    response=Response(
                        status=rejection.status,
                        headers=(("content-type", fake.content_type),),
                        body=fake.body,
                    ),
                    flags=(FIDELITY_FLAGS[fake.answered_by], *fake.flags),
                    precondition="rejected",
                    rejection_code=rejection.code,
                    issued=issued,
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
            fake = echo.Fake(b"{}", echo.JSON_CT)
        answer = Answer(
            answered_by=fake.answered_by,
            response=Response(
                status=200,
                headers=(("content-type", fake.content_type),),
                body=fake.body,
            ),
            flags=(FIDELITY_FLAGS[fake.answered_by], *fake.flags),
            precondition=precondition,
            issued=issued,
        )
        _remember(self.idempotency, slot, answer)
        return answer

    def _precondition(
        self,
        request: Request,
        classification: Classification,
        route: Route,
        write_log: Sequence[Exchange],
        run_id: str,
    ) -> tuple[PreconditionOutcome | None, tuple[Exchange, ...], services.Rejection | None]:
        """Check the write against real state plus this run's overlay, before faking it (#45).

        Its own guard, not the one `answer` already has: that one degrades a *faker* failure to
        the L0 echo, and a precondition that could not be evaluated is a different thing - the
        write is still faked at its ordinary level, and the exchange says the check did not happen.
        Collapsing them would answer a perfectly fakeable write with `{}`.
        """
        # This policy does not do L3. Nothing was asked, so nothing is claimed: `None`, the state
        # of a route with no `precondition:`, and not `not_evaluable`, which is for a question
        # irimi put and could not get an answer to (#45).
        if isinstance(self.reader, NoReader):
            return None, (), None
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
                return "not_evaluable", issued, None
            proposal = services.Proposal(classification.operation, echo.reflect(request))
            probe = check.probe(proposal)
            if probe is None:
                return "not_evaluable", issued, None
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
                return "not_evaluable", issued, None
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
                return "not_evaluable", issued, None
            # Only a 200 is evaluable. A 429, a 404 or a 5xx says nothing about the write (#45).
            if response.status != 200:
                return "not_evaluable", issued, None
            # The reader's body never passes the engine's `response` hook, so this is the only
            # place Slack's `ts` watermark can learn from the one read that exists to make the
            # fake honest (#42).
            echo.observe_read(service, response.body)
            document: Any = writelog.json_object(response.body)
            if document is None:
                return "not_evaluable", issued, None
            effects = services.EFFECTS.get(service)
            if effects is not None:
                writes = writelog.decode(service, probe_request, write_log)
                if writes:
                    read = services.Read(
                        operation=probe_cls.operation,
                        request=probe_request,
                        posted=echo.reflect(probe_request),
                    )
                    # The same effects the overlay applies to a live read, so the check sees the
                    # world the agent would see. For Slack's `conversations.info` they hand the
                    # document back untouched, and that is right rather than a gap: no write irimi
                    # models changes whether a channel exists, is archived or has the bot in it.
                    # One path for both services is one place to read and one shape to replay.
                    applied = effects(read, document, writes)
                    if applied.partial:
                        # The overlay is saying the world it can show is incomplete, and a write
                        # checked against a world known to be incomplete was not checked. Passing
                        # it could bless a write production would refuse; rejecting it could
                        # invent a refusal that never would have happened (#45).
                        return "not_evaluable", issued, None
                    document = applied.document
            verdict = check.verdict(proposal, document)
            if isinstance(verdict, services.NotEvaluable):
                # The check read the document and could not tell - a Slack `missing_scope`, a
                # charge whose own numbers will not parse. Distinct from a pass, which claims
                # the write was checked and would have been taken (#45).
                return "not_evaluable", issued, None
            return ("rejected" if verdict is not None else "passed"), issued, verdict
        except Exception:
            # An exception means irimi did put the question and could not answer it, so this is
            # `not_evaluable` and never None.
            logger.exception("irimi: the precondition check failed; faking the write at L2")
            return "not_evaluable", issued, None


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


def _replayed_answer(stored: idempotency.Stored) -> Answer:
    """The first answer to this key, again (#46).

    Every field is the stored one. `answered_by` in particular is not re-derived: a fixture that
    has become unreadable since the first call would make the replay `fake-L0` over an original
    stamped `fake-L1`, and the trace would disagree with itself about one write. `issued` is `()`
    because a replay makes no precondition read - the write was checked once, when it was made.
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
    return Answer(
        answered_by=fake.answered_by,
        response=Response(
            status=refusal.status,
            headers=(("content-type", fake.content_type),),
            body=fake.body,
        ),
        flags=(FIDELITY_FLAGS[fake.answered_by], *fake.flags, IDEMPOTENCY_CONFLICT_FLAG),
        rejection_code=refusal.code,
    )


def _remember(
    store: idempotency.Store,
    slot: tuple[tuple[str, ...], idempotency.Canonical, str] | None,
    answer: Answer,
) -> None:
    """Keep this answer under its key, so the agent's retry gets it back (#46).

    Stripe stores the first response under a key whether it was accepted or REJECTED, so this is
    called on both of `answer`'s local paths. Guarded like the lookup: a store that cannot
    remember costs the next retry its replay, and nothing else - raising here would forward a
    write that has already been answered.
    """
    if slot is None or answer.response is None:
        return
    try:
        content_type = next(
            (v for k, v in answer.response.headers if k.lower() == "content-type"), echo.JSON_CT
        )
        store.put(
            slot[0],
            slot[1],
            idempotency.Stored(
                status=answer.response.status,
                body=answer.response.body,
                content_type=content_type,
                answered_by=answer.answered_by,
                flags=answer.flags,
                precondition=answer.precondition,
                rejection_code=answer.rejection_code,
            ),
        )
    except Exception:
        logger.exception("irimi: this write's answer could not be stored for a retry")
