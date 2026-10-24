"""The engine-agnostic core: what irimi does with one request, from admission to recording.

    plan(admitted)            worker thread: classify, translate a read, ask the policy
    complete(plan, received)  answer arrived: observe a read, overlay, log a faked write
    fail(plan, received)      the connection failed before an answer arrived
    refuse(refusal)           irimi's own error for a request it will not answer
    settle(plan)              the exchange a plan irimi answered in full is recorded as
    record(exchange)          the store, then the engine's callback

Nothing here touches a flow; the engine carries a finished `Plan` onto it.
"""

import logging
import socket
import time
from collections.abc import Callable
from dataclasses import dataclass, replace

from irimi.core.exchange import (
    LIVE_KINDS,
    AnsweredBy,
    Door,
    Exchange,
    Flag,
    OverlayCoverage,
    Request,
    Response,
    WriteOutcome,
    is_authored_write,
)
from irimi.core.limits import MAX_DECODED_BODY
from irimi.core.netaddr import Target
from irimi.decide import targets
from irimi.decide.overlay import Overlaid, Overlay
from irimi.decide.policy import AnswerPolicy, Decided, Decision, Delegate, Forward, Respond
from irimi.decide.writelog import Translation, WriteLog
from irimi.engine import ingress, wire
from irimi.engine.ingress import Admitted, Refusal
from irimi.protocol.headers import ANSWERED_BY_HEADER
from irimi.record.store import RunStore
from irimi.servicemaps import MapIndex
from irimi.servicemaps.classify import Classification, classify, unclassified

logger = logging.getLogger(__name__)

# The probe runs on the decision's worker thread, so a slow target stalls only its own request.
TARGET_PROBE_TIMEOUT_S = 2.0


class RequestBodyCut(Exception):
    """A write whose body decoded past MAX_DECODED_BODY, which `plan` refuses."""


@dataclass(frozen=True)
class Plan:
    """How irimi will answer one request, before the engine acts on it.

    `request` is as the trace shows it, including any overlay translation. `issued` are irimi's
    own reads, recorded after the answer. `target` is the target URL, if any. `settled` means
    irimi answered in full with its own error, so no later hook has anything to add.
    """

    request: Request
    classification: Classification
    run_id: str
    door: Door
    started_at: float
    decision: Decision
    flags: tuple[Flag, ...] = ()
    issued: tuple[Exchange, ...] = ()
    target: str = ""
    settled: bool = False

    @property
    def answered_by(self) -> AnsweredBy:
        return self.decision.answered_by


@dataclass(frozen=True)
class Received:
    """The answer a planned request got; `streamed` means it already went to the agent."""

    response: Response | None
    streamed: bool = False
    stream_chunks: tuple[int, ...] = ()
    cut: Flag | None = None


@dataclass(frozen=True)
class Completed:
    """A completed exchange, and the response to send instead, or None to send it unchanged."""

    exchange: Exchange
    send: Response | None = None


def _cut_flags(cut: bool) -> tuple[Flag, ...]:
    """Return `body-truncated` for a request whose body was cut at the cap."""
    return (Flag.BODY_TRUNCATED,) if cut else ()


class ShadowSession:
    """One engine's policy, overlay, per-run write log and store; `run_id` is the default run."""

    def __init__(
        self,
        policy: AnswerPolicy,
        store: RunStore,
        overlay: Overlay,
        maps: MapIndex,
        run_id: str,
        on_exchange: Callable[[Exchange], None] | None = None,
        write_log: WriteLog | None = None,
    ) -> None:
        self.policy = policy
        self.store = store
        self.overlay = overlay
        self.maps = maps
        self.run_id = run_id
        self.on_exchange = on_exchange
        # The composition root ends its runs as the store reports them ended.
        self.write_log = write_log if write_log is not None else WriteLog()

    # ------------------------------------------------------------------------------ deciding

    def plan(self, admitted: Admitted) -> Plan:
        """Return how to answer `admitted`, without touching a flow.

        Never raises (fail-closed hook rule): any exception becomes irimi's 502 flagged
        `decision-failed`.
        """
        request = admitted.request
        try:
            cls = classify(request, self.maps)
            # A body cut at the cap reaches the decision empty, and a faked write built from it
            # would confirm a write the agent never sent. Live kinds forward the agent's own bytes.
            if admitted.cut and cls.kind not in LIVE_KINDS:
                cut = RequestBodyCut(
                    "request body decompresses to more than "
                    f"{MAX_DECODED_BODY // (1024 * 1024)} MiB; irimi does not fake a write it "
                    "cannot read whole"
                )
                return self.decision_failed(admitted, cut, classification=cls)
            target = targets.resolve(request, cls)
            writes = self.write_log.for_run(admitted.run_id)
            # Translated before the policy sees it, so policy, trace and replay share one request.
            # A read sent to a target is never translated: the target owns that service's state.
            translation = None
            if cls.kind == "read" and writes and target is None:
                translation = self._translate(writes, cls, request)
            asked = request if translation is None else translation.request
            decided = self.policy.decide(asked, cls, writes, admitted.run_id, target)
            return self._planned(admitted, asked, cls, decided, translation)
        except Exception as exc:
            return self.decision_failed(admitted, exc)

    def _planned(
        self,
        admitted: Admitted,
        request: Request,
        cls: Classification,
        decided: Decided,
        translation: Translation | None,
    ) -> Plan:
        decision = decided.decision
        target = ""
        if isinstance(decision, Forward):
            decision = Forward(translation)
        elif isinstance(decision, Delegate):
            target = decision.target.url
        else:
            decision = replace(
                decision, response=wire.stamp(decision.response, decision.answered_by)
            )
        plan = Plan(
            request,
            cls,
            admitted.run_id,
            admitted.door,
            admitted.started_at,
            decision,
            decided.flags + _cut_flags(admitted.cut),
            decided.issued,
            target,
        )
        if isinstance(decision, Delegate):
            refused = _target_refusal(decision.target, admitted.listener)
            if refused is not None:
                return self.target_failed(plan, str(refused))
        return plan

    def _translate(
        self, writes: tuple[Exchange, ...], cls: Classification, request: Request
    ) -> Translation | None:
        """Return the overlay's translation of a read, or None when nothing changed.

        A failed translation forwards the read unchanged rather than 502-ing it: a read changes
        nothing on the service.
        """
        try:
            return self.overlay.rewrite(writes, cls, request)
        except Exception:
            logger.exception("irimi: the overlay's rewrite raised; forwarding the read unchanged")
            return None

    # -------------------------------------------------------------------------- irimi's errors

    def decision_failed(
        self,
        admitted: Admitted,
        cause: Exception,
        *,
        request: Request | None = None,
        classification: Classification | None = None,
        issued: tuple[Exchange, ...] = (),
    ) -> Plan:
        """Return irimi's 502 flagged `decision-failed`, unclassified unless told otherwise."""
        if isinstance(cause, RequestBodyCut):  # the agent's body, not irimi's failure
            logger.warning("irimi: refused write: %s", cause)
        else:
            logger.error("irimi: the decision raised; answering with irimi's 502", exc_info=cause)
        request = request if request is not None else admitted.request
        return self._irimi_error(
            request,
            classification if classification is not None else unclassified(request),
            admitted.run_id,
            admitted.door,
            admitted.started_at,
            status=502,
            error_type="irimi_decision_failed",
            message=f"irimi could not decide how to answer this request: {cause}",
            flags=(Flag.DECISION_FAILED, *_cut_flags(admitted.cut)),
            issued=issued,
        )

    def target_failed(self, plan: Plan, message: str) -> Plan:
        """Return irimi's 502 flagged `target-failed`; never fall back to the fake and hide it."""
        logger.warning("irimi: %s", message)
        return self._irimi_error(
            plan.request,
            plan.classification,
            plan.run_id,
            plan.door,
            plan.started_at,
            status=502,
            error_type="irimi_target_failed",
            message=message,
            flags=(*plan.flags, Flag.TARGET_FAILED),
            target=plan.target,
            issued=plan.issued,
        )

    def refuse(self, refusal: Refusal) -> Plan:
        """Return irimi's error flagged `refused`, recorded under the run the request names.

        A parsed request keeps its classification, so a refused GET is not counted as a write.
        """
        request, run_id = refusal.request, self.run_id
        try:
            run_id, request = ingress.attributed(request, run_id)
        except Exception:
            logger.debug("irimi: could not attribute a refused request", exc_info=True)
        classification = unclassified(request)
        if refusal.parsed:
            try:
                classification = classify(request, self.maps)
            except Exception:
                logger.debug("irimi: could not classify a refused request", exc_info=True)
        return self._irimi_error(
            request,
            classification,
            run_id,
            refusal.door,
            refusal.started_at,
            status=refusal.status,
            error_type="irimi_refused",
            message=f"irimi refused this request: {refusal.reason}",
            flags=(Flag.REFUSED, *_cut_flags(refusal.cut)),
        )

    def _irimi_error(
        self,
        request: Request,
        classification: Classification,
        run_id: str,
        door: Door,
        started_at: float,
        *,
        status: int,
        error_type: str,
        message: str,
        flags: tuple[Flag, ...],
        target: str = "",
        issued: tuple[Exchange, ...] = (),
    ) -> Plan:
        """Return a settled plan answered with irimi's JSON error, stamped `irimi`."""
        response = wire.irimi_error(status, error_type, message)
        return Plan(
            request,
            classification,
            run_id,
            door,
            started_at,
            Respond(wire.stamp(response, wire.IRIMI_ANSWER), wire.IRIMI_ANSWER),
            flags,
            issued,
            target,
            settled=True,
        )

    # ------------------------------------------------------------------------------ recording

    def settle(self, plan: Plan) -> Exchange:
        """Return a settled plan's exchange; the stamp is on the wire only, not recorded.

        An answer to HEAD is recorded with no content, as it is sent.
        """
        assert isinstance(plan.decision, Respond)
        response = plan.decision.response.without_header(ANSWERED_BY_HEADER)
        if plan.request.method == "HEAD":
            response = replace(response, body=b"")
        return self._exchange(plan, response, plan.answered_by, plan.flags)

    def complete(self, plan: Plan, received: Received) -> Completed:
        """Return the exchange a planned request became, and what to send instead of its answer.

        A streamed response is already sent, so it is not overlaid. A body cut at the cap is not
        observed or overlaid: the agent gets the service's bytes and only the recording is short.
        """
        upstream = received.response
        assert upstream is not None
        flags = plan.flags
        if received.cut is not None and received.cut not in flags:
            flags += (received.cut,)
        cut = received.cut is not None
        cls = plan.classification
        decision = plan.decision
        # Later faked writes sort after the real values seen here; a target's read counts too,
        # since its values are what the agent sees.
        if not received.streamed and not cut and cls.kind == "read":
            self._observe_read(cls.service, upstream.body)
        answered_by = plan.answered_by
        response = upstream
        overlay: OverlayCoverage | None = None
        send: Response | None = None
        if isinstance(decision, Respond):
            # Recorded without the stamp. A replayed read keeps the recorded read's fidelity.
            response = upstream.without_header(ANSWERED_BY_HEADER)
            overlay = decision.overlay
        elif isinstance(decision, Forward):
            # A target's read is not a Forward, so the overlay never touches it.
            if not received.streamed and cls.kind == "read" and self.write_log.for_run(plan.run_id):
                overlaid = self._overlaid(plan, decision, upstream)
                if cut:
                    # `partial` when the run has a write this read would have shown.
                    overlay = None if overlaid.fidelity is None else "partial"
                else:
                    response, overlay = overlaid.response, overlaid.fidelity
                if response is not upstream:
                    # The bytes are no longer the service's, so the answer must carry a header.
                    answered_by = "overlay"
        exchange = self._exchange(
            plan, response, answered_by, flags, overlay, received.stream_chunks
        )
        # The summary files reads under writes by the same `is_authored_write` rule.
        if is_authored_write(exchange):
            self.write_log.add(exchange)
        if not received.streamed and not isinstance(decision, Respond):
            stamped = wire.stamp(response, answered_by)
            if stamped is not upstream:
                send = stamped
        return Completed(exchange, send)

    def fail(self, plan: Plan, received: Received | None) -> Exchange:
        """Return the exchange for a connection that failed before an answer arrived.

        `received` is what of a streamed answer reached the agent, or None. A live forward is
        flagged `upstream-error`, a target `target-failed`; the plan's own flags are always kept.
        """
        flags = plan.flags
        if plan.answered_by == "live":
            flags += (Flag.UPSTREAM_ERROR,)
        elif plan.answered_by == "delegated":
            flags += (Flag.TARGET_FAILED,)
        response: Response | None = None
        stream_chunks: tuple[int, ...] = ()
        if received is not None:
            response, stream_chunks = received.response, received.stream_chunks
            flags += (Flag.STREAM_TRUNCATED,)
        overlay = plan.decision.overlay if isinstance(plan.decision, Respond) else None
        return self._exchange(plan, response, plan.answered_by, flags, overlay, stream_chunks)

    def record(self, exchange: Exchange) -> None:
        """Store the exchange, then hand it to the engine's callback.

        A store failure is logged by type only: its message may quote an unredacted value.
        """
        try:
            self.store.record(exchange)
        except Exception as exc:
            logger.warning(
                "irimi: the trace store raised %s; the exchange is not stored", type(exc).__name__
            )
            logger.debug("irimi: the trace store's traceback", exc_info=True)
        if self.on_exchange:
            self.on_exchange(exchange)

    def _exchange(
        self,
        plan: Plan,
        response: Response | None,
        answered_by: AnsweredBy,
        flags: tuple[Flag, ...],
        overlay: OverlayCoverage | None = None,
        stream_chunks: tuple[int, ...] = (),
    ) -> Exchange:
        """Return the exchange a plan became, ending now."""
        decision, cls = plan.decision, plan.classification
        outcome = WriteOutcome()
        if isinstance(decision, Respond):
            outcome = decision.outcome
        elif isinstance(decision, Delegate):
            outcome = WriteOutcome(decision.precondition)
        return Exchange(
            request=plan.request,
            response=response,
            service=cls.service,
            operation=cls.operation,
            kind=cls.kind,
            answered_by=answered_by,
            run_id=plan.run_id,
            door=plan.door,
            flags=cls.flags + flags,
            target=plan.target,
            overlay=overlay,
            precondition=outcome.precondition,
            would_fire=outcome.would_fire,
            started_at=plan.started_at,
            ended_at=time.time(),
            stream_chunks=stream_chunks,
        )

    # ---------------------------------------------------------------------------- the guards

    def _observe_read(self, service: str, body: bytes) -> None:
        """Hand a read to the policy, whose faked writes sort after it. Never raises."""
        try:
            self.policy.observe_read(service, body)
        except Exception:
            logger.exception("irimi: the policy could not observe a read")

    def _overlaid(self, plan: Plan, decision: Forward, upstream: Response) -> Overlaid:
        """Return the overlay's answer for a live read, or the upstream one if it raised."""
        try:
            writes = self.write_log.for_run(plan.run_id)
            removed = decision.translation.removed if decision.translation is not None else None
            return self.overlay(writes, plan.classification, plan.request, upstream, removed)
        except Exception:
            logger.exception("irimi: the overlay raised; the agent gets the upstream read")
            return Overlaid(upstream, "partial")


def _probe_target(target: Target) -> None:
    """Raise TargetUnreachable when nothing accepts a connection at a target.

    A failed dial by the proxy yields mitmproxy's HTML page instead of irimi's JSON 502, so dial
    first. Best-effort: a target that dies between probe and dial still gets mitmproxy's page.
    """
    try:
        socket.create_connection((target.host, target.port), TARGET_PROBE_TIMEOUT_S).close()
    except OSError as exc:
        raise targets.TargetUnreachable(
            f"target {target.url!r} could not be reached: {exc}"
        ) from None


def _target_refusal(
    forward: targets.ForwardTo, listener: tuple[str, int]
) -> targets.TargetRefused | None:
    """Return why the request may not go to its target (irimi itself, unreachable), or None.

    Runs on the worker thread, so the probe never blocks the event loop.
    """
    try:
        targets.refuse_self_target(forward.target, listener[1], listener[0])
        _probe_target(forward.target)
    except targets.TargetRefused as exc:
        return exc
    except Exception as exc:
        return targets.TargetRefused(f"target {forward.url!r} could not be applied: {exc}")
    return None
