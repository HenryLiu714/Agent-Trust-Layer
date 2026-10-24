"""The engine-agnostic core every engine drives: what irimi does with one request, from the
moment it is admitted to the exchange it is recorded as.

    plan(admitted)            on a worker thread: classify, refuse a cut write, resolve the
                              answer target once, translate a read, ask the policy, probe it
    complete(plan, received)  when the answer arrived: observe a read, apply the overlay, add a
                              faked write to the run's write log, build the exchange
    fail(plan, received)      when the connection failed before an answer arrived
    refuse(refusal)           a request irimi answers with its own error before any decision
    settle(plan)              the exchange a plan irimi answered in full is recorded as
    record(exchange)          the store, then the engine's callback

A `Plan` is complete before the engine touches its flow, and nothing here touches one: an engine
turns flows into `Request`s and `Response`s, carries a plan onto its flow, and calls back here.
`plan` never raises; a decision that raised, a write whose body irimi could not read whole and a
target irimi will not dial each become a plan irimi answers with its own 502.
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

# How long the probe below waits for a target to accept a connection. It runs on the decision's
# worker thread, so a slow target stalls only the request that names it.
TARGET_PROBE_TIMEOUT_S = 2.0


class RequestBodyCut(Exception):
    """A write whose body decoded past MAX_DECODED_BODY, which `plan` refuses to answer."""


@dataclass(frozen=True)
class Plan:
    """Everything irimi decided about one request, before the engine acts on it.

    `request` is the request as the trace shows it: rewritten, if the overlay translated a
    cursor, whoever answers it. `flags` are the decision's own and the cut request's. `issued` are
    the reads the decision made on its own account, which the engine records after the answer.
    `target` is the answer target's address, for a delegated request and one whose target failed.

    `settled` is a plan irimi answered in full with its own error: the engine sends its response
    and records it at once (`settle`), and no later hook has anything to add."""

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
    """The answer a planned request got, as the trace records it.

    `streamed` says its headers and chunks went to the agent as they arrived, so nothing may be
    written back. `cut` is the flag saying the recorded body is not all of it, or None."""

    response: Response | None
    streamed: bool = False
    stream_chunks: tuple[int, ...] = ()
    cut: Flag | None = None


@dataclass(frozen=True)
class Completed:
    """A completed request: its exchange, and the response the engine sends instead of the one it
    received, or None to leave the received one exactly as it is."""

    exchange: Exchange
    send: Response | None = None


def _cut_flags(cut: bool) -> tuple[Flag, ...]:
    """`body-truncated` for a request whose body decoded only up to the cap, so the exchange it
    becomes says its recorded request is not all of it."""
    return (Flag.BODY_TRUNCATED,) if cut else ()


class ShadowSession:
    """One engine's requests: the policy that decides them, the overlay, the per-run write log
    and the store they are recorded in. `run_id` is the run a request that names none belongs
    to."""

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
        # Given by the composition root, which ends its runs as the store reports them ended.
        self.write_log = write_log if write_log is not None else WriteLog()

    # ------------------------------------------------------------------------------ deciding

    def plan(self, admitted: Admitted) -> Plan:
        """The whole decision, with no flow in sight. Never raises: mitmproxy forwards a flow
        untouched when a hook raises, so a raise here would send the agent's request - a write
        included - to the real service. Any exception is a plan irimi answers with its own 502."""
        request = admitted.request
        try:
            cls = classify(request, self.maps)
            # irimi never answers a write from part of its body. A body that decodes past
            # MAX_DECODED_BODY reaches the decision empty, as it is recorded, and a fake built from
            # that would tell the agent a write it never sent succeeded. So the write fails
            # closed, a delegated one too, because irimi cannot record what it was. Classifying
            # reads no body, so the kind is still the agent's; a live kind is forwarded with the
            # agent's own bytes, which no hook edits.
            if admitted.cut and cls.kind not in LIVE_KINDS:
                cut = RequestBodyCut(
                    "request body decompresses to more than "
                    f"{MAX_DECODED_BODY // (1024 * 1024)} MiB; irimi does not fake a write it "
                    "cannot read whole"
                )
                return self.decision_failed(admitted, cut, classification=cls)
            target = targets.delegate(request, cls)
            writes = self.write_log.for_run(admitted.run_id)
            # A read is recorded as irimi would ask it, whoever answers it. It is translated
            # before the policy decides, so the policy, the trace and a later replay of that trace
            # all see the one request; only a live forward carries the translation onto the wire.
            # A delegated read is never translated: its target owns that service's state.
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
            # Stamped here, so the response the engine sends already says who answered it.
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
        """Let the overlay translate a read before the policy decides about it. None when nothing
        changed.

        Guarded on its own inside `plan`'s guard, because the two failures want opposite answers:
        a decision that raises is answered with a 502, since the thing it could not decide about
        may be a write, while a translation that fails forwards the read unchanged - nothing is
        performed by letting a read through, and 502-ing it would break a run over a cosmetic
        translation.
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
        """irimi's 502 for a request it will not answer as decided, flagged `decision-failed`. A
        body cut at the cap keeps the classification it was refused under; a decision that raised
        is recorded unclassified."""
        if isinstance(cause, RequestBodyCut):  # the agent's body, not irimi's failure
            logger.warning("irimi: refused write: %s", cause)
        else:
            logger.error("irimi: answering locally; the decision raised", exc_info=cause)
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
        """irimi's 502 for a request whose answer target it refused or could not reach. Never a
        silent fall back to the fake, which would hide a broken setup and look like a working
        shadow run: `target-failed` says why."""
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
        """irimi's answer to a request it will not decide about, flagged `refused` and recorded
        under the run it names. A parsed request keeps its usual classification, so a refused GET
        is not counted as a write; one irimi could not parse is recorded unclassified. Nothing of
        it is forwarded, so only the recorded request loses irimi's own headers."""
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
        """A settled plan answered with irimi's own JSON error, stamped `irimi`: the one path for
        every error irimi answers with itself."""
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
        """The exchange a settled plan is recorded as, ending now. The response is irimi's error
        without its stamp: the stamp is on the wire only. An answer to HEAD is recorded with no
        content, as it is sent."""
        assert isinstance(plan.decision, Respond)
        response = plan.decision.response.without_header(ANSWERED_BY_HEADER)
        if plan.request.method == "HEAD":
            response = replace(response, body=b"")
        return self._exchange(plan, response, plan.answered_by, plan.flags)

    def complete(self, plan: Plan, received: Received) -> Completed:
        """The exchange a planned request became once its answer arrived, and what to send
        instead of that answer.

        A streamed response is already on its way out: its headers went before this was called
        and its body is never assembled. So the overlay is not asked, since whatever it returned
        could not reach the agent, and nothing is sent back. A body that decodes past the cap is
        recorded empty, never as the document, so no read observer reads it and nothing the
        overlay returns is sent: the agent gets the service's bytes, and only the recording is
        short.
        """
        upstream = received.response
        assert upstream is not None
        flags = plan.flags
        if received.cut is not None and received.cut not in flags:
            # Once, though the request may have been cut too: a flag is a fact, not a count.
            flags += (received.cut,)
        cut = received.cut is not None
        cls = plan.classification
        decision = plan.decision
        # A read irimi forwarded is the only place the real values a later fake has to sort
        # against appear, so every read's body goes past the policy's `observe_read`. A
        # delegated read is observed too: its target's values are then the ones the agent sees.
        if not received.streamed and not cut and cls.kind == "read":
            self._observe_read(cls.service, upstream.body)
        answered_by = plan.answered_by
        response = upstream
        overlay: OverlayCoverage | None = None
        send: Response | None = None
        if isinstance(decision, Respond):
            # The answer irimi planned, as the engine read it back: stamped when it was planned,
            # recorded without the stamp. A read a replay answered from the recording carries
            # the recorded read's fidelity, the overlay's work in the run it replays.
            response = upstream.without_header(ANSWERED_BY_HEADER)
            overlay = decision.overlay
        elif isinstance(decision, Forward):
            # The overlay stays off a delegated read: the target owns that service's state. And
            # it is asked only once there is a write to show, as the translation is.
            if not received.streamed and cls.kind == "read" and self.write_log.for_run(plan.run_id):
                overlaid = self._overlaid(plan, decision, upstream)
                if cut:
                    # A read cut at the cap is never overlaid, and says what it could not show:
                    # `partial` when the run has a write it would have shown in this read.
                    overlay = None if overlaid.fidelity is None else "partial"
                else:
                    response, overlay = overlaid.response, overlaid.fidelity
                if response is not upstream:
                    # The bytes are no longer the service's, so the answer has to say whose they
                    # are: a changed body with no header says "the real service sent this".
                    answered_by = "overlay"
        exchange = self._exchange(
            plan, response, answered_by, flags, overlay, received.stream_chunks
        )
        # The write log is what the overlay applies to live reads. `is_authored_write` is the one
        # statement of what may enter it, because the summary files a read under a write by the
        # same rule.
        if is_authored_write(exchange):
            self.write_log.add(exchange)
        if not received.streamed and not isinstance(decision, Respond):
            stamped = wire.stamp(response, answered_by)
            if stamped is not upstream:
                send = stamped
        return Completed(exchange, send)

    def fail(self, plan: Plan, received: Received | None) -> Exchange:
        """The exchange a planned request became when its connection failed before an answer
        arrived. `received` is what of a streamed answer reached the agent, or None.

        What failed depends on who was going to answer. A live forward lost the real service; a
        delegated one could not reach its target, which must be said out loud rather than fall
        back to the fake. A request irimi answered itself never opened a connection at all, so
        the failure is the client going away, and the answer keeps only the flags it carried.
        The answer's own flags are kept on every path: a replay's miss forwarded live says
        `replay-miss` whether its upstream then answered or failed.
        """
        flags = plan.flags
        if plan.answered_by == "live":
            flags += (Flag.UPSTREAM_ERROR,)
        elif plan.answered_by == "delegated":
            flags += (Flag.TARGET_FAILED,)
        response: Response | None = None
        stream_chunks: tuple[int, ...] = ()
        if received is not None:
            # A stream that ended in an error had its headers and some chunks reach the agent
            # already: the recording keeps what was copied and says it is not the whole stream.
            response, stream_chunks = received.response, received.stream_chunks
            flags += (Flag.STREAM_TRUNCATED,)
        overlay = plan.decision.overlay if isinstance(plan.decision, Respond) else None
        return self._exchange(plan, response, plan.answered_by, flags, overlay, stream_chunks)

    def record(self, exchange: Exchange) -> None:
        """Store the exchange, then hand it to the engine's callback.

        A recording failure never affects traffic. `RunStore.record` may not raise, and this
        guards the call anyway: a store swapped in later does not inherit `DirectoryStore`'s
        care. It logs the exception's type only, as the store logs its own failures: a broken
        store fails on every exchange, and a message may quote the value it choked on.
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
        """The exchange a plan became, ending now, carrying what the decision said of a write."""
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
        """Hand a forwarded read to the policy, whose fakes sort after what it saw. Never
        raises: an observer swapped in later does not inherit the policy's care."""
        try:
            self.policy.observe_read(service, body)
        except Exception:
            logger.exception("irimi: the policy could not observe a read")

    def _overlaid(self, plan: Plan, decision: Forward, upstream: Response) -> Overlaid:
        """The overlay's answer for one live read, over its own run's writes, or the upstream one
        if it failed. Guarded here as `ServiceOverlay` guards itself: an overlay swapped in later
        - a test double, a replay overlay - does not inherit its care."""
        try:
            writes = self.write_log.for_run(plan.run_id)
            removed = decision.translation.removed if decision.translation is not None else None
            return self.overlay(writes, plan.classification, plan.request, upstream, removed)
        except Exception:
            logger.exception("irimi: the overlay raised; the agent gets the upstream read")
            return Overlaid(upstream, "partial")


def _probe_target(target: Target) -> None:
    """Raise TargetUnreachable when nothing accepts a connection at an answer target.

    The engine lets its proxy dial the target, which keeps a streamed target response streaming.
    When that dial fails, mitmproxy answers with its own HTML page, which names neither irimi nor
    the target. So the dial is tried here first, and a target that cannot be reached gets irimi's
    JSON 502 instead. Best-effort: a target that dies between the probe and the dial still gets
    mitmproxy's page.
    """
    try:
        socket.create_connection((target.host, target.port), TARGET_PROBE_TIMEOUT_S).close()
    except OSError as exc:
        raise targets.TargetUnreachable(
            f"answer target {target.url!r} could not be reached: {exc}"
        ) from None


def _target_refusal(
    forward: targets.ForwardTo, listener: tuple[str, int]
) -> targets.TargetRefused | None:
    """Why irimi will not send this request to its answer target, or None when it may: the target
    is irimi's own listener, which would make the engine dial itself in a loop, or it cannot be
    reached. On the decision's worker thread, so the probe never blocks the event loop."""
    try:
        targets.refuse_self_target(forward.target, listener[1], listener[0])
        _probe_target(forward.target)
    except targets.TargetRefused as exc:
        return exc
    except Exception as exc:
        return targets.TargetRefused(f"answer target {forward.url!r} could not be applied: {exc}")
    return None
