"""Overlay: applies the run's faked writes to a live read. Pure function so replay can reuse it."""

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Protocol

from irimi import bodies, pipeline, services, writelog
from irimi.exchange import Exchange, OverlayFidelity, Request, Response
from irimi.servicemap import MapIndex

logger = logging.getLogger(__name__)

# The engine never calls the overlay for two kinds of read, so an effects table need not handle
# them:
#   - Reads of a service with an answer target (`target_reads: true`). The target owns that
#     service's state, and adding irimi's faked objects on top would show the agent its own write
#     twice. Neither a delegated read nor a failed target's `502` (flagged `target-failed`) enters
#     the write log, and irimi runs no precondition check on a write it delegates.
#   - Streamed responses. mitmproxy does not assemble a streamed body (the overlay would see
#     `b""`), and its headers are already sent before the overlay could run.
# Both are enforced in `IrimiAddon.response` and covered by tests/test_engine_mitm.py.


@dataclass(frozen=True)
class Overlaid:
    """What the overlay did to one live read.

    `response` is the very object the overlay was handed when nothing changed, which is how the
    engine tells an untouched read - live, unstamped, byte-identical to what the service sent -
    from one it must stamp `overlay`. `fidelity` is independent of that: a page the effects
    cannot model is `partial` with the body left alone, and the exchange has to say so.

    The response carries a status as well as a body, and an effect may replace it - for
    one case only, a read of an object this run minted, which the service answers with an error
    because it has never heard of the id. `services.Applied.status` is how an effect says so, and
    `IrimiAddon.response` stamps `overlay` off the same identity check either way, so a status of
    irimi's is never handed back as the service's.
    """

    response: Response
    fidelity: OverlayFidelity | None = None


class Overlay(Protocol):
    def __call__(
        self, write_log: Sequence[Exchange], read_request: Request, upstream_response: Response
    ) -> Overlaid: ...

    def rewrite(self, write_log: Sequence[Exchange], read_request: Request) -> Request: ...


class NoOverlay:
    def __call__(
        self, write_log: Sequence[Exchange], read_request: Request, upstream_response: Response
    ) -> Overlaid:
        return Overlaid(upstream_response)

    def rewrite(self, write_log: Sequence[Exchange], read_request: Request) -> Request:
        return read_request


class ServiceOverlay:
    """The real overlay: `irimi.services`' effect tables, applied to a live read.

    Constructed with the loaded maps because the seam hands it a `Request` and not a
    `Classification`, while the effect tables key on the read's service and operation.
    `pipeline.classify` is the one place that answers that question, so it is asked again here
    rather than a second matcher being written; it is a pure dict lookup and its answer is
    identical to the engine's.

    Both entry points swallow everything. They are called from inside mitmproxy hooks, where a
    raise forwards the flow untouched: on the response side that would hand the agent a read the
    overlay half-edited, and on the request side it would turn a read into a 502.
    """

    def __init__(self, maps: MapIndex) -> None:
        self.maps = maps

    def __call__(
        self, write_log: Sequence[Exchange], read_request: Request, upstream_response: Response
    ) -> Overlaid:
        try:
            return self._apply(write_log, read_request, upstream_response)
        except Exception:
            logger.exception("irimi: the overlay failed; the agent gets the upstream read")
            return Overlaid(upstream_response, "partial")

    def rewrite(self, write_log: Sequence[Exchange], read_request: Request) -> Request:
        # Any header of this name on the agent's own request came from somewhere else; ours is
        # the one that is true here. Same rule as `pipeline.respond`'s. It is stripped before the
        # guard, so no path - a rewrite, an early return, a raise - hands `_apply` and
        # `stripe._refunds_list` a header the agent sent to suppress the minted refund.
        # The engine strips it too, off every flow and before the decision, because this one only
        # runs on the rewrite path and that path is closed until the log holds a write. Both stay:
        # this is the guard for a caller that is not the engine, such as `tests/test_overlay.py`.
        # A replay is not one: the engine translates its reads.
        # The same object when there was nothing to strip, so the engine's `is` check means
        # "nothing to do".
        stripped = read_request.without_header(pipeline.REWROTE_HEADER)
        try:
            return self._rewrite(write_log, stripped)
        except Exception:
            logger.exception("irimi: the overlay's rewrite failed; forwarding the read unchanged")
            return stripped

    # --------------------------------------------------------------------------------- inside

    def _apply(
        self, write_log: Sequence[Exchange], read_request: Request, upstream_response: Response
    ) -> Overlaid:
        service, operation = self._route(read_request)
        found = writelog.scoped_writes(services.EFFECTS, service, read_request, write_log)
        if found is None:
            return Overlaid(upstream_response)
        effects, writes = found
        document = bodies.json_object(upstream_response.body)
        if document is None:
            # A body the overlay cannot read is a body it cannot apply the run's writes to. The
            # agent still gets exactly what the service sent; the exchange says it is incomplete.
            return Overlaid(upstream_response, "partial")
        # Reflected here, past the early returns, so a service with no effects table or a run
        # with no writes for it parses nothing. `bodies.reflect` never raises and reflects `{}` for
        # a body it cannot read, so it needs no guard of its own.
        applied = effects(writelog.read_of(operation, read_request), document, writes)
        rewrote = read_request.header(pipeline.REWROTE_HEADER) is not None
        fidelity: OverlayFidelity | None = None
        if applied.partial:
            fidelity = "partial"
        elif applied.changed or rewrote or applied.status is not None:
            # A translated cursor is an effect too, even when the page itself needed no edit. So is
            # a replaced status: the effects that set one also set `changed`, but an
            # exchange stamped `overlay` with `overlay: None` would print a `saw it  overlay` line
            # off a fidelity the trace says was never considered, so it is held here by
            # construction rather than by that convention.
            fidelity = "full"
        # An effect may replace the status only for a read of an object this run minted, which the
        # service refused because it never heard of the id (`Applied.status`). Both halves of
        # the test, so a status-only answer is rebuilt rather than dropped: identity is how the
        # engine tells an untouched read from one to stamp.
        status = upstream_response.status if applied.status is None else applied.status
        if not applied.changed and status == upstream_response.status:
            return Overlaid(upstream_response, fidelity)
        return Overlaid(
            replace(
                upstream_response,
                status=status,
                body=json.dumps(applied.document, allow_nan=False).encode(),
            ),
            fidelity,
        )

    def _rewrite(self, write_log: Sequence[Exchange], read_request: Request) -> Request:
        """`read_request` arrives with no `irimi-rewrote` of its own: `rewrite` stripped it."""
        service, operation = self._route(read_request)
        found = writelog.scoped_writes(services.REWRITES, service, read_request, write_log)
        if found is None:
            return read_request
        rewrite, writes = found
        # Past the early returns, like `_apply`'s; `bodies.reflect` never raises.
        rewritten = rewrite(writelog.read_of(operation, read_request), writes)
        if rewritten is None:
            return read_request
        return replace(
            read_request,
            query=rewritten.query,
            headers=read_request.headers + ((pipeline.REWROTE_HEADER, rewritten.removed),),
        )

    def _route(self, request: Request) -> tuple[str, str]:
        classification = pipeline.classify(request, self.maps)
        return classification.service, classification.operation
