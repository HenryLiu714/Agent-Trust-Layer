"""Overlay: applies the run's faked writes to a live read. Pure function so replay can reuse it."""

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Any, Protocol

from irimi import echo, pipeline, services
from irimi.exchange import Exchange, OverlayFidelity, Request, Response
from irimi.servicemap import MapIndex

logger = logging.getLogger(__name__)

# A live body this size is not a Stripe object the effects model, and parsing it on the answer
# path would cost more than the read it is trying to improve. It is flagged, not silently passed.
MAX_BODY_BYTES = 2_000_000

# ------------------------------------------------ TWO THINGS THE FIRST REAL OVERLAY MUST KNOW
#
# 1. A DELEGATED SERVICE GETS NO OVERLAY (design D20, §4.4; issue #20).
#    `target_reads: true` makes the target, not production, the world the agent sees. The target
#    owns read-after-write consistency for that service, and layering irimi's minted objects over
#    its state would corrupt it - the agent would see its own faked refund twice, or once over a
#    charge the target already refunded. The Phase 2 overlay is per-service and skips every
#    service `servicemap.is_delegated` is true for. For the same reason, L3 preconditions for a
#    delegated service must read from the target rather than from the real upstream, or they
#    check the write against a world the agent is not in.
#
#    Half of this is already enforced upstream of here and should stay that way:
#    `IrimiAddon.response` does not call the overlay for a delegated read, and `write_log` does
#    not collect one either - a delegated `GET`, and a failed target's `502`, are not writes, and
#    replaying them onto other services' live reads is the bug that condition was narrowed to
#    prevent. The `502` half is excluded by its `target-failed` flag, not by never reaching the
#    guard: a target that could not be *dialled* is handled in `error()`, but one irimi *refused*
#    is answered in `request()` and does reach `response()`.
#
# 2. A STREAMED READ NEVER REACHES YOU AT ALL (issue #28).
#    `IrimiAddon.responseheaders` streams any live `text/event-stream` response, `kind: read`
#    included, and mitmproxy never assembles the body of a streamed response. An overlay handed
#    such a response would see `body == b""` - not what the service sent - and returning anything
#    for it would turn a streamed read into a buffered, empty-bodied one.
#
#    This half is enforced by the engine rather than left to you to remember: `IrimiAddon.response`
#    skips the overlay for a streamed flow, and writes nothing back to one either, because those
#    headers went out before the hook ran. Do not build an overlay that depends on being asked
#    about streams; build one that is correct for the bodies it is given.
#
# Both hazards are pinned by tests now that `ServiceOverlay` exists (#43): `test_engine_mitm` holds
# that a delegated read and a delegated write both stay out of `write_log`, and that a streamed read
# is neither overlaid nor rewritten. They stay written out here because the next service's
# effects table is the reader who needs them, and a test says what breaks, not why it matters.


@dataclass(frozen=True)
class Overlaid:
    """What the overlay did to one live read.

    `response` is the very object the overlay was handed when nothing changed, which is how the
    engine tells an untouched read - live, unstamped, byte-identical to what the service sent -
    from one it must stamp `overlay`. `fidelity` is independent of that: a page the effects
    cannot model is `partial` with the body left alone, and the exchange has to say so.
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
    """The real overlay: `irimi.services`' effect tables, applied to a live read (#43).

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
        # `stripe._refunds_list` a header the agent sent to suppress the minted refund (#43).
        kept = tuple((k, v) for k, v in read_request.headers if k != pipeline.REWROTE_HEADER)
        # The same object when there was nothing to strip, so the engine's `is` check still
        # means "nothing to do".
        stripped = (
            read_request if kept == read_request.headers else replace(read_request, headers=kept)
        )
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
        effects = services.EFFECTS.get(service)
        if effects is None:
            return Overlaid(upstream_response)
        writes = self._writes(service, read_request, write_log)
        if not writes:
            return Overlaid(upstream_response)
        document = _json_object(upstream_response.body)
        if document is None:
            # A body the overlay cannot read is a body it cannot apply the run's writes to. The
            # agent still gets exactly what the service sent; the exchange says it is incomplete.
            return Overlaid(upstream_response, "partial")
        applied = effects(operation, read_request, document, writes)
        rewrote = any(k == pipeline.REWROTE_HEADER for k, _ in read_request.headers)
        fidelity: OverlayFidelity | None = None
        if applied.partial:
            fidelity = "partial"
        elif applied.changed or rewrote:
            # A translated cursor is an effect too, even when the page itself needed no edit.
            fidelity = "full"
        if not applied.changed:
            return Overlaid(upstream_response, fidelity)
        return Overlaid(
            replace(upstream_response, body=json.dumps(applied.document, allow_nan=False).encode()),
            fidelity,
        )

    def _rewrite(self, write_log: Sequence[Exchange], read_request: Request) -> Request:
        """`read_request` arrives with no `irimi-rewrote` of its own: `rewrite` stripped it."""
        service, operation = self._route(read_request)
        rewrite = services.REWRITES.get(service)
        if rewrite is None:
            return read_request
        writes = self._writes(service, read_request, write_log)
        if not writes:
            return read_request
        rewritten = rewrite(operation, read_request, writes)
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

    def _writes(
        self, service: str, read_request: Request, write_log: Sequence[Exchange]
    ) -> list[services.Write]:
        """This service's faked writes, in the scope the read is asking about.

        A write made against another connected account or another API version says nothing about
        this read, so it is left out. The scope is taken from each write's OWN request headers,
        because that is the world it was made in.
        """
        scope = _scope(service, read_request)
        out: list[services.Write] = []
        for exchange in write_log:
            if exchange.service != service or exchange.response is None:
                continue
            if _scope(service, exchange.request) != scope:
                continue
            answer = _json_object(exchange.response.body)
            if answer is None:
                continue
            out.append(
                services.Write(
                    operation=exchange.operation,
                    posted=echo.reflect(exchange.request),
                    answer=answer,
                )
            )
        return out


def _scope(service: str, request: Request) -> tuple[str, ...]:
    names = services.SCOPE_HEADERS.get(service, ())
    return tuple(next((v for k, v in request.headers if k == name), "") for name in names)


def _json_object(body: bytes) -> dict[str, Any] | None:
    """`body` as a JSON object, or None when it is not one this overlay should touch."""
    if not body or len(body) > MAX_BODY_BYTES:
        return None
    try:
        parsed = json.loads(body)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None
