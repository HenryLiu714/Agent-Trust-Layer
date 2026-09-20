"""Overlay: applies the run's faked writes to a live read. Pure function so replay can reuse it."""

from collections.abc import Sequence
from typing import Protocol

from irimi.exchange import Exchange, Request, Response

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
# Phase 1 ships neither the overlay nor L3, so hazard 1 is a note rather than a test: the Phase 2
# issue should inherit it instead of rediscovering it.


class Overlay(Protocol):
    def __call__(
        self, write_log: Sequence[Exchange], read_request: Request, upstream_response: Response
    ) -> Response: ...


class NoOverlay:
    def __call__(
        self, write_log: Sequence[Exchange], read_request: Request, upstream_response: Response
    ) -> Response:
        return upstream_response
