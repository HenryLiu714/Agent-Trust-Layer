"""Targets: the user URL that answers a request instead of the service."""

import logging
from dataclasses import dataclass, replace

from irimi.core import netaddr
from irimi.core.exchange import Request
from irimi.core.netaddr import Target
from irimi.servicemaps import MapIndex, service_target_for, target_for
from irimi.servicemaps.classify import Classification

logger = logging.getLogger(__name__)


class TargetRefused(ValueError):
    """A target irimi will not dial. str(exc) is the one-line explanation."""


class TargetUnreachable(TargetRefused):
    """A target irimi could not reach; handled and flagged as a refusal is."""


@dataclass(frozen=True)
class ForwardTo:
    """The target for this request; `forward_auth` keeps its `Authorization` header."""

    target: Target
    forward_auth: bool = False

    @property
    def url(self) -> str:
        """Return the address the request is sent to, as `Exchange.target` records it."""
        return self.target.url


def target_url(target: Target, request: Request, matched: bool) -> Target:
    """Return where a request sent to a target goes, with nginx `proxy_pass` path semantics.

    `#` is escaped to `%23` so the recorded URL re-parses to the same path and query on replay.
    """
    base = target.path.rstrip("/")
    if not base:
        path = request.path
    elif matched:
        path = base
    else:
        path = base + request.path
    query = f"?{request.query}" if request.query else ""
    return replace(target, path=f"{path}{query}".replace("#", "%23"))


def refuse_self_target(target: Target, listen_port: int, local_address: str = "") -> None:
    """Raise TargetRefused when `target` is irimi's own listener, which would dial itself."""
    if target.port == listen_port and (
        netaddr.is_self_host(target.host) or netaddr.resolves_to_self(target.host, local_address)
    ):
        raise TargetRefused(
            f"target {target.url!r} is irimi's own listener on port {listen_port}; "
            "point it at the address that answers the route instead"
        )


def refuse_self_targets(index: MapIndex, listen_port: int, bind: str = "") -> None:
    """Raise TargetRefused at startup when any target in `index` is this listener (port 0 skips)."""
    if not listen_port:
        return
    for sm in index.services:
        for target in (sm.target, *(r.target for r in sm.routes)):
            if target is not None:
                refuse_self_target(target, listen_port, bind)


def resolve(request: Request, classification: Classification) -> ForwardTo | None:
    """Return the target for this request, or None when irimi answers it itself."""
    service_map = classification.service_map
    if service_map is None:
        return None
    route = classification.route
    if route is not None:
        target, forward_auth = target_for(service_map, route), route.forward_auth
    else:
        # An unmatched request still goes to the service's target, so a stub is not half irimi.
        target, forward_auth = service_target_for(service_map, classification.kind), False
    if target is None:
        return None
    sent_to = target_url(target, request, matched=route is not None)
    # Scope rule: every path on a credential-path host is a credential, so its target is loopback.
    if request.host in service_map.credential_hosts and not sent_to.is_loopback:
        logger.error(
            "irimi: refusing to send %s to target %s: the request path is the credential on %s, "
            "so its target must be loopback",
            request.path,
            sent_to.url,
            request.host,
        )
        return None
    return ForwardTo(sent_to, forward_auth=forward_auth)
