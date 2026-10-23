"""Answer targets: the address that answers a request instead of irimi.

`delegate` is the decision - which target, if any, this request goes to. The rest is what the
engine needs to act on that decision safely: where to send the request (`target_url`) and what
it may never be sent to (`refuse_self_target`, and `refuse_self_targets` for every target in the
maps before a listener starts). Which headers it may not carry is
`redact.is_credential_header`, the rule disk is held to too.
"""

import logging
from dataclasses import dataclass, replace

from irimi import netaddr
from irimi.classify import Classification
from irimi.exchange import Request
from irimi.netaddr import Target
from irimi.servicemap import TARGETABLE_KINDS, MapIndex, target_for

logger = logging.getLogger(__name__)


class TargetRefused(ValueError):
    """An answer target irimi will not dial. str(exc) is the one-line explanation."""


class TargetUnreachable(TargetRefused):
    """An answer target irimi tried to reach and could not. A subclass, so every handler that
    already answers a refusal with the `irimi_target_failed` body answers this one the same way -
    the agent cannot act differently on the two, and the flag it earns is the same."""


@dataclass(frozen=True)
class ForwardTo:
    """An answer target: the address that answers this request instead of irimi.

    `target.path` already carries the path and query the target should see. `forward_auth` is the
    route opting in to keeping its `Authorization` header; the engine strips it otherwise.
    """

    target: Target
    forward_auth: bool = False

    @property
    def url(self) -> str:
        """The address the request is sent to, as `Exchange.target` records it."""
        return self.target.url


def target_url(target: Target, request: Request, matched: bool) -> Target:
    """Where a delegated request is sent, with nginx `proxy_pass` path semantics.

    A **bare origin** (`http://127.0.0.1:3000`) keeps the request's own path. A target that
    **carries a path** (`http://127.0.0.1:3000/refund`) replaces the part of the path the route
    matched. A route pattern always matches the request's whole path - `model.match_path` in
    `servicemap` requires the same number of segments - so for a mapped route there is no
    remainder and the target's path is the whole new path. A request that matched no route (a
    service-level target covering a route its map does not list) has nothing matched, so its own
    path is appended instead.

    The query string is always kept; method, body and headers are not this function's business.

    `Exchange.target` records the result as a URL, which a replay reads back, so anything in the
    request that would re-parse differently has to be escaped on the way in. A literal `#` is the
    one that bites: `POST /unlisted#x?a=1` would come back out as path `/unlisted` with no query
    at all, and `Exchange.target` would record a URL that was never used. `#` is legal in a request
    target and means nothing there; `%23` is the same path to the target and survives the round
    trip.
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
    """Raise TargetRefused when `target` is this listener's own address.

    Targets are loopback-only, so the host alone cannot tell a stub from ourselves - the port is
    what separates them. Forwarding to our own listener makes the proxy dial itself until it runs
    out of ports, the same loop `detect_door` prevents on the reverse door.

    `local_address` is the address the request's connection reached irimi at. Bound wider than
    loopback, irimi is that address too, so a target `--allow-remote-target` lets name it, or a
    name resolving to it, is our own listener by the rule `detect_door` applies to a request:
    refused here, rather than dialled and refused again by the reverse door.
    """
    if target.port == listen_port and (
        netaddr.is_self_host(target.host) or netaddr.resolves_to_self(target.host, local_address)
    ):
        raise TargetRefused(
            f"answer target {target.url!r} is irimi's own listener on port {listen_port}; "
            "point it at the address that answers the route instead"
        )


def refuse_self_targets(index: MapIndex, listen_port: int, bind: str = "") -> None:
    """Raise TargetRefused when any service or route target in `index` is the listener on
    `listen_port`, bound at `bind`. A port of 0 is not known until the listener binds, so nothing
    is refused for it.

    The engine repeats this per request against the port it actually bound, which is the
    authoritative check; this one lets `--port 4000` with a target on 4000 fail before anything
    starts rather than on the first request.
    """
    if not listen_port:
        return
    for sm in index.services:
        for target in (sm.target, *(r.target for r in sm.routes)):
            if target is not None:
                refuse_self_target(target, listen_port, bind)


def delegate(request: Request, classification: Classification) -> ForwardTo | None:
    """The answer target for this request, or None when irimi answers it itself.

    A matched route asks `servicemap.target_for`, which is the one place the route-over-service
    precedence lives. A request that matched no route can still be delegated by a **service**
    target: the map's author pointed the whole service at their stub, and answering the routes
    their map happens not to list with our own fake would give the agent a world that is half
    theirs and half ours. `target_reads` is what extends that to reads; `llm` and `telemetry` are
    never delegated, which is `target_for`'s rule restated here for the unmatched case.
    """
    service_map = classification.service_map
    if service_map is None:
        return None
    route = classification.route
    if route is not None:
        target, forward_auth = target_for(service_map, route), route.forward_auth
    elif classification.kind in TARGETABLE_KINDS or (
        classification.kind == "read" and service_map.target_reads
    ):
        target, forward_auth = service_map.target, False
    else:
        return None
    if target is None:
        return None
    sent_to = target_url(target, request, matched=route is not None)
    # THE SCOPE RULE, decision half (`ServiceMap.credential_hosts`). The loader already
    # refuses a non-loopback target on a service claiming one of these hosts, whichever layer it
    # arrived through. This asks the same question of the request and the answer actually in
    # front of us, so a target that reaches here some other way - a map built in code, a new
    # layer or flag - inherits the rule instead of escaping it. On this host the
    # path IS the credential, for every path and not only the ones a map lists.
    if request.host in service_map.credential_hosts and not sent_to.is_loopback:
        logger.error(
            "irimi: refusing to delegate %s to %s: the request path is the credential on %s, "
            "so its answer target must be loopback",
            request.path,
            sent_to.url,
            request.host,
        )
        return None
    return ForwardTo(sent_to, forward_auth=forward_auth)
