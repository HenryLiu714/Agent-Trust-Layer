"""Answer targets (design D20): the address that answers a request instead of irimi.

`delegate` is the decision - which target, if any, this request goes to. The rest is what the
engine needs to act on that decision safely: where to send the request (`target_url`), what it
may never be sent to (`refuse_self_target`), and which headers it may not carry
(`is_credential_header`).
"""

import logging
from dataclasses import dataclass
from urllib.parse import urlsplit

from irimi import netaddr
from irimi.exchange import Request
from irimi.pipeline import Classification

# The credential-header rule is stated once, in `redact` (#69): a target may not be handed a
# credential header, and disk may not be handed one either. The `as` form re-exports it, because
# the engine asks `delegation.is_credential_header` which headers a target is not handed.
from irimi.redact import is_credential_header as is_credential_header
from irimi.servicemap import CREDENTIAL_PATH_HOSTS, SELF_TARGET, TARGETABLE_KINDS, target_for

logger = logging.getLogger(__name__)


class TargetRefused(ValueError):
    """An answer target irimi will not dial. str(exc) is the one-line explanation."""


class TargetUnreachable(TargetRefused):
    """An answer target irimi tried to reach and could not. A subclass, so every handler that
    already answers a refusal with the `irimi_target_failed` body answers this one the same way -
    the agent cannot act differently on the two, and the flag it earns is the same."""


@dataclass(frozen=True)
class ForwardTo:
    """An answer target: the address that answers this request instead of irimi (design D20).

    `url` is absolute and already carries the path and query the target should see. `forward_auth`
    is the route opting in to keeping its `Authorization` header; the engine strips it otherwise.
    """

    url: str
    forward_auth: bool = False


def target_url(target: str, request: Request, matched: bool) -> str:
    """Where a delegated request is sent, with nginx `proxy_pass` path semantics (design D20).

    A **bare origin** (`http://127.0.0.1:3000`) keeps the request's own path. A target that
    **carries a path** (`http://127.0.0.1:3000/refund`) replaces the part of the path the route
    matched. A route pattern always matches the request's whole path - `_match_path` requires the
    same number of segments - so for a mapped route there is no remainder and the target's path
    is the whole new path. A request that matched no route (a service-level target covering a
    route its map does not list) has nothing matched, so its own path is appended instead.

    The query string is always kept; method, body and headers are not this function's business.

    The result is a URL *string*, which the engine splits again to rewrite the flow, so anything
    in the request that would re-parse differently has to be escaped on the way in. A literal `#`
    is the one that bites: `POST /unlisted#x?a=1` would come back out as path `/unlisted` with no
    query at all, so the target would be sent a different request than the agent made and
    `Exchange.target` would record a URL that was never used. `#` is legal in a request target
    and means nothing there; `%23` is the same path to the target and survives the round trip.
    """
    parts = urlsplit(target)
    base = parts.path.rstrip("/")
    if not base:
        path = request.path
    elif matched:
        path = base
    else:
        path = base + request.path
    query = f"?{request.query}" if request.query else ""
    return f"{parts.scheme}://{parts.netloc}{path}{query}".replace("#", "%23")


def refuse_self_target(target: str, listen_port: int) -> None:
    """Raise TargetRefused when `target` is this listener's own address.

    Targets are loopback-only, so the host alone cannot tell a stub from ourselves - the port is
    what separates them. Forwarding to our own listener makes the proxy dial itself until it runs
    out of ports, which is the loop `detect_door` was written for on the reverse door (#4).
    """
    parts = urlsplit(target)
    port = parts.port or (443 if parts.scheme == "https" else 80)
    host = parts.hostname or ""
    if port == listen_port and (netaddr.is_self_host(host) or netaddr.resolves_to_self(host)):
        raise TargetRefused(
            f"answer target {target!r} is irimi's own listener on port {listen_port}; "
            "point it at the address that answers the route instead"
        )


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
    if target == SELF_TARGET:
        return None
    url = target_url(target, request, matched=route is not None)
    # THE SCOPE RULE, decision half (servicemap.CREDENTIAL_PATH_HOSTS). The loader already
    # refuses a non-loopback target on a service claiming one of these hosts, whichever layer it
    # arrived through. This asks the same question of the request and the answer actually in
    # front of us, so a target that reaches here some other way - a layer added later, a map
    # built in code, a future flag - inherits the rule instead of escaping it. On this host the
    # path IS the credential, for every path and not only the ones a map lists.
    if request.host in CREDENTIAL_PATH_HOSTS and not netaddr.is_local_target(url):
        logger.error(
            "irimi: refusing to delegate %s to %s: the request path is the credential on %s, "
            "so its answer target must be loopback",
            request.path,
            url,
            request.host,
        )
        return None
    return ForwardTo(url=url, forward_auth=forward_auth)
