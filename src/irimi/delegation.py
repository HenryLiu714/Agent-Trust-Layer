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
from irimi.servicemap import CREDENTIAL_PATH_HOSTS, SELF_TARGET, TARGETABLE_KINDS, target_for

logger = logging.getLogger(__name__)

# Header names that carry a credential and are removed before a request reaches an answer target,
# unless the route sets `forward_auth: true`. Stripping `Authorization` alone was narrower than
# the rule it implements - "a local stub does not need your real key" - and left `Cookie`,
# `x-api-key` and `DD-API-KEY` on the request (#32).
#
# A marker list rather than a vendor list, because the vendor list is never finished: every new
# service brings its own spelling, and the one it is missing is the one that leaks. Over-stripping
# costs a stub a header it probably did not want; under-stripping hands it a live key, so the rule
# is deliberately wide and `forward_auth` is the one way to turn it off.
CREDENTIAL_MARKERS: tuple[str, ...] = (
    "auth",  # authorization, proxy-authorization, x-sentry-auth, x-authenticated-*
    "api-key",  # x-api-key, dd-api-key, x-goog-api-key
    "api_key",
    "apikey",
    "token",  # x-auth-token, x-amz-security-token, x-csrf-token
    "secret",
    "credential",
    "password",
    "signature",  # x-slack-signature and friends: a signature over a shared secret
)
# The ones no marker catches: a cookie jar is a credential, and these two vendor headers are
# spelled with none of the words above.
CREDENTIAL_HEADERS: frozenset[str] = frozenset({"cookie", "dd-application-key", "x-honeycomb-team"})


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


def is_credential_header(name: str) -> bool:
    """True when this header's value is a credential, so a target must not be handed it.

    Compared case-insensitively and by substring, so a vendor header nobody has written down yet
    (`x-acme-api-key`) is covered the day it appears. See CREDENTIAL_MARKERS for why the rule is
    wide rather than exact.
    """
    lowered = name.strip().lower()
    return lowered in CREDENTIAL_HEADERS or any(m in lowered for m in CREDENTIAL_MARKERS)


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
    route = classification.matched[1] if classification.matched is not None else None
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
