"""The request pipeline as plain functions. No mitmproxy here; the engine calls these in order."""

import ipaddress
import socket
from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from irimi.exchange import (
    LIVE_KINDS,
    SAFE_METHODS,
    AnsweredBy,
    Door,
    Exchange,
    Kind,
    Request,
    Response,
    Validation,
)

if TYPE_CHECKING:  # quoted annotations only: servicemap imports this module, so no runtime import
    from irimi.servicemap import MapIndex, Route, ServiceMap

RUN_HEADER = "irimi-run"  # header names are compared case-insensitively; stored lower-case
UNCLASSIFIED_FLAG = "unclassified"
# A live-forwarding kind was refused because it did not name this method (see
# `_refuse_live_on_an_unnamed_method`). The exchange says `unknown`, and this says why.
DOWNGRADED_FLAG = "kind-downgraded"

REVERSE_SCHEME = "https"
REVERSE_DEFAULT_PORT = 443

TARGET_FAILED_FLAG = "target-failed"
# The answer could not be decided at all - see `IrimiAddon.request`. It is not `target-failed`:
# that one says a target was chosen and could not be reached, this one says we never got as far
# as choosing.
DECISION_FAILED_FLAG = "decision-failed"
FIDELITY_DELEGATED_FLAG = "fidelity:delegated"
AUTH_HEADER = "authorization"


class ReverseDoorRefused(ValueError):
    """The reverse door will not relay this request. str(exc) is the one-line explanation."""


class TargetRefused(ValueError):
    """An answer target irimi will not dial. str(exc) is the one-line explanation."""


@dataclass(frozen=True)
class Classification:
    service: str
    operation: str
    kind: Kind
    flags: tuple[str, ...]
    # What MapIndex.route_for returned, so the faker (#11), the summary (#13) and the answer
    # target (#16) do not have to look the route up a second time. None when nothing matched.
    matched: "tuple[ServiceMap, Route] | None" = None
    # The service claiming this host, which is not the same question as `matched`: a service-level
    # `target:` delegates the routes its own map does not list too, and for those `matched` is
    # None while this is set. None when no map claims the host at all (#16).
    service_map: "ServiceMap | None" = None


def parse(
    method: str,
    scheme: str,
    host: str,
    port: int,
    path_and_query: str,
    headers: Iterable[tuple[str, str]],
    body: bytes | None,
) -> Request:
    """Normalize wire values into a Request: upper-case method, lower-case host and header names,
    path split from query, empty path becomes "/", None body becomes b""."""
    path, _, query = path_and_query.partition("?")
    return Request(
        method=method.upper(),
        scheme=scheme.lower(),
        host=host.lower(),
        port=port,
        path=path or "/",
        query=query,
        headers=tuple((k.lower(), v) for k, v in headers),
        body=body or b"",
    )


def detect_door(request: Request, listen_port: int) -> Door:
    """A request addressed to the listener itself (`Host: 127.0.0.1:4000`, or an absolute URL
    naming the listener under any spelling of loopback) came through the reverse door. Everything
    else is the forward door. A name on our own port that is not a recognised literal is resolved,
    because forwarding it would make the proxy connect to itself in a loop."""
    if request.port != listen_port:
        return "forward"
    if is_self_host(request.host) or _resolves_to_self(request.host):
        return "reverse"
    return "forward"


def is_self_host(host: str) -> bool:
    """True when `host` is a literal for this machine's loopback: "localhost", any 127/8 or ::1
    address in any spelling inet_aton accepts (127.1, 0177.0.0.1), an IPv4-mapped one
    (::ffff:127.0.0.1), or the unspecified address (0.0.0.0, ::), which also connects locally."""
    if host.rstrip(".") == "localhost":
        return True
    ip: ipaddress.IPv4Address | ipaddress.IPv6Address
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        try:
            ip = ipaddress.IPv4Address(socket.inet_aton(host))
        except OSError:
            return False
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    return ip.is_loopback or ip.is_unspecified


def _resolves_to_self(host: str) -> bool:
    """DNS backstop for names such as the machine's own hostname. Only consulted for requests on
    our own port, so the hot path never resolves anything."""
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except (OSError, UnicodeError):
        return False
    return any(is_self_host(info[4][0]) for info in infos)


def is_loopback(address: str) -> bool:
    """True for 127.0.0.0/8 and ::1. Anything unparseable is not loopback."""
    try:
        return ipaddress.ip_address(address).is_loopback
    except ValueError:
        return False


def is_local_target(url: str) -> bool:
    """True when this answer target names loopback. Anything unparseable is not local.

    One spelling of the question, because three surfaces ask it: `policy.delegate` refuses a
    non-loopback target on a credential-path host, and the banner and `irimi maps list` paint a
    non-loopback target red. Three copies would drift, and the one that drifts is the one that
    lets a credential off the machine.
    """
    try:
        host = urlsplit(url).hostname or ""
    except ValueError:
        return False
    return host == "localhost" or is_loopback(host)


def rewrite_reverse(request: Request, allowed_hosts: frozenset[str]) -> Request:
    """`/<host>[:<port>]/<rest>` on the listener becomes `https://<host>[:<port>]/<rest>`.

    The scheme is always https and the port defaults to 443. The query, method, body and headers
    are kept; the `host` header is rewritten to the upstream authority, or added if the request
    had none. Raises ReverseDoorRefused when the first segment is missing, is an IPv6 literal, has
    a bad port, or names a host (compared lower-case) that is not in `allowed_hosts`.
    """
    segment, _, rest = request.path.lstrip("/").partition("/")
    if segment.startswith("["):
        raise ReverseDoorRefused("reverse door: IPv6 literal upstream hosts are not supported")
    host, _, port_text = segment.partition(":")
    host = host.lower()
    if not host:
        raise ReverseDoorRefused(
            "reverse door: path must be /<upstream-host>/<path>, e.g. /api.stripe.com/v1/charges"
        )
    if host not in allowed_hosts:
        raise ReverseDoorRefused(
            f"reverse door: host {host!r} is not in a loaded map or --allow-host"
        )
    try:
        port = int(port_text) if port_text else REVERSE_DEFAULT_PORT
    except ValueError:
        raise ReverseDoorRefused(f"reverse door: bad port in {segment!r}") from None
    if not 1 <= port <= 65535:
        raise ReverseDoorRefused(f"reverse door: bad port in {segment!r}")
    authority = host if port == REVERSE_DEFAULT_PORT else f"{host}:{port}"
    headers = tuple((k, authority if k == "host" else v) for k, v in request.headers)
    if not any(k == "host" for k, _ in headers):
        headers += (("host", authority),)
    return replace(
        request, scheme=REVERSE_SCHEME, host=host, port=port, path="/" + rest, headers=headers
    )


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
    if port == listen_port and (is_self_host(host) or _resolves_to_self(host)):
        raise TargetRefused(
            f"answer target {target!r} is irimi's own listener on port {listen_port}; "
            "point it at the address that answers the route instead"
        )


def classify(request: Request, maps: "MapIndex | None" = None) -> Classification:
    """What this request is, by the precedence the design fixes (§4.3, D4).

    A route rule in a service map wins; then that service's `default_kind`; then the RFC 9110
    fallback, where `GET`/`HEAD`/`OPTIONS` are reads; then `unknown`. A host no map claims keeps
    its host name as the service and goes straight to the fallback, so it is still MITM'd: its
    reads forward live and everything else is answered locally. `unknown` always carries the
    `unclassified` flag, whether a map declared it or the fallback reached it.

    `maps=None` means the empty index: the fallback alone. Pure function, no I/O.
    """
    matched = (
        maps.route_for(request.host, request.method, request.path) if maps is not None else None
    )
    if matched is not None:
        # route_for found it through service_for, so this costs no second lookup.
        service_map, route = matched
        service, operation, kind = service_map.service, route.operation, route.kind
    else:
        service_map = maps.service_for(request.host) if maps is not None else None
        service = service_map.service if service_map is not None else request.host
        operation = f"{request.method} {request.path}"
        if service_map is not None and service_map.default_kind is not None:
            kind = service_map.default_kind
        elif request.method in SAFE_METHODS:
            kind = "read"
        else:
            kind = "unknown"
    kind, downgraded = _refuse_live_on_an_unnamed_method(kind, request, matched)
    flags = (UNCLASSIFIED_FLAG,) if kind == "unknown" else ()
    if downgraded:
        flags += (DOWNGRADED_FLAG,)
    return Classification(service, operation, kind, flags, matched, service_map)


def _refuse_live_on_an_unnamed_method(
    kind: Kind, request: Request, matched: "tuple[ServiceMap, Route] | None"
) -> tuple[Kind, bool]:
    """THE SCOPE RULE, decision half (see `servicemap`): no classification that forwards live may
    apply to an unsafe method it did not name explicitly.

    The loader refuses a live kind on `*` and makes a destructive one justify itself, so no map
    can reach this today. This asks the question of the request in front of us instead of of the
    configuration, so a live kind that arrives by some route the loader does not police - a
    `default_kind` rule that drifts again, a wildcard host, a map built in code, a layer added
    later - is answered locally rather than performed on the real service. Downgrading to
    `unknown` is the fail-safe direction: a write answered locally costs the agent an echo, and
    a DELETE forwarded live cannot be undone.
    """
    if kind not in LIVE_KINDS or request.method in SAFE_METHODS:
        return kind, False
    route = matched[1] if matched is not None else None
    if route is not None and route.method == request.method:
        return kind, False  # the route named this verb; that is the explicit part
    return "unknown", True


def attribute_run(request: Request, default_run_id: str) -> str:
    """The run this exchange belongs to: the Irimi-Run header if the client sent one, else the
    engine's own run id."""
    for name, value in request.headers:
        if name == RUN_HEADER and value.strip():
            return value.strip()
    return default_run_id


def annotate(
    request: Request,
    response: Response | None,
    classification: Classification,
    answered_by: AnsweredBy,
    run_id: str,
    extra_flags: tuple[str, ...] = (),
    door: Door = "forward",
    target: str = "",
) -> Exchange:
    """Build the Exchange. Anything the engine answered is unvalidated; live forwards are also
    unvalidated for now (validated is reserved for record mode, later phases)."""
    validation: Validation = "unvalidated"
    return Exchange(
        request=request,
        response=response,
        service=classification.service,
        operation=classification.operation,
        kind=classification.kind,
        answered_by=answered_by,
        validation=validation,
        run_id=run_id,
        door=door,
        flags=classification.flags + extra_flags,
        target=target,
    )


def respond(exchange: Exchange) -> Response | None:
    """What goes back to the client. Identity for now; issue #12 adds the Irimi-Answered-By
    header."""
    return exchange.response
