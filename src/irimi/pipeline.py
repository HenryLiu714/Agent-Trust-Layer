"""The request pipeline as plain functions. No mitmproxy here; the engine calls these in order."""

import ipaddress
import socket
from collections.abc import Iterable
from dataclasses import dataclass, replace

from irimi.exchange import (
    SAFE_METHODS,
    AnsweredBy,
    Door,
    Exchange,
    Kind,
    Request,
    Response,
    Validation,
)

RUN_HEADER = "irimi-run"  # header names are compared case-insensitively; stored lower-case
UNCLASSIFIED_FLAG = "unclassified"

REVERSE_SCHEME = "https"
REVERSE_DEFAULT_PORT = 443


class ReverseDoorRefused(ValueError):
    """The reverse door will not relay this request. str(exc) is the one-line explanation."""


@dataclass(frozen=True)
class Classification:
    service: str
    operation: str
    kind: Kind
    flags: tuple[str, ...]


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


def classify(request: Request) -> Classification:
    """RFC 9110 fallback only (issue #7 adds map lookup in front of this):
    safe methods are reads; everything else is unknown and flagged unclassified."""
    operation = f"{request.method} {request.path}"
    if request.method in SAFE_METHODS:
        return Classification(request.host, operation, "read", ())
    return Classification(request.host, operation, "unknown", (UNCLASSIFIED_FLAG,))


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
    )


def respond(exchange: Exchange) -> Response | None:
    """What goes back to the client. Identity for now; issue #12 adds the Irimi-Answered-By
    header."""
    return exchange.response
