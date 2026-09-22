"""The reverse door: plain HTTP on the listener itself, `/<host>/<path>`, relayed to `https://<host>`.

For SDKs that ship their own CA bundle or ignore the proxy variables (stripe-python is the
canonical case). The door is loopback-only and relays to an allow-list of exact hosts. The engine
enforces the loopback half (`IrimiAddon._through_reverse_door`); these functions decide the rest.
"""

from dataclasses import replace

from irimi import netaddr
from irimi.exchange import Door, Request

REVERSE_SCHEME = "https"
REVERSE_DEFAULT_PORT = 443


class ReverseDoorRefused(ValueError):
    """The reverse door will not relay this request. str(exc) is the one-line explanation."""


def detect_door(request: Request, listen_port: int) -> Door:
    """A request addressed to the listener itself (`Host: 127.0.0.1:4000`, or an absolute URL
    naming the listener under any spelling of loopback) came through the reverse door. Everything
    else is the forward door. A name on our own port that is not a recognised literal is resolved,
    because forwarding it would make the proxy connect to itself in a loop."""
    if request.port != listen_port:
        return "forward"
    if netaddr.is_self_host(request.host) or netaddr.resolves_to_self(request.host):
        return "reverse"
    return "forward"


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
