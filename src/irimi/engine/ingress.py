"""How a request reached irimi, and what irimi makes of it before deciding about it.

A request comes through the proxy (the forward door), or as plain HTTP addressed to the listener
itself, `/<host>/<path>`, which is base-URL mode for SDKs that ignore proxy settings (the reverse
door). `admit` tells the two apart, answers the control endpoint's requests apart from both,
rewrites a base-URL request to its upstream, and takes irimi's own headers off the request.
Pure: the engine carries what it returns onto its flow.

Base-URL mode is loopback-only and relays to an allow-list of exact hosts, whatever address the
listener is bound to.
"""

import logging
from dataclasses import dataclass, replace

from irimi.core import netaddr
from irimi.core.exchange import Door, Request
from irimi.engine import wire
from irimi.protocol.control import CONTROL_PREFIX
from irimi.protocol.headers import REWROTE_HEADER, RUN_HEADER

logger = logging.getLogger(__name__)

REVERSE_SCHEME = "https"
REVERSE_DEFAULT_PORT = 443
# Why a request irimi could not tell a proxy request from a base-URL one for is refused.
ROUTE_FAILED = "could not route this request"
# Why a base-URL request irimi could not point at its upstream is refused.
REWRITE_FAILED = "could not rewrite this request"


class ReverseDoorRefused(ValueError):
    """irimi will not relay this base-URL request. str(exc) is the one-line explanation."""


@dataclass(frozen=True)
class Admitted:
    """A request irimi decides about: as the trace shows it, rewritten to its upstream when it
    came by base URL and without irimi's own headers, and the run it belongs to.

    `listener` is the address and port the request reached irimi at, which no answer target may
    be. `started_at` is when the engine first saw it. `cut` is whether its body decoded past the
    cap, so `request.body` is not all of it."""

    request: Request
    run_id: str
    door: Door
    listener: tuple[str, int]
    started_at: float = 0.0
    cut: bool = False


@dataclass(frozen=True)
class ControlCall:
    """A request to irimi's control endpoint, answered by `control.ControlEndpoint` and nothing
    else. `whole_body` is False when its body decoded past the cap."""

    request: Request
    whole_body: bool


@dataclass(frozen=True)
class Refusal:
    """A request irimi answers with its own error before deciding about it: the status and the
    one-line reason. `parsed` is False for a request irimi could not parse, which is recorded
    unclassified."""

    request: Request
    door: Door
    status: int
    reason: str
    started_at: float = 0.0
    cut: bool = False
    parsed: bool = True


def admit(
    request: Request,
    *,
    listener: tuple[str, int],
    peer: str,
    reverse_hosts: frozenset[str],
    default_run_id: str,
    started_at: float = 0.0,
    cut: bool = False,
) -> Admitted | ControlCall | Refusal:
    """What irimi makes of a request it parsed. `peer` is the address of the client that sent it.

    Never a raise for a request irimi cannot route or rewrite: a raise in the engine's hook would
    forward the flow, and the request may be one addressed to irimi itself. It is a `Refusal`.
    """
    try:
        door = detect_door(request, listener[1], listener[0])
    except Exception as exc:
        logger.warning("irimi: could not tell whether this is a proxy or base-URL request: %s", exc)
        return Refusal(request, "forward", 400, ROUTE_FAILED, started_at, cut)
    # The control endpoint is answered before the base-URL rewrite, which would read `_irimi` as
    # an upstream host and refuse it, and before `Irimi-Run` is read: a control request names its
    # run in its path. It is never forwarded, never decided and never recorded as an exchange.
    if is_control_request(request, door):
        return ControlCall(request, whole_body=not cut)
    if door == "reverse":
        try:
            request = _through_reverse_door(request, peer, reverse_hosts)
        except ReverseDoorRefused as exc:
            logger.warning("irimi: %s", exc)
            return Refusal(request, door, 403, str(exc), started_at, cut)
        except Exception as exc:
            logger.warning(
                "irimi: refusing a base-URL request that could not be rewritten: %s", exc
            )
            return Refusal(request, door, 400, REWRITE_FAILED, started_at, cut)
    # irimi's own wire vocabulary is never the agent's. `Irimi-Run` names the run this request
    # belongs to, which only irimi asks, so it is read and then taken off: `Exchange.run_id` holds
    # it, and no forward, target or check read carries it to the service. irimi sends no
    # `Irimi-Rewrote`, so one the agent sent goes too.
    run_id = wire.attribute_run(request, default_run_id)
    request = request.without_header(REWROTE_HEADER).without_header(RUN_HEADER)
    return Admitted(request, run_id, door, listener, started_at, cut)


def is_control_request(request: Request, door: Door) -> bool:
    """A request addressed to irimi's own listener (`detect_door` says `reverse`) whose path
    starts with CONTROL_PREFIX. Asked BEFORE the base-URL rewrite, which would read `_irimi` as an
    upstream host and refuse it with a 403. An absolute-form request through the forward proxy
    that names the listener is `reverse` too, so it is handled the same."""
    return door == "reverse" and request.path.startswith(CONTROL_PREFIX)


def detect_door(request: Request, listen_port: int, local_address: str = "") -> Door:
    """A request addressed to the listener itself (`Host: 127.0.0.1:4000`, or an absolute URL
    naming the listener under any spelling of loopback) came through the reverse door. Everything
    else is the forward door. A name on our own port that is not a recognised literal is resolved,
    because forwarding it would make the proxy connect to itself in a loop.

    `local_address` is the address the connection arrived on. A listener bound wider than loopback
    is also this machine at that address, which an agent in another container reaches by name
    (`http://irimi:4000/...`): a host that is it, or resolves to it, is the listener too."""
    if request.port != listen_port:
        return "forward"
    if netaddr.is_self_host(request.host) or netaddr.resolves_to_self(request.host, local_address):
        return "reverse"
    return "forward"


def _through_reverse_door(request: Request, peer: str, allowed_hosts: frozenset[str]) -> Request:
    """A base-URL request rewritten to its upstream. Raises ReverseDoorRefused for a client that is
    not on this machine or a host that is not allowed."""
    if not netaddr.is_loopback(peer):
        raise ReverseDoorRefused(f"base-URL requests come from loopback only; refusing {peer!r}")
    return rewrite_reverse(request, allowed_hosts)


def rewrite_reverse(request: Request, allowed_hosts: frozenset[str]) -> Request:
    """`/<host>[:<port>]/<rest>` on the listener becomes `https://<host>[:<port>]/<rest>`.

    The scheme is always https and the port defaults to 443. The query, method, body and headers
    are kept; the `host` header is rewritten to the upstream authority, or added if the request
    had none. Raises ReverseDoorRefused when the first segment is missing, is an IPv6 literal, has
    a bad port, or names a host (compared lower-case) that is not in `allowed_hosts`.
    """
    segment, _, rest = request.path.lstrip("/").partition("/")
    if segment.startswith("["):
        raise ReverseDoorRefused(
            "base-URL request names an IPv6 literal host, which irimi does not relay"
        )
    host, _, port_text = segment.partition(":")
    host = host.lower()
    if not host:
        raise ReverseDoorRefused(
            "a base-URL request's path must be /<upstream-host>/<path>, "
            "e.g. /api.stripe.com/v1/charges"
        )
    if host not in allowed_hosts:
        raise ReverseDoorRefused(
            f"base-URL host {host!r} is not in a loaded map or --base-url-host"
        )
    try:
        port = int(port_text) if port_text else REVERSE_DEFAULT_PORT
    except ValueError:
        raise ReverseDoorRefused(f"bad port in base-URL host {segment!r}") from None
    if not 1 <= port <= 65535:
        raise ReverseDoorRefused(f"bad port in base-URL host {segment!r}")
    authority = host if port == REVERSE_DEFAULT_PORT else f"{host}:{port}"
    headers = tuple((k, authority if k == "host" else v) for k, v in request.headers)
    if not any(k == "host" for k, _ in headers):
        headers += (("host", authority),)
    return replace(
        request, scheme=REVERSE_SCHEME, host=host, port=port, path="/" + rest, headers=headers
    )
