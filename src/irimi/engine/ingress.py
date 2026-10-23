"""Admission: tell a proxied request from a base-URL one, and prepare it for answering.

A base-URL request is plain HTTP as `/<host>/<path>`, loopback-only, to allow-listed hosts.
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
ROUTE_FAILED = "could not route this request"
REWRITE_FAILED = "could not rewrite this request"


class ReverseDoorRefused(ValueError):
    """irimi will not relay this base-URL request; str(exc) is the one-line reason."""


@dataclass(frozen=True)
class Admitted:
    """A request to answer, rewritten to its upstream and stripped of irimi's headers, and its run.

    `listener` is where it reached irimi, which no target may be; `cut` means a truncated body.
    """

    request: Request
    run_id: str
    door: Door
    listener: tuple[str, int]
    started_at: float = 0.0
    cut: bool = False


@dataclass(frozen=True)
class ControlCall:
    """A request to the control endpoint; `whole_body` is False when it decoded past the cap."""

    request: Request
    whole_body: bool


@dataclass(frozen=True)
class Refusal:
    """A request irimi refuses with its own error. `parsed=False` records it unclassified."""

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
    """Classify a parsed request by how it arrived. `peer` is the client's address.

    Never raises (fail-closed hook rule): a request irimi cannot route or rewrite is a `Refusal`.
    """
    try:
        door = detect_door(request, listener[1], listener[0])
    except Exception as exc:
        logger.warning("irimi: could not tell whether this is a proxy or base-URL request: %s", exc)
        return Refusal(request, "forward", 400, ROUTE_FAILED, started_at, cut)
    # Checked before the rewrite, which refuses `_irimi` as a host; a control path names its run.
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
    # Only irimi may set irimi's headers, so neither reaches the service.
    run_id = wire.attribute_run(request, default_run_id)
    request = request.without_header(REWROTE_HEADER).without_header(RUN_HEADER)
    return Admitted(request, run_id, door, listener, started_at, cut)


def is_control_request(request: Request, door: Door) -> bool:
    """Return True for a request to irimi's own listener whose path starts with CONTROL_PREFIX."""
    return door == "reverse" and request.path.startswith(CONTROL_PREFIX)


def detect_door(request: Request, listen_port: int, local_address: str = "") -> Door:
    """Return `reverse` for a request to the listener itself (never forwarded), else `forward`."""
    if request.port != listen_port:
        return "forward"
    if netaddr.is_self_host(request.host) or netaddr.resolves_to_self(request.host, local_address):
        return "reverse"
    return "forward"


def _through_reverse_door(request: Request, peer: str, allowed_hosts: frozenset[str]) -> Request:
    """Rewrite a base-URL request to its upstream; raise ReverseDoorRefused off loopback."""
    if not netaddr.is_loopback(peer):
        raise ReverseDoorRefused(f"base-URL requests come from loopback only; refusing {peer!r}")
    return rewrite_reverse(request, allowed_hosts)


def rewrite_reverse(request: Request, allowed_hosts: frozenset[str]) -> Request:
    """Rewrite `/<host>[:<port>]/<rest>` on the listener to `https://<host>[:<port>]/<rest>`.

    Raises ReverseDoorRefused for a missing or IPv6 host, a bad port, or a host not allowed.
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
