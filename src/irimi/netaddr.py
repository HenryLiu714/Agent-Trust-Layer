"""Which addresses are this machine.

Three surfaces ask "is this loopback?" - the reverse door, the answer-target rules, and the banner
that paints a non-loopback target red - and they must agree, because the one that drifts is the
one that lets a credential off the machine. So the question is spelled once, here, and nothing
else in irimi parses an address.
"""

import ipaddress
import socket
from urllib.parse import urlsplit


def is_self_host(host: str) -> bool:
    """True when `host` is a literal for this machine: loopback in any spelling `_local` reads,
    or the unspecified address (0.0.0.0, ::), which also connects locally."""
    return _local(host) is not None


def resolves_to_self(host: str, local_address: str = "") -> bool:
    """DNS backstop for names such as the machine's own hostname. Only consulted for requests on
    our own port, so the hot path never resolves anything.

    `local_address` is the address the request's connection arrived on. Bound wider than loopback,
    the listener is this machine at that address too: an agent in another container calls it by
    name (`irimi:4000`), and that name resolves to the address it connected to, never to loopback.
    A host that is that address, or resolves to it, is this machine as well, or forwarding it
    would make the proxy connect to itself until the ports ran out. A literal is compared without
    a lookup."""
    local = _address(local_address)
    if local is not None and _address(host) == local:
        return True
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except (OSError, UnicodeError):
        return False
    found = [str(info[4][0]) for info in infos]
    return any(is_self_host(a) or (local is not None and _address(a) == local) for a in found)


def _address(text: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    """`text` as an address, an IPv4-mapped one as its IPv4 address and with no IPv6 zone, so the
    two spellings a dual-stack socket reports of one address compare equal. None when `text` is
    not an address literal."""
    try:
        ip = ipaddress.ip_address(text.split("%", 1)[0])
    except ValueError:
        return None
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        return ip.ipv4_mapped
    return ip


def _local(host: str) -> str | None:
    """`loopback` or `unspecified` when `host` names this machine, else None. The one reading of
    a host that the predicates here share: "localhost" (with or without its trailing dot), any
    127/8 or ::1 address in any spelling inet_aton accepts (127.1, 0177.0.0.1), an IPv4-mapped one
    (::ffff:127.0.0.1), and 0.0.0.0 or ::. Anything unparseable is neither."""
    if host.rstrip(".").lower() == "localhost":
        return "loopback"
    ip = _address(host)
    if ip is None:
        try:
            ip = ipaddress.IPv4Address(socket.inet_aton(host))
        except (OSError, ValueError):
            return None
    if ip.is_loopback:
        return "loopback"
    return "unspecified" if ip.is_unspecified else None


def is_loopback(address: str) -> bool:
    """True for loopback as `_local` reads it, so not for 0.0.0.0 or ::."""
    return _local(address) == "loopback"


def authority(host: str, port: int, default_port: int | None = None) -> str:
    """`host:port` as a URL spells it: an IPv6 literal in brackets (`[::1]:4000`), so the port is
    not read as one more group of the address. The port is left out when it is `default_port`."""
    literal = f"[{host}]" if ":" in host else host
    return literal if port == default_port else f"{literal}:{port}"


def is_local_target(url: str) -> bool:
    """True when this answer target names loopback. Anything unparseable is not local.

    One spelling of the question, because three surfaces ask it: `delegation.delegate` refuses a
    non-loopback target on a credential-path host, and the banner and `irimi maps list` paint a
    non-loopback target red. The one intended difference from `is_self_host`: a target may not be
    0.0.0.0 or ::, which reach this machine only by an accident of the OS, while a request whose
    Host is one of them does reach irimi's listener.
    """
    try:
        host = urlsplit(url).hostname or ""
    except ValueError:
        return False
    return is_loopback(host)
