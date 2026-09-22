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


def resolves_to_self(host: str) -> bool:
    """DNS backstop for names such as the machine's own hostname. Only consulted for requests on
    our own port, so the hot path never resolves anything."""
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except (OSError, UnicodeError):
        return False
    return any(is_self_host(str(info[4][0])) for info in infos)


def is_loopback(address: str) -> bool:
    """True for 127.0.0.0/8 and ::1. Anything unparseable is not loopback."""
    try:
        return ipaddress.ip_address(address).is_loopback
    except ValueError:
        return False


def is_local_target(url: str) -> bool:
    """True when this answer target names loopback. Anything unparseable is not local.

    One spelling of the question, because three surfaces ask it: `delegation.delegate` refuses a
    non-loopback target on a credential-path host, and the banner and `irimi maps list` paint a
    non-loopback target red. Three copies would drift, and the one that drifts is the one that
    lets a credential off the machine.
    """
    try:
        host = urlsplit(url).hostname or ""
    except ValueError:
        return False
    return host == "localhost" or is_loopback(host)
