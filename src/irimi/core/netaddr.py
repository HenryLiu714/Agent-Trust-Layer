"""Decide which addresses are this machine; a wrong answer can let a credential off it."""

import ipaddress
import socket
from dataclasses import dataclass
from urllib.parse import urlsplit


def is_self_host(host: str) -> bool:
    """Return True if `host` is a literal for this machine: loopback, 0.0.0.0 or ::."""
    return _local(host) is not None


def resolves_to_self(host: str, local_address: str = "") -> bool:
    """Return True if `host` resolves to this machine, by DNS if it is not a literal.

    A host equal to or resolving to `local_address` counts: forwarding it would loop the proxy.
    """
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
    """Return `text` as an address, IPv4-mapped unwrapped, zone dropped; None if not a literal."""
    try:
        ip = ipaddress.ip_address(text.split("%", 1)[0])
    except ValueError:
        return None
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        return ip.ipv4_mapped
    return ip


def _local(host: str) -> str | None:
    """Return `loopback` or `unspecified` when `host` names this machine in any spelling."""
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
    """Return True for loopback only, not 0.0.0.0 or ::."""
    return _local(address) == "loopback"


def authority(host: str, port: int, default_port: int | None = None) -> str:
    """Return `host:port` as a URL spells it, IPv6 bracketed, the port omitted if `default_port`."""
    literal = f"[{host}]" if ":" in host else host
    return literal if port == default_port else f"{literal}:{port}"


def default_port(scheme: str) -> int:
    """Return the port a `scheme` URL implies when it names none."""
    return 443 if scheme == "https" else 80


@dataclass(frozen=True)
class Target:
    """An http(s) address that answers a request instead of the service: a target."""

    scheme: str
    host: str  # lower-case, no brackets
    port: int
    path: str = ""  # replaces the route's path; a send address's includes the query

    @classmethod
    def parse(cls, url: str) -> "Target":
        """Parse `url`. Raises ValueError for anything but an http(s) URL with a host."""
        parts = urlsplit(url)
        port = parts.port
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise ValueError(f"{url!r} is not an http(s) URL with a host")
        path = parts.path + (f"?{parts.query}" if parts.query else "")
        return cls(parts.scheme, parts.hostname.lower(), port or default_port(parts.scheme), path)

    @property
    def authority(self) -> str:
        """Return `host[:port]`, the port omitted when it is the scheme's default."""
        return authority(self.host, self.port, default_port(self.scheme))

    @property
    def is_loopback(self) -> bool:
        """Return True for loopback only; 0.0.0.0 and :: reach this machine by OS accident."""
        return is_loopback(self.host)

    @property
    def url(self) -> str:
        return f"{self.scheme}://{self.authority}{self.path}"
