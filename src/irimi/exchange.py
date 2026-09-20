"""Engine-independent record of one HTTP exchange (design doc §2)."""

from dataclasses import dataclass, field
from typing import Literal

Kind = Literal["read", "write", "llm", "telemetry", "unknown"]
AnsweredBy = Literal["live", "fake-L0"]
Validation = Literal["validated", "unvalidated"]
Door = Literal["forward", "reverse"]

KINDS: tuple[Kind, ...] = ("read", "write", "llm", "telemetry", "unknown")
# The kinds shadow mode forwards to the real service instead of answering locally. It lives here,
# beside KINDS, rather than in policy.py, because the map loader has to refuse a `default_kind`
# that names one (#30) and servicemap must not import policy.
LIVE_KINDS: tuple[Kind, ...] = ("read", "llm", "telemetry")
SAFE_METHODS: frozenset[str] = frozenset({"GET", "HEAD", "OPTIONS"})

Headers = tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class Request:
    method: str  # upper-case
    scheme: str  # "http" | "https"
    host: str  # lower-case, no port
    port: int
    path: str  # path only, no query, always starts with "/"
    query: str  # raw query string without the leading "?", "" if none
    headers: Headers
    body: bytes

    @property
    def url(self) -> str:
        default = 443 if self.scheme == "https" else 80
        netloc = self.host if self.port == default else f"{self.host}:{self.port}"
        q = f"?{self.query}" if self.query else ""
        return f"{self.scheme}://{netloc}{self.path}{q}"


@dataclass(frozen=True)
class Response:
    status: int
    headers: Headers
    body: bytes


@dataclass
class Exchange:
    request: Request
    response: Response | None
    service: str
    operation: str
    kind: Kind
    answered_by: AnsweredBy
    validation: Validation
    run_id: str
    door: Door = "forward"  # "reverse" = came in as /<host>/<path> on the listener itself
    flags: tuple[str, ...] = field(default_factory=tuple)
