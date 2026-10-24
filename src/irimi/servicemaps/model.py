"""The service map model: routes, services, host index, matching and target precedence. No I/O."""

from dataclasses import dataclass, field
from typing import Literal
from urllib.parse import unquote

from irimi.core.exchange import ANY_METHOD, KINDS, LIVE_KINDS, Kind
from irimi.core.netaddr import Target

SELF_TARGET = "self"

# Hosts whose path is a credential (each hooks.slack.com path is a webhook secret): a target on
# one must be loopback, even with `--allow-remote-target`, and their paths never reach disk.
CREDENTIAL_PATH_HOSTS: frozenset[str] = frozenset({"hooks.slack.com"})
TARGETABLE_KINDS: frozenset[str] = frozenset({"write", "unknown"})
# Methods a live kind must justify with `live_because:`; not POST, which live inference uses.
DESTRUCTIVE_METHODS: frozenset[str] = frozenset({"DELETE", "PUT", "PATCH"})
DEFAULT_KINDS: tuple[Kind, ...] = tuple(k for k in KINDS if k not in LIVE_KINDS)


Style = Literal["rest", "rpc"]
IdFormat = Literal["prefix", "int", "uuid"]


class MapError(ValueError):
    """A map or overrides file the loader refuses. str(exc) names the source and the rule."""


@dataclass(frozen=True)
class IdSpec:
    """How a faked write mints an id: `<prefix><24 alphanumerics>`, a positive int, or a UUID."""

    format: IdFormat = "prefix"
    prefix: str = ""


@dataclass(frozen=True)
class Route:
    """One entry of a map's `routes:`. Fields by map key; "" or empty means none.

    method           `match.method`, upper-case; "*" when the map names none
    path             `match.path`; a `{name}` segment matches exactly one segment
    operation        the route's name, such as `refunds.create`
    kind             `read`, `write`, `llm`, `telemetry` or `unknown`
    human            the summary template
    ids              response field -> how a faked write mints its id
    fixture          object in `irimi/fixtures/<service>.json` a faked write starts from
    precondition     `Service.preconditions` entry checked before the write is faked
    fires            webhook events the real write would send; names unchecked, never sent
    compare_ignores  body fields a retry, a replay match and `irimi compare` ignore
    live_because     why a live route persists nothing on an unsafe method
    target           who answers instead of irimi; None (`self`) means irimi
    forward_auth     send `Authorization` to the target instead of stripping it
    """

    method: str
    path: str
    operation: str
    kind: Kind
    human: str = ""
    ids: dict[str, IdSpec] = field(default_factory=dict)
    fixture: str = ""
    precondition: str = ""
    fires: tuple[str, ...] = ()
    compare_ignores: tuple[str, ...] = ()
    live_because: str = ""
    target: Target | None = None
    forward_auth: bool = False


@dataclass(frozen=True)
class ServiceMap:
    """One service's map, from one YAML document. Fields by map key.

    service                the name `services.REGISTRY` keys on
    hosts                  exact host names and `*.` wildcard patterns
    routes                 the route rules, in file order
    style                  `rest` (a POST read needs `live_because:`) or `rpc` (all calls POST)
    default_kind           the kind for unlisted routes, never a live one; None: RFC 9110
    target                 answers `write` and `unknown` routes; None (`self`): irimi
    target_reads           send reads to `target` as well
    credential_path_hosts  hosts whose path is the credential, beyond the built-in set
    source                 where the map and its overrides came from, for errors
    """

    service: str
    hosts: frozenset[str]
    routes: tuple[Route, ...]
    style: Style = "rest"
    default_kind: Kind | None = None
    target: Target | None = None
    target_reads: bool = False
    credential_path_hosts: frozenset[str] = frozenset()
    source: str = ""

    @property
    def credential_hosts(self) -> frozenset[str]:
        """Return every host whose path is the credential: the built-in set and the map's own."""
        return CREDENTIAL_PATH_HOSTS | self.credential_path_hosts


@dataclass
class MapIndex:
    """Every loaded service, by host: an exact host beats a pattern; the longest pattern wins."""

    services: tuple[ServiceMap, ...] = ()
    by_host: dict[str, ServiceMap] = field(init=False, default_factory=dict)
    by_suffix: dict[str, ServiceMap] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.by_host = {h: sm for sm in self.services for h in sm.hosts if not is_pattern(h)}
        self.by_suffix = {h[1:]: sm for sm in self.services for h in sm.hosts if is_pattern(h)}

    @property
    def hosts(self) -> frozenset[str]:
        """Return the base-URL allow-list: exact hosts; a `*.` host needs `--base-url-host`."""
        return frozenset(self.by_host)

    @property
    def patterns(self) -> tuple[str, ...]:
        """Return every wildcard host spelling (`*.…`), sorted, for `irimi maps list`."""
        return tuple(sorted("*" + suffix for suffix in self.by_suffix))

    def service_for(self, host: str) -> ServiceMap | None:
        """Return the service claiming `host`: exact match first, then the longest pattern."""
        host = host.lower()
        exact = self.by_host.get(host)
        if exact is not None:
            return exact
        best: ServiceMap | None = None
        best_length = 0
        for suffix, sm in self.by_suffix.items():
            if host.endswith(suffix) and len(suffix) > best_length:
                best, best_length = sm, len(suffix)
        return best

    def route_for(self, host: str, method: str, path: str) -> tuple[ServiceMap, Route] | None:
        """Return the service and route for a request, or None when either is unmapped."""
        sm = self.service_for(host)
        if sm is None:
            return None
        route = match_route(sm, method, path)
        return None if route is None else (sm, route)

    def route_named(self, host: str, method: str, operation: str) -> Route | None:
        """Return the one `method` route naming `operation`, else None; for redacted paths."""
        sm = self.service_for(host)
        if sm is None:
            return None
        method = method.upper()
        named = [
            route
            for route in sm.routes
            if route.operation == operation and route.method in (ANY_METHOD, method)
        ]
        return named[0] if len(named) == 1 else None

    def operation_route(self, host: str, operation: str) -> Route | None:
        """Return the one route naming `operation`, whatever its method, else None."""
        sm = self.service_for(host)
        if sm is None:
            return None
        named = [route for route in sm.routes if route.operation == operation]
        return named[0] if len(named) == 1 else None


def match_route(sm: ServiceMap, method: str, path: str) -> Route | None:
    """Return the matching route with the fewest `{name}` segments, first in file order, or None."""
    method = method.upper()
    segments = path_segments(path)
    best: Route | None = None
    best_holes = 0
    for route in sm.routes:
        if route.method not in (ANY_METHOD, method):
            continue
        holes = match_path(route.path, segments)
        if holes is None:
            continue
        if best is None or holes < best_holes:
            best, best_holes = route, holes
    return best


def path_params(pattern: str, path: str) -> dict[str, str]:
    """Return the percent-decoded `{name}` captures, or {}; literals are matched undecoded."""
    parts = path_segments(pattern)
    segments = path_segments(path)
    if match_path(pattern, segments) is None:
        return {}
    return {
        part[1:-1]: unquote(segment)
        for part, segment in zip(parts, segments, strict=True)
        if is_hole(part)
    }


def fill_path(pattern: str, params: dict[str, str]) -> str | None:
    """Fill each `{name}` from `params`, or None if one is empty or holds `/`, `?` or `#`."""
    parts: list[str] = []
    for part in path_segments(pattern):
        if not is_hole(part):
            parts.append(part)
            continue
        value = params.get(part[1:-1], "")
        if not value or any(c in value for c in "/?#"):
            return None
        parts.append(value)
    return "/" + "/".join(parts) + ("/" if pattern.endswith("/") and parts else "")


def target_for(sm: ServiceMap, route: Route) -> Target | None:
    """Return the target answering this route, or None when irimi does; never for a live kind."""
    if route.target is not None:
        return route.target
    return service_target_for(sm, route.kind)


def service_target_for(sm: ServiceMap, kind: Kind) -> Target | None:
    """Return the service's own target for `kind`: writes and unknowns, reads under `target_reads`.

    A route's target beats it.
    """
    if kind in TARGETABLE_KINDS or (kind == "read" and sm.target_reads):
        return sm.target
    return None


def is_delegated(sm: ServiceMap) -> bool:
    """Return True when the service has any target, which the summary and banner report."""
    return sm.target is not None or any(r.target is not None for r in sm.routes)


def is_pattern(host: str) -> bool:
    """Return True for a `*.` host: it matches one or more leading labels, not the bare suffix."""
    return host.startswith("*.")


def is_hole(part: str) -> bool:
    """Return True for a `{name}` segment of a route pattern."""
    return part.startswith("{") and part.endswith("}")


def path_segments(path: str) -> tuple[str, ...]:
    return tuple(s for s in path.split("/") if s)


def match_path(pattern: str, segments: tuple[str, ...]) -> int | None:
    """Return how many `{name}` segments the match used, or None when it does not match."""
    parts = path_segments(pattern)
    if len(parts) != len(segments):
        return None
    holes = 0
    for part, segment in zip(parts, segments, strict=True):
        if is_hole(part):
            holes += 1
        elif part != segment:
            return None
    return holes
