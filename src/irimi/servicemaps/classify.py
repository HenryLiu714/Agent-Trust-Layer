"""Classify a request by the maps and RFC 9110, enforcing the scope rule at the decision. No I/O."""

from dataclasses import dataclass

from irimi.core.exchange import LIVE_KINDS, SAFE_METHODS, Flag, Kind, Request
from irimi.servicemaps import MapIndex, Route, ServiceMap


@dataclass(frozen=True)
class Classification:
    service: str
    operation: str
    kind: Kind
    flags: tuple[Flag, ...]
    matched: tuple[ServiceMap, Route] | None = None
    # Set even when no route matched: a service `target:` also answers unlisted routes.
    service_map: ServiceMap | None = None

    @property
    def route(self) -> Route | None:
        """Return the matched route, or None."""
        return None if self.matched is None else self.matched[1]


def classify(request: Request, maps: MapIndex | None = None) -> Classification:
    """Return the kind from a route, else `default_kind`, else RFC 9110; flag `unknown`.

    An unmapped host is its own service; `maps=None` means RFC 9110 alone.
    """
    matched = (
        maps.route_for(request.host, request.method, request.path) if maps is not None else None
    )
    if matched is not None:
        service_map: ServiceMap | None
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
    kind, downgraded = _apply_scope_rule(kind, request, matched)
    flags: tuple[Flag, ...] = (Flag.UNCLASSIFIED,) if kind == "unknown" else ()
    if downgraded:
        flags += (Flag.DOWNGRADED,)
    return Classification(service, operation, kind, flags, matched, service_map)


def _apply_scope_rule(
    kind: Kind, request: Request, matched: tuple[ServiceMap, Route] | None
) -> tuple[Kind, bool]:
    """Return `unknown` and True for a live kind on an unsafe method its route did not name."""
    if kind not in LIVE_KINDS or request.method in SAFE_METHODS:
        return kind, False
    route = matched[1] if matched is not None else None
    if route is not None and route.method == request.method:
        return kind, False
    return "unknown", True


def unclassified(request: Request) -> Classification:
    """Return `unknown` under the request's host, for a refused request or one that raised."""
    return Classification(
        service=request.host, operation="", kind="unknown", flags=(Flag.UNCLASSIFIED,)
    )
