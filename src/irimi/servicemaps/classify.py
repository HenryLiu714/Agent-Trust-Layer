"""What a request is: its service, operation and kind, by the maps and the RFC fallback.

The classifier holds the decision half of the scope rule (`servicemaps.rules` holds the load-time
half). Pure functions, no I/O: the engine, the policy's precondition read and the overlay all ask
it the same question.
"""

from dataclasses import dataclass

from irimi.core.exchange import LIVE_KINDS, SAFE_METHODS, Flag, Kind, Request
from irimi.servicemaps import MapIndex, Route, ServiceMap


@dataclass(frozen=True)
class Classification:
    service: str
    operation: str
    kind: Kind
    flags: tuple[Flag, ...]
    # What MapIndex.route_for returned, so the faker, the summary and the answer
    # target do not have to look the route up a second time. None when nothing matched.
    matched: tuple[ServiceMap, Route] | None = None
    # The service claiming this host, which is not the same question as `matched`: a service-level
    # `target:` delegates the routes its own map does not list too, and for those `matched` is
    # None while this is set. None when no map claims the host at all.
    service_map: ServiceMap | None = None

    @property
    def route(self) -> Route | None:
        """The matched route alone, or None when nothing matched."""
        return None if self.matched is None else self.matched[1]


def classify(request: Request, maps: MapIndex | None = None) -> Classification:
    """What this request is, by a fixed precedence, most specific first.

    A route rule in a service map wins; then that service's `default_kind`; then the RFC 9110
    fallback, where `GET`/`HEAD`/`OPTIONS` are reads; then `unknown`. A host no map claims keeps
    its host name as the service and goes straight to the fallback, so it is still MITM'd: its
    reads forward live and everything else is answered locally. `unknown` always carries the
    `unclassified` flag, whether a map declared it or the fallback reached it.

    `maps=None` means the empty index: the fallback alone. Pure function, no I/O.
    """
    matched = (
        maps.route_for(request.host, request.method, request.path) if maps is not None else None
    )
    if matched is not None:
        # route_for found it through service_for, so this costs no second lookup.
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
    """THE SCOPE RULE, decision half (see `servicemaps.rules`): no classification that forwards
    live may apply to an unsafe method it did not name explicitly.

    The loader refuses a live kind on `*` and makes a destructive one justify itself, so no
    shipped map reaches this. This asks the question of the request in front of us instead of of the
    configuration, so a live kind that arrives by some route the loader does not police - a
    `default_kind` rule that drifts, a wildcard host, a map built in code, a new layer - is
    answered locally rather than performed on the real service. Downgrading to
    `unknown` is the fail-safe direction: a write answered locally costs the agent an echo, and
    a DELETE forwarded live cannot be undone.
    """
    if kind not in LIVE_KINDS or request.method in SAFE_METHODS:
        return kind, False
    route = matched[1] if matched is not None else None
    if route is not None and route.method == request.method:
        return kind, False  # the route named this verb; that is the explicit part
    return "unknown", True


def unclassified(request: Request) -> Classification:
    """How a request irimi could not decide about is recorded: `unknown` under its own host,
    flagged `unclassified`. How the engine records a request it refused, or whose decision
    raised."""
    return Classification(
        service=request.host, operation="", kind="unknown", flags=(Flag.UNCLASSIFIED,)
    )
