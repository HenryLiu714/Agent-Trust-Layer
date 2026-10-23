"""What a loaded map may not say. Every refusal is a MapError naming the source and the rule."""

from irimi.core.exchange import ANY_METHOD, LIVE_KINDS, SAFE_METHODS
from irimi.core.netaddr import Target
from irimi.servicemaps.model import (
    DESTRUCTIVE_METHODS,
    TARGETABLE_KINDS,
    MapError,
    Route,
    ServiceMap,
    is_hole,
    path_segments,
    target_for,
)

# The kinds whose request bodies irimi compares (retries, replay matching, `compare`), and so the
# kinds a route's `compare_ignores:` may name fields of.
COMPARED_KINDS: frozenset[str] = TARGETABLE_KINDS | {"llm"}

# Scope rule: a safety property is checked at the scope it belongs to (method, route, host or
# service), and twice: when maps load, so a bad config is refused by name, and again at the
# decision, so a config that slips past the loader still cannot act. For example, "forwarded
# live" is per method, so a live route must name its methods: no classification that forwards
# live may apply to an unsafe method it did not name explicitly. "The URL is the credential" is
# per host, so it covers unlisted paths. The decision half is `classify._apply_scope_rule`.


def validate_targets(sm: ServiceMap, allow_target_hosts: frozenset[str]) -> None:
    """Check the target rules, which hold only once overrides and `--target` are merged in."""
    if sm.target_reads and sm.target is None:
        raise MapError(
            f"{sm.source}: `target_reads: true` needs a `target:` URL to send the reads to"
        )
    _check_target_host(sm.target, allow_target_hosts, f"{sm.source}: `target`")
    for route in sm.routes:
        where = f"{sm.source}: route {route.method} {route.path}"
        if route.target is not None and route.kind not in TARGETABLE_KINDS:
            raise MapError(
                f"{where}: a `target:` is valid on `write` and `unknown` routes only "
                f"(this one is `{route.kind}`); delegate reads with `target_reads:` on the service"
            )
        if route.forward_auth and target_for(sm, route) is None:
            raise MapError(f"{where}: `forward_auth: true` needs a target to forward to")
        _check_target_host(route.target, allow_target_hosts, f"{where}: `target`")
    _check_credential_path_targets(sm)


def _check_target_host(
    target: Target | None, allow_target_hosts: frozenset[str], where: str
) -> None:
    """Refuse a non-loopback target unless `--allow-remote-target` names its host."""
    if target is None or target.is_loopback:
        return
    host = target.host
    if host in allow_target_hosts:
        return
    raise MapError(
        f"{where}: target host {host!r} is not loopback; targets must be 127.0.0.1, ::1 or "
        f"localhost unless you pass --allow-remote-target {host}"
    )


def _check_live_kind_methods(sm: ServiceMap, route: Route, where: str) -> None:
    """Enforce the method scope of the scope rule on a live-forwarded route.

    A route with no `method:` matches `*`, which includes DELETE, so a live kind is refused there.
    A destructive method needs `live_because:`. POST is allowed: inference and telemetry intake
    use it.
    """
    if route.kind not in LIVE_KINDS:
        return
    if route.method == ANY_METHOD:
        raise MapError(
            f"{where}: `kind: {route.kind}` is forwarded to the real service, so it may not "
            f"match every method. This route names no `method:`, which means `{ANY_METHOD}` - "
            "and that includes DELETE. Name the methods this route really serves"
        )
    if route.method in DESTRUCTIVE_METHODS and not route.live_because:
        raise MapError(
            f"{where}: `kind: {route.kind}` is forwarded to the real service, so `{route.method}` "
            'would be performed for real. Add `live_because: "<why this persists nothing>"`'
        )


def _check_credential_path_targets(sm: ServiceMap) -> None:
    """Enforce the host scope of the scope rule: a credential-path service's targets are loopback.

    Every target is checked, not only webhook routes: unlisted paths on the host carry the
    credential too, and routes match by path within the service, so any route can answer a
    request to the credential host.
    """
    hosts = sorted(h for h in sm.hosts if h in sm.credential_hosts)
    if not hosts:
        return
    because = f"{sm.source}: service {sm.service!r} claims {hosts[0]}"
    _require_loopback_target(sm.target, f"{because} (`target`)")
    for route in sm.routes:
        _require_loopback_target(
            target_for(sm, route), f"{because} (route {route.method} {route.path})"
        )


def _require_loopback_target(target: Target | None, where: str) -> None:
    """Refuse a non-loopback target; `--allow-remote-target` does not apply here."""
    if target is None or target.is_loopback:
        return
    raise MapError(
        f"{where}: a target on this host may only be loopback. The request path is the "
        "credential there, so --allow-remote-target does not apply to it - and that covers every "
        "path on the host, not only the routes this map happens to list"
    )


def check_route_rules(sm: ServiceMap) -> None:
    """Check each route's rules, some of which need the service's own fields (`style`)."""
    seen: set[tuple[str, str]] = set()
    for route in sm.routes:
        where = f"{sm.source}: route {route.method} {route.path}"
        _check_unique_holes(route.path, where)
        key = (route.method, route.path)
        if key in seen:
            raise MapError(f"{where}: appears twice in the same map")
        seen.add(key)
        if route.live_because and route.kind not in LIVE_KINDS:
            raise MapError(
                f"{where}: `live_because` belongs on a route that is forwarded live "
                f"({', '.join(LIVE_KINDS)}); it is what justifies forwarding an unsafe method"
            )
        if route.compare_ignores and route.kind not in COMPARED_KINDS:
            raise MapError(
                f"{where}: `compare_ignores` belongs on a `write`, `unknown` or `llm` route, "
                f"whose bodies irimi compares; a `{route.kind}` route's never are"
            )
        # A live route is answered by the real service, so `fixture:`, `precondition:` and
        # `fires:` would never be used there; a key that silently does nothing is refused.
        if route.fixture and route.kind in LIVE_KINDS:
            raise MapError(
                f"{where}: `fixture:` names the object a locally answered write starts from, "
                f"and a `{route.kind}` route is forwarded to the real service, so it would "
                "never be used"
            )
        if route.precondition and route.kind in LIVE_KINDS:
            raise MapError(
                f"{where}: `precondition:` names the check run before a write is faked, and a "
                f"`{route.kind}` route is forwarded to the real service, which checks its own "
                "preconditions; it would never be used"
            )
        if route.fires and route.kind in LIVE_KINDS:
            raise MapError(
                f"{where}: `fires:` names the webhooks a FAKED write would have sent, and a "
                f"`{route.kind}` route is forwarded to the real service, which sends its own; "
                "they would never be listed"
            )
        repeated_events = sorted({e for e in route.fires if route.fires.count(e) > 1})
        if repeated_events:
            # The summary prints this list verbatim, so a repeat would promise two
            # `refund.created` events for one refund.
            raise MapError(
                f"{where}: `fires:` names {repeated_events[0]!r} more than once. A write fires an "
                "event once, and the summary prints this list as it is given"
            )
        _check_live_kind_methods(sm, route, where)
        if (
            sm.style == "rest"
            and route.kind == "read"
            and route.method not in SAFE_METHODS
            and not route.live_because
        ):
            raise MapError(
                f"{where}: `kind: read` on an unsafe method downgrades a write. Add "
                '`live_because: "<why this persists nothing>"`'
            )


def _check_unique_holes(pattern: str, where: str) -> None:
    """Refuse a pattern that repeats a `{name}`: `path_params` would keep only the last capture."""
    holes = [p[1:-1] for p in path_segments(pattern) if is_hole(p)]
    repeated = sorted({name for name in holes if holes.count(name) > 1})
    if repeated:
        raise MapError(
            f"{where}: `{{{repeated[0]}}}` appears more than once in the path. A parameter name "
            "binds one segment, so a repeat would silently drop every capture but the last"
        )


def check_unique(maps: list[ServiceMap]) -> None:
    services: dict[str, str] = {}
    hosts: dict[str, str] = {}
    for sm in maps:
        if sm.service in services:
            raise MapError(
                f"{sm.source}: service {sm.service!r} is already defined in {services[sm.service]}"
            )
        services[sm.service] = sm.source
        for host in sorted(sm.hosts):
            if host in hosts:
                raise MapError(f"{sm.source}: host {host!r} is already mapped by {hosts[host]}")
            hosts[host] = sm.source
