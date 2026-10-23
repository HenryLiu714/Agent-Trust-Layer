"""What a loaded map may not say. Every refusal is a MapError naming the source and the rule.

`loader` calls the three public checks; the rest are the pieces they are made of.
"""

from urllib.parse import urlsplit

from irimi import netaddr
from irimi.exchange import ANY_METHOD, LIVE_KINDS, SAFE_METHODS
from irimi.servicemap.model import (
    DESTRUCTIVE_METHODS,
    SELF_TARGET,
    TARGETABLE_KINDS,
    MapError,
    Route,
    ServiceMap,
    is_hole,
    path_segments,
    target_for,
)

# The kinds whose request bodies irimi compares (idempotent retries, replay matching, `compare`),
# and so the kinds a route's `compare_ignores:` may name fields of.
COMPARED_KINDS: frozenset[str] = TARGETABLE_KINDS | {"llm"}

# Scope rule: a safety property is checked at the scope it belongs to (method, route, host or
# service), and twice: when maps are loaded, so a bad configuration is refused by name whichever
# layer it came through (shipped map, overrides file or `--target` flag), and again at the
# decision, so a configuration that slips past the loader still cannot act. Examples:
#   - "telemetry is forwarded live" is per route, so it may not be a service's `default_kind`;
#   - "forwarded live" is per method, so a live route must name its unsafe methods;
#   - "the URL is the credential" is per host (hooks.slack.com), so it covers unlisted paths.
# The decision-time check asks only about the request in front of it, so a new way of reaching
# delegation or classification inherits the rule instead of escaping it.


def validate_targets(sm: ServiceMap, allow_target_hosts: frozenset[str]) -> None:
    """The target rules that can only be checked once overrides are merged in."""
    if sm.target_reads and sm.target == SELF_TARGET:
        raise MapError(
            f"{sm.source}: `target_reads: true` needs a `target:` URL to send the reads to"
        )
    _check_target_host(sm.target, allow_target_hosts, f"{sm.source}: `target`")
    for route in sm.routes:
        where = f"{sm.source}: route {route.method} {route.path}"
        if route.target != SELF_TARGET and route.kind not in TARGETABLE_KINDS:
            raise MapError(
                f"{where}: a `target:` is valid on `write` and `unknown` routes only "
                f"(this one is `{route.kind}`); delegate reads with `target_reads:` on the service"
            )
        if route.forward_auth and target_for(sm, route) == SELF_TARGET:
            raise MapError(f"{where}: `forward_auth: true` needs a target to forward to")
        _check_target_host(route.target, allow_target_hosts, f"{where}: `target`")
    _check_credential_path_targets(sm)


def _check_target_host(target: str, allow_target_hosts: frozenset[str], where: str) -> None:
    """Targets must be loopback unless `--allow-target-host` names the host."""
    if target == SELF_TARGET:
        return
    if netaddr.is_local_target(target):
        return
    host = urlsplit(target).hostname or ""
    if host in allow_target_hosts:
        return
    raise MapError(
        f"{where}: target host {host!r} is not loopback; targets must be 127.0.0.1, ::1 or "
        f"localhost unless you pass --allow-target-host {host}"
    )


def _check_live_kind_methods(sm: ServiceMap, route: Route, where: str) -> None:
    """A live-forwarded kind may only apply to a method the route named explicitly.

    The METHOD scope of THE SCOPE RULE. `kind` is written per route, but "forward this to the
    real service" is a decision about a *verb*: `match: {path: /api/{v}/{thing}}` with no
    `method:` means `*`, and `*` includes DELETE. A route like that would classify
    `DELETE /api/v1/dashboard` as telemetry and perform it for real, reached by omission rather
    than by deliberately writing `"*"`.

    The rule: **no classification that forwards live may apply to an unsafe method it did not
    name explicitly.** `*` names nothing, so it never satisfies it. A route that really does
    serve a destructive verb live has to say so, and then justify it the way `kind: read`
    already has to, with `live_because:`. POST is left alone: it is the honest verb for inference
    and for telemetry intake, which is what these kinds exist for.
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
    """Every target of a service claiming a credential-path host must be loopback.

    The HOST scope of THE SCOPE RULE. Keyed on the webhook *route*, it would leave a
    service-level target free to delegate every path the map does not list, and on
    `hooks.slack.com` the unlisted paths (`/workflows/...`, `/triggers/...`) carry the credential
    just as the listed one does. Checking every target the service can answer with covers the
    service target, each route target, the overrides file and `--target` alike, because all four
    have already been merged into `sm` by the time this runs.

    Every route target, and not only the ones whose path looks like a webhook, because routes are
    matched by path within a SERVICE and not within a host: `route_for("hooks.slack.com", "POST",
    "/api/chat.postMessage")` matches, so a target on that route would answer a request addressed
    to the credential host. Routes being service-scoped while the credential is host-scoped is
    the same confusion one level down.
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


def _require_loopback_target(target: str, where: str) -> None:
    """A credential-path target must be loopback, with no escape hatch (`credential_hosts`)."""
    if target == SELF_TARGET:
        return
    if netaddr.is_local_target(target):
        return
    raise MapError(
        f"{where}: a target on this host may only be loopback. The request path is the "
        "credential there, so --allow-target-host does not apply to it - and that covers every "
        "path on the host, not only the routes this map happens to list"
    )


def check_route_rules(sm: ServiceMap) -> None:
    """Per-route rules that need the service's own fields (`style`) to judge."""
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
        if route.fixture and route.kind in LIVE_KINDS:
            # A key that silently does nothing reads as configured and is never consulted, so it
            # is refused. A live route is answered by the real service, so it has no body of ours
            # to start from.
            raise MapError(
                f"{where}: `fixture:` names the object a locally answered write starts from, "
                f"and a `{route.kind}` route is forwarded to the real service, so it would "
                "never be used"
            )
        if route.precondition and route.kind in LIVE_KINDS:
            # The same rule as `fixture:`, for the same reason: a live route is forwarded, so a
            # check that runs before a write is faked would never run on it.
            raise MapError(
                f"{where}: `precondition:` names the check run before a write is faked, and a "
                f"`{route.kind}` route is forwarded to the real service, which checks its own "
                "preconditions; it would never be used"
            )
        if route.fires and route.kind in LIVE_KINDS:
            # The same rule as `fixture:` and `precondition:`, for the same reason: a
            # live route is forwarded, the real service performs the write and sends its own
            # webhooks, so ours would never be listed.
            raise MapError(
                f"{where}: `fires:` names the webhooks a FAKED write would have sent, and a "
                f"`{route.kind}` route is forwarded to the real service, which sends its own; "
                "they would never be listed"
            )
        repeated_events = sorted({e for e in route.fires if route.fires.count(e) > 1})
        if repeated_events:
            # One write does not fire one event twice, and the baseline report prints this list
            # verbatim - a repeat would promise two `refund.created` for one refund.
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
    """A `{name}` may appear once in a pattern: `path_params` returns one entry per name.

    `/a/{x}/b/{x}` would bind `x` to the last segment and drop the first silently, which is how
    `named_id` would come to echo the wrong id. Refusing the pattern is the fail-closed answer,
    and no shipped route repeats a name.
    """
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
