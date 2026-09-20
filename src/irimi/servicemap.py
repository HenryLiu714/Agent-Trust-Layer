"""Service maps: the YAML that says what a route is and where its answer comes from.

A map names a service, the hosts it uses, and its routes. Each route carries an operation name, a
kind (`read` / `write` / `llm` / `telemetry` / `unknown`), a human template for the summary, the id
prefixes the faker mints, the fields duplicate detection must ignore, and its answer target.

A host is a bare name, or a **wildcard pattern**: one leading `*.` label followed by at least two
more labels (`*.ingest.sentry.io`). A pattern matches one or more leading labels, so
`*.posthog.com` matches `eu.posthog.com` and `eu.i.posthog.com` but never the bare `posthog.com`.
An exact host always beats a pattern, and among patterns the longest suffix wins. A pattern
classifies traffic through the forward door; it is deliberately **not** a reverse-door allow-list
entry, because that door takes one literal host per request (see `MapIndex.hosts`).

The **answer target** (design D20) is `self` by default, meaning irimi answers the route itself
with the local fake. A service or a route may name an `http(s)` URL instead, and `target_reads:
true` on a service sends that service's reads to its target as well. Issue #16 does the
forwarding; this module only loads, validates and reports.

Shipped maps live in `irimi/maps/*.yaml` and never set a target. A user's overrides file
(`./irimi.maps.yaml`, else `$IRIMI_HOME/maps.yaml`) may set targets on top of them and nothing
else: an override able to change a route's `kind` would be a way to turn a write into a read.
"""

from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

from irimi import paths
from irimi.exchange import KINDS, LIVE_KINDS, SAFE_METHODS, Kind
from irimi.pipeline import is_loopback

SCHEMA_VERSION = 1
SELF_TARGET = "self"
ANY_METHOD = "*"

MAPS_DIR_NAME = "maps"
CWD_OVERRIDE_NAME = "irimi.maps.yaml"  # in the working directory, checked first
HOME_OVERRIDE_NAME = "maps.yaml"  # in $IRIMI_HOME, the fallback

VERB_STYLES = ("honest", "post-only")  # does the HTTP method carry information for this service?

# Routes where the URL *is* the credential: a Slack incoming webhook URL is the whole secret, so
# sending one to a host off this machine hands it to whoever is listening. These may be targeted
# at loopback and never through `--allow-target-host` (design §7, threat 8; issue #16). Keyed by
# service and operation, not by host, because `hooks.slack.com` and `slack.com` are one service
# and both resolve to this route.
WEBHOOK_ROUTES: frozenset[tuple[str, str]] = frozenset({("slack", "incoming_webhook")})
TARGETABLE_KINDS: frozenset[str] = frozenset({"write", "unknown"})
# What `default_kind` may say: every kind that is *not* forwarded live. Derived, not listed, so a
# live-forwarding kind added to LIVE_KINDS later is refused as a default the day it is added (#30).
DEFAULT_KINDS: tuple[Kind, ...] = tuple(k for k in KINDS if k not in LIVE_KINDS)

SERVICE_KEYS = frozenset(
    {"version", "service", "hosts", "verbs", "default_kind", "target", "target_reads", "routes"}
)
ROUTE_KEYS = frozenset(
    {
        "match",
        "operation",
        "kind",
        "human",
        "ids",
        "volatile",
        "persists",
        "comment",
        "target",
        "forward_auth",
    }
)
MATCH_KEYS = frozenset({"method", "path"})
OVERRIDE_SERVICE_KEYS = frozenset({"service", "target", "target_reads", "routes"})
OVERRIDE_ROUTE_KEYS = frozenset({"match", "target", "forward_auth"})


class MapError(ValueError):
    """A map or overrides file the loader refuses. str(exc) names the source and the rule."""


@dataclass(frozen=True)
class Route:
    """One route rule. `path` is a pattern: a `{name}` segment matches exactly one path segment."""

    method: str  # upper-case, or "*" for any method
    path: str
    operation: str
    kind: Kind
    human: str = ""
    ids: dict[str, str] = field(default_factory=dict)  # response field -> minted id prefix
    volatile: tuple[str, ...] = ()
    persists: bool | None = None
    comment: str = ""
    target: str = SELF_TARGET
    forward_auth: bool = False


@dataclass(frozen=True)
class ServiceMap:
    """One service's hosts and routes, as loaded from one YAML document."""

    service: str
    hosts: frozenset[str]
    routes: tuple[Route, ...]
    verbs: str = "honest"
    default_kind: Kind | None = None  # the kind for routes this map does not list; None = fallback
    target: str = SELF_TARGET
    target_reads: bool = False
    source: str = ""  # where it came from, for error messages


@dataclass
class MapIndex:
    """Every loaded service, with a host lookup. `MapIndex()` is the empty index.

    Two lookups, because a map may claim a host either exactly or by wildcard pattern. Duplicate
    detection does not distinguish them: `*.posthog.com` in two maps is the same "already mapped"
    refusal as `api.stripe.com` in two maps, while `*.posthog.com` in one map and `eu.posthog.com`
    in another is allowed, because the exact host wins and the result is unambiguous.
    """

    services: tuple[ServiceMap, ...] = ()
    by_host: dict[str, ServiceMap] = field(init=False, default_factory=dict)
    by_suffix: dict[str, ServiceMap] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.by_host = {h: sm for sm in self.services for h in sm.hosts if not _is_pattern(h)}
        # `*.posthog.com` is keyed by `.posthog.com`, so a match is a plain `str.endswith` and the
        # leading dot is what stops it from matching the bare `posthog.com`.
        self.by_suffix = {h[1:]: sm for sm in self.services for h in sm.hosts if _is_pattern(h)}

    @property
    def hosts(self) -> frozenset[str]:
        """Every exact host in every loaded map. This is the reverse door's allow-list.

        Wildcard patterns are deliberately left out. The reverse door relays one literal host per
        request and `pipeline.rewrite_reverse` compares it with `host not in allowed_hosts`; an
        allow-list holding `*.datadoghq.com` would either never match or have to grow a matcher
        that quietly widens what the door relays. Reach a wildcard host through the door with an
        explicit `--allow-host <host>`.
        """
        return frozenset(self.by_host)

    @property
    def patterns(self) -> tuple[str, ...]:
        """Every wildcard host spelling (`*.…`), sorted. For `irimi maps list`."""
        return tuple(sorted("*" + suffix for suffix in self.by_suffix))

    def service_for(self, host: str) -> ServiceMap | None:
        """The service claiming `host`: exact match first, then the longest matching pattern."""
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
        """The service and route for a request, or None when the host or the route is unmapped."""
        sm = self.service_for(host)
        if sm is None:
            return None
        route = match_route(sm, method, path)
        return None if route is None else (sm, route)


def match_route(sm: ServiceMap, method: str, path: str) -> Route | None:
    """The route matching `method` and `path`, or None.

    A literal path beats a `{name}` pattern of the same shape, so `/v1/charges/search` wins over
    `/v1/charges/{charge}`; among equally specific routes the first in the file wins.
    """
    method = method.upper()
    segments = _segments(path)
    best: Route | None = None
    best_holes = 0
    for route in sm.routes:
        if route.method != ANY_METHOD and route.method != method:
            continue
        holes = _match_path(route.path, segments)
        if holes is None:
            continue
        if best is None or holes < best_holes:
            best, best_holes = route, holes
    return best


def path_params(pattern: str, path: str) -> dict[str, str]:
    """The `{name}` segments of a route pattern bound to this request path's own segments.

    `/v1/customers/{customer}` against `/v1/customers/cus_REAL123` gives
    `{"customer": "cus_REAL123"}`. Empty when the pattern has no holes, or when it does not match
    the path at all. It lives in this module, not in the one that uses it, because the `{name}`
    pattern language belongs to the map schema: a second parser anywhere else would drift from
    `_match_path` and bind the captures to the wrong segments.

    "Does not match" is `_match_path`'s own answer, not a second opinion. Counting segments is
    not matching: `/v1/customers/{customer}` and `/v9/charges/cus_X` have three segments each and
    share no literal, and binding `customer` there is the drift this function exists to prevent.
    """
    parts = _segments(pattern)
    segments = _segments(path)
    if _match_path(pattern, segments) is None:
        return {}
    return {
        part[1:-1]: segment
        for part, segment in zip(parts, segments, strict=True)
        if part.startswith("{") and part.endswith("}")
    }


def target_for(sm: ServiceMap, route: Route) -> str:
    """The target that answers this route: `self`, or the URL that answers it instead.

    A route's own target wins. Otherwise the service target applies to `write` and `unknown`
    routes, and to `read` routes only when the service sets `target_reads: true`. `llm` and
    `telemetry` are always forwarded live and are never targeted. A route cannot opt back out of
    a service target in Phase 1.
    """
    if route.target != SELF_TARGET:
        return route.target
    if route.kind in TARGETABLE_KINDS:
        return sm.target
    if route.kind == "read" and sm.target_reads:
        return sm.target
    return SELF_TARGET


def is_delegated(sm: ServiceMap) -> bool:
    """True when this service has a target at all, so the summary and banner must say so (#20)."""
    return sm.target != SELF_TARGET or any(r.target != SELF_TARGET for r in sm.routes)


# --------------------------------------------------------------------------------------- loading


def shipped_dir() -> Path:
    """The directory holding the maps that ship inside the package."""
    from importlib.resources import files

    return Path(str(files("irimi").joinpath(MAPS_DIR_NAME)))


def override_path(cwd: Path | None = None) -> Path | None:
    """`./irimi.maps.yaml` if it exists, else `$IRIMI_HOME/maps.yaml`, else None."""
    local = (cwd if cwd is not None else Path.cwd()) / CWD_OVERRIDE_NAME
    if local.is_file():
        return local
    home = paths.irimi_home() / HOME_OVERRIDE_NAME
    return home if home.is_file() else None


def load(
    allow_target_hosts: frozenset[str] = frozenset(),
    cwd: Path | None = None,
    maps_dir: Path | None = None,
    targets: Sequence[tuple[str, str, str]] = (),
    target_reads: Sequence[str] = (),
) -> MapIndex:
    """Load the shipped maps, merge what the user says over them, and validate the result.

    Three layers, each beating the one before it: the shipped maps, the overrides file, then the
    `--target` / `--target-reads` flags. Validation runs last, over the merged result, so a
    non-loopback target is refused however it arrived. Raises MapError on anything wrong, naming
    the source and the rule. `allow_target_hosts` is the set of non-loopback hosts a target may
    name (`--allow-target-host`).
    """
    maps = load_shipped(maps_dir)
    override = override_path(cwd)
    if override is not None:
        maps = apply_overrides(maps, override)
    if targets or target_reads:
        maps = apply_cli_targets(maps, targets, target_reads)
    for sm in maps:
        _validate_targets(sm, allow_target_hosts)
    return MapIndex(tuple(maps))


def load_shipped(maps_dir: Path | None = None) -> list[ServiceMap]:
    """Parse every `*.yaml` in the maps directory, in file-name order.

    A directory that cannot be scanned, or that holds no maps at all, is a MapError like every
    other refusal here. It is not an empty index: an empty index silently empties the reverse
    door's allow-list and sends every route to the RFC fallback, so a damaged install would look
    like a working one that classifies nothing.
    """
    directory = maps_dir if maps_dir is not None else shipped_dir()
    try:
        entries = sorted(p for p in directory.iterdir() if p.suffix == ".yaml")
    except OSError as exc:
        raise MapError(f"{directory}: cannot read the service maps directory: {exc}") from None
    maps: list[ServiceMap] = []
    for path in entries:
        for doc in _read_documents(path):
            maps.append(parse_service(doc, str(path)))
    if not maps:
        raise MapError(f"{directory}: no service maps found (expected at least one *.yaml file)")
    _check_unique(maps)
    return maps


def parse_service(doc: Any, source: str) -> ServiceMap:
    """Validate one YAML document and build a ServiceMap. Raises MapError."""
    if not isinstance(doc, dict):
        raise MapError(f"{source}: a map document must be a mapping, got {type(doc).__name__}")
    _reject_unknown_keys(doc, SERVICE_KEYS, source, "map")
    version = doc.get("version")
    if version != SCHEMA_VERSION:
        raise MapError(f"{source}: `version` must be {SCHEMA_VERSION}, got {version!r}")
    service = _require_name(doc.get("service"), f"{source}: `service`")
    verbs = doc.get("verbs", "honest")
    if verbs not in VERB_STYLES:
        raise MapError(f"{source}: `verbs` must be one of {list(VERB_STYLES)}, got {verbs!r}")
    hosts = _parse_hosts(doc.get("hosts"), source)
    raw_routes = doc.get("routes")
    if not isinstance(raw_routes, list) or not raw_routes:
        raise MapError(f"{source}: `routes` must be a non-empty list")
    sm = ServiceMap(
        service=service,
        hosts=hosts,
        routes=tuple(_parse_route(r, source) for r in raw_routes),
        verbs=verbs,
        default_kind=_parse_default_kind(doc.get("default_kind"), f"{source}: `default_kind`"),
        target=_parse_target(doc.get("target", SELF_TARGET), f"{source}: `target`"),
        target_reads=_parse_bool(doc.get("target_reads", False), f"{source}: `target_reads`"),
        source=source,
    )
    _check_route_rules(sm)
    return sm


def apply_overrides(maps: list[ServiceMap], path: Path) -> list[ServiceMap]:
    """Merge the user's overrides file over the shipped maps, by service name and exact route.

    An override may set `target`, `target_reads` and `forward_auth` and nothing else. Naming a
    service or a route that does not exist is an error, not a silent no-op.
    """
    source = str(path)
    by_name = {sm.service: sm for sm in maps}
    for doc in _read_documents(path):
        if not isinstance(doc, dict):
            raise MapError(f"{source}: an override document must be a mapping")
        _reject_unknown_keys(doc, OVERRIDE_SERVICE_KEYS, source, "override")
        name = _require_name(doc.get("service"), f"{source}: `service`")
        base = by_name.get(name)
        if base is None:
            raise MapError(
                f"{source}: no shipped map for service {name!r} "
                f"(known: {', '.join(sorted(by_name))})"
            )
        routes = list(base.routes)
        for raw in doc.get("routes") or []:
            if not isinstance(raw, dict):
                raise MapError(f"{source}: each entry of `routes` must be a mapping")
            _reject_unknown_keys(raw, OVERRIDE_ROUTE_KEYS, source, "override route")
            method, route_path = _parse_match(raw.get("match"), source)
            index = next(
                (i for i, r in enumerate(routes) if r.method == method and r.path == route_path),
                None,
            )
            if index is None:
                raise MapError(
                    f"{source}: override names no route in the {name!r} map: {method} {route_path}"
                )
            routes[index] = replace(
                routes[index],
                target=_parse_target(
                    raw.get("target", routes[index].target), f"{source}: `target`"
                ),
                forward_auth=_parse_bool(
                    raw.get("forward_auth", routes[index].forward_auth),
                    f"{source}: `forward_auth`",
                ),
            )
        by_name[name] = replace(
            base,
            routes=tuple(routes),
            target=_parse_target(doc.get("target", base.target), f"{source}: `target`"),
            target_reads=_parse_bool(
                doc.get("target_reads", base.target_reads), f"{source}: `target_reads`"
            ),
            source=f"{base.source} + {source}",
        )
    return [by_name[sm.service] for sm in maps]


def apply_cli_targets(
    maps: list[ServiceMap],
    targets: Sequence[tuple[str, str, str]],
    target_reads: Sequence[str],
) -> list[ServiceMap]:
    """Apply `--target '<host>[<path>]=<url>'` and `--target-reads <host>` over the loaded maps.

    A host with no path sets the **service** target. A host with a path sets it on every
    targetable route of that service the path names - matched against the route pattern literally
    (`/v1/customers/{customer}`) or as a concrete request path (`/v1/customers/cus_1`), so the
    caller does not have to know how the map spells it. `read`, `llm` and `telemetry` routes are
    skipped, because a route target is only valid on `write` and `unknown`; delegate reads with
    `--target-reads`.

    Naming a host or a path no loaded map claims is a MapError, not a silent no-op: a typo that
    quietly changed nothing would look exactly like a working delegation.
    """
    index = MapIndex(tuple(maps))  # built once, before by_name starts changing underneath it
    by_name = {sm.service: sm for sm in maps}
    for host, route_path, url in targets:
        claimed = index.service_for(host)
        if claimed is None:
            raise MapError(f"--target: no loaded service map claims host {host!r}")
        base = by_name[claimed.service]
        target = _parse_target(url, f"--target {host}{route_path}")
        if not route_path:
            by_name[base.service] = replace(base, target=target, source=_plus(base, "--target"))
            continue
        routes = list(base.routes)
        hits = [
            i
            for i, route in enumerate(routes)
            if route.kind in TARGETABLE_KINDS and _names_route(route, route_path)
        ]
        if not hits:
            raise MapError(
                f"--target: no `write` or `unknown` route of service {base.service!r} matches "
                f"{route_path!r}"
            )
        for i in hits:
            routes[i] = replace(routes[i], target=target)
        by_name[base.service] = replace(base, routes=tuple(routes), source=_plus(base, "--target"))
    for host in target_reads:
        claimed = index.service_for(host)
        if claimed is None:
            raise MapError(f"--target-reads: no loaded service map claims host {host!r}")
        base = by_name[claimed.service]
        by_name[base.service] = replace(
            base, target_reads=True, source=_plus(base, "--target-reads")
        )
    return [by_name[sm.service] for sm in maps]


def _names_route(route: Route, spec: str) -> bool:
    """True when `spec` names `route`: its pattern verbatim, or a path that pattern matches."""
    return route.path == spec or _match_path(route.path, _segments(spec)) is not None


def _plus(sm: ServiceMap, what: str) -> str:
    return sm.source if sm.source.endswith(what) else f"{sm.source} + {what}"


# ------------------------------------------------------------------------------------ validation


def _validate_targets(sm: ServiceMap, allow_target_hosts: frozenset[str]) -> None:
    """The target rules that can only be checked once overrides are merged in."""
    if sm.target_reads and sm.target == SELF_TARGET:
        raise MapError(
            f"{sm.source}: `target_reads: true` needs a `target:` URL — a service irimi answers "
            "end to end would be a twin, not a shadow"
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
        if (sm.service, route.operation) in WEBHOOK_ROUTES:
            _require_loopback_target(target_for(sm, route), f"{where} ({route.operation})")


def _check_target_host(target: str, allow_target_hosts: frozenset[str], where: str) -> None:
    """Targets must be loopback until the sandbox exists; `--allow-target-host` is the way out."""
    if target == SELF_TARGET:
        return
    host = urlsplit(target).hostname or ""
    if host == "localhost" or is_loopback(host):
        return
    if host in allow_target_hosts:
        return
    raise MapError(
        f"{where}: target host {host!r} is not loopback. Phase 1 forwards to 127.0.0.1, ::1 or "
        "localhost only; pass --allow-target-host to override it deliberately"
    )


def _require_loopback_target(target: str, where: str) -> None:
    """A webhook route's target must be loopback, with no escape hatch (see WEBHOOK_ROUTES)."""
    if target == SELF_TARGET:
        return
    host = urlsplit(target).hostname or ""
    if host == "localhost" or is_loopback(host):
        return
    raise MapError(
        f"{where}: a webhook route may only be targeted at loopback. Its URL is the credential, "
        "so --allow-target-host does not apply to it"
    )


def _check_route_rules(sm: ServiceMap) -> None:
    """Per-route rules that need the service's own fields (`verbs`) to judge."""
    seen: set[tuple[str, str]] = set()
    for route in sm.routes:
        where = f"{sm.source}: route {route.method} {route.path}"
        _check_unique_holes(route.path, where)
        key = (route.method, route.path)
        if key in seen:
            raise MapError(f"{where}: appears twice in the same map")
        seen.add(key)
        if route.persists is not None and route.kind != "read":
            raise MapError(f"{where}: `persists` belongs on a `kind: read` route only")
        if route.volatile and route.kind not in TARGETABLE_KINDS:
            raise MapError(f"{where}: `volatile` belongs on a write, where duplicates are checked")
        if (
            sm.verbs == "honest"
            and route.kind == "read"
            and route.method not in SAFE_METHODS
            and not (route.persists is False and route.comment.strip())
        ):
            raise MapError(
                f"{where}: `kind: read` on an unsafe method downgrades a write. Say "
                "`persists: false` and add a `comment:` explaining why it persists nothing"
            )


def _check_unique_holes(pattern: str, where: str) -> None:
    """A `{name}` may appear once in a pattern: `path_params` returns one entry per name.

    `/a/{x}/b/{x}` would bind `x` to the last segment and drop the first silently, which is how
    `named_id` would come to echo the wrong id. Refusing the pattern is the fail-closed answer,
    and no shipped route repeats a name.
    """
    holes = [p[1:-1] for p in _segments(pattern) if p.startswith("{") and p.endswith("}")]
    repeated = sorted({name for name in holes if holes.count(name) > 1})
    if repeated:
        raise MapError(
            f"{where}: `{{{repeated[0]}}}` appears more than once in the path. A parameter name "
            "binds one segment, so a repeat would silently drop every capture but the last"
        )


def _check_unique(maps: list[ServiceMap]) -> None:
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


# ----------------------------------------------------------------------------------- YAML pieces


def _read_documents(path: Path) -> list[Any]:
    """Every non-empty YAML document in a file. A map file has one; an overrides file may have N."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise MapError(f"{path}: cannot read: {exc}") from None
    try:
        docs = [d for d in yaml.safe_load_all(text) if d is not None]
    except yaml.YAMLError as exc:
        raise MapError(f"{path}: not valid YAML: {exc}") from None
    if not docs:
        raise MapError(f"{path}: file is empty")
    return docs


def _parse_route(raw: Any, source: str) -> Route:
    if not isinstance(raw, dict):
        raise MapError(f"{source}: each entry of `routes` must be a mapping")
    _reject_unknown_keys(raw, ROUTE_KEYS, source, "route")
    method, path = _parse_match(raw.get("match"), source)
    where = f"{source}: route {method} {path}"
    kind = raw.get("kind")
    if kind not in KINDS:
        raise MapError(f"{where}: `kind` must be one of {list(KINDS)}, got {kind!r}")
    return Route(
        method=method,
        path=path,
        operation=_require_name(raw.get("operation"), f"{where}: `operation`"),
        kind=kind,
        human=_parse_str(raw.get("human", ""), f"{where}: `human`"),
        ids=_parse_ids(raw.get("ids", {}), where),
        volatile=_parse_str_list(raw.get("volatile", []), f"{where}: `volatile`"),
        persists=_parse_optional_bool(raw.get("persists"), f"{where}: `persists`"),
        comment=_parse_str(raw.get("comment", ""), f"{where}: `comment`"),
        target=_parse_target(raw.get("target", SELF_TARGET), f"{where}: `target`"),
        forward_auth=_parse_bool(raw.get("forward_auth", False), f"{where}: `forward_auth`"),
    )


def _parse_match(raw: Any, source: str) -> tuple[str, str]:
    if not isinstance(raw, dict):
        raise MapError(f"{source}: every route needs a `match:` mapping with `method` and `path`")
    _reject_unknown_keys(raw, MATCH_KEYS, source, "match")
    method = _parse_str(raw.get("method", ANY_METHOD), f"{source}: `match.method`").upper()
    if method != ANY_METHOD and not method.isalpha():
        raise MapError(f"{source}: `match.method` must be an HTTP method or '*', got {method!r}")
    path = _parse_str(raw.get("path", ""), f"{source}: `match.path`")
    if not path.startswith("/"):
        raise MapError(f"{source}: `match.path` must start with '/', got {path!r}")
    return method, path


def _is_pattern(host: str) -> bool:
    """True for a wildcard host spelling. Only `_parse_hosts` decides whether one is well formed."""
    return host.startswith("*.")


def _parse_hosts(raw: Any, source: str) -> frozenset[str]:
    if not isinstance(raw, list) or not raw:
        raise MapError(f"{source}: `hosts` must be a non-empty list of bare host names")
    hosts: set[str] = set()
    for item in raw:
        host = _parse_str(item, f"{source}: `hosts`").strip().lower()
        # The wildcard rule runs first, so every bad `*` spelling gets the wildcard message
        # rather than the generic one (`*.` would otherwise trip the trailing-dot check).
        if "*" in host:
            _check_pattern(host, item, source)
        if not host or "/" in host or ":" in host or host != host.strip("."):
            raise MapError(
                f"{source}: host {item!r} must be a bare name with no scheme, port or path"
            )
        if host in hosts:
            raise MapError(f"{source}: host {host!r} is listed twice")
        hosts.add(host)
    return frozenset(hosts)


def _check_pattern(host: str, item: Any, source: str) -> None:
    """A wildcard host is exactly one leading `*.` label plus two or more labels of its own.

    The rule is label counting, not public suffixes: `*.com` is refused because one label after
    the star is almost always a mistake, while `*.co.uk` and `*.github.io` satisfy it and would
    each claim a whole public suffix. No shipped map uses one. `*foo.com`, `foo.*.com` and
    `**.foo.com` are refused as the silent-never-matches shape issue #9 was opened about.
    """
    rest = host[2:] if _is_pattern(host) else ""
    if not _is_pattern(host) or "*" in rest or "." not in rest or "" in rest.split("."):
        raise MapError(
            f"{source}: host {item!r} may use a wildcard only as a leading '*.' label in front of "
            "two or more labels, e.g. *.ingest.sentry.io"
        )


def _parse_ids(raw: Any, where: str) -> dict[str, str]:
    if not isinstance(raw, dict):
        raise MapError(f"{where}: `ids` must be a mapping of field name to id prefix")
    out: dict[str, str] = {}
    for key, value in raw.items():
        name = _parse_str(key, f"{where}: `ids` key")
        prefix = _parse_str(value, f"{where}: `ids.{name}`")
        if not prefix:
            raise MapError(f"{where}: `ids.{name}` must be a non-empty id prefix")
        out[name] = prefix
    return out


# Why each live-forwarding kind is refused as a service-wide default. The *rule* is LIVE_KINDS;
# these only say why, because a generic message would not tell a map author what to write instead.
DEFAULT_KIND_REASONS: dict[str, str] = {
    "read": (
        "that forwards every route this map does not list to the real service. List the read "
        "routes instead"
    ),
    "llm": (
        "`llm` is a route-level kind. On an LLM host only the inference routes are `llm`; "
        "everything else is a real write"
    ),
    "telemetry": (
        "most telemetry vendors serve their REST control plane from the same host as their "
        "intake, so this forwards `DELETE /api/v1/dashboard/{id}` to the real service. List the "
        "intake routes instead"
    ),
}


def _parse_default_kind(raw: Any, where: str) -> Kind | None:
    """The service-wide kind for routes the map does not list, or None to use the RFC fallback.

    A default may not name a kind shadow mode forwards live (`exchange.LIVE_KINDS`): that sends
    every route this map does not list to the real service, which is the bypass the maps exist to
    close. `DEFAULT_KINDS` is derived from that same set rather than listed by hand, so the rule
    covers a live kind added later; DEFAULT_KIND_REASONS only explains each one.
    """
    if raw is None:
        return None
    kind = _parse_str(raw, where)
    if kind in LIVE_KINDS:
        reason = DEFAULT_KIND_REASONS.get(kind, "irimi forwards that kind to the real service")
        raise MapError(f"{where} may not be `{kind}`: {reason}")
    if kind not in DEFAULT_KINDS:
        raise MapError(f"{where} must be one of {list(DEFAULT_KINDS)}, got {kind!r}")
    return kind


def _parse_target(raw: Any, where: str) -> str:
    """`self`, or an http(s) origin with an optional path. Query, fragment and userinfo are out.

    Every target reaches this function - shipped map, overrides file and `--target` alike - so it
    is the one place a malformed URL may turn into a MapError. Both `urlsplit` and its `.port`
    accessor raise ValueError on input a user can type (`http://[::1/x`, `http://h:99999`), and a
    ValueError escaping here is an uncaught traceback out of `irimi serve` instead of the one-line
    refusal every other bad target gets.
    """
    target = _parse_str(raw, where).strip()
    if target == SELF_TARGET:
        return target
    try:
        parts = urlsplit(target)
        port = parts.port
    except ValueError as exc:
        raise MapError(f"{where}: {target!r} is not a URL irimi can parse ({exc})") from None
    if port is not None and not 1 <= port <= 65535:
        raise MapError(f"{where}: {target!r} has a bad port")
    if parts.scheme not in ("http", "https"):
        raise MapError(
            f"{where}: must be {SELF_TARGET!r} or an http(s) URL, got {target!r} "
            "(e.g. http://127.0.0.1:3000/refund)"
        )
    if not parts.hostname:
        raise MapError(f"{where}: {target!r} has no host")
    if parts.query or parts.fragment or parts.username or parts.password:
        raise MapError(
            f"{where}: {target!r} must be a scheme, host, port and path only — no query, "
            "fragment or credentials"
        )
    return target.rstrip("/") if parts.path in ("", "/") else target


def _require_name(raw: Any, where: str) -> str:
    name = _parse_str(raw, where).strip()
    if not name:
        raise MapError(f"{where} is required and must be a non-empty string")
    return name


def _parse_str(raw: Any, where: str) -> str:
    if not isinstance(raw, str):
        raise MapError(f"{where} must be a string, got {type(raw).__name__}")
    return raw


def _parse_str_list(raw: Any, where: str) -> tuple[str, ...]:
    if not isinstance(raw, list):
        raise MapError(f"{where} must be a list of strings")
    return tuple(_require_name(item, f"{where} entry") for item in raw)


def _parse_bool(raw: Any, where: str) -> bool:
    if not isinstance(raw, bool):
        raise MapError(f"{where} must be true or false, got {raw!r}")
    return raw


def _parse_optional_bool(raw: Any, where: str) -> bool | None:
    return None if raw is None else _parse_bool(raw, where)


def _reject_unknown_keys(
    doc: dict[Any, Any], allowed: frozenset[str], source: str, what: str
) -> None:
    unknown = sorted(str(k) for k in doc if k not in allowed)
    if unknown:
        raise MapError(
            f"{source}: unknown {what} key(s) {', '.join(unknown)} "
            f"(allowed: {', '.join(sorted(allowed))})"
        )


def _segments(path: str) -> tuple[str, ...]:
    return tuple(s for s in path.split("/") if s)


def _match_path(pattern: str, segments: tuple[str, ...]) -> int | None:
    """None when the pattern does not match; otherwise how many `{name}` segments it used."""
    parts = _segments(pattern)
    if len(parts) != len(segments):
        return None
    holes = 0
    for part, segment in zip(parts, segments, strict=True):
        if part.startswith("{") and part.endswith("}"):
            holes += 1
        elif part != segment:
            return None
    return holes
