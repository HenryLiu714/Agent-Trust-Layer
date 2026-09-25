"""The service map model: routes, services, the host index, matching and the target precedence.

Pure data and pure functions. Loading is `loader`, validation is `rules`; this module imports
neither, so the engine's hot path (`pipeline.classify` -> `MapIndex.route_for`) depends on nothing
that reads a file.
"""

from dataclasses import dataclass, field
from urllib.parse import unquote

from irimi.exchange import ANY_METHOD, KINDS, LIVE_KINDS, Kind

SELF_TARGET = "self"

# Hosts whose request path IS the credential. A Slack incoming webhook URL is the entire secret:
# `/services/{team}/{bot}/{token}`, `/workflows/...` and `/triggers/...` are all real forms, and
# sending any of them to a host off this machine hands the secret to whoever is listening. Keyed
# by HOST, not by (service, operation): `hooks.slack.com` and `slack.com` are one service, so a
# rule keyed on the service's routes leaves every unlisted path on the webhook host uncovered.
# A target on such a host must be loopback, with no `--allow-target-host` escape (design §7,
# threat 8; issue #16).
CREDENTIAL_PATH_HOSTS: frozenset[str] = frozenset({"hooks.slack.com"})
TARGETABLE_KINDS: frozenset[str] = frozenset({"write", "unknown"})
# Methods that destroy or replace a resource. Not the complement of SAFE_METHODS: POST is unsafe
# in RFC 9110's sense but is also the honest verb for inference and telemetry intake, which are
# exactly the kinds that forward live. These are the ones a live kind has to justify.
DESTRUCTIVE_METHODS: frozenset[str] = frozenset({"DELETE", "PUT", "PATCH"})
# What `default_kind` may say: every kind that is *not* forwarded live. Derived, not listed, so a
# live-forwarding kind added to LIVE_KINDS later is refused as a default the day it is added (#30).
DEFAULT_KINDS: tuple[Kind, ...] = tuple(k for k in KINDS if k not in LIVE_KINDS)


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
    # The vendored response object a locally answered write starts from (`irimi.fixture`), by
    # name. "" means the L0 echo, which is what every route had before #41 and what a route
    # whose fixture cannot be read falls back to.
    fixture: str = ""
    # The L3 check consulted before a mapped write is faked, by the name of its entry in
    # `services.PRECONDITIONS`. "" means the write is never precondition-checked (#45).
    precondition: str = ""
    # The webhook event names this write would have caused the real service to send (#47). A
    # faked write sends none of them, so the exchange carries them as `would_fire` and the
    # baseline report says what did not fire. Free text: unlike `precondition:` there is no table
    # of a service's events to check a name against, so a misspelled EVENT is not catchable here -
    # a misspelled KEY is, by `ROUTE_KEYS`. Nothing is delivered; signed delivery is design v0.3.
    fires: tuple[str, ...] = ()
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
        self.by_host = {h: sm for sm in self.services for h in sm.hosts if not is_pattern(h)}
        # `*.posthog.com` is keyed by `.posthog.com`, so a match is a plain `str.endswith` and the
        # leading dot is what stops it from matching the bare `posthog.com`.
        self.by_suffix = {h[1:]: sm for sm in self.services for h in sm.hosts if is_pattern(h)}

    @property
    def hosts(self) -> frozenset[str]:
        """Every exact host in every loaded map. This is the reverse door's allow-list.

        Wildcard patterns are deliberately left out. The reverse door relays one literal host per
        request and `reverse_door.rewrite_reverse` compares it with `host not in allowed_hosts`; an
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
    segments = path_segments(path)
    best: Route | None = None
    best_holes = 0
    for route in sm.routes:
        if route.method != ANY_METHOD and route.method != method:
            continue
        holes = match_path(route.path, segments)
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
    `match_path` and bind the captures to the wrong segments.

    "Does not match" is `match_path`'s own answer, not a second opinion. Counting segments is
    not matching: `/v1/customers/{customer}` and `/v9/charges/cus_X` have three segments each and
    share no literal, and binding `customer` there is the drift this function exists to prevent.

    A captured segment is **percent-decoded**, because the service decodes it: `cus%5FREAL123` and
    `cus_REAL123` address the same customer, and reading the raw segment made `echo.named_id`
    miss the id the request already named and mint a fresh one - #26 reinstated for any caller
    that over-encodes (#33). Literal segments are still compared raw, so decoding cannot widen
    what a route matches; it only says what the matched hole held. The two surfaces that read
    these captures - the minted id and the summary's human template - agree because they ask here.
    """
    parts = path_segments(pattern)
    segments = path_segments(path)
    if match_path(pattern, segments) is None:
        return {}
    return {
        part[1:-1]: unquote(segment)
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


def is_pattern(host: str) -> bool:
    """True for a wildcard host spelling. Only `_parse_hosts` decides whether one is well formed."""
    return host.startswith("*.")


def path_segments(path: str) -> tuple[str, ...]:
    return tuple(s for s in path.split("/") if s)


def match_path(pattern: str, segments: tuple[str, ...]) -> int | None:
    """None when the pattern does not match; otherwise how many `{name}` segments it used."""
    parts = path_segments(pattern)
    if len(parts) != len(segments):
        return None
    holes = 0
    for part, segment in zip(parts, segments, strict=True):
        if part.startswith("{") and part.endswith("}"):
            holes += 1
        elif part != segment:
            return None
    return holes
