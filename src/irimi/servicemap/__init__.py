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
forwarding; this package only loads, validates and reports.

Shipped maps live in `irimi/maps/*.yaml` and never set a target. A user's overrides file
(`./irimi.maps.yaml`, else `$IRIMI_HOME/maps.yaml`) may set targets on top of them and nothing
else: an override able to change a route's `kind` would be a way to turn a write into a read.

The package is three modules with one direction of dependency: `model` (the dataclasses, the
index, matching and the target precedence), `rules` (the validation the loader runs, THE SCOPE
RULE included) and `loader` (YAML in, `MapIndex` out). This module is the public surface.
"""

from irimi.servicemap.loader import (
    CWD_OVERRIDE_NAME,
    DEFAULT_KIND_REASONS,
    HOME_OVERRIDE_NAME,
    SCHEMA_VERSION,
    apply_cli_targets,
    apply_overrides,
    load,
    load_shipped,
    override_path,
    parse_service,
    shipped_dir,
)
from irimi.servicemap.model import (
    CREDENTIAL_PATH_HOSTS,
    DEFAULT_KINDS,
    DESTRUCTIVE_METHODS,
    SELF_TARGET,
    TARGETABLE_KINDS,
    MapError,
    MapIndex,
    Route,
    ServiceMap,
    is_delegated,
    match_route,
    path_params,
    target_for,
)

__all__ = [
    "CREDENTIAL_PATH_HOSTS",
    "CWD_OVERRIDE_NAME",
    "DEFAULT_KINDS",
    "DEFAULT_KIND_REASONS",
    "DESTRUCTIVE_METHODS",
    "HOME_OVERRIDE_NAME",
    "SCHEMA_VERSION",
    "SELF_TARGET",
    "TARGETABLE_KINDS",
    "MapError",
    "MapIndex",
    "Route",
    "ServiceMap",
    "apply_cli_targets",
    "apply_overrides",
    "is_delegated",
    "load",
    "load_shipped",
    "match_route",
    "override_path",
    "parse_service",
    "path_params",
    "shipped_dir",
    "target_for",
]
