"""Service maps: the YAML that says what a route is and where its answer comes from.

A map names a service, the hosts it uses, and its routes. Each route carries an operation name, a
kind (`read` / `write` / `llm` / `telemetry` / `unknown`), a human template for the summary, how
the faker mints its ids, the body fields a comparison of two sendings ignores, and its answer
target.

A host is a bare name, or a **wildcard pattern**: one leading `*.` label followed by at least two
more labels (`*.ingest.sentry.io`). A pattern matches one or more leading labels, so
`*.posthog.com` matches `eu.posthog.com` and `eu.i.posthog.com` but never the bare `posthog.com`.
An exact host always beats a pattern, and among patterns the longest suffix wins. A pattern
classifies traffic through the forward door; it is deliberately **not** a reverse-door allow-list
entry, because that door takes one literal host per request (see `MapIndex.hosts`).

The **answer target** is `self` by default, meaning irimi answers the route itself with the
local fake. A service or a route may name an `http(s)` URL instead, and `target_reads: true` on a
service sends that service's reads to its target as well. The engine does the forwarding; this
package only loads, validates and reports.

Maps ship with irimi in `irimi/maps/*.yaml` and never set a target. A user's overrides file
(`./irimi.maps.yaml`, else `$IRIMI_HOME/maps.yaml`) may set targets on top of them and nothing
else: an override able to change a route's `kind` would be a way to turn a write into a read.

The package is three modules with one direction of dependency: `model` (the dataclasses, the
index, matching and the target precedence), `rules` (the validation the loader runs, THE SCOPE
RULE included) and `loader` (YAML in, `MapIndex` out). This module exports what the rest of irimi
uses; tests import the submodules.
"""

from irimi.servicemaps.loader import load, override_path, shipped_credential_hosts
from irimi.servicemaps.model import (
    SELF_TARGET,
    TARGETABLE_KINDS,
    IdSpec,
    MapError,
    MapIndex,
    Route,
    ServiceMap,
    fill_path,
    is_delegated,
    path_params,
    target_for,
)

__all__ = [
    "SELF_TARGET",
    "TARGETABLE_KINDS",
    "IdSpec",
    "MapError",
    "MapIndex",
    "Route",
    "ServiceMap",
    "fill_path",
    "is_delegated",
    "load",
    "override_path",
    "path_params",
    "shipped_credential_hosts",
    "target_for",
]
