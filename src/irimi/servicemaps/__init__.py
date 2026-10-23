"""Service maps: the YAML that says what each route of a service is and who answers it.

Maps ship with irimi in `irimi/maps/*.yaml`; add one by pull request. A shipped map never sets a
target. The overrides file (`./irimi.maps.yaml`, else `$IRIMI_HOME/maps.yaml`) and `--target`
may set targets and nothing else, so no override can turn a write into a read.

Modules, in dependency order: `model` (dataclasses, host index, matching, target precedence),
`rules` (load-time validation, the scope rule included) and `loader` (YAML in, `MapIndex` out).
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
