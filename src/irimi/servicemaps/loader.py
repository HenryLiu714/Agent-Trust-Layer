"""YAML in, `MapIndex` out: the shipped maps, the overrides file, and the `--target` flags.

Three layers, each beating the one before it, merged and then validated as one so a rule holds
whichever layer a value arrived through. See `rules` for the rules and `model` for what comes out.
"""

import re
from collections.abc import Sequence
from dataclasses import replace
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

from irimi.config import paths
from irimi.core.exchange import ANY_METHOD, KINDS, LIVE_KINDS, Kind
from irimi.core.netaddr import Target, default_port
from irimi.servicemaps.model import (
    CREDENTIAL_PATH_HOSTS,
    DEFAULT_KINDS,
    SELF_TARGET,
    TARGETABLE_KINDS,
    IdSpec,
    MapError,
    MapIndex,
    Route,
    ServiceMap,
    Style,
    is_pattern,
    match_path,
    path_segments,
)
from irimi.servicemaps.rules import check_route_rules, check_unique, validate_targets

MAP_SCHEMA_VERSION = 1

MAPS_DIR_NAME = "maps"
CWD_OVERRIDE_NAME = "irimi.maps.yaml"  # in the working directory, checked first
HOME_OVERRIDE_NAME = "maps.yaml"  # in $IRIMI_HOME, the fallback

STYLES: tuple[Style, ...] = ("rest", "rpc")
ID_FORMATS = ("int", "uuid")
# The one format a `human:` hole may name: `{amount:money}` prints minor units as money.
HUMAN_FORMATS = frozenset({"money"})
_HUMAN_HOLE = re.compile(r"\{[A-Za-z_][A-Za-z0-9_]*(?::([^}]*))?\}")

SERVICE_KEYS = frozenset(
    {
        "version",
        "service",
        "hosts",
        "style",
        "default_kind",
        "target",
        "target_reads",
        "credential_path_hosts",
        "routes",
    }
)
ROUTE_KEYS = frozenset(
    {
        "match",
        "operation",
        "kind",
        "human",
        "ids",
        "fixture",
        "fires",
        "precondition",
        "compare_ignores",
        "live_because",
        "target",
        "forward_auth",
    }
)
MATCH_KEYS = frozenset({"method", "path"})
OVERRIDE_SERVICE_KEYS = frozenset({"service", "target", "target_reads", "routes"})
OVERRIDE_ROUTE_KEYS = frozenset({"match", "target", "forward_auth"})
# Retired keys, and what to write instead.
RETIRED_SERVICE_KEYS = {"verbs": "is now `style:` (`honest` is `rest`, `post-only` is `rpc`)"}
RETIRED_ROUTE_KEYS = {
    "volatile": "is now `compare_ignores:`",
    "persists": 'is now `live_because: "<why this persists nothing>"`, on a live route',
    "comment": (
        "is gone: a live route says why it may be forwarded with `live_because:`, and any other "
        "note is a YAML comment"
    ),
}


# --------------------------------------------------------------------------------------- loading


def shipped_dir() -> Path:
    """Return the directory holding the maps that ship inside the package."""
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
    """Load the shipped maps, merge the overrides file and flags over them, and validate.

    Raises MapError naming the source and the rule. `allow_target_hosts` is the non-loopback
    hosts a target may name (`--allow-remote-target`).
    """
    maps = load_shipped(maps_dir)
    override = override_path(cwd)
    if override is not None:
        maps = apply_overrides(maps, override)
    if targets or target_reads:
        maps = apply_cli_targets(maps, targets, target_reads)
    for sm in maps:
        validate_targets(sm, allow_target_hosts)
    return MapIndex(tuple(maps))


def load_shipped(maps_dir: Path | None = None) -> list[ServiceMap]:
    """Parse every `*.yaml` in the maps directory, in file-name order.

    An unreadable or empty directory is a MapError, not an empty index, so a damaged install
    cannot pass for a working one that classifies nothing.
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
    check_unique(maps)
    return maps


def parse_service(doc: Any, source: str) -> ServiceMap:
    """Validate one YAML document and build a ServiceMap. Raises MapError."""
    if not isinstance(doc, dict):
        raise MapError(f"{source}: a map document must be a mapping, got {type(doc).__name__}")
    _reject_retired_keys(doc, RETIRED_SERVICE_KEYS, source)
    _reject_unknown_keys(doc, SERVICE_KEYS, source, "map")
    version = doc.get("version")
    if version != MAP_SCHEMA_VERSION:
        raise MapError(f"{source}: `version` must be {MAP_SCHEMA_VERSION}, got {version!r}")
    service = _require_name(doc.get("service"), f"{source}: `service`")
    style = doc.get("style", "rest")
    if style not in STYLES:
        raise MapError(f"{source}: `style` must be one of {list(STYLES)}, got {style!r}")
    hosts = _parse_hosts(doc.get("hosts"), source)
    raw_routes = doc.get("routes")
    if not isinstance(raw_routes, list) or not raw_routes:
        raise MapError(f"{source}: `routes` must be a non-empty list")
    sm = ServiceMap(
        service=service,
        hosts=hosts,
        routes=tuple(_parse_route(r, source, i) for i, r in enumerate(raw_routes)),
        style=style,
        default_kind=_parse_default_kind(doc.get("default_kind"), f"{source}: `default_kind`"),
        target=_parse_target(doc.get("target", SELF_TARGET), f"{source}: `target`"),
        target_reads=_parse_bool(doc.get("target_reads", False), f"{source}: `target_reads`"),
        credential_path_hosts=_parse_credential_hosts(
            doc.get("credential_path_hosts", []), hosts, f"{source}: `credential_path_hosts`"
        ),
        source=source,
    )
    check_route_rules(sm)
    return sm


def apply_overrides(maps: list[ServiceMap], path: Path) -> list[ServiceMap]:
    """Merge the overrides file over the shipped maps, by service name and exact route.

    An override may set `target`, `target_reads` and `forward_auth` only. Naming a service or a
    route that does not exist is a MapError.
    """
    source = str(path)
    by_name = {sm.service: sm for sm in maps}
    for doc in _read_documents(path):
        if not isinstance(doc, dict):
            raise MapError(f"{source}: an override document must be a mapping")
        if "credential_path_hosts" in doc:
            raise MapError(
                f"{source}: `credential_path_hosts` belongs to the shipped map; an override may "
                "not change which paths are credentials"
            )
        _reject_unknown_keys(doc, OVERRIDE_SERVICE_KEYS, source, "override")
        name = _require_name(doc.get("service"), f"{source}: `service`")
        base = by_name.get(name)
        if base is None:
            raise MapError(
                f"{source}: no shipped map for service {name!r} "
                f"(known: {', '.join(sorted(by_name))})"
            )
        routes = list(base.routes)
        for i, raw in enumerate(doc.get("routes") or []):
            if not isinstance(raw, dict):
                raise MapError(f"{source}: each entry of `routes` must be a mapping")
            method, route_path = _parse_match(raw.get("match"), f"{source}: routes[{i}]")
            where = f"{source}: route {method} {route_path}"
            _reject_unknown_keys(raw, OVERRIDE_ROUTE_KEYS, where, "override route")
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
                target=(
                    _parse_target(raw["target"], f"{source}: `target`")
                    if "target" in raw
                    else routes[index].target
                ),
                forward_auth=_parse_bool(
                    raw.get("forward_auth", routes[index].forward_auth),
                    f"{source}: `forward_auth`",
                ),
            )
        by_name[name] = replace(
            base,
            routes=tuple(routes),
            target=(
                _parse_target(doc["target"], f"{source}: `target`")
                if "target" in doc
                else base.target
            ),
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

    A host alone sets the service target. A host with a path sets it on every `write` and
    `unknown` route the path names, as the pattern itself or a concrete path that matches it.
    A host or path no map claims is a MapError, so a typo cannot pass for a working target.
    """
    index = MapIndex(tuple(maps))  # built before by_name starts changing
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
    """Return True when `spec` names `route`: its pattern verbatim, or a path it matches."""
    return route.path == spec or match_path(route.path, path_segments(spec)) is not None


def _plus(sm: ServiceMap, what: str) -> str:
    return sm.source if sm.source.endswith(what) else f"{sm.source} + {what}"


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


def _parse_route(raw: Any, source: str, position: int) -> Route:
    if not isinstance(raw, dict):
        raise MapError(f"{source}: each entry of `routes` must be a mapping")
    method, path = _parse_match(raw.get("match"), f"{source}: routes[{position}]")
    where = f"{source}: route {method} {path}"
    _reject_retired_keys(raw, RETIRED_ROUTE_KEYS, where)
    _reject_unknown_keys(raw, ROUTE_KEYS, where, "route")
    kind = raw.get("kind")
    if kind not in KINDS:
        raise MapError(f"{where}: `kind` must be one of {list(KINDS)}, got {kind!r}")
    return Route(
        method=method,
        path=path,
        operation=_require_name(raw.get("operation"), f"{where}: `operation`"),
        kind=kind,
        human=_parse_human(raw.get("human", ""), f"{where}: `human`"),
        ids=_parse_ids(raw.get("ids", {}), where),
        fixture=_parse_str(raw.get("fixture", ""), f"{where}: `fixture`").strip(),
        fires=_parse_str_list(raw.get("fires", []), f"{where}: `fires`"),
        precondition=_parse_str(raw.get("precondition", ""), f"{where}: `precondition`").strip(),
        compare_ignores=_parse_str_list(
            raw.get("compare_ignores", []), f"{where}: `compare_ignores`"
        ),
        live_because=_parse_str(raw.get("live_because", ""), f"{where}: `live_because`").strip(),
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
    """Refuse a wildcard host that is not one leading `*.` label plus two or more labels.

    The rule counts labels, not public suffixes, so `*.co.uk` passes. `*foo.com`, `foo.*.com`
    and `**.foo.com` would load and never match.
    """
    rest = host[2:] if is_pattern(host) else ""
    if not is_pattern(host) or "*" in rest or "." not in rest or "" in rest.split("."):
        raise MapError(
            f"{source}: host {item!r} may use a wildcard only as a leading '*.' label in front of "
            "two or more labels, e.g. *.ingest.sentry.io"
        )


def _parse_ids(raw: Any, where: str) -> dict[str, IdSpec]:
    """`{field: <prefix>}`, or `{field: {format: int}}` / `{field: {format: uuid}}`."""
    if not isinstance(raw, dict):
        raise MapError(f"{where}: `ids` must be a mapping of field name to how its id is minted")
    out: dict[str, IdSpec] = {}
    for key, value in raw.items():
        name = _parse_str(key, f"{where}: `ids` key")
        if isinstance(value, dict):
            _reject_unknown_keys(value, frozenset({"format"}), f"{where}: `ids.{name}`", "id")
            id_format = value.get("format")
            if id_format not in ID_FORMATS:
                raise MapError(
                    f"{where}: `ids.{name}.format` must be one of {list(ID_FORMATS)}, "
                    f"got {id_format!r}"
                )
            out[name] = IdSpec(format=id_format)
            continue
        prefix = _parse_str(value, f"{where}: `ids.{name}`")
        if not prefix:
            raise MapError(f"{where}: `ids.{name}` must be a non-empty id prefix")
        out[name] = IdSpec(prefix=prefix)
    return out


def _parse_human(raw: Any, where: str) -> str:
    """Return a summary template; a hole may name only the `money` format, `{amount:money}`."""
    template = _parse_str(raw, where)
    for spec in _HUMAN_HOLE.findall(template):
        if spec and spec not in HUMAN_FORMATS:
            raise MapError(
                f"{where}: `:{spec}` is not a format a hole may name "
                f"(known: {', '.join(sorted(HUMAN_FORMATS))})"
            )
    return template


def _parse_credential_hosts(raw: Any, hosts: frozenset[str], where: str) -> frozenset[str]:
    """Return the map's credential-path hosts; each must be one of its own exact `hosts`."""
    named = frozenset(name.lower() for name in _parse_str_list(raw, where))
    for host in sorted(named):
        if host not in hosts or is_pattern(host):
            raise MapError(f"{where}: {host!r} is not one of this map's exact `hosts`")
    return named


@lru_cache(maxsize=1)
def shipped_credential_hosts() -> frozenset[str]:
    """Return `CREDENTIAL_PATH_HOSTS` plus every shipped map's `credential_path_hosts:`.

    For code with no `MapIndex` at hand. Never raises: if the maps do not load, the built-in set.
    """
    try:
        maps = load_shipped()
    except Exception:
        return CREDENTIAL_PATH_HOSTS
    return CREDENTIAL_PATH_HOSTS.union(*(sm.credential_path_hosts for sm in maps))


# Why each live kind is refused as `default_kind`, so the map author learns what to write
# instead. The rule itself is LIVE_KINDS.
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
    """Return the kind for unlisted routes, or None for the RFC 9110 fallback.

    A live kind is refused: it would forward every unlisted route to the real service.
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


def _parse_target(raw: Any, where: str) -> Target | None:
    """Return None for `self`, or an http(s) origin with an optional path. Raises MapError.

    `urlsplit` and `.port` raise ValueError on typed input (`http://[::1/x`, `http://h:99999`),
    so it is turned into a MapError rather than a traceback.
    """
    target = _parse_str(raw, where).strip()
    if target == SELF_TARGET:
        return None
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
            f"{where}: {target!r} must be a scheme, host, port and path only, with no "
            "query, fragment or credentials"
        )
    scheme, host = parts.scheme, parts.hostname.lower()
    path = "" if parts.path in ("", "/") else parts.path
    return Target(scheme, host, port if port is not None else default_port(scheme), path)


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


def _reject_retired_keys(doc: dict[Any, Any], retired: dict[str, str], where: str) -> None:
    for key in sorted(str(k) for k in doc if k in retired):
        raise MapError(f"{where}: `{key}:` {retired[key]}")


def _reject_unknown_keys(
    doc: dict[Any, Any], allowed: frozenset[str], source: str, what: str
) -> None:
    unknown = sorted(str(k) for k in doc if k not in allowed)
    if unknown:
        raise MapError(
            f"{source}: unknown {what} key(s) {', '.join(unknown)} "
            f"(allowed: {', '.join(sorted(allowed))})"
        )
