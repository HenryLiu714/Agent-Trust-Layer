"""What `irimi maps list` prints: every host the loaded maps route, and where each is answered."""

from pathlib import Path

from irimi.servicemaps import SELF_TARGET, MapIndex


def maps_lines(index: MapIndex, override_path: Path | None) -> list[str]:
    """A header counting services, hosts, wildcard patterns and routes, then a line per exact host
    and per pattern with its service, route count and target, and the overrides file when one is
    loaded.

    A pattern is listed so a map whose hosts are all wildcards still appears, and counted apart
    from the exact hosts, since only those are relayed in base-URL mode. Route targets are listed
    under their host, because the host's column is the service's target."""
    hosts = sorted(index.hosts)
    patterns = index.patterns
    rows = [(host, index.by_host[host]) for host in hosts]
    rows += [(pattern, index.by_suffix[pattern[1:]]) for pattern in patterns]
    width = max((len(name) for name, _ in rows), default=0)
    service_width = max((len(sm.service) for _, sm in rows), default=0)
    routes = sum(len(sm.routes) for sm in index.services)
    lines = [
        f"irimi maps · {len(index.services)} service(s) · {len(hosts)} host(s) · "
        f"{len(patterns)} pattern(s) · {routes} route(s)"
    ]
    for name, sm in rows:
        reads = " + reads" if sm.target_reads else ""
        lines.append(
            f"  {name:<{width}}  {sm.service:<{service_width}}  {len(sm.routes):>3} routes  "
            f"target: {SELF_TARGET if sm.target is None else sm.target.url}{reads}"
        )
        for route in sm.routes:
            if route.target is not None:
                route_line = f"route {route.method} {route.path} -> {route.target.url}"
                lines.append(f"  {'':<{width}}  {route_line}")
    if override_path is not None:
        lines.append(f"  overrides from {override_path}")
    return lines
