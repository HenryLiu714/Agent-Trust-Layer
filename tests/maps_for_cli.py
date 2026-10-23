"""Test maps the composition root accepts: a directory of test maps, completed from the shipped
maps with what `services.check` requires of every map set irimi starts with."""

from pathlib import Path

import yaml

import irimi
from irimi import services

SHIPPED = Path(irimi.__file__).parent / "maps"


def complete_for_check(maps_dir: Path) -> None:
    """Add to `maps_dir` each service irimi's Python keys on that no test map defines (the
    shipped file), and to each test map the shipped routes of the operations it leaves out.

    The added routes are ones the test's agent never calls, so they change no answer it gets.
    """
    shipped: dict[str, tuple[Path, dict]] = {}
    for path in sorted(SHIPPED.glob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text()):
            if doc is not None:
                shipped[doc["service"]] = (path, doc)
    defined: dict[str, tuple[Path, dict]] = {}
    for path in sorted(maps_dir.glob("*.yaml")):
        docs = [d for d in yaml.safe_load_all(path.read_text()) if d is not None]
        if len(docs) == 1:
            defined[docs[0]["service"]] = (path, docs[0])
    for service, operations in services.OPERATIONS.items():
        source, shipped_doc = shipped[service]
        if service not in defined:
            (maps_dir / f"shipped-{source.name}").write_text(yaml.safe_dump(shipped_doc))
            continue
        path, doc = defined[service]
        named = {route["operation"] for route in doc["routes"]}
        missing = [r for r in shipped_doc["routes"] if r["operation"] in operations - named]
        if missing:
            doc["routes"].extend(missing)
            path.write_text(yaml.safe_dump(doc, sort_keys=False))
