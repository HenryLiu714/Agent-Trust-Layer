"""The vendored response objects the L1 faker starts from (#41).

One JSON file per service in `irimi/fixtures/<service>.json`, shipped as package data beside
`irimi/maps/*.yaml`. Each file is a `_source` line naming where the objects came from and a
`resources` mapping of object name to one example object, which is stripe-mock's own
`fixtures3.json` shape so that regenerating a subset stays a plain projection of the upstream
file. A route names the object it starts from with `fixture:` in the service map.

Nothing here raises. It is read from inside a mitmproxy hook, where an exception forwards the
flow and a forwarded write escapes shadow mode: a file that is missing, unreadable or malformed
answers "no such object" and the caller degrades to the L0 echo, which is the floor.

Layer 1 (see docs/architecture.md): it reads a file that ships with the package and imports
nothing from irimi, exactly like `servicemap`'s loader does with the maps.
"""

import copy
import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

FIXTURES_DIR_NAME = "fixtures"
SOURCE_KEY = "_source"
RESOURCES_KEY = "resources"

# A service name that may become a file name. The name comes from a map's `service:`, which is any
# non-empty string the loader accepted, and `<service>.json` would otherwise let one spelled
# `../../etc/passwd` choose the file that is read. Shipped maps are the only maps that can set
# `fixture:` today; the guard is here so that stays true of the ones added later.
_SERVICE_NAME = re.compile(r"[A-Za-z0-9_-]+")


def fixtures_dir() -> Path:
    """The directory holding the fixture files that ship inside the package."""
    from importlib.resources import files

    return Path(str(files("irimi").joinpath(FIXTURES_DIR_NAME)))


@lru_cache(maxsize=32)
def _document(service: str) -> dict[str, Any]:
    """One service's fixture file, parsed and cached. `{}` when there is no usable file.

    Cached because it is read on the answer path of every mapped write, and the file is package
    data that cannot change under a running process. `get` deep-copies the object it hands out,
    so a caller can never mutate what is cached here.
    """
    if not _SERVICE_NAME.fullmatch(service):
        return {}
    try:
        text = (fixtures_dir() / f"{service}.json").read_text(encoding="utf-8")
        document = json.loads(text)
    except Exception:  # missing, unreadable, or not JSON: the caller falls back to L0
        return {}
    return document if isinstance(document, dict) else {}


def objects(service: str) -> dict[str, Any]:
    """Every fixture object this service ships, by name. `{}` when the file is missing or bad."""
    resources = _document(service).get(RESOURCES_KEY)
    if not isinstance(resources, dict):
        return {}
    return {name: obj for name, obj in resources.items() if isinstance(obj, dict)}


def get(service: str, name: str) -> dict[str, Any] | None:
    """A private deep copy of one fixture object, or None when this install does not have it.

    A copy, because the L1 faker writes the request's own fields over the object it is given and
    the cached document has to stay the fixture for the next request.
    """
    obj = objects(service).get(name)
    return None if obj is None else copy.deepcopy(obj)


def source(service: str) -> str:
    """The `_source` line of this service's fixture file, or "" when it names none."""
    value = _document(service).get(SOURCE_KEY)
    return value if isinstance(value, str) else ""


def clear_cache() -> None:
    """Forget every parsed fixture file. For tests that write a fixtures directory of their own."""
    _document.cache_clear()
