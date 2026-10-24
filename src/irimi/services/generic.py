"""Echo and fixture bodies for faked writes, and their ids. Never raises (fail-closed hook rule)."""

import copy
import json
import re
import secrets
import string
import uuid
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from typing import Any

from irimi.core.bodies import reflect
from irimi.core.exchange import Request
from irimi.servicemaps import IdSpec, Route, path_params
from irimi.services.model import SERVICE_OWNED, AnswerClock

ID_ALPHABET = string.ascii_letters + string.digits
ID_LENGTH = 24
# Ten digits: positive, and well inside the 2**53 a JSON reader keeps exact.
INT_ID_FLOOR = 10**9
INT_ID_SPAN = 9 * 10**9

# An id after its prefix has no second `_`, so `sub_2` is told from `sub_sched_1`.
_ID_TOKEN = re.compile(r"[A-Za-z0-9]+")
# A PaymentIntent client secret `pi_<id>_secret_<token>` shares the id's prefix; never echo it.
SECRET_MARKER = "_secret"  # noqa: S105  # a marker in an id, not a credential


def mint_id(prefix: str) -> str:
    """Return `prefix` plus 24 characters from [A-Za-z0-9], drawn with secrets."""
    return prefix + "".join(secrets.choice(ID_ALPHABET) for _ in range(ID_LENGTH))


def mint(spec: IdSpec) -> str | int:
    """Return a fresh id in the route's format: prefixed token, ten-digit integer, or UUID."""
    if spec.format == "int":
        return INT_ID_FLOOR + secrets.randbelow(INT_ID_SPAN)
    if spec.format == "uuid":
        return str(uuid.uuid4())
    return mint_id(spec.prefix)


def named_id(route: Route, path: str, prefix: str) -> str | None:
    """Return the prefixed id the path already names (id-shaped first, never a secret), or None."""
    values = [v for v in path_params(route.path, path).values() if v.startswith(prefix)]
    shaped = [v for v in values if _ID_TOKEN.fullmatch(v[len(prefix) :])]
    if shaped:
        return shaped[0]
    return next((v for v in values if SECRET_MARKER not in v), None)


def _named(route: Route, path: str, name: str, spec: IdSpec) -> str | int | None:
    """Return the path's id for `name` in `spec`'s format, or None; int and UUID match by name."""
    if spec.format == "prefix":
        return named_id(route, path, spec.prefix)
    value = path_params(route.path, path).get(name)
    if value is None:
        return None
    if spec.format == "int":
        return int(value) if value.isascii() and value.isdigit() else None
    try:
        return str(uuid.UUID(value))
    except ValueError:
        return None


def mint_ids(body: dict[str, Any], route: Route, path: str) -> None:
    """Set each `ids:` field in place, over any posted value: the path's id, else a minted one."""
    for name, spec in route.ids.items():
        named = _named(route, path, name, spec)
        body[name] = named if named is not None else mint(spec)


def l0_body(request: Request, route: Route | None) -> dict[str, Any]:
    """Return the echo: the request's own fields, plus the route's ids."""
    body = reflect(request)
    if route is not None:
        mint_ids(body, route, request.path)
    return body


def _same_json_type(current: Any, value: Any) -> bool:
    """Return True when `value` has `current`'s JSON type; ints and floats are one, bools apart."""
    if isinstance(current, bool) or isinstance(value, bool):
        return isinstance(current, bool) and isinstance(value, bool)
    if isinstance(current, int | float):
        return isinstance(value, int | float)
    return type(current) is type(value)


def reflect_over(obj: dict[str, Any], reflected: dict[str, Any], route: Route) -> None:
    """Write request fields over the fixture's same-typed or null fields, in place."""
    for name, value in reflected.items():
        if name in SERVICE_OWNED or name in route.ids or name not in obj:
            continue
        if obj[name] is None or _same_json_type(obj[name], value):
            obj[name] = value


def l1_body(request: Request, route: Route, obj: dict[str, Any]) -> dict[str, Any]:
    """Return the fixture body: `obj` (mutated) with the request's fields over it, and ids."""
    reflect_over(obj, reflect(request), route)
    mint_ids(obj, route, request.path)
    return obj


def body(
    request: Request, route: Route | None, obj: dict[str, Any] | None, clock: AnswerClock
) -> dict[str, Any]:
    """Return the fixture body when there is a fixture object, else the echo. Ignores `clock`."""
    if route is not None and obj is not None:
        return l1_body(request, route, obj)
    return l0_body(request, route)


FIXTURES_DIR_NAME = "fixtures"
SOURCE_KEY = "_source"
RESOURCES_KEY = "resources"

# So a `service:` like `../../etc/passwd` reads nothing.
_SERVICE_NAME = re.compile(r"[A-Za-z0-9_-]+")


def fixtures_dir() -> Path:
    """Return the shipped fixtures directory; files are in stripe-mock's `fixtures3.json` shape."""
    return Path(str(files("irimi").joinpath(FIXTURES_DIR_NAME)))


@lru_cache(maxsize=32)
def _document(service: str) -> dict[str, Any]:
    """Return one service's parsed, cached fixture file, or {}; callers must not mutate it."""
    if not _SERVICE_NAME.fullmatch(service):
        return {}
    try:
        text = (fixtures_dir() / f"{service}.json").read_text(encoding="utf-8")
        document = json.loads(text)
    except Exception:  # missing, unreadable, or not JSON: the caller falls back to the echo
        return {}
    return document if isinstance(document, dict) else {}


def fixture_objects(service: str) -> dict[str, Any]:
    """Return every fixture object this service ships, by name; `{}` when the file is unusable."""
    resources = _document(service).get(RESOURCES_KEY)
    if not isinstance(resources, dict):
        return {}
    return {name: obj for name, obj in resources.items() if isinstance(obj, dict)}


def fixture_object(service: str, name: str) -> dict[str, Any] | None:
    """Return a private deep copy of one fixture object, or None when this install lacks it."""
    obj = fixture_objects(service).get(name)
    return None if obj is None else copy.deepcopy(obj)


def fixture_source(service: str) -> str:
    """Return the `_source` line of this service's fixture file, or ""."""
    value = _document(service).get(SOURCE_KEY)
    return value if isinstance(value, str) else ""


def clear_fixture_cache() -> None:
    """Forget every parsed fixture file, for tests that write their own fixtures directory."""
    _document.cache_clear()
