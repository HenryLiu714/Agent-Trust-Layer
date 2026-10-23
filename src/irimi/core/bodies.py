"""Parse request and response bodies; never raises (fail-closed hook rule).

`reflect` reads a request's fields (JSON or a Stripe-bracketed form); `json_object` a response's.
"""

import json
import math
import re
from collections.abc import Iterable
from typing import Any
from urllib.parse import parse_qsl

from irimi.core.exchange import Request, media_type
from irimi.core.limits import MAX_BODY_BYTES

JSON_CT = "application/json"
FORM_CT = "application/x-www-form-urlencoded"

_BRACKET = re.compile(r"\[([^\[\]]*)\]")
# Marker for `expand[]`; a literal `[]` segment cannot collide because `_BRACKET` strips brackets.
_APPEND = "[]"
# Stripe nests four deep at most; deeper keys stay flat so a hostile nest cannot recurse in a hook.
_MAX_FORM_DEPTH = 8

# Canonical integers echo as numbers; "007" and over 15 digits (a double's exact limit) stay text.
_INTEGER = re.compile(r"0|[1-9][0-9]{0,14}")


def content_type(request: Request) -> str:
    """Return the request's media type, lower-cased, or "" when there is none."""
    return media_type(request.header("content-type") or "")


def _form_value(value: str) -> Any:
    return int(value) if _INTEGER.fullmatch(value) else value


def _is_json(ct: str) -> bool:
    """Return True for a JSON content type, including `+json` suffixes and `text/json`."""
    return ct in (JSON_CT, "text/json") or ct.endswith("+json")


def _has_non_finite(value: Any) -> bool:
    """Return True if `value` holds NaN or an infinity, which strict JSON parsers refuse."""
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, dict):
        return any(_has_non_finite(v) for v in value.values())
    if isinstance(value, list):
        return any(_has_non_finite(v) for v in value)
    return False


def _split_key(key: str) -> tuple[str, list[str]]:
    """Split `a[0][b]` into `("a", ["0", "b"])`; a malformed or over-deep key stays flat."""
    head, bracket, rest = key.partition("[")
    if not bracket or not head:
        return key, []
    tail = bracket + rest
    segments = _BRACKET.findall(tail)
    if not segments or len(segments) > _MAX_FORM_DEPTH:
        return key, []
    if "".join(f"[{s}]" for s in segments) != tail:
        return key, []
    if segments == [""]:
        return head, [_APPEND]
    if "" in segments:  # `a[][b]`, `a[b][]`: an append in the middle of a path means nothing here
        return key, []
    return head, segments


def _is_string_valued(names: Iterable[str]) -> bool:
    """Return True if a value under these names is caller text, so never coerced to a number."""
    return any(name in STRING_KEYED_FIELDS for name in names)


def _assign(node: dict[str, Any], head: str, segments: list[str], value: str) -> None:
    """Set the leaf at the bracket path, creating dicts; an existing dict at the leaf wins."""
    for segment in segments[:-1]:
        child = node.get(segment)
        if not isinstance(child, dict):
            child = {}
            node[segment] = child
        node = child
    leaf = segments[-1]
    if isinstance(node.get(leaf), dict):
        return
    node[leaf] = value if _is_string_valued((head, *segments)) else _form_value(value)


# Fields whose keys the caller chooses, so `metadata[0]` is the key "0", not an array index.
STRING_KEYED_FIELDS: frozenset[str] = frozenset({"metadata"})


def _to_lists(node: Any, field: str = "") -> Any:
    """Turn, depth first, every dict keyed exactly `0`..`n-1` into a list.

    Stripe form-encodes arrays as `expand[0]=a&expand[1]=b`; `STRING_KEYED_FIELDS` stay dicts.
    """
    if not isinstance(node, dict):
        return node
    converted = {key: _to_lists(value, key) for key, value in node.items()}
    if field in STRING_KEYED_FIELDS:
        return converted
    if converted and set(converted) == {str(i) for i in range(len(converted))}:
        return [converted[str(i)] for i in range(len(converted))]
    return converted


def parse_form(text: str) -> dict[str, Any]:
    """Return a form body as the object the live API would answer with.

    Structure beats a same-named scalar and a bracket path beats an append, whatever the order.
    """
    root: dict[str, Any] = {}
    bare: dict[str, list[Any]] = {}
    appended: dict[str, list[Any]] = {}
    for key, value in parse_qsl(text, keep_blank_values=True):
        head, segments = _split_key(key)
        if not segments:
            if isinstance(root.get(head), dict) or head in appended:
                continue
            bare.setdefault(head, []).append(_form_value(value))
            root.setdefault(head, None)  # hold the position; the value is filled in below
            continue
        if segments == [_APPEND]:
            if isinstance(root.get(head), dict):
                continue
            bare.pop(head, None)
            item = value if _is_string_valued((head,)) else _form_value(value)
            appended.setdefault(head, []).append(item)
            root.setdefault(head, None)
            continue
        node = root.get(head)
        if not isinstance(node, dict):
            node = {}
            root[head] = node
        bare.pop(head, None)
        appended.pop(head, None)
        _assign(node, head, segments, value)
    for head, items in appended.items():
        root[head] = items
    for head, values in bare.items():
        root[head] = values[0] if len(values) == 1 else values
    return {key: _to_lists(value, key) for key, value in root.items()}


def reflect(request: Request) -> dict[str, Any]:
    """Return a JSON-object or form body's fields as a dict, else {}. Never raises."""
    ct = content_type(request)
    try:
        if _is_json(ct):
            return json_object(request.body, max_bytes=None, finite=True) or {}
        if ct == FORM_CT:
            return parse_form(request.body.decode("utf-8", "replace"))
    except Exception:  # a malformed body reflects nothing; it must never reach the caller
        return {}
    return {}


def json_object(
    body: bytes, *, max_bytes: int | None = MAX_BODY_BYTES, finite: bool = False
) -> dict[str, Any] | None:
    """Return `body` as a JSON object, or None if it is not one. Never raises.

    `max_bytes` refuses a larger body unread (None: any size); `finite` also refuses NaN and inf.
    """
    if not body or (max_bytes is not None and len(body) > max_bytes):
        return None
    try:
        parsed = json.loads(body)
    except Exception:  # not JSON, not UTF-8, or nested past the parser's depth
        return None
    if not isinstance(parsed, dict) or (finite and _has_non_finite(parsed)):
        return None
    return parsed


def is_int(value: Any) -> bool:
    """Return True for a JSON integer: an `int` that is not a `bool`."""
    return isinstance(value, int) and not isinstance(value, bool)
