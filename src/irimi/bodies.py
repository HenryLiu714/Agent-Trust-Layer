"""What a request or response body says, parsed without ever raising.

`reflect` reads a request's own fields - a JSON object, or a form body with Stripe's bracketed
encoding - and `json_object` reads a response's. They are the one parser for each, shared by the
faker (`echo`), the write log, the idempotency store, the overlay, the policy and the summary, so
all of them see the same view of one body. Nothing here raises: every caller is on the answer
path of a mitmproxy hook, where a raise forwards the flow and a forwarded write escapes shadow
mode. Layer 1: it imports only `exchange`.
"""

import json
import math
import re
from collections.abc import Iterable
from typing import Any
from urllib.parse import parse_qsl

from irimi.exchange import Request, media_type

JSON_CT = "application/json"
FORM_CT = "application/x-www-form-urlencoded"

# One bracket segment of a form field name: the `[order_id]` of `metadata[order_id]`.
_BRACKET = re.compile(r"\[([^\[\]]*)\]")
# What `_split_key` returns for the one empty bracket pair it understands, `expand[]`. A real
# segment can never be this: `_BRACKET` captures what is between the brackets, so a key spelling
# `[]` out literally arrives as the two characters and not as this marker.
APPEND = "[]"
# How deep a bracket path may nest before the key is kept flat instead. Stripe's deepest real key
# is `line_items[0][price_data][product_data][name]`, four levels; a hostile body can otherwise
# nest thousands, and unwinding one that deep is a RecursionError inside a mitmproxy hook.
_MAX_FORM_DEPTH = 8

# A form value that is a canonical decimal integer of at most 15 digits is echoed as a number:
# stripe-python posts `amount=4900` and the agent that reads `refund.amount` wants 4900 back.
# "007" and a 20-digit account number are not canonical, so they stay strings - coercing them
# would hand back something other than what the caller sent, and 15 digits is the floor where an
# integer still survives a JSON parser that stores numbers as doubles.
_INTEGER = re.compile(r"0|[1-9][0-9]{0,14}")


def content_type(request: Request) -> str:
    """The request's content type without parameters, lower-cased. "" when there is none."""
    return media_type(request.header("content-type") or "")


def _form_value(value: str) -> Any:
    return int(value) if _INTEGER.fullmatch(value) else value


def _is_json(ct: str) -> bool:
    """Every content type that carries a JSON document.

    RFC 6839's `+json` structured suffix covers `application/vnd.api+json` and
    `application/json-patch+json`; `text/json` is a legacy spelling clients still send. Before
    this, only the exact `application/json` was parsed and the rest reflected nothing, which is
    indistinguishable from a malformed body (#29).
    """
    return ct == JSON_CT or ct == "text/json" or ct.endswith("+json")


def _has_non_finite(value: Any) -> bool:
    """True when `value` holds a float `json.dumps` would write as NaN, Infinity or -Infinity.

    json.loads accepts all three and json.dumps writes them straight back out, so the echo would
    be a body a strict JSON parser refuses. This keeps them out of the single `json.dumps` in
    `fake_response`; the `try` in `answer` is what makes that call safe outright. It replaces a
    throwaway serialization of the whole body, which cost a second full pass over a large one and
    was discarded either way (#29).
    """
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, dict):
        return any(_has_non_finite(v) for v in value.values())
    if isinstance(value, list):
        return any(_has_non_finite(v) for v in value)
    return False


def _split_key(key: str) -> tuple[str, list[str]]:
    """`metadata[order_id]` -> `("metadata", ["order_id"])`; `a[0][b]` -> `("a", ["0", "b"])`.

    `expand[]` -> `("expand", [APPEND])`: an empty bracket pair on its own is the one bracketed
    shape with an unambiguous meaning, and it is Stripe's own documented `curl` spelling for a
    repeated array field. It stayed flat before, so `expand[]=a&expand[]=b` echoed the literal
    JSON key `"expand[]"` (#33).

    A key with no brackets, or bracketed in a shape we will not guess at - `a[b`, `a[b]c`,
    `a[[b]]`, `a[][b]`, `[b]`, or more than `_MAX_FORM_DEPTH` levels - comes back with no segments
    and stays flat. Echoing such a key unchanged is wrong in a small, visible way; guessing at its
    structure is wrong in an unpredictable one.
    """
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
        return head, [APPEND]
    if "" in segments:  # `a[][b]`, `a[b][]`: an append in the middle of a path means nothing here
        return key, []
    return head, segments


def _is_string_valued(names: Iterable[str]) -> bool:
    """True when a value reached through these field names is one the caller chose the text of.

    `STRING_KEYED_FIELDS` names the fields whose *keys* are the caller's, and their values are the
    caller's for the same reason: a Stripe metadata value is always a string on the live API, so
    coercing `metadata[order_id]=6735` to an int hands back something other than what was sent
    (#27). Anywhere else a nested value is coerced exactly like a top-level one.
    """
    return any(name in STRING_KEYED_FIELDS for name in names)


def _assign(node: dict[str, Any], head: str, segments: list[str], value: str) -> None:
    """Walk the bracket path, creating a dict per level, and set the leaf.

    The leaf is coerced by `_form_value` unless some name on the way to it is a
    `STRING_KEYED_FIELDS` one. `line_items[0][quantity]=2` echoed the string `"2"` while the same
    number at the top level echoed `2`, so `quantity * 2` was `"22"` with no raise (#33).

    An existing dict at the leaf is left alone: `a[0][b]=1&a[0]=2` and `a[0]=2&a[0][b]=1` both keep
    the structure, which is the rule `parse_form` already applies to a bare key one level up.
    """
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


# Fields whose keys are arbitrary strings chosen by the caller, so `0`, `1`, ... are key names
# and not array indices. `metadata[0]=zero` and `expand[0]=charge` are byte-wise the same shape on
# the wire; the field name is the only thing that tells them apart, so it is the field name that
# decides. Stripe, Slack and Segment all spell this field `metadata`.
STRING_KEYED_FIELDS: frozenset[str] = frozenset({"metadata"})


def _to_lists(node: Any, field: str = "") -> Any:
    """Depth first, a dict whose keys are exactly `0`..`n-1` becomes a list.

    Stripe form-encodes an array as `expand[0]=a&expand[1]=b` and the live API answers with a
    JSON array. A gap, a repeat or an out-of-range index leaves it a dict: inventing the missing
    elements would echo fields the caller never sent. The comparison is against canonical decimal
    strings, so "007" and non-ASCII digits cannot reach it - `str.isdigit()` would let both in.

    A `STRING_KEYED_FIELDS` name is never promoted, at any depth: `metadata[0]=zero` is the key
    `"0"`, which the live API answers as `{"metadata": {"0": "zero"}}`. Promoting it hands the
    agent a list, and `refund.metadata["0"]` then raises TypeError instead of returning the value
    the caller just sent - #27's failure class moved one step along.
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
    """A form body as the object the live API would answer with (#27, #33).

    Four rules, one per bug in the flat `dict(parse_qsl(...))` this replaces. A bracket-nested key
    becomes a nested container, so `metadata[order_id]=6735` echoes `metadata` and an SDK can read
    it. `expand[]=a&expand[]=b` becomes the list `expand`. A repeated bare key collects into a
    list instead of keeping only the last value. And a value is coerced by `_form_value` wherever
    it sits, except under a `STRING_KEYED_FIELDS` name.

    Structure always beats a scalar of the same name, whichever order the two arrive in, and a
    bracket path beats an append. Those are the shapes no real caller sends, so the rule is chosen
    to be *stable* rather than clever: `a[0][b]=1&a[0]=2` and `a[0]=2&a[0][b]=1` echo the same
    thing, which is what stops the echo from depending on dict ordering (#33).
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
        if segments == [APPEND]:
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
    """The request's own fields, as a JSON-serializable dict. Never raises.

    A JSON content type counts only when the body decodes to an object;
    `application/x-www-form-urlencoded` goes through `parse_form`. Any other content type, and
    any parse failure, reflects nothing.
    """
    ct = content_type(request)
    try:
        if _is_json(ct):
            parsed = json.loads(request.body)
            if not isinstance(parsed, dict) or _has_non_finite(parsed):
                return {}
            return parsed
        if ct == FORM_CT:
            return parse_form(request.body.decode("utf-8", "replace"))
    except Exception:  # a malformed body reflects nothing; it must never reach the caller
        return {}
    return {}


# A live body this size is not an object any service's effects model, and parsing it on the answer
# path would cost more than the read or the check it is trying to improve.
MAX_BODY_BYTES = 2_000_000


def json_object(body: bytes) -> dict[str, Any] | None:
    """`body` as a JSON object, or None when it is not one irimi should decode."""
    if not body or len(body) > MAX_BODY_BYTES:
        return None
    try:
        parsed = json.loads(body)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


# Fields of a fixture object the service owns, which the request may therefore never write over.
# `id` is here as well as in every route's `ids:`, because a body posting `object=charge` to a
# refund route would otherwise hand stripe-python the wrong class to build, and `created` and
# `livemode` are facts about this answer rather than values the caller gets to choose.
# The read side keeps the same rule: `services.stripe._customer` may not move one.
SERVICE_OWNED: frozenset[str] = frozenset({"id", "object", "created", "livemode"})


def is_int(value: Any) -> bool:
    """A JSON integer: an `int` that is not a `bool`, which Python counts as one."""
    return isinstance(value, int) and not isinstance(value, bool)
