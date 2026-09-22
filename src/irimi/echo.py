"""The L0 echo (#11): the body a locally answered write gets.

The request's own fields reflected back as a JSON object, plus `created`, plus an id minted for
every field the matched route names in `ids:`. It is the floor, not a fixture: an SDK has to be
able to parse it and read the fields it just sent. Fixture reflection (L1) replaces this for
mapped routes in a later phase.

Nothing here may raise. An exception inside a mitmproxy hook makes the flow forward untouched, and
a forwarded write escapes shadow mode - so a body that cannot be parsed reflects nothing instead,
and `ShadowPolicy` wraps the one serialization in a guard of its own.
"""

import json
import math
import re
import secrets
import string
import threading
import time
from collections.abc import Callable, Iterable
from typing import Any
from urllib.parse import parse_qsl

from irimi.exchange import Request
from irimi.pipeline import Classification
from irimi.servicemap import Route, path_params

ID_ALPHABET = string.ascii_letters + string.digits
ID_LENGTH = 24
JSON_CT = "application/json"
TEXT_CT = "text/plain"
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

# What an id looks like after its prefix: one run of id characters, no second `_`. Stripe, OpenAI
# and Slack all mint `<prefix><token>`, which is what lets `named_id` tell `sub_2` from
# `sub_sched_1` when a route captures both.
_ID_TOKEN = re.compile(r"[A-Za-z0-9]+")
# Stripe spells a PaymentIntent's client secret `pi_<id>_secret_<token>`, so it starts with the
# same prefix as the id and is the one value in a path that must never be echoed back as one.
SECRET_MARKER = "_secret"

# A form value that is a canonical decimal integer of at most 15 digits is echoed as a number:
# stripe-python posts `amount=4900` and the agent that reads `refund.amount` wants 4900 back.
# "007" and a 20-digit account number are not canonical, so they stay strings - coercing them
# would hand back something other than what the caller sent, and 15 digits is the floor where an
# integer still survives a JSON parser that stores numbers as doubles.
_INTEGER = re.compile(r"0|[1-9][0-9]{0,14}")


def mint_id(prefix: str) -> str:
    """`re_` -> `re_` + 24 characters from [A-Za-z0-9], drawn with secrets."""
    return prefix + "".join(secrets.choice(ID_ALPHABET) for _ in range(ID_LENGTH))


def object_name(operation: str) -> str:
    """The `object` an operation's route mints, derived from the operation name.

    The first dot-segment with a single trailing `s` stripped: `refunds.create` -> `refund`,
    `payment_intents.cancel` -> `payment_intent`. There is no `object:` field in the route schema,
    and a segment that does not end in `s` is returned unchanged.
    """
    head = operation.partition(".")[0]
    return head[:-1] if len(head) > 1 and head.endswith("s") else head


def content_type(request: Request) -> str:
    """The request's content type without parameters, lower-cased. "" when there is none."""
    for name, value in request.headers:  # pipeline.parse already lower-cased the names
        if name == "content-type":
            return value.partition(";")[0].strip().lower()
    return ""


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


def named_id(route: Route, path: str, prefix: str) -> str | None:
    """The id this request already names in its own path, or None when it names none.

    A create posts to a collection (`POST /v1/refunds`) and the service mints the id. Every other
    write addresses a resource that exists (`POST /v1/customers/cus_REAL123`,
    `POST /v1/payment_intents/pi_REAL999/cancel`) and the live API answers with the id it was
    given, so minting a fresh one hands the agent an id for a resource that never existed (#26).

    The captured segment is matched by `prefix`, not by parameter name, because the two are
    spelled differently: the cancel route captures `{payment_intent}` and mints `id: pi_`, and the
    prefix is the only thing that ties them together. `path_params` percent-decodes the segment,
    so an over-encoded `cus%5FREAL123` is recognised as the id it is.

    A prefix can match more than one capture, and the first one is not always the right one:
    `sub_` matches `sub_sched_1` before `sub_2`, a real Stripe pair, and it matches a
    PaymentIntent **client secret** (`pi_ABC_secret_XYZ`) before anything else in that path. So a
    capture shaped like an id - the prefix and then one run of id characters, which is how Stripe,
    OpenAI and Slack all mint them - is preferred over one that merely starts with the prefix.
    That settles both: `sub_sched_1` and the client secret each carry a second `_` and neither is
    id-shaped. Nothing id-shaped leaves the old answer in place, minus a value carrying Stripe's
    own `_secret` marker, which is never an id and must not be echoed back as one (#33).
    """
    values = [v for v in path_params(route.path, path).values() if v.startswith(prefix)]
    shaped = [v for v in values if _ID_TOKEN.fullmatch(v[len(prefix) :])]
    if shaped:
        return shaped[0]
    return next((v for v in values if SECRET_MARKER not in v), None)


def l0_body(request: Request, route: Route | None) -> dict[str, Any]:
    """The generic L0 body: reflected fields, `created`, and the route's ids.

    An id is minted only when the request does not already name one - see `named_id`. A minted id
    still overwrites a reflected *body* field of the same name: what the service would have
    returned wins over what the caller happened to post. `object` is only added when the route
    carries an `id`, because that is the only case where we know the echo names a resource.
    """
    body = reflect(request)
    body["created"] = int(time.time())
    if route is not None and route.ids:
        for name, prefix in route.ids.items():
            body[name] = named_id(route, request.path, prefix) or mint_id(prefix)
        if "id" in route.ids:
            body["object"] = object_name(route.operation)
    return body


# The last `ts` slack_ts handed out, as (seconds, sub-second counter). Guarded by a lock: the
# hooks run on the proxy's event loop today, but `mint_id` is the only other minting function here
# and it is thread-safe by construction, so this one says so too rather than resting on that.
_last_slack_ts: tuple[int, int] = (0, 0)
_slack_ts_lock = threading.Lock()


def slack_ts() -> str:
    """A Slack `ts`: seconds, a dot, and six digits. Distinct and increasing for the whole run.

    Slack uses `ts` as a message's identifier and as `thread_ts`, so two messages sharing one is
    two messages that are the same message - an agent that posts twice in a second and then
    replies in a thread addresses whichever of them it collided with. Drawing the sub-second part
    at random gave 181,346 distinct values in 200,000 draws (#29).

    A counter rather than a wider random draw, because distinctness is only half of it: real `ts`
    values increase with time, and code that sorts a transcript by `ts` or asks "is this reply
    after that message" reads the same answer here as it would from Slack. The counter carries
    into the next second if a run ever posts more than a million messages inside one.
    """
    global _last_slack_ts
    with _slack_ts_lock:
        seconds, counter = int(time.time()), 0
        last_seconds, last_counter = _last_slack_ts
        if seconds <= last_seconds:
            seconds, counter = last_seconds, last_counter + 1
        if counter > 999_999:
            seconds, counter = seconds + 1, 0
        _last_slack_ts = (seconds, counter)
    return f"{seconds}.{counter:06d}"


def slack_body(fields: dict[str, Any]) -> dict[str, Any]:
    """Slack's own envelope. slack_sdk raises SlackApiError on any body without `ok: true`.

    It replaces the generic body rather than extending it: a Slack response carries no `created`
    and no `object`, and an SDK that sees them would be reading fields the real API never sends.
    """
    body: dict[str, Any] = {"ok": True, "ts": slack_ts()}
    if "channel" in fields:
        body["channel"] = fields["channel"]
    return body


# Keyed on the classified service name, not on a host or a route, so every route of that service -
# including ones other batches add later - gets the shape without a second entry here. The name is
# the map's when a map claims the host, and the host itself when none does.
SHAPES: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {"slack": slack_body}


# Routes whose real service does not answer with JSON at all, keyed by service and operation.
# A Slack incoming webhook answers the literal body `ok` as `text/plain`; the Web API envelope it
# got instead (the shape keys on the service, and hooks.slack.com is part of `slack`) breaks the
# common raw-requests idiom `assert resp.text == "ok"` (#29).
LITERAL_BODIES: dict[tuple[str, str], tuple[bytes, str]] = {
    ("slack", "incoming_webhook"): (b"ok", TEXT_CT),
}


def fake_body(request: Request, classification: Classification) -> dict[str, Any]:
    """The body of a locally answered write: the service's own shape when it has one.

    A shape is only used for a route the maps actually claim. An unmapped path on a shaped
    service gets the generic echo instead, because `ok: true` is a claim of success and we only
    know what success looks like for a route we mapped. Answering an unmapped call that way sends
    slack_sdk down its success branch into an uncatchable crash later (`files_upload_v2` reads
    `upload_url = None` and dies inside urllib); the generic echo leaves it raising the
    `SlackApiError` that callers already catch.
    """
    route = classification.matched[1] if classification.matched is not None else None
    shaper = SHAPES.get(classification.service) if route is not None else None
    if shaper is not None:
        return shaper(reflect(request))
    return l0_body(request, route)


def fake_response(request: Request, classification: Classification) -> tuple[bytes, str]:
    """The encoded body and content type of a locally answered write.

    The single place the body is serialized. It used to be encoded twice - once inside `reflect`
    purely to validate it and throw it away, once here - and this call was the only one outside
    the never-raise guard in `answer` (#29).
    """
    route = classification.matched[1] if classification.matched is not None else None
    if route is not None:
        literal = LITERAL_BODIES.get((classification.service, route.operation))
        if literal is not None:
            return literal
    return json.dumps(fake_body(request, classification), allow_nan=False).encode(), JSON_CT
