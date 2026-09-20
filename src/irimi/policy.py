"""AnswerPolicy: decides whether an exchange is forwarded live or answered locally.

A locally answered write gets the L0 echo (#11): the request's own fields reflected back as a JSON
object, plus `created`, plus an id minted for every field the matched route names in `ids:`. It is
the floor, not a fixture: an SDK has to be able to parse it and read the fields it just sent.
Fixture reflection (L1) replaces this for mapped routes in a later phase.

Nothing here may raise. An exception inside a mitmproxy hook makes the flow forward untouched, and
a forwarded write escapes shadow mode - so a body that cannot be parsed reflects nothing instead.
"""

import json
import logging
import math
import re
import secrets
import string
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal, Protocol
from urllib.parse import parse_qsl

from irimi.exchange import LIVE_KINDS, AnsweredBy, Request, Response
from irimi.pipeline import (
    FIDELITY_DELEGATED_FLAG,
    Classification,
    is_local_target,
    target_url,
)
from irimi.servicemap import (
    CREDENTIAL_PATH_HOSTS,
    SELF_TARGET,
    TARGETABLE_KINDS,
    Route,
    path_params,
    target_for,
)

logger = logging.getLogger(__name__)

FIDELITY_L0_FLAG = "fidelity:L0"
ID_ALPHABET = string.ascii_letters + string.digits
ID_LENGTH = 24
JSON_CT = "application/json"
TEXT_CT = "text/plain"
FORM_CT = "application/x-www-form-urlencoded"

# One bracket segment of a form field name: the `[order_id]` of `metadata[order_id]`.
_BRACKET = re.compile(r"\[([^\[\]]*)\]")
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


@dataclass(frozen=True)
class ForwardTo:
    """An answer target: the address that answers this request instead of irimi (design D20).

    `url` is absolute and already carries the path and query the target should see. `forward_auth`
    is the route opting in to keeping its `Authorization` header; the engine strips it otherwise.
    """

    url: str
    forward_auth: bool = False


@dataclass(frozen=True)
class Answer:
    answered_by: AnsweredBy
    response: Response | None  # None means "forward live"; set means "send this, do not forward"
    flags: tuple[str, ...] = ()  # merged into the Exchange by the engine
    # Set only on a `delegated` answer, and then `response` is None: the engine forwards there
    # instead of to the real service, and what comes back is what the agent receives.
    forward_to: ForwardTo | None = None


class AnswerPolicy(Protocol):
    name: str

    def answer(self, request: Request, classification: Classification) -> Answer: ...


def delegate(request: Request, classification: Classification) -> ForwardTo | None:
    """The answer target for this request, or None when irimi answers it itself.

    A matched route asks `servicemap.target_for`, which is the one place the route-over-service
    precedence lives. A request that matched no route can still be delegated by a **service**
    target: the map's author pointed the whole service at their stub, and answering the routes
    their map happens not to list with our own fake would give the agent a world that is half
    theirs and half ours. `target_reads` is what extends that to reads; `llm` and `telemetry` are
    never delegated, which is `target_for`'s rule restated here for the unmatched case.
    """
    service_map = classification.service_map
    if service_map is None:
        return None
    route = classification.matched[1] if classification.matched is not None else None
    if route is not None:
        target, forward_auth = target_for(service_map, route), route.forward_auth
    elif classification.kind in TARGETABLE_KINDS or (
        classification.kind == "read" and service_map.target_reads
    ):
        target, forward_auth = service_map.target, False
    else:
        return None
    if target == SELF_TARGET:
        return None
    url = target_url(target, request, matched=route is not None)
    # THE SCOPE RULE, decision half (servicemap.CREDENTIAL_PATH_HOSTS). The loader already
    # refuses a non-loopback target on a service claiming one of these hosts, whichever layer it
    # arrived through. This asks the same question of the request and the answer actually in
    # front of us, so a target that reaches here some other way - a layer added later, a map
    # built in code, a future flag - inherits the rule instead of escaping it. On this host the
    # path IS the credential, for every path and not only the ones a map lists.
    if request.host in CREDENTIAL_PATH_HOSTS and not is_local_target(url):
        logger.error(
            "irimi: refusing to delegate %s to %s: the request path is the credential on %s, "
            "so its answer target must be loopback",
            request.path,
            url,
            request.host,
        )
        return None
    return ForwardTo(url=url, forward_auth=forward_auth)


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

    A key with no brackets, or bracketed in a shape we will not guess at - `a[b`, `a[b]c`,
    `a[[b]]`, `a[]`, `[b]`, or more than `_MAX_FORM_DEPTH` levels - comes back with no segments
    and stays flat. Echoing such a key unchanged is wrong in a small, visible way; guessing at its
    structure is wrong in an unpredictable one.
    """
    head, bracket, rest = key.partition("[")
    if not bracket or not head:
        return key, []
    tail = bracket + rest
    segments = _BRACKET.findall(tail)
    if not segments or len(segments) > _MAX_FORM_DEPTH or "" in segments:
        return key, []
    if "".join(f"[{s}]" for s in segments) != tail:
        return key, []
    return head, segments


def _assign(node: dict[str, Any], segments: list[str], value: str) -> None:
    """Walk the bracket path, creating a dict per level, and set the leaf to `value`.

    The value stays a string. A Stripe metadata value is always a string on the live API, so
    coercing `metadata[order_id]=6735` to an int hands back something other than what the caller
    sent - the same reason `_INTEGER` refuses "007" (#27).
    """
    for segment in segments[:-1]:
        child = node.get(segment)
        if not isinstance(child, dict):
            child = {}
            node[segment] = child
        node = child
    node[segments[-1]] = value


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
    """A form body as the object the live API would answer with (#27).

    Three rules, one per bug in the flat `dict(parse_qsl(...))` this replaces. A bracket-nested
    key becomes a nested container, so `metadata[order_id]=6735` echoes `metadata` and an SDK can
    read it. A value inside a bracket path stays a string. A repeated bare key collects into a
    list instead of keeping only the last value.
    """
    root: dict[str, Any] = {}
    bare: dict[str, list[Any]] = {}
    for key, value in parse_qsl(text, keep_blank_values=True):
        head, segments = _split_key(key)
        if not segments:
            # A bracketed spelling of the same name always wins, whichever order they arrive in:
            # `a[0]=1&a=2` and `a=2&a[0]=1` both keep the structure, because structure surviving
            # is the whole point of this function and a bare value cannot carry any.
            if isinstance(root.get(head), dict):
                continue
            bare.setdefault(head, []).append(_form_value(value))
            root.setdefault(head, None)  # hold the position; the value is filled in below
            continue
        node = root.get(head)
        if not isinstance(node, dict):
            node = {}
            root[head] = node
        bare.pop(head, None)
        _assign(node, segments, value)
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
    prefix is the only thing that ties them together.
    """
    return next(
        (value for value in path_params(route.path, path).values() if value.startswith(prefix)),
        None,
    )


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


def slack_body(fields: dict[str, Any]) -> dict[str, Any]:
    """Slack's own envelope. slack_sdk raises SlackApiError on any body without `ok: true`.

    It replaces the generic body rather than extending it: a Slack response carries no `created`
    and no `object`, and an SDK that sees them would be reading fields the real API never sends.
    """
    body: dict[str, Any] = {
        "ok": True,
        "ts": f"{int(time.time())}.{secrets.randbelow(1_000_000):06d}",
    }
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


class ShadowPolicy:
    """Reads, llm and telemetry go live. write and unknown are answered with the L0 echo."""

    name: Literal["shadow"] = "shadow"

    def answer(self, request: Request, classification: Classification) -> Answer:
        forward = delegate(request, classification)
        if forward is not None:
            return Answer(
                answered_by="delegated",
                response=None,
                flags=(FIDELITY_DELEGATED_FLAG,),
                forward_to=forward,
            )
        if classification.kind in LIVE_KINDS:
            return Answer(answered_by="live", response=None)
        try:
            body, ct = fake_response(request, classification)
        except Exception:
            # The belt to reflect()'s braces. Raising here would make mitmproxy forward the flow,
            # and a forwarded write escapes shadow mode - an empty object is far better.
            logger.exception("irimi: the L0 echo failed; answering with an empty object")
            body, ct = b"{}", JSON_CT
        return Answer(
            answered_by="fake-L0",
            response=Response(status=200, headers=(("content-type", ct),), body=body),
            flags=(FIDELITY_L0_FLAG,),
        )
