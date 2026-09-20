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
import re
import secrets
import string
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Protocol
from urllib.parse import parse_qsl

from irimi.exchange import AnsweredBy, Request, Response
from irimi.pipeline import FIDELITY_DELEGATED_FLAG, Classification, target_url
from irimi.servicemap import SELF_TARGET, TARGETABLE_KINDS, target_for

if TYPE_CHECKING:  # Route is only an annotation here; the names above are imported for real
    from irimi.servicemap import Route

logger = logging.getLogger(__name__)

FIDELITY_L0_FLAG = "fidelity:L0"
ID_ALPHABET = string.ascii_letters + string.digits
ID_LENGTH = 24
JSON_CT = "application/json"
FORM_CT = "application/x-www-form-urlencoded"

# A form value that is a canonical decimal integer of at most 15 digits is echoed as a number:
# stripe-python posts `amount=4900` and the agent that reads `refund.amount` wants 4900 back.
# "007" and a 20-digit account number are not canonical, so they stay strings - coercing them
# would hand back something other than what the caller sent, and 15 digits is the floor where an
# integer still survives a JSON parser that stores numbers as doubles.
_INTEGER = re.compile(r"0|[1-9][0-9]{0,14}")

LIVE_KINDS = ("read", "llm", "telemetry")


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
    return ForwardTo(
        url=target_url(target, request, matched=route is not None), forward_auth=forward_auth
    )


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


def reflect(request: Request) -> dict[str, Any]:
    """The request's own fields, as a JSON-serializable dict. Never raises.

    `application/json` counts only when it decodes to an object; `application/x-www-form-urlencoded`
    is parsed with `parse_qsl`. Any other content type, and any parse failure, reflects nothing.
    """
    ct = content_type(request)
    try:
        if ct == JSON_CT:
            parsed = json.loads(request.body)
            if not isinstance(parsed, dict):
                return {}
            # json.loads accepts NaN, Infinity and 1e400, and json.dumps writes them straight
            # back out, so the echo would be a body a strict JSON parser refuses. Checking here
            # is also what makes the json.dumps in answer() unable to fail.
            json.dumps(parsed, allow_nan=False)
            return parsed
        if ct == FORM_CT:
            text = request.body.decode("utf-8", "replace")
            return {k: _form_value(v) for k, v in parse_qsl(text, keep_blank_values=True)}
    except Exception:  # a malformed body reflects nothing; it must never reach the caller
        return {}
    return {}


def l0_body(request: Request, route: "Route | None") -> dict[str, Any]:
    """The generic L0 body: reflected fields, `created`, and the route's minted ids.

    A minted id overwrites a reflected field of the same name - what the service would have
    returned wins over what the caller happened to post. `object` is only added when the route
    mints an `id`, because that is the only case where we know the echo names a resource.
    """
    body = reflect(request)
    body["created"] = int(time.time())
    if route is not None and route.ids:
        for name, prefix in route.ids.items():
            body[name] = mint_id(prefix)
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
            body = fake_body(request, classification)
        except Exception:
            # The belt to reflect()'s braces. Raising here would make mitmproxy forward the flow,
            # and a forwarded write escapes shadow mode - an empty object is far better.
            logger.exception("irimi: the L0 echo failed; answering with an empty object")
            body = {}
        return Answer(
            answered_by="fake-L0",
            response=Response(
                status=200,
                headers=(("content-type", JSON_CT),),
                body=json.dumps(body).encode(),
            ),
            flags=(FIDELITY_L0_FLAG,),
        )
