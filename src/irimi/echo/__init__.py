"""The body a locally answered write gets: the L0 echo (#11) and the L1 fixture (#41).

**L0** is the floor and the fallback: the request's own fields reflected back as a JSON object,
plus `created`, plus an id minted for every field the matched route names in `ids:`. It is not a
fixture - an SDK has to be able to parse it and read the fields it just sent, and nothing more.

**L1** applies to a route whose map names a `fixture:`. It starts from that vendored response
object (`irimi.fixture`) and writes the request's own fields over it, so the agent receives every
field the real service would have sent and not only the ones it posted. A fixture this install
cannot read is not an error: the answer degrades to L0 and the exchange is flagged.

Nothing here may raise. An exception inside a mitmproxy hook makes the flow forward untouched, and
a forwarded write escapes shadow mode - so a body that cannot be parsed reflects nothing instead,
and `ShadowPolicy` wraps the one serialization in a guard of its own.

The package is three modules, mirroring `irimi.services`: `generic` (the L0 echo and the L1
fixture body, for any service), `slack` (Slack's envelopes and its `ts` sequence), and this one,
which picks between them per service and serializes the answer. A service with a write-side shape
of its own is added by an entry in `ENVELOPED` / `SHAPES` / `READ_OBSERVERS`, not a branch here.
"""

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from irimi import fixture
from irimi.bodies import JSON_CT, reflect
from irimi.echo import slack
from irimi.echo.generic import (
    LIVEMODE,
    Built,
    l0_body,
    l1_body,
    mint_id,
    named_id,
    object_name,
    reflect_over,
)
from irimi.echo.slack import (
    SLACK_ENVELOPES,
    SlackEnvelope,
    observe_slack_history,
    observe_slack_ts,
    slack_body,
    slack_l1_body,
    slack_ts,
)
from irimi.exchange import FIXTURE_FAILED_FLAG, FakeLevel, Request
from irimi.pipeline import Classification
from irimi.servicemap import Route

__all__ = [
    "ENVELOPED",
    "LITERAL_BODIES",
    "LIVEMODE",
    "READ_OBSERVERS",
    "SHAPES",
    "SLACK_ENVELOPES",
    "TEXT_CT",
    "Fake",
    "SlackEnvelope",
    "fake_body",
    "fake_rejection",
    "fake_response",
    "l0_body",
    "l1_body",
    "mint_id",
    "named_id",
    "object_name",
    "observe_read",
    "observe_slack_history",
    "observe_slack_ts",
    "reflect_over",
    "slack_body",
    "slack_l1_body",
    "slack_ts",
]

TEXT_CT = "text/plain"


# What a live read's body teaches the faker, keyed on service. Same convention as `SHAPES` below:
# every service-specific fact about a body lives in this package, so the engine can hand it every
# read it sees without naming a service of its own (#42).
READ_OBSERVERS: dict[str, Callable[[bytes], None]] = {slack.SERVICE: slack.observe_slack_history}


def observe_read(service: str, body: bytes) -> None:
    """Let a service learn from a read irimi forwarded rather than answered. Never raises.

    A read's body is the only place the real values a fake has to sort against appear: the write
    log holds writes, and the trace store is write-only. So a body is observed as it goes past
    rather than looked up later, and a service with nothing to learn is a no-op (#42).
    """
    observer = READ_OBSERVERS.get(service)
    if observer is not None:
        observer(body)


# Keyed on the classified service name, not on a host or a route, so every route of that service -
# including ones other batches add later - gets the shape without a second entry here. The name is
# the map's when a map claims the host, and the host itself when none does.
SHAPES: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {slack.SERVICE: slack.slack_body}


# Routes whose real service does not answer with JSON at all, keyed by service and operation.
# A Slack incoming webhook answers the literal body `ok` as `text/plain`; the Web API envelope it
# got instead (the shape keys on the service, and hooks.slack.com is part of `slack`) breaks the
# common raw-requests idiom `assert resp.text == "ok"` (#29).
LITERAL_BODIES: dict[tuple[str, str], tuple[bytes, str]] = {
    (slack.SERVICE, "incoming_webhook"): (b"ok", TEXT_CT),
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
    route = classification.route
    shaper = SHAPES.get(classification.service) if route is not None else None
    if shaper is not None:
        return shaper(reflect(request))
    return l0_body(request, route)


# Services whose mapped writes are answered inside an envelope of the service's own, keyed on the
# classified service name like `SHAPES`. The entry answers None for an operation it has no
# envelope for, and that write is built the generic way.
ENVELOPED: dict[str, Callable[[Request, Route], Built | None]] = {slack.SERVICE: slack.enveloped}


@dataclass(frozen=True)
class Fake:
    """A locally answered write: the encoded body, its content type, and how faithful it is.

    `answered_by` is the value the Exchange and the `Irimi-Answered-By` header carry, so the level
    is decided in the one place that knows which body was built and is never re-derived from the
    route by a caller.
    """

    body: bytes
    content_type: str
    answered_by: FakeLevel = "fake-L0"
    flags: tuple[str, ...] = ()


def _fake_dict(request: Request, classification: Classification, route: Route | None) -> Built:
    """The body as a dict, the level it was built at, and any flag that level owes the trace."""
    if route is not None:
        build = ENVELOPED.get(classification.service)
        built = build(request, route) if build is not None else None
        if built is not None:
            return built
    if route is not None and route.fixture:
        obj = fixture.get(classification.service, route.fixture)
        if obj is not None:
            return l1_body(request, route, obj), "fake-L1", ()
        # The map promised a fixture this install cannot read. L0 is the floor and still answers
        # the write, but the trace has to say the fidelity is not the one the map named (#41).
        return fake_body(request, classification), "fake-L0", (FIXTURE_FAILED_FLAG,)
    return fake_body(request, classification), "fake-L0", ()


def fake_response(request: Request, classification: Classification) -> Fake:
    """The body, content type and fidelity of a locally answered write.

    The single place the body is serialized. It used to be encoded twice - once inside `reflect`
    purely to validate it and throw it away, once here - and this call was the only one outside
    the never-raise guard in `answer` (#29).
    """
    route = classification.route
    if route is not None:
        literal = LITERAL_BODIES.get((classification.service, route.operation))
        if literal is not None:
            return Fake(literal[0], literal[1])
    body, answered_by, flags = _fake_dict(request, classification, route)
    return Fake(json.dumps(body, allow_nan=False).encode(), JSON_CT, answered_by, flags)


def fake_rejection(request: Request, classification: Classification, body: dict[str, Any]) -> Fake:
    """A modeled error body, carried at the level this route's write WOULD have been answered at.

    There is no `fake-L3`: the header says who answered, the exchange field says what L3 decided
    (#45). The level is not "L1 if the route names a fixture" - a Slack `reactions.add` has no
    fixture and is honestly L0, a `chat.postMessage` whose fixture will not load is L0 with the
    `fixture-failed` flag - so it is taken from the one place that knows, by building the body
    this route would have had and discarding it. One dict, once, on a path that is about to make
    a network-free answer either way.
    """
    _, answered_by, flags = _fake_dict(request, classification, classification.route)
    return Fake(json.dumps(body, allow_nan=False).encode(), JSON_CT, answered_by, flags)
