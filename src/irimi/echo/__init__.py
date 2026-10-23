"""The body a locally answered write gets: the L0 echo and the L1 fixture.

**L0** is the floor and the fallback: the request's own fields reflected back as a JSON object,
plus an id minted for every field the matched route names in `ids:`. It is not a fixture - an SDK
has to be able to parse it and read the fields it just sent, and nothing more.

**L1** applies to a route whose map names a `fixture:`. It starts from that vendored response
object (`irimi.fixture`) and writes the request's own fields over it, so the agent receives every
field the real service would have sent and not only the ones it posted. A fixture this install
cannot read is not an error: the answer degrades to L0 and the exchange is flagged.

Nothing here may raise. An exception inside a mitmproxy hook makes the flow forward untouched, and
a forwarded write escapes shadow mode - so a body that cannot be parsed reflects nothing instead,
and `ShadowPolicy` wraps the one serialization in a guard of its own.

The package mirrors `irimi.services`: `generic` (the echo and fixture body any service gets),
`stripe` (the fields every Stripe object carries), `slack` (Slack's envelopes and its `ts`
sequence), and this module, which picks between them per service and serializes the answer. A
service with a write-side shape of its own is an entry in the tables below, not a branch here.
"""

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from irimi import fixture, services
from irimi.bodies import JSON_CT
from irimi.echo import generic, slack, stripe
from irimi.echo.generic import Built
from irimi.echo.slack import TsClock
from irimi.exchange import FIXTURE_FAILED_FLAG, FakeLevel, Request
from irimi.pipeline import Classification
from irimi.servicemap import Route

__all__ = ["Fake", "TsClock", "fake_level", "fake_rejection", "fake_response", "observe_read"]

TEXT_CT = "text/plain"

# `(request, route or None) -> L0 body` and `(request, route, fixture object) -> L1 body`.
L0Body = Callable[[Request, Route | None], dict[str, Any]]
L1Body = Callable[[Request, Route, dict[str, Any]], dict[str, Any]]


# What a live read's body teaches the faker, keyed on service. Every service-specific fact about a
# body lives in this package, so the engine can hand it every read it sees without naming a
# service of its own.
READ_OBSERVERS: dict[str, Callable[[TsClock, bytes], None]] = {
    services.slack.SERVICE: TsClock.observe_history
}


def observe_read(service: str, body: bytes, clock: TsClock) -> None:
    """Let a service learn from a read irimi forwarded rather than answered. Never raises.

    A read's body is the only place the real values a fake has to sort against appear: the write
    log holds writes, and the trace store is write-only. So a body is observed as it goes past
    rather than looked up later, and a service with nothing to learn is a no-op.
    """
    observer = READ_OBSERVERS.get(service)
    if observer is not None:
        observer(clock, body)


# The echo and fixture bodies of a service whose objects carry fields of their own, keyed on the
# classified service name. A service with no entry gets `generic`'s.
BODIES: dict[str, tuple[L0Body, L1Body]] = {
    services.stripe.SERVICE: (stripe.l0_body, stripe.l1_body),
}
GENERIC_BODIES: tuple[L0Body, L1Body] = (generic.l0_body, generic.l1_body)


# Routes whose real service does not answer with JSON at all, keyed by service and operation.
# A Slack incoming webhook answers the literal body `ok` as `text/plain`; the Web API envelope it
# would otherwise get (hooks.slack.com is part of `slack`) breaks the common raw-requests idiom
# `assert resp.text == "ok"`.
LITERAL_BODIES: dict[tuple[str, str], tuple[bytes, str]] = {
    (services.slack.SERVICE, "incoming_webhook"): (b"ok", TEXT_CT),
}


# Services whose mapped writes are answered inside an envelope of the service's own, keyed on the
# classified service name. An unmapped path on such a service gets the generic echo instead,
# because an envelope's `ok: true` is a claim of success and we only know what success looks like
# for a route we mapped: slack_sdk's `files_upload_v2` reads `upload_url = None` off a success and
# dies inside urllib, while the echo leaves it raising the `SlackApiError` callers already catch.
ENVELOPED: dict[str, Callable[[Request, Route, dict[str, Any] | None, TsClock], dict[str, Any]]] = {
    services.slack.SERVICE: slack.enveloped
}


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


def _level(
    classification: Classification,
) -> tuple[FakeLevel, tuple[str, ...], dict[str, Any] | None]:
    """The level, its flags, and the route's fixture object when the level is L1."""
    route = classification.route
    if route is None or not route.fixture:
        return "fake-L0", (), None
    obj = fixture.get(classification.service, route.fixture)
    if obj is None:
        return "fake-L0", (FIXTURE_FAILED_FLAG,), None
    return "fake-L1", (), obj


def fake_level(classification: Classification) -> tuple[FakeLevel, tuple[str, ...]]:
    """The level a write on this route is answered at, and the flag it owes the trace, without
    building (or minting) a body: L1 when the route names a fixture this install can read, L0
    with `fixture-failed` when it names one it cannot, and L0 otherwise."""
    level, flags, _ = _level(classification)
    return level, flags


def _fake_dict(request: Request, classification: Classification, clock: TsClock) -> Built:
    """The body as a dict, the level it was built at, and any flag that level owes the trace."""
    route = classification.route
    service = classification.service
    level, flags, obj = _level(classification)
    build = ENVELOPED.get(service)
    if route is not None and build is not None:
        return build(request, route, obj, clock), level, flags
    l0_body, l1_body = BODIES.get(service, GENERIC_BODIES)
    if route is not None and obj is not None:
        return l1_body(request, route, obj), level, flags
    return l0_body(request, route), level, flags


def fake_response(request: Request, classification: Classification, clock: TsClock) -> Fake:
    """The body, content type and fidelity of a locally answered write. `clock` is the engine's
    Slack `ts` sequence.

    The single place the body is serialized.
    """
    route = classification.route
    if route is not None:
        literal = LITERAL_BODIES.get((classification.service, route.operation))
        if literal is not None:
            return Fake(literal[0], literal[1])
    body, answered_by, flags = _fake_dict(request, classification, clock)
    return Fake(json.dumps(body, allow_nan=False).encode(), JSON_CT, answered_by, flags)


def fake_rejection(classification: Classification, body: dict[str, Any]) -> Fake:
    """A modeled error body, carried at the level this route's write WOULD have been answered at.

    There is no `fake-L3`: the header says who answered, the exchange field says what L3 decided.
    The level is `fake_level`'s, so a Slack `reactions.add` (no fixture) is L0 and a
    `chat.postMessage` whose fixture will not load is L0 with `fixture-failed`, and nothing is
    minted for a write that never happened.
    """
    answered_by, flags = fake_level(classification)
    return Fake(json.dumps(body, allow_nan=False).encode(), JSON_CT, answered_by, flags)
