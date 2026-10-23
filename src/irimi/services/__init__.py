"""What irimi knows about each service, and the body a locally answered write gets.

Each service with Python knowledge of its own is a package (`stripe`, `slack`) whose `SERVICE` is
one `model.Service` spec, and `REGISTRY` holds them by name. A service with no spec, or a field a
spec leaves unset, gets the generic behaviour of `generic`. Nothing in this package knows about
mitmproxy, exchanges or flows, which is what lets a replay run the same effects over a recording.
`check` holds the specs to the loaded maps.

The body of a faked write:

**L0** is the floor and the fallback: the request's own fields reflected back as a JSON object,
plus an id minted for every field the matched route names in `ids:`. It is not a fixture - an SDK
has to be able to parse it and read the fields it just sent, and nothing more.

**L1** applies to a route whose map names a `fixture:`. It starts from that vendored response
object and writes the request's own fields over it, so the agent receives every field the real
service would have sent and not only the ones it posted. A fixture this install cannot read is not
an error: the answer degrades to L0 and the exchange is flagged.

Nothing here may raise. An exception inside a mitmproxy hook makes the flow forward untouched, and
a forwarded write escapes shadow mode - so a body that cannot be parsed reflects nothing instead,
and `ShadowPolicy` wraps the one serialization in a guard of its own.
"""

import json
from dataclasses import dataclass
from typing import Any

from irimi.core.bodies import JSON_CT
from irimi.core.exchange import FakeLevel, Flag, Request
from irimi.servicemaps import MapError, MapIndex
from irimi.servicemaps.classify import Classification
from irimi.services import generic, slack, stripe
from irimi.services.model import (
    NOT_EVALUABLE,
    Answer,
    AnswerClock,
    Applied,
    Check,
    Idempotency,
    NotEvaluable,
    Probe,
    Proposal,
    QueryRewrite,
    Read,
    ReadEffects,
    Rejection,
    Rewritten,
    Service,
    Write,
)
from irimi.services.slack.answers import TsClock

__all__ = [
    "NOT_EVALUABLE",
    "REGISTRY",
    "Answer",
    "AnswerClock",
    "Applied",
    "Check",
    "Fake",
    "Idempotency",
    "NotEvaluable",
    "Probe",
    "Proposal",
    "QueryRewrite",
    "Read",
    "ReadEffects",
    "Rejection",
    "Rewritten",
    "Service",
    "TsClock",
    "Write",
    "check",
    "fake_level",
    "fake_rejection",
    "fake_response",
    "observe_read",
    "of",
    "probe_headers_of",
    "scope_of",
]

# Every service irimi's Python knows, by its map's `service:` name.
REGISTRY: dict[str, Service] = {spec.name: spec for spec in (slack.SERVICE, stripe.SERVICE)}
GENERIC_ANSWER = Answer(body=generic.body)


def of(service: str) -> Service:
    """The spec of `service`, or an empty one for a service irimi has no Python knowledge of."""
    return REGISTRY.get(service) or Service(name=service)


def scope_of(service: str, request: Request) -> tuple[str, ...]:
    """The values of the headers that scope `service`'s state on this request
    (`Service.scope_headers`), "" for each one it does not carry. Two requests in different scopes
    say nothing about each other: the write log, the idempotency store and the precondition read
    all compare this."""
    return tuple(request.header(name) or "" for name in of(service).scope_headers)


def probe_headers_of(service: str) -> tuple[str, ...]:
    """The credential headers a precondition read for `service` carries."""
    return of(service).probe_headers


def check(index: MapIndex) -> None:
    """Refuse maps the registry does not agree with. Raises MapError naming the file.

    The specs key on the maps' service and operation names, and a route names its
    `precondition:` and `fixture:` by name, so a rename or a typo on either side would silently
    switch an effect, a check or a fixture off. Called once, when the maps load.
    """
    by_name = {sm.service: sm for sm in index.services}
    for service, spec in REGISTRY.items():
        sm = by_name.get(service)
        if sm is None:
            raise MapError(
                f"the service maps define no service {service!r}, which irimi's built-in "
                "tables key on"
            )
        missing = sorted(spec.operations - {route.operation for route in sm.routes})
        if missing:
            raise MapError(
                f"{sm.source}: no route names operation {missing[0]!r}, which irimi's "
                f"{service} tables key on"
            )
    for sm in index.services:
        for route in sm.routes:
            where = f"{sm.source}: route {route.method} {route.path}"
            if route.precondition and route.precondition not in of(sm.service).preconditions:
                raise MapError(
                    f"{where}: `precondition: {route.precondition}` names no check irimi has "
                    f"for {sm.service}"
                )
            if route.fixture and route.fixture not in generic.fixture_objects(sm.service):
                raise MapError(
                    f"{where}: `fixture: {route.fixture}` names no object in "
                    f"fixtures/{sm.service}.json"
                )


# --------------------------------------------------------------------------- answering a write


def observe_read(service: str, body: bytes, clock: AnswerClock) -> None:
    """Let a service learn from a read irimi forwarded rather than answered. Never raises.

    A read's body is the only place the real values a fake has to sort against appear: the write
    log holds writes, and the trace store is write-only. So a body is observed as it goes past
    rather than looked up later, and a service with nothing to learn is a no-op.
    """
    observer = of(service).observe_read
    if observer is not None:
        observer(clock, body)


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
    flags: tuple[Flag, ...] = ()


def _level(
    classification: Classification,
) -> tuple[FakeLevel, tuple[Flag, ...], dict[str, Any] | None]:
    """The level, its flags, and the route's fixture object when the level is L1."""
    route = classification.route
    if route is None or not route.fixture:
        return "fake-L0", (), None
    obj = generic.fixture_object(classification.service, route.fixture)
    if obj is None:
        return "fake-L0", (Flag.FIXTURE_FAILED,), None
    return "fake-L1", (), obj


def fake_level(classification: Classification) -> tuple[FakeLevel, tuple[Flag, ...]]:
    """The level a write on this route is answered at, and the flag it owes the trace, without
    building (or minting) a body: L1 when the route names a fixture this install can read, L0
    with `fixture-failed` when it names one it cannot, and L0 otherwise."""
    level, flags, _ = _level(classification)
    return level, flags


def fake_response(request: Request, classification: Classification, clock: AnswerClock) -> Fake:
    """The body, content type and fidelity of a locally answered write. `clock` is the
    policy's own sequence, which Slack's envelopes take their `ts` from.

    The single place the body is serialized.
    """
    route = classification.route
    answer = of(classification.service).answer or GENERIC_ANSWER
    if route is not None:
        literal = answer.literal.get(route.operation)
        if literal is not None:
            return Fake(literal[0], literal[1])
    level, flags, obj = _level(classification)
    body = answer.body(request, route, obj, clock)
    return Fake(json.dumps(body, allow_nan=False).encode(), JSON_CT, level, flags)


def fake_rejection(classification: Classification, body: dict[str, Any]) -> Fake:
    """A modeled error body, carried at the level this route's write WOULD have been answered at.

    There is no `fake-L3`: the header says who answered, the exchange field says what L3 decided.
    The level is `fake_level`'s, so a Slack `reactions.add` (no fixture) is L0 and a
    `chat.postMessage` whose fixture will not load is L0 with `fixture-failed`, and nothing is
    minted for a write that never happened.
    """
    answered_by, flags = fake_level(classification)
    return Fake(json.dumps(body, allow_nan=False).encode(), JSON_CT, answered_by, flags)
