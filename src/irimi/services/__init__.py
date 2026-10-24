"""What irimi knows about each service, and the body a faked write is answered with.

`REGISTRY` holds one `model.Service` per service package; unset fields behave as `generic`.
An unreadable fixture falls back to the echo, flagged `fixture-failed`.
Only `check`, run at load, raises; the answer path never does (fail-closed hook rule).
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
    """Return the spec of `service`, or an empty one when irimi has none."""
    return REGISTRY.get(service) or Service(name=service)


def scope_of(service: str, request: Request) -> tuple[str, ...]:
    """Return the request's `Service.scope_headers` values, "" for each one it lacks."""
    return tuple(request.header(name) or "" for name in of(service).scope_headers)


def probe_headers_of(service: str) -> tuple[str, ...]:
    """Return the credential headers a precondition read for `service` carries."""
    return of(service).probe_headers


def check(index: MapIndex) -> None:
    """Refuse maps the registry disagrees with. Raises MapError naming the file.

    Specs key on service, operation, `precondition:` and `fixture:` names, so a rename or typo on
    either side would silently switch an effect, check or fixture off.
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


def observe_read(service: str, body: bytes, clock: AnswerClock) -> None:
    """Let a service learn from a live read's body as it passes. Never raises."""
    observer = of(service).observe_read
    if observer is not None:
        observer(clock, body)


@dataclass(frozen=True)
class Fake:
    """A faked write's answer: encoded body, content type and fidelity."""

    body: bytes
    content_type: str
    answered_by: FakeLevel = "fake-L0"  # also the `Irimi-Answered-By` value
    flags: tuple[Flag, ...] = ()


def _level(
    classification: Classification,
) -> tuple[FakeLevel, tuple[Flag, ...], dict[str, Any] | None]:
    """Return the answer level, its flags, and the fixture object when there is one."""
    route = classification.route
    if route is None or not route.fixture:
        return "fake-L0", (), None
    obj = generic.fixture_object(classification.service, route.fixture)
    if obj is None:
        return "fake-L0", (Flag.FIXTURE_FAILED,), None
    return "fake-L1", (), obj


def fake_level(classification: Classification) -> tuple[FakeLevel, tuple[Flag, ...]]:
    """Return the answer level and flags for a write on this route, without building a body."""
    level, flags, _ = _level(classification)
    return level, flags


def fake_response(request: Request, classification: Classification, clock: AnswerClock) -> Fake:
    """Build a faked write's answer. `clock` is the policy's sequence (Slack's `ts`)."""
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
    """Answer a precondition rejection with `body`, at the level `fake_level` gives the route.

    There is no `fake-L3`: the header says who answered; the exchange records the check's result.
    """
    answered_by, flags = fake_level(classification)
    return Fake(json.dumps(body, allow_nan=False).encode(), JSON_CT, answered_by, flags)
