"""Values the tests build over and over, each with defaults a test overrides by keyword.

A test that needs a different default says so in its own one-line wrapper, so the difference is
visible where it matters.
"""

import dataclasses
import enum
import itertools
import types
import typing
from dataclasses import replace

from irimi.core.exchange import Exchange, Precondition, Request, Response
from irimi.decide import idempotency
from irimi.decide.idempotency import FirstAnswer
from irimi.protocol import trace
from irimi.protocol.trace import RunRecord, ToolCall, Trigger

FORM_CONTENT_TYPE = "application/x-www-form-urlencoded"
FORM_HEADER = ("content-type", FORM_CONTENT_TYPE)


def request(**overrides) -> Request:
    """A GET of Stripe's charges list over HTTPS."""
    base = Request(
        method="GET",
        scheme="https",
        host="api.stripe.com",
        port=443,
        path="/v1/charges",
        query="",
        headers=(),
        body=b"",
    )
    return replace(base, **overrides)


def response(status: int = 200, headers: tuple[tuple[str, str], ...] = (), body: bytes = b""):
    """An answer with no headers and no body unless given."""
    return Response(status=status, headers=headers, body=body)


def exchange(**overrides) -> Exchange:
    """A live read of Stripe's charges list in run `7f3a`, answered 200 with an empty body."""
    fields = {
        "request": request(),
        "response": response(),
        "service": "api.stripe.com",
        "operation": "GET /v1/charges",
        "kind": "read",
        "answered_by": "live",
        "run_id": "7f3a",
    }
    return Exchange(**(fields | overrides))


def run_record(run_id: str = "7f3a", started_at: float | None = 10.0, **overrides) -> RunRecord:
    """A shadow-mode process run that has started and not ended."""
    record = RunRecord(
        schema_version=trace.SCHEMA_VERSION,
        run_id=run_id,
        mode="shadow",
        attribution="process",
        trigger=Trigger(name="agent", entrypoint=None, args=None, replayable=True),
        agent_version="1.2.3",
        engine_version="0.0.0",
        sdk_version=None,
        started_at=started_at,
    )
    return replace(record, **overrides)


def tool_call(**overrides) -> ToolCall:
    """A write tool's fake that ran in run `7f3a` and returned nothing."""
    call = ToolCall(
        tool_call_id="t",
        run_id="7f3a",
        name="db.mark_refunded",
        kind="write",
        ran="shadow",
        args={},
        result=None,
        error=None,
        started_at=1.0,
        ended_at=2.0,
    )
    return replace(call, **overrides)


# What the idempotency store keys a 100-cent refund of `ch_REAL1` on.
REFUND_PARAMS: idempotency.Canonical = (
    "POST",
    "/v1/refunds",
    "",
    {"charge": "ch_REAL1", "amount": 100},
)


def first_answer(body: bytes = b'{"id": "re_MINTED1"}') -> FirstAnswer:
    """The answer a keyed refund got the first time, which a retry is answered with."""
    return FirstAnswer(
        status=200,
        body=body,
        content_type="application/json",
        answered_by="fake-L1",
        flags=(),
        precondition=Precondition("passed", "", "usd"),
    )


# `filled(cls)`: a value in every field, built from the dataclass's own field types, so a field
# added later is filled - or, when its type is new here, refused - without anyone remembering to
# touch this file. Every value differs from the field's default: a field a codec forgot then
# decodes as its default, and a round trip fails. And every value differs from every other
# field's, of any type, in the same record and the records nested in it: a codec that writes one
# field's value under another's key (`"ended_at": ex.started_at`) then decodes to a different
# record, and fails the round trip too.


def sample(tp, name, n, default=dataclasses.MISSING):
    """A value of type `tp` for the field `name`, unique by way of `n` (one counter per record)."""
    if tp is trace.JSONValue:
        return {"field": name, "n": next(n), "list": [1, 2.5, "s", None, True], "nested": {"k": []}}
    if dataclasses.is_dataclass(tp):
        return filled(tp, n)
    if tp is str:
        return f"{name}-{next(n)}"
    if tp is bool:
        return True
    if tp is int:
        return next(n)
    if tp is float:
        # Epoch seconds, in quarters: exactly representable, so equality after JSON is exact.
        return 1790600000.0 + next(n) / 4
    if tp is bytes:
        return f"{name} body {next(n)}".encode()
    if isinstance(tp, type) and issubclass(tp, enum.Enum):
        members = list(tp)
        return members[next(n) % len(members)]
    origin, args = typing.get_origin(tp), typing.get_args(tp)
    if origin is typing.Literal:
        choices = [a for a in args if a != default]
        return choices[next(n) % len(choices)]
    if origin in (typing.Union, types.UnionType):
        (arm,) = [a for a in args if a is not type(None)]
        return sample(arm, name, n, default)
    if origin is tuple and len(args) == 2 and args[1] is Ellipsis:
        # Three distinct items, neither ascending nor descending: a codec that sorts, reverses or
        # dedups the sequence fails. For `stream_chunks: tuple[int, ...]` as much as `flags`.
        first, second, third = (sample(args[0], f"{name}{i}", n) for i in range(3))
        return (second, first, third)
    if origin is tuple:
        return tuple(sample(a, f"{name}{i}", n) for i, a in enumerate(args))
    if isinstance(tp, typing.TypeAliasType):
        return sample(tp.__value__, name, n, default)
    raise AssertionError(f"no sample for field {name!r} of type {tp!r}: teach sample the type")


def filled(cls, n=None):
    """A `cls` with every field set to a value unlike its default and unlike every other field's."""
    n = itertools.count(1) if n is None else n
    values = {}
    for f in dataclasses.fields(cls):
        default = f.default
        if f.default_factory is not dataclasses.MISSING:
            default = f.default_factory()
        value = sample(f.type, f.name, n, default)
        if default is not dataclasses.MISSING:
            assert value != default, f"{cls.__name__}.{f.name} sample equals its default"
        values[f.name] = value
    return cls(**values)


def demo_exchange(answered_by, kind="write", headers=()):
    """The demo map's `things.create`, answered by `answered_by` with `headers`, in the engine's
    run: what the invariant checkers are fed to show they catch a forged answer."""
    return exchange(
        request=request(method="POST", host="127.0.0.1", path="/things"),
        response=response(headers=headers, body=b"{}"),
        service="demo",
        operation="things.create",
        kind=kind,
        answered_by=answered_by,
        run_id="t3st",
    )


def refund_exchange(amount: int = 4900, **overrides) -> Exchange:
    """A refund of `ch_1` that irimi faked at L1 once its precondition check passed."""
    body = f"charge=ch_1&amount={amount}&currency=usd".encode()
    fields = {
        "request": request(method="POST", path="/v1/refunds", headers=(FORM_HEADER,), body=body),
        "response": response(body=b"{}"),
        "service": "stripe",
        "operation": "refunds.create",
        "kind": "write",
        "answered_by": "fake-L1",
        "run_id": "",
        "precondition": Precondition("passed"),
    }
    return exchange(**(fields | overrides))
