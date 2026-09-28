"""Trace format v1 (#68): every record survives its codec, and the reference page decodes."""

import dataclasses
import hashlib
import json
import re
import types
import typing
from pathlib import Path

import pytest

from irimi import trace
from irimi.exchange import Exchange, Request, Response
from irimi.trace import (
    SCHEMA_VERSION,
    BodyRef,
    ErrorInfo,
    RunRecord,
    TelemetrySeen,
    ToolCall,
    TraceFormatError,
    Trigger,
)

DOC = Path(__file__).resolve().parents[1] / "docs" / "trace-format.md"

# ------------------------------------------------------------------------ a value in every field
#
# Built from the dataclass's own field types, so a field added later is filled - or, when its
# type is new here, refused - without anyone remembering to touch this file. Every value differs
# from the field's default: a field the codec forgot then decodes as its default, and the round
# trip below fails.


def _sample(tp, name):
    if tp is trace.JSONValue:
        return {"list": [1, 2.5, "s", None, True], "nested": {"k": []}}
    if dataclasses.is_dataclass(tp):
        return _filled(tp)
    if tp is str:
        return f"{name}-value"
    if tp is bool:
        return True
    if tp is int:
        return 7
    if tp is float:
        return 1790600000.25
    if tp is bytes:
        return f"{name} body".encode()
    origin, args = typing.get_origin(tp), typing.get_args(tp)
    if origin is typing.Literal:
        return args[-1]
    if origin in (typing.Union, types.UnionType):
        (arm,) = [a for a in args if a is not type(None)]
        return _sample(arm, name)
    if origin is tuple and len(args) == 2 and args[1] is Ellipsis:
        return (_sample(args[0], f"{name}0"), _sample(args[0], f"{name}1"))
    if origin is tuple:
        return tuple(_sample(a, f"{name}{i}") for i, a in enumerate(args))
    if isinstance(tp, typing.TypeAliasType):
        return _sample(tp.__value__, name)
    raise AssertionError(f"no sample for field {name!r} of type {tp!r}: teach _sample the type")


def _filled(cls):
    values = {}
    for f in dataclasses.fields(cls):
        value = _sample(f.type, f.name)
        if f.default is not dataclasses.MISSING:
            assert value != f.default, f"{cls.__name__}.{f.name} sample equals its default"
        if f.default_factory is not dataclasses.MISSING:
            assert value != f.default_factory(), f"{cls.__name__}.{f.name} sample is its default"
        values[f.name] = value
    return cls(**values)


class _Blobs:
    """An in-memory body store with the contract the codec assumes of the real one (#70)."""

    def __init__(self):
        self.blobs: dict[str, bytes] = {}
        self.puts: list[bytes] = []

    def put(self, body: bytes) -> BodyRef | None:
        self.puts.append(body)
        if not body:
            return None
        ref = trace.body_ref(body)
        self.blobs[ref.sha256] = body
        return ref

    def get(self, ref: BodyRef) -> bytes:
        return self.blobs[ref.sha256]


def _json_round_trip(d):
    """What a store does to a record: through text and back, with no NaN or Infinity allowed."""
    return json.loads(json.dumps(d, allow_nan=False))


# ------------------------------------------------------------------------------ the round trips


def test_every_exchange_field_survives_the_codec():
    ex = _filled(Exchange)
    blobs = _Blobs()
    encoded = trace.exchange_to_json(ex, blobs.put)
    assert trace.exchange_from_json(_json_round_trip(encoded), blobs.get) == ex


def test_every_run_record_field_survives_the_codec():
    record = _filled(RunRecord)
    record = dataclasses.replace(record, schema_version=SCHEMA_VERSION)
    assert trace.run_from_json(_json_round_trip(trace.run_to_json(record))) == record


def test_every_tool_call_field_survives_the_codec():
    call = _filled(ToolCall)
    assert trace.tool_call_from_json(_json_round_trip(trace.tool_call_to_json(call))) == call


def test_every_telemetry_field_survives_the_codec():
    seen = _filled(TelemetrySeen)
    assert trace.telemetry_from_json(_json_round_trip(trace.telemetry_to_json(seen))) == seen


def test_the_optional_fields_survive_as_null():
    """The other arm of every `X | None`: `_filled` only ever builds the X."""
    record = RunRecord(
        schema_version=SCHEMA_VERSION,
        run_id="r1",
        mode="shadow",
        attribution="header",
        trigger=None,
        agent_version=None,
        engine_version="0.0.1",
        sdk_version=None,
        started_at=None,
        ended_at=None,
        outcome=None,
        error=None,
        exit_code=None,
    )
    assert trace.run_from_json(_json_round_trip(trace.run_to_json(record))) == record
    ex = dataclasses.replace(_filled(Exchange), response=None, overlay=None, precondition=None)
    blobs = _Blobs()
    encoded = _json_round_trip(trace.exchange_to_json(ex, blobs.put))
    assert encoded["response"] is None
    assert trace.exchange_from_json(encoded, blobs.get) == ex


def test_trigger_args_hold_any_json():
    trigger = Trigger("t", None, [1, {"a": None}], False)
    record = dataclasses.replace(_filled(RunRecord), schema_version=1, trigger=trigger)
    assert trace.run_from_json(_json_round_trip(trace.run_to_json(record))).trigger == trigger


@pytest.mark.parametrize(
    "event",
    [_filled(Exchange), _filled(ToolCall), _filled(TelemetrySeen)],
    ids=["exchange", "tool_call", "telemetry"],
)
def test_an_event_line_carries_seq_and_type_and_decodes_back(event):
    blobs = _Blobs()
    line = trace.event_to_json(42, event, blobs.put)
    assert list(line)[:2] == ["seq", "type"]
    assert line["seq"] == 42
    assert trace.event_from_json(_json_round_trip(line), blobs.get) == (42, event)


# ---------------------------------------------------------------------------- bodies and headers


def test_bodies_never_go_inline_and_an_empty_one_is_null_and_stores_nothing():
    ex = dataclasses.replace(
        _filled(Exchange),
        request=dataclasses.replace(_filled(Request), body=b""),
        response=Response(status=200, headers=(), body=b'{"ok": true}'),
    )
    blobs = _Blobs()
    encoded = trace.exchange_to_json(ex, blobs.put)
    assert encoded["request"]["body"] is None
    assert encoded["response"]["body"] == {
        "sha256": hashlib.sha256(b'{"ok": true}').hexdigest(),
        "size": 12,
        "truncated": False,
    }
    assert blobs.puts == [b'{"ok": true}'], "an empty body reached the store"
    assert b'"ok"' not in json.dumps(encoded).encode()


def test_headers_keep_their_order_their_repeats_and_their_case():
    headers = (("Set-Cookie", "a=1"), ("X-B", "2"), ("Set-Cookie", "c=3"))
    ex = dataclasses.replace(_filled(Exchange), response=Response(200, headers, b""))
    blobs = _Blobs()
    encoded = _json_round_trip(trace.exchange_to_json(ex, blobs.put))
    assert encoded["response"]["headers"] == [
        ["Set-Cookie", "a=1"],
        ["X-B", "2"],
        ["Set-Cookie", "c=3"],
    ]
    assert trace.exchange_from_json(encoded, blobs.get).response.headers == headers


def test_a_body_ref_that_is_not_a_digest_is_refused_before_the_store_is_asked():
    """A store joins the digest onto `blobs/`, so a ref read off disk is a path it cannot trust."""
    blobs = _Blobs()
    encoded = trace.exchange_to_json(_filled(Exchange), blobs.put)
    encoded["request"]["body"]["sha256"] = "../../etc/passwd"

    def never(ref):
        raise AssertionError("get_body was asked for a ref that is not a digest")

    with pytest.raises(TraceFormatError, match="sha256"):
        trace.exchange_from_json(encoded, never)


def test_body_ref_is_the_bodys_own_digest_and_length():
    assert trace.body_ref(b"abc") == BodyRef(hashlib.sha256(b"abc").hexdigest(), 3, False)
    assert trace.body_ref(b"abc", truncated=True).truncated


# ---------------------------------------------------------------------- versions and bad records


def _encoded_run():
    return trace.run_to_json(dataclasses.replace(_filled(RunRecord), schema_version=SCHEMA_VERSION))


def test_a_newer_schema_version_is_refused():
    d = _encoded_run() | {"schema_version": SCHEMA_VERSION + 1}
    with pytest.raises(TraceFormatError, match="schema_version 2"):
        trace.run_from_json(d)


def test_an_unknown_extra_field_is_ignored():
    d = _encoded_run() | {"added_in_a_later_version": {"x": 1}}
    assert trace.run_from_json(d) == trace.run_from_json(_encoded_run())
    blobs = _Blobs()
    ex = _filled(Exchange)
    line = trace.exchange_to_json(ex, blobs.put) | {"added_later": True}
    line["request"] = line["request"] | {"added_later": True}
    assert trace.exchange_from_json(line, blobs.get) == ex


@pytest.mark.parametrize("key", ["schema_version", "run_id", "trigger", "dropped_events"])
def test_a_missing_required_field_is_a_trace_format_error(key):
    d = _encoded_run()
    del d[key]
    with pytest.raises(TraceFormatError, match=key):
        trace.run_from_json(d)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("kind", "delete"),  # outside its vocabulary
        ("started_at", "yesterday"),  # the wrong type
        ("flags", "fidelity:L0"),  # a string where a list belongs
        ("would_fire", [1]),
        ("request", None),  # required, never null
    ],
)
def test_a_malformed_exchange_is_a_trace_format_error_and_nothing_else(key, value):
    blobs = _Blobs()
    d = trace.exchange_to_json(_filled(Exchange), blobs.put) | {key: value}
    with pytest.raises(TraceFormatError, match=key):
        trace.exchange_from_json(d, blobs.get)


@pytest.mark.parametrize("pairs", [{"a": "1"}, [["a"]], [["a", 1]], ["a: 1"]])
def test_headers_that_are_not_name_value_pairs_are_refused(pairs):
    blobs = _Blobs()
    d = trace.exchange_to_json(_filled(Exchange), blobs.put)
    d["request"]["headers"] = pairs
    with pytest.raises(TraceFormatError, match="headers"):
        trace.exchange_from_json(d, blobs.get)


def test_a_boolean_is_not_an_integer_or_a_timestamp():
    with pytest.raises(TraceFormatError, match="dropped_events"):
        trace.run_from_json(_encoded_run() | {"dropped_events": True})
    with pytest.raises(TraceFormatError, match="started_at"):
        trace.telemetry_from_json({"run_id": "r", "host": "h", "started_at": False})


def test_an_integer_timestamp_is_read_as_a_float():
    seen = trace.telemetry_from_json({"run_id": "r", "host": "h", "started_at": 1790600000})
    assert seen.started_at == 1790600000.0
    assert isinstance(seen.started_at, float)


def test_an_unknown_event_type_is_refused():
    with pytest.raises(TraceFormatError, match="type"):
        trace.event_from_json({"seq": 1, "type": "screenshot"}, _Blobs().get)


# ---------------------------------------------------------------------------------- the helpers


@pytest.mark.parametrize(
    "run_id", ["t3st", "4f1c9a2b7d3e8a60", "a" * 64, "run_1-A", trace.UNATTRIBUTED]
)
def test_a_valid_run_id(run_id):
    assert trace.is_valid_run_id(run_id)


@pytest.mark.parametrize("run_id", ["", "../x", "a" * 65, "a b", "a/b", "a.b", "..", "a\n", "é"])
def test_an_invalid_run_id(run_id):
    assert not trace.is_valid_run_id(run_id)


def test_error_info_names_the_exception_by_module_and_qualname_and_caps_the_message():
    class Boom(Exception):
        pass

    info = ErrorInfo.from_exception(Boom("x" * 5000))
    assert info.type == f"{__name__}.{Boom.__qualname__}"
    assert info.message == "x" * trace.MAX_ERROR_MESSAGE
    assert ErrorInfo.from_exception(ValueError("bad")) == ErrorInfo("builtins.ValueError", "bad")


# --------------------------------------------------------------------------- the reference page

EXAMPLE_BLOBS = (
    b'{"id":"ch_3PqA1","object":"charge","amount":4900,"amount_refunded":0,"currency":"usd",'
    b'"refunded":false}',
    b"charge=ch_3PqA1&amount=4900",
    b'{"id":"re_Kd82nQ4xT1bV9mZ3pL6wR0yS","object":"refund","amount":4900,"charge":"ch_3PqA1",'
    b'"currency":"usd","status":"succeeded"}',
)


def _doc_blocks(info: str) -> list[str]:
    return re.findall(rf"^```json {re.escape(info)}\n(.*?)^```$", DOC.read_text(), re.M | re.S)


def test_the_documented_example_decodes_with_the_codecs():
    (run_text,) = _doc_blocks("run.json")
    record = trace.run_from_json(json.loads(run_text))
    assert (record.schema_version, record.attribution, record.outcome) == (1, "sdk", "ok")

    blobs = {trace.body_ref(b).sha256: b for b in EXAMPLE_BLOBS}

    def get_body(ref: BodyRef) -> bytes:
        body = blobs[ref.sha256]  # a ref the page names must be a real digest of a listed blob
        assert ref.size == len(body)
        return body

    lines = [
        trace.event_from_json(json.loads(text), get_body) for text in _doc_blocks("events.jsonl")
    ]
    assert [seq for seq, _ in lines] == [1, 2, 3, 4]
    (_, check), (_, refund), (_, tool), (_, seen) = lines
    assert isinstance(check, Exchange) and isinstance(refund, Exchange)
    assert (check.issued_by, refund.precondition) == ("engine", "passed")
    assert refund.request.body == b"charge=ch_3PqA1&amount=4900"
    assert isinstance(tool, ToolCall) and isinstance(seen, TelemetrySeen)
    assert all(ev.run_id == record.run_id for _, ev in lines)
    # THE ORDERING RULE, as the page shows it: `seq` is completion order, `started_at` start order.
    assert refund.started_at < check.started_at <= check.ended_at < refund.ended_at


def test_the_documented_example_is_what_the_encoders_write():
    """The page is not a hand-written approximation: re-encoding each decoded record gives back
    the page's own JSON, key for key."""
    (run_text,) = _doc_blocks("run.json")
    assert trace.run_to_json(trace.run_from_json(json.loads(run_text))) == json.loads(run_text)
    blobs = {trace.body_ref(b).sha256: b for b in EXAMPLE_BLOBS}
    for text in _doc_blocks("events.jsonl"):
        documented = json.loads(text)
        seq, event = trace.event_from_json(documented, lambda ref: blobs[ref.sha256])
        assert trace.event_to_json(seq, event, trace.body_ref) == documented
