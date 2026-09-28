"""Trace format v1 (#68): every record survives its codec, and the reference page decodes."""

import dataclasses
import hashlib
import itertools
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
# trip below fails. And every value differs from every other field's, of any type, in the same
# record and the records nested in it: a codec that writes one field's value under another's key
# (`"ended_at": ex.started_at`) then decodes to a different record, and fails the round trip too.


def _sample(tp, name, n, default=dataclasses.MISSING):
    """A value of type `tp` for the field `name`, unique by way of `n` (one counter per record)."""
    if tp is trace.JSONValue:
        return {"field": name, "n": next(n), "list": [1, 2.5, "s", None, True], "nested": {"k": []}}
    if dataclasses.is_dataclass(tp):
        return _filled(tp, n)
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
    origin, args = typing.get_origin(tp), typing.get_args(tp)
    if origin is typing.Literal:
        choices = [a for a in args if a != default]
        return choices[next(n) % len(choices)]
    if origin in (typing.Union, types.UnionType):
        (arm,) = [a for a in args if a is not type(None)]
        return _sample(arm, name, n, default)
    if origin is tuple and len(args) == 2 and args[1] is Ellipsis:
        # Three distinct items, neither ascending nor descending: a codec that sorts, reverses or
        # dedups the sequence fails. For #71's `stream_chunks: tuple[int, ...]` as much as `flags`.
        first, second, third = (_sample(args[0], f"{name}{i}", n) for i in range(3))
        return (second, first, third)
    if origin is tuple:
        return tuple(_sample(a, f"{name}{i}", n) for i, a in enumerate(args))
    if isinstance(tp, typing.TypeAliasType):
        return _sample(tp.__value__, name, n, default)
    raise AssertionError(f"no sample for field {name!r} of type {tp!r}: teach _sample the type")


def _filled(cls, n=None):
    n = itertools.count(1) if n is None else n
    values = {}
    for f in dataclasses.fields(cls):
        default = f.default
        if f.default_factory is not dataclasses.MISSING:
            default = f.default_factory()
        value = _sample(f.type, f.name, n, default)
        if default is not dataclasses.MISSING:
            assert value != default, f"{cls.__name__}.{f.name} sample equals its default"
        values[f.name] = value
    return cls(**values)


def _run_record(**changes):
    """A filled RunRecord this version reads: `_filled` knows nothing of `SCHEMA_VERSION`."""
    return dataclasses.replace(_filled(RunRecord), schema_version=SCHEMA_VERSION, **changes)


def test_the_samples_differ_from_field_to_field():
    """The sampler's own promise, checked: no two leaf values in a filled record are equal."""
    leaves = []

    def walk(value):
        if dataclasses.is_dataclass(value):
            for f in dataclasses.fields(value):
                walk(getattr(value, f.name))
        elif isinstance(value, tuple):
            for item in value:
                walk(item)
        elif not isinstance(value, bool):  # one True per record is all a bool can offer
            leaves.append(json.dumps(value) if isinstance(value, dict) else value)

    for cls in (Exchange, RunRecord, ToolCall, TelemetrySeen):
        leaves.clear()
        walk(_filled(cls))
        assert len(leaves) == len(set(leaves)), f"{cls.__name__} repeats a sample value"


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
    record = _run_record()
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
    record = _run_record(trigger=trigger)
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


def test_get_body_is_handed_the_ref_as_stored_truncated_and_size_included():
    """The codec does not rebuild a ref from the bytes: a truncating store (#70) says what it kept,
    and that is what comes back to it."""
    body = b"the first bytes of a longer body"
    stored = BodyRef(hashlib.sha256(body).hexdigest(), 8 * 1024 * 1024, True)
    ex = dataclasses.replace(
        _filled(Exchange), request=dataclasses.replace(_filled(Request), body=body), response=None
    )
    encoded = _json_round_trip(trace.exchange_to_json(ex, lambda _: stored))
    assert encoded["request"]["body"] == {
        "sha256": stored.sha256,
        "size": stored.size,
        "truncated": True,
    }
    asked = []

    def get_body(ref: BodyRef) -> bytes:
        asked.append(ref)
        return body

    assert trace.exchange_from_json(encoded, get_body).request.body == body
    assert asked == [stored]


def test_body_ref_is_the_bodys_own_digest_and_length():
    assert trace.body_ref(b"abc") == BodyRef(hashlib.sha256(b"abc").hexdigest(), 3, False)
    assert trace.body_ref(b"abc", truncated=True).truncated


# ---------------------------------------------------------------------- versions and bad records


def _encoded_run():
    return trace.run_to_json(_run_record())


def test_a_newer_schema_version_is_refused():
    d = _encoded_run() | {"schema_version": SCHEMA_VERSION + 1}
    with pytest.raises(TraceFormatError, match=f"schema_version {SCHEMA_VERSION + 1}"):
        trace.run_from_json(d)


def test_an_unknown_extra_field_is_ignored():
    d = _encoded_run() | {"added_in_a_later_version": {"x": 1}}
    assert trace.run_from_json(d) == trace.run_from_json(_encoded_run())
    blobs = _Blobs()
    ex = _filled(Exchange)
    line = trace.exchange_to_json(ex, blobs.put) | {"added_later": True}
    line["request"] = line["request"] | {"added_later": True}
    assert trace.exchange_from_json(line, blobs.get) == ex


@pytest.mark.parametrize("version", [0, -1])
def test_a_schema_version_below_the_first_is_refused(version):
    with pytest.raises(TraceFormatError, match=f"schema_version {version}"):
        trace.run_from_json(_encoded_run() | {"schema_version": version})


# Values of any JSON, so not records whose keys a decoder requires.
_FREE_JSON = frozenset({"args", "result"})
_EVENTS = {
    "exchange": _filled(Exchange),
    "tool_call": _filled(ToolCall),
    "telemetry": _filled(TelemetrySeen),
}


def _decodable(name):
    """A freshly encoded record of kind `name`, and the one-argument decoder that reads it."""
    blobs = _Blobs()
    if name == "run":
        return _encoded_run(), trace.run_from_json
    if name == "exchange":
        encoded = trace.exchange_to_json(_filled(Exchange), blobs.put)
        return encoded, lambda d: trace.exchange_from_json(d, blobs.get)
    if name == "tool_call":
        return trace.tool_call_to_json(_filled(ToolCall)), trace.tool_call_from_json
    if name == "telemetry":
        return trace.telemetry_to_json(_filled(TelemetrySeen)), trace.telemetry_from_json
    line = trace.event_to_json(1, _EVENTS[name.removesuffix(" line")], blobs.put)
    return line, lambda d: trace.event_from_json(d, blobs.get)


def _key_paths(d, path=()):
    """Every key of `d` and of every record nested in it, as a path from the top."""
    for key, value in d.items():
        yield (*path, key)
        if isinstance(value, dict) and key not in _FREE_JSON:
            yield from _key_paths(value, (*path, key))


_REQUIRED = [
    (name, path)
    for name in ("run", "exchange", "tool_call", "telemetry")
    for path in _key_paths(_decodable(name)[0])
] + [(f"{event} line", (key,)) for event in _EVENTS for key in ("seq", "type")]


@pytest.mark.parametrize(
    ("name", "path"), _REQUIRED, ids=[f"{name}:{'.'.join(path)}" for name, path in _REQUIRED]
)
def test_a_missing_required_field_is_a_trace_format_error(name, path):
    """Every key every encoder writes - the run, its trigger and error, the exchange, its request,
    response and body refs, the tool call and its error, telemetry, and an event line's own `seq`
    and `type` - is one v1 requires. A field added later within v1 decodes as its default when
    absent instead (see Versioning in the page), and whoever adds it exempts it here."""
    d, decode = _decodable(name)
    *parents, key = path
    holder = d
    for parent in parents:
        holder = holder[parent]
    del holder[key]
    with pytest.raises(TraceFormatError, match=re.escape(f"missing required field {key!r}")):
        decode(d)


def _never(ref):
    raise AssertionError(f"get_body was asked for {ref!r}")


_DECODERS = {
    "run_from_json": trace.run_from_json,
    "tool_call_from_json": trace.tool_call_from_json,
    "telemetry_from_json": trace.telemetry_from_json,
    "exchange_from_json": lambda d: trace.exchange_from_json(d, _never),
    "event_from_json": lambda d: trace.event_from_json(d, _never),
}


def test_every_public_decoder_is_in_the_not_an_object_test():
    assert set(_DECODERS) == {
        name for name in dir(trace) if name.endswith("_from_json") and not name.startswith("_")
    }


@pytest.mark.parametrize("decoder", list(_DECODERS))
@pytest.mark.parametrize("text", ["null", "5", '"seq"', "[]"])
def test_a_line_that_is_not_an_object_is_a_trace_format_error(decoder, text):
    """What `json.loads` makes of a damaged line need not be a dict, and TypeError is not the
    one exception a store catches (#70)."""
    with pytest.raises(TraceFormatError, match="object, not"):
        _DECODERS[decoder](json.loads(text))


@pytest.mark.parametrize(
    "number",
    ["1" + "0" * 400, "NaN", "Infinity", "-Infinity"],
    ids=["10**400", "nan", "inf", "-inf"],
)
def test_a_timestamp_that_is_not_a_finite_float_is_a_trace_format_error(number):
    """`json.loads` reads each of these by default: an int no float can hold, and the three
    non-finite values `json.dumps(..., allow_nan=False)` could never write back."""
    value = json.loads(number)
    with pytest.raises(TraceFormatError, match="started_at"):
        trace.telemetry_from_json(
            json.loads(f'{{"run_id": "r", "host": "h", "started_at": {number}}}')
        )
    with pytest.raises(TraceFormatError, match="ended_at"):
        trace.run_from_json(_encoded_run() | {"ended_at": value})
    blobs = _Blobs()
    encoded = trace.exchange_to_json(_filled(Exchange), blobs.put) | {"started_at": value}
    with pytest.raises(TraceFormatError, match="started_at"):
        trace.exchange_from_json(encoded, blobs.get)


@pytest.mark.parametrize("number", ["NaN", "Infinity", "-Infinity"])
def test_free_json_holding_a_number_that_is_not_finite_is_a_trace_format_error(number):
    """`args` and `result` hold any JSON, but not what `json.loads` alone would let in: a record
    holding `NaN` could not be written back with `allow_nan=False` (#68)."""
    nested = json.loads(f'{{"a": [1, {{"b": {number}}}]}}')
    call = trace.tool_call_to_json(_filled(ToolCall))
    for key in ("args", "result"):
        with pytest.raises(TraceFormatError, match=key):
            trace.tool_call_from_json(call | {key: nested})
    trigger = trace.run_to_json(_run_record())["trigger"] | {"args": nested}
    with pytest.raises(TraceFormatError, match="args"):
        trace.run_from_json(_encoded_run() | {"trigger": trigger})


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


def test_error_info_survives_an_exception_it_cannot_print():
    """The SDK records an agent's exception while it is in flight (#74, #76): a `__str__` that
    raises, or returns something that is not a string, must not raise a second one."""

    class Raises(Exception):
        def __str__(self):
            raise RuntimeError("no")

    class NotAString(Exception):
        def __str__(self):
            return 5

    for kind in (Raises, NotAString):
        assert ErrorInfo.from_exception(kind()) == ErrorInfo(
            f"{__name__}.{kind.__qualname__}", f"<unprintable {kind.__qualname__}>"
        )


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


def _doc_table(marker: str) -> list[str]:
    """The first column of the first table after `marker` on the page: one field name per row."""
    text = DOC.read_text()
    table = re.search(r"^\|.*?(?=^[^|]|\Z)", text[text.index(marker) :], re.M | re.S)
    assert table is not None, f"no table after {marker}"
    names = [row.split("|")[1].strip() for row in table.group(0).strip().splitlines()[2:]]
    assert all(re.fullmatch(r"`\w+`", name) for name in names), f"one field per row: {names}"
    return [name.strip("`") for name in names]


@pytest.mark.parametrize(
    ("marker", "encode"),
    [
        ("(`trace.RunRecord`)", lambda: trace.run_to_json(_run_record())),
        ("(`trace.Trigger`)", lambda: trace.run_to_json(_run_record())["trigger"]),
        (
            "(`irimi.exchange.Exchange`)",
            lambda: trace.exchange_to_json(_filled(Exchange), _Blobs().put),
        ),
        ("(`trace.ToolCall`)", lambda: trace.tool_call_to_json(_filled(ToolCall))),
    ],
    ids=["run", "trigger", "exchange", "tool_call"],
)
def test_each_field_table_on_the_page_is_what_its_encoder_writes(marker, encode):
    """A field the codec gained and the page did not, or the reverse, fails here: the page's table
    lists exactly the keys the encoder writes, in the order it writes them."""
    assert _doc_table(marker) == list(encode())


def test_the_documented_example_decodes_with_the_codecs():
    (run_text,) = _doc_blocks("run.json")
    record = trace.run_from_json(json.loads(run_text))
    assert (record.schema_version, record.attribution, record.outcome) == (
        SCHEMA_VERSION,
        "sdk",
        "ok",
    )

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
