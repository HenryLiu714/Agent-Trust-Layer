"""The trace format: every record survives its codec, and the reference page decodes."""

import asyncio
import dataclasses
import hashlib
import json
import re
import typing
from pathlib import Path

import pytest

from irimi.core import limits
from irimi.core.exchange import (
    AnsweredBy,
    Door,
    Exchange,
    Flag,
    IssuedBy,
    Kind,
    OverlayCoverage,
    Precondition,
    PreconditionOutcome,
    Request,
    Response,
)
from irimi.protocol import control as protocol_control
from irimi.protocol import trace
from irimi.protocol.control import (
    RecordedOutcome,
    recorded_outcome_from_json,
    recorded_outcome_to_json,
)
from irimi.protocol.trace import (
    SCHEMA_VERSION,
    BodyRef,
    ErrorInfo,
    RunRecord,
    TelemetrySeen,
    ToolCall,
    TraceFormatError,
    Trigger,
)
from tests.support import builders

DOC = Path(__file__).resolve().parents[1] / "docs" / "trace-format.md"


def _run_record(**changes):
    """A filled RunRecord this version reads.

    `filled` knows neither `SCHEMA_VERSION` nor that a trigger holds args or a command, not both."""
    filled = builders.filled(RunRecord)
    trigger = dataclasses.replace(filled.trigger, argv=None)
    changes = {"schema_version": SCHEMA_VERSION, "trigger": trigger, **changes}
    return dataclasses.replace(filled, **changes)


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
        walk(builders.filled(cls))
        assert len(leaves) == len(set(leaves)), f"{cls.__name__} repeats a sample value"


class _Blobs:
    """An in-memory body store with the contract the codec assumes of the real one."""

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
    ex = builders.filled(Exchange)
    blobs = _Blobs()
    encoded = trace.exchange_to_json(ex, blobs.put)
    assert trace.exchange_from_json(_json_round_trip(encoded), blobs.get) == ex


def test_every_run_record_field_survives_the_codec():
    record = _run_record()
    assert trace.run_from_json(_json_round_trip(trace.run_to_json(record))) == record


def test_every_tool_call_field_survives_the_codec():
    call = builders.filled(ToolCall)
    assert trace.tool_call_from_json(_json_round_trip(trace.tool_call_to_json(call))) == call


def test_every_telemetry_field_survives_the_codec():
    seen = builders.filled(TelemetrySeen)
    assert trace.telemetry_from_json(_json_round_trip(trace.telemetry_to_json(seen))) == seen


def test_the_optional_fields_survive_as_null():
    """The other arm of every `X | None`: `filled` only ever builds the X."""
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
        end=None,
    )
    assert trace.run_from_json(_json_round_trip(trace.run_to_json(record))) == record
    ex = dataclasses.replace(
        builders.filled(Exchange), response=None, overlay=None, precondition=Precondition()
    )
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
    [builders.filled(Exchange), builders.filled(ToolCall), builders.filled(TelemetrySeen)],
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
        builders.filled(Exchange),
        request=dataclasses.replace(builders.filled(Request), body=b""),
        response=Response(status=200, headers=(), body=b'{"ok": true}'),
    )
    blobs = _Blobs()
    encoded: typing.Any = trace.exchange_to_json(ex, blobs.put)
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
    ex = dataclasses.replace(builders.filled(Exchange), response=Response(200, headers, b""))
    blobs = _Blobs()
    encoded = _json_round_trip(trace.exchange_to_json(ex, blobs.put))
    assert encoded["response"]["headers"] == [
        ["Set-Cookie", "a=1"],
        ["X-B", "2"],
        ["Set-Cookie", "c=3"],
    ]
    decoded = trace.exchange_from_json(encoded, blobs.get).response
    assert decoded is not None
    assert decoded.headers == headers


def test_a_body_ref_that_is_not_a_digest_is_refused_before_the_store_is_asked():
    """A store joins the digest onto `blobs/`, so a ref read off disk is a path it cannot trust."""
    blobs = _Blobs()
    encoded: typing.Any = trace.exchange_to_json(builders.filled(Exchange), blobs.put)
    encoded["request"]["body"]["sha256"] = "../../etc/passwd"

    def never(ref):
        raise AssertionError("get_body was asked for a ref that is not a digest")

    with pytest.raises(TraceFormatError, match="sha256"):
        trace.exchange_from_json(encoded, never)


def test_get_body_is_handed_the_ref_as_stored_truncated_and_size_included():
    """The codec hands back the ref as stored, so a truncating store gets back what it kept."""
    body = b"the first bytes of a longer body"
    stored = BodyRef(hashlib.sha256(body).hexdigest(), 8 * 1024 * 1024, True)
    ex = dataclasses.replace(
        builders.filled(Exchange),
        request=dataclasses.replace(builders.filled(Request), body=body),
        response=None,
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


@pytest.mark.parametrize(
    "version", [SCHEMA_VERSION + 1, 10**5000], ids=["the-next", "past-the-digit-limit"]
)
def test_a_newer_schema_version_is_refused(version):
    """A newer schema version is refused without quoting it.

    `str` of an int past `sys.get_int_max_str_digits()` raises ValueError, and a decoder raises only
    TraceFormatError."""
    d = _encoded_run() | {"schema_version": version}
    with pytest.raises(TraceFormatError, match="schema_version is newer than this irimi reads"):
        trace.run_from_json(d)


def test_an_unknown_extra_field_is_ignored():
    d = _encoded_run() | {"added_in_a_later_version": {"x": 1}}
    assert trace.run_from_json(d) == trace.run_from_json(_encoded_run())
    blobs = _Blobs()
    ex = builders.filled(Exchange)
    line: typing.Any = trace.exchange_to_json(ex, blobs.put) | {"added_later": True}
    line["request"] = line["request"] | {"added_later": True}
    assert trace.exchange_from_json(line, blobs.get) == ex


def test_a_schema_2_exchange_reads_without_its_validation_and_fidelity_flag():
    """A schema 2 exchange reads; its `validation` and its unknown flags are ignored."""
    blobs = _Blobs()
    ex = dataclasses.replace(builders.filled(Exchange), flags=(Flag.FROM_RECORDING,))
    line = trace.exchange_to_json(ex, blobs.put) | {
        "validation": "unvalidated",
        "flags": ["fidelity:L1", "from-recording", "added-later"],
    }
    assert trace.exchange_from_json(_json_round_trip(line), blobs.get) == ex


@pytest.mark.parametrize("version", [0, -1, pytest.param(-(10**5000), id="past-the-digit-limit")])
def test_a_schema_version_below_the_first_is_refused(version):
    with pytest.raises(TraceFormatError, match="schema_version is below 1"):
        trace.run_from_json(_encoded_run() | {"schema_version": version})


# Values of any JSON, so not records whose keys a decoder requires.
_FREE_JSON = frozenset({"args", "result"})

_EVENTS = {
    "exchange": builders.filled(Exchange),
    "tool_call": builders.filled(ToolCall),
    "telemetry": builders.filled(TelemetrySeen),
}


def _decodable(name):
    """A freshly encoded record of kind `name`, and the one-argument decoder that reads it."""
    blobs = _Blobs()
    if name == "run":
        return _encoded_run(), trace.run_from_json
    if name == "exchange":
        encoded = trace.exchange_to_json(builders.filled(Exchange), blobs.put)
        return encoded, lambda d: trace.exchange_from_json(d, blobs.get)
    if name == "tool_call":
        return trace.tool_call_to_json(builders.filled(ToolCall)), trace.tool_call_from_json
    if name == "telemetry":
        return trace.telemetry_to_json(builders.filled(TelemetrySeen)), trace.telemetry_from_json
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
    """Every key every encoder writes is required, an event line's own `seq` and `type` included."""
    d, decode = _decodable(name)
    *parents, key = path
    holder = d
    for parent in parents:
        holder = holder[parent]
    del holder[key]
    with pytest.raises(TraceFormatError, match=re.escape(f"missing required field {key!r}")):
        decode(d)


def _nested_list(depth):
    """A list nested `depth` deep, built without recursion."""
    value: list = []
    for _ in range(depth):
        value = [value]
    return value


# Deeper than `repr` goes on any Python or stack irimi runs on, and within what 3.14's `json.loads`
# reads, so what an agent may post to the control endpoint.
_DEEP = 100_000

# A field that holds a list is refused at its first item, by its place.
_FIRST_ITEM_REFUSED = {
    "flags": "'flags'[0] is a list, not a string",
    "would_fire": "'would_fire'[0] is a list, not a string",
    "headers": "'headers'[0] is not a [name, value] pair of strings",
    "stream_chunks": "'stream_chunks'[0] is not a positive integer",
}

_TYPED = [(name, path) for name, path in _REQUIRED if path[-1] not in _FREE_JSON]


@pytest.mark.parametrize(
    ("name", "path"), _TYPED, ids=[f"{name}:{'.'.join(path)}" for name, path in _TYPED]
)
def test_a_value_nested_too_deep_to_quote_is_refused_by_its_type(name, path):
    """A 100,000-deep list outside a free JSON field is refused by its type, never quoted.

    `repr` of one raises RecursionError on some Pythons and stacks, so quoting it would make the
    control endpoint's answer differ between machines."""
    d, decode = _decodable(name)
    *parents, key = path
    holder = d
    for parent in parents:
        holder = holder[parent]
    holder[key] = _nested_list(_DEEP)
    with pytest.raises(TraceFormatError) as refused:
        decode(d)
    expected = _FIRST_ITEM_REFUSED.get(key, f"{key!r} is a list, not ")
    assert str(refused.value).startswith(expected)
    assert "[[" not in str(refused.value)


def _nested_object(depth):
    """An object nested `depth` deep, built without recursion."""
    value: dict = {}
    for _ in range(depth):
        value = {"a": value}
    return value


_FREE = [(name, path) for name, path in _REQUIRED if path[-1] in _FREE_JSON]


@pytest.mark.parametrize("nested", [_nested_list, _nested_object], ids=["a-list", "an-object"])
@pytest.mark.parametrize(
    ("name", "path"), _FREE, ids=[f"{name}:{'.'.join(path)}" for name, path in _FREE]
)
def test_a_free_json_field_nested_too_deep_to_quote_is_read_back_whole(name, path, nested):
    """A deep `args` or `result` reads back whole: the non-finite check does not recurse."""
    d, decode = _decodable(name)
    *parents, key = path
    holder = d
    for parent in parents:
        holder = holder[parent]
    holder[key] = value = nested(_DEEP)
    record = decode(d)
    for field in path:
        record = getattr(record, field)
    assert record is value


@pytest.mark.parametrize(
    ("value", "named"),
    [
        (None, "null"),
        (True, "a boolean"),
        (0, "an integer"),
        (10**5000, "an integer"),
        (0.5, "a number"),
        ("", "a string"),
        ([], "a list"),
        ({}, "an object"),
        ((), "a tuple"),
    ],
    ids=[
        "null",
        "boolean",
        "integer",
        "integer-past-the-digit-limit",
        "number",
        "string",
        "list",
        "object",
        "not-json",
    ],
)
def test_a_refused_value_is_named_by_its_json_type(value, named):
    """A refused value is named by its JSON type, never quoted; a boolean is not an integer."""
    assert trace.json_type(value) == named


@pytest.mark.parametrize(
    ("length", "quoted"),
    [
        (trace.MAX_QUOTED, f"'{'z' * trace.MAX_QUOTED}'"),
        (trace.MAX_QUOTED + 1, f"'{'z' * trace.MAX_QUOTED}'…"),
        (5_000_000, f"'{'z' * trace.MAX_QUOTED}'…"),
    ],
    ids=["max-quoted", "one-past", "5-mb"],
)
def test_a_string_a_refusal_quotes_is_cut_to_max_quoted_characters(length, quoted):
    """A quoted bad word or digest is cut, so a damaged 5 MB string is never a 5 MB message."""
    blobs = _Blobs()
    d: typing.Any = trace.exchange_to_json(builders.filled(Exchange), blobs.put) | {
        "kind": "z" * length
    }
    with pytest.raises(TraceFormatError) as refused:
        trace.exchange_from_json(d, blobs.get)
    assert str(refused.value) == f"'kind' is {quoted}, not one of {typing.get_args(Kind)}"
    d = trace.exchange_to_json(builders.filled(Exchange), blobs.put)
    d["request"]["body"]["sha256"] = "z" * length
    with pytest.raises(TraceFormatError) as refused:
        trace.exchange_from_json(d, blobs.get)
    assert str(refused.value) == f"{quoted} is not a sha256 digest"


def test_a_quoted_string_spells_its_control_characters():
    """A quoted string goes through `repr`, so a terminal shows an escape sequence, not obeys it."""
    with pytest.raises(TraceFormatError) as refused:
        trace.run_from_json(_encoded_run() | {"mode": "\x1b[2J\n"})
    assert str(refused.value) == "'mode' is '\\x1b[2J\\n', not one of ('shadow', 'replay')"


def test_a_v2_run_reads_under_v3_as_written():
    """v3 adds the `answered_by` value `irimi` and nothing else, so a v2 run reads unchanged."""
    record = trace.run_from_json(_encoded_run() | {"schema_version": 2})
    assert record.schema_version == 2


def test_a_v4_run_is_refused():
    with pytest.raises(TraceFormatError, match="schema_version is newer than this irimi reads"):
        trace.run_from_json(_encoded_run() | {"schema_version": 4})


def test_a_replay_names_the_run_it_replays():
    record = _run_record(mode="replay", replay_of="4f1c9a2b7d3e8a60")
    assert trace.run_from_json(trace.run_to_json(record)) == record


@pytest.mark.parametrize(
    "replay_of",
    ["x" * 5_000_000, "../x", "", "a  b", "run\n", "a\nb\x1b", "x" * 65],
    ids=["five-megabytes", "a-path", "empty", "two-spaces", "a-newline", "control", "too-long"],
)
def test_a_replay_of_that_is_no_run_id_is_refused_by_its_length(replay_of):
    """A `replay_of` that `is_valid_run_id` refuses is refused unquoted, as a run's own id is.

    A damaged run.json can hold a 5 MB string, a path, or text that breaks `runs list`'s columns."""
    with pytest.raises(TraceFormatError) as refused:
        trace.run_from_json(_encoded_run() | {"mode": "replay", "replay_of": replay_of})
    assert str(refused.value) == f"'replay_of' is not a run id ({len(replay_of)} characters)"


@pytest.mark.parametrize(
    ("mode", "replay_of", "refusal"),
    [
        ("replay", None, "'mode' is 'replay', but 'replay_of' is null"),
        ("shadow", "4f1c9a2b7d3e8a60", "'replay_of' names a run, but 'mode' is 'shadow'"),
    ],
)
def test_a_run_whose_mode_and_replay_of_disagree_is_refused(mode, replay_of, refusal):
    """A record whose `mode` and `replay_of` disagree is damage, since `mode_for` sets both."""
    with pytest.raises(TraceFormatError) as refused:
        trace.run_from_json(_encoded_run() | {"mode": mode, "replay_of": replay_of})
    assert str(refused.value) == refusal


def test_a_recorded_outcome_round_trips_and_holds_a_tool_call_s_outcome():
    """What the control endpoint answers a replay's lookup with, and the SDK reads."""
    call = dataclasses.replace(
        builders.filled(ToolCall),
        result=None,
        error=ErrorInfo("builtins.KeyError", "'k'"),
        awaited=True,
    )
    d = recorded_outcome_to_json(call)
    assert d == {
        "result": None,
        "error": {"type": "builtins.KeyError", "message": "'k'"},
        "awaited": True,
    }
    assert recorded_outcome_from_json(d) == RecordedOutcome(None, call.error, True)
    with pytest.raises(TraceFormatError, match="'awaited'"):
        recorded_outcome_from_json(d | {"awaited": "yes"})


@pytest.mark.parametrize(
    "value",
    [
        None,
        "12",
        12,
        {"0": 12},
        [1.5],
        [4.0],
        [True],
        [0],
        [-3],
        [4, "5"],
        [4, None],
        [[4]],
        # Past `sys.get_int_max_str_digits()`, where `repr` raises ValueError: the message must not
        # quote the value, or the decoder raises something other than TraceFormatError.
        pytest.param([-(10**5000)], id="a-negative-past-the-digit-limit"),
        pytest.param([10**5000, 0], id="a-zero-after-a-length-past-the-digit-limit"),
        pytest.param(10**5000, id="an-integer-past-the-digit-limit"),
    ],
)
def test_stream_chunks_that_are_not_positive_integers_are_refused(value):
    blobs = _Blobs()
    d = trace.exchange_to_json(builders.filled(Exchange), blobs.put) | {"stream_chunks": value}
    with pytest.raises(TraceFormatError, match="stream_chunks"):
        trace.exchange_from_json(d, blobs.get)


def _never(ref):
    raise AssertionError(f"get_body was asked for {ref!r}")


_DECODERS = {
    "run_from_json": trace.run_from_json,
    "tool_call_from_json": trace.tool_call_from_json,
    "telemetry_from_json": trace.telemetry_from_json,
    "trigger_from_json": trace.trigger_from_json,
    "recorded_outcome_from_json": recorded_outcome_from_json,
    "run_start_from_json": protocol_control.run_start_from_json,
    "run_end_from_json": protocol_control.run_end_from_json,
    "exchange_from_json": lambda d: trace.exchange_from_json(d, _never),
    "event_from_json": lambda d: trace.event_from_json(d, _never),
}


@pytest.mark.parametrize("decoder", list(_DECODERS))
def test_a_line_that_is_a_list_nested_too_deep_to_quote_is_named_a_list(decoder):
    """A line `json.loads` reads as a deep list, which 3.14's does, is named by its type."""
    with pytest.raises(TraceFormatError, match=r"must be read from an object, not a list$"):
        _DECODERS[decoder](_nested_list(_DEEP))


def test_every_public_decoder_is_in_the_not_an_object_test():
    assert set(_DECODERS) == {
        name
        for module in (trace, protocol_control)
        for name in dir(module)
        if name.endswith("_from_json") and not name.startswith("_")
    }


@pytest.mark.parametrize("decoder", list(_DECODERS))
@pytest.mark.parametrize("text", ["null", "5", '"seq"', "[]"])
def test_a_line_that_is_not_an_object_is_a_trace_format_error(decoder, text):
    """A line that is not a JSON object raises TraceFormatError, the exception a store catches."""
    with pytest.raises(TraceFormatError, match="object, not"):
        _DECODERS[decoder](json.loads(text))


@pytest.mark.parametrize(
    "number",
    ["1" + "0" * 400, "NaN", "Infinity", "-Infinity"],
    ids=["10**400", "nan", "inf", "-inf"],
)
def test_a_timestamp_that_is_not_a_finite_float_is_a_trace_format_error(number):
    """A timestamp no float holds, or a non-finite one, is refused though `json.loads` reads it."""
    value = json.loads(number)
    with pytest.raises(TraceFormatError, match="started_at"):
        trace.telemetry_from_json(
            json.loads(f'{{"run_id": "r", "host": "h", "started_at": {number}}}')
        )
    with pytest.raises(TraceFormatError, match="ended_at"):
        trace.run_from_json(_encoded_run() | {"ended_at": value})
    blobs = _Blobs()
    encoded = trace.exchange_to_json(builders.filled(Exchange), blobs.put) | {"started_at": value}
    with pytest.raises(TraceFormatError, match="started_at"):
        trace.exchange_from_json(encoded, blobs.get)


@pytest.mark.parametrize("number", ["NaN", "Infinity", "-Infinity"])
def test_free_json_holding_a_number_that_is_not_finite_is_a_trace_format_error(number):
    """`args` and `result` refuse `NaN`, which `json.dumps(..., allow_nan=False)` cannot write."""
    nested = json.loads(f'{{"a": [1, {{"b": {number}}}]}}')
    call = trace.tool_call_to_json(builders.filled(ToolCall))
    for key in ("args", "result"):
        with pytest.raises(TraceFormatError, match=key):
            trace.tool_call_from_json(call | {key: nested})
    encoded: typing.Any = trace.run_to_json(_run_record())["trigger"]
    trigger = encoded | {"args": nested}
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
    d = trace.exchange_to_json(builders.filled(Exchange), blobs.put) | {key: value}
    with pytest.raises(TraceFormatError, match=key):
        trace.exchange_from_json(d, blobs.get)


@pytest.mark.parametrize("pairs", [{"a": "1"}, [["a"]], [["a", 1]], ["a: 1"]])
def test_headers_that_are_not_name_value_pairs_are_refused(pairs):
    blobs = _Blobs()
    d: typing.Any = trace.exchange_to_json(builders.filled(Exchange), blobs.put)
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
def test_a_valid_run_id_is_accepted(run_id):
    assert trace.is_valid_run_id(run_id)


@pytest.mark.parametrize("run_id", ["", "../x", "a" * 65, "a b", "a/b", "a.b", "..", "a\n", "é"])
def test_an_invalid_run_id_is_refused(run_id):
    assert not trace.is_valid_run_id(run_id)


def test_error_info_names_the_exception_by_module_and_qualname_and_keeps_its_message_whole():
    """The message is posted whole for the store to redact before it cuts it.

    Past MAX_POSTED_MESSAGE it is cut outside any secret: a key straddling the cut is left out
    whole."""

    class Boom(Exception):
        pass

    info = ErrorInfo.from_exception(Boom("x" * 5000))
    assert info.type == f"{__name__}.{Boom.__qualname__}"
    assert info.message == "x" * 5000
    assert ErrorInfo.from_exception(ValueError("bad")) == ErrorInfo("builtins.ValueError", "bad")
    said = "x" * (limits.MAX_POSTED_MESSAGE - 10) + " ghp_" + "k" * 36
    assert ErrorInfo.from_exception(Boom(said)).message == said[: limits.MAX_POSTED_MESSAGE - 9]
    blob = "x" * (2 * limits.MAX_POSTED_MESSAGE)
    assert ErrorInfo.from_exception(Boom(blob)).message == "x" * limits.MAX_POSTED_MESSAGE


def test_error_info_cuts_a_type_no_class_statement_would_make():
    """A `type()`-made class's name is cut to MAX_POSTED_TYPE outside any secret.

    So its post stays under the control endpoint's 2 MiB and the run keeps its end."""
    long = type("L" * (3 * 1024 * 1024), (Exception,), {})
    assert (
        ErrorInfo.from_exception(long()).type
        == f"{__name__}.{'L' * 3 * 1024 * 1024}"[: limits.MAX_POSTED_TYPE]
    )
    head = "K" * (limits.MAX_POSTED_TYPE - len(__name__) - 10) + "_"
    keyed = type(head + "ghp_" + "k" * 36, (Exception,), {})
    assert ErrorInfo.from_exception(keyed()).type == f"{__name__}.{head}"


def test_error_info_survives_an_exception_it_cannot_print():
    """A `__str__` that raises or returns no string does not raise a second exception."""

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
    b'{"id": "ch_demo1", "object": "charge", "amount": 4900, "amount_refunded": 0, '
    b'"refunded": false, "currency": "usd", "customer": null, "status": "succeeded", '
    b'"paid": true, "created": 1790000000, "metadata": {}}',
    b"charge=ch_demo1&amount=4900",
    b'{"amount": 4900, "balance_transaction": "txn_kYRDGB3FUDFCLA08rTInOvaw", '
    b'"charge": "ch_demo1", "created": 1792803261, "currency": "usd", "customer": null, '
    b'"customer_account": null, "destination_details": {"card": {"type": "reversal"}, '
    b'"type": "card"}, "id": "re_a7CNxM1DbNFjcVBH0Or8Ihpu", "metadata": {}, "object": "refund", '
    b'"payment_intent": null, "payment_method": null, "reason": null, "receipt_number": null, '
    b'"source_transfer_reversal": null, "status": "succeeded", "transfer_reversal": null}',
)


def _doc_blocks(info: str) -> list[str]:
    return re.findall(rf"^```json {re.escape(info)}\n(.*?)^```$", DOC.read_text(), re.M | re.S)


def _doc_column(marker: str) -> list[str]:
    """The first column of the first table after `marker` on the page, each cell unquoted."""
    text = DOC.read_text()
    table = re.search(r"^\|.*?(?=^[^|]|\Z)", text[text.index(marker) :], re.M | re.S)
    assert table is not None, f"no table after {marker}"
    cells = [row.split("|")[1].strip() for row in table.group(0).strip().splitlines()[2:]]
    assert all(re.fullmatch(r"`[^`]+`", cell) for cell in cells), f"one value per row: {cells}"
    return [cell.strip("`") for cell in cells]


def _doc_table(marker: str) -> list[str]:
    """The first column of the first table after `marker` on the page: one field name per row."""
    names = _doc_column(marker)
    assert all(re.fullmatch(r"\w+", name) for name in names), f"one field per row: {names}"
    return names


def _exchange_json() -> dict[str, typing.Any]:
    return trace.exchange_to_json(builders.filled(Exchange), _Blobs().put)


@pytest.mark.parametrize(
    ("marker", "encode"),
    [
        ("(`trace.RunRecord`)", lambda: trace.run_to_json(_run_record())),
        ("(`trace.Trigger`)", lambda: trace.run_to_json(_run_record())["trigger"]),
        ("(`trace.ErrorInfo`)", lambda: trace.error_to_json(builders.filled(ErrorInfo))),
        ("(`irimi.core.exchange.Exchange`)", _exchange_json),
        ("(`exchange.Request`)", lambda: _exchange_json()["request"]),
        ("(`exchange.Response`)", lambda: _exchange_json()["response"]),
        ("(`trace.BodyRef`)", lambda: _exchange_json()["request"]["body"]),
        ("(`trace.ToolCall`)", lambda: trace.tool_call_to_json(builders.filled(ToolCall))),
        (
            "(`trace.TelemetrySeen`)",
            lambda: trace.telemetry_to_json(builders.filled(TelemetrySeen)),
        ),
    ],
    ids=[
        "run",
        "trigger",
        "error",
        "exchange",
        "request",
        "response",
        "body_ref",
        "tool_call",
        "telemetry",
    ],
)
def test_each_field_table_on_the_page_is_what_its_encoder_writes(marker, encode):
    """Each field table on the page lists exactly the keys its encoder writes, in that order."""
    assert _doc_table(marker) == list(encode())


@pytest.mark.parametrize(
    ("marker", "values"),
    [
        ("(`trace.Mode`)", typing.get_args(trace.Mode)),
        ("(`trace.Attribution`)", typing.get_args(trace.Attribution)),
        ("(`trace.RunStatus`)", typing.get_args(trace.RunStatus)),
        ("(`exchange.Kind`)", typing.get_args(Kind)),
        ("(`exchange.AnsweredBy`)", typing.get_args(AnsweredBy)),
        ("(`exchange.Door`)", typing.get_args(Door)),
        ("(`exchange.IssuedBy`)", typing.get_args(IssuedBy)),
        ("(`exchange.OverlayCoverage`)", typing.get_args(OverlayCoverage)),
        ("(`exchange.PreconditionOutcome`)", typing.get_args(PreconditionOutcome)),
        ("(`trace.ToolKind`)", typing.get_args(trace.ToolKind)),
        ("(`trace.Ran`)", typing.get_args(trace.Ran)),
        ("(`trace.EventType`)", typing.get_args(trace.EventType)),
        ("(`exchange.Flag`)", tuple(flag.value for flag in Flag)),
    ],
    ids=lambda value: value if isinstance(value, str) else "values",
)
def test_each_vocabulary_table_on_the_page_lists_every_value_the_code_knows(marker, values):
    """Each vocabulary table lists exactly its vocabulary's values, in the code's order."""
    assert _doc_column(marker) == list(values)


def test_the_documented_example_decodes_with_the_codecs():
    (run_text,) = _doc_blocks("run.json")
    record = trace.run_from_json(json.loads(run_text))
    assert record.end is not None
    assert (record.schema_version, record.attribution, record.end.outcome) == (
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
    assert [seq for seq, _ in lines] == [1, 4, 5, 6]
    (_, lookup), (_, check), (_, refund), (_, fake) = lines
    assert isinstance(lookup, ToolCall) and (lookup.kind, lookup.ran) == ("read", "real")
    assert isinstance(check, Exchange) and isinstance(refund, Exchange)
    assert (check.issued_by, refund.precondition.outcome) == ("engine", "passed")
    assert refund.request.body == b"charge=ch_demo1&amount=4900"
    assert isinstance(fake, ToolCall) and (fake.kind, fake.ran) == ("write", "shadow")
    assert all(ev.run_id == record.run_id for _, ev in lines)
    # The page's ordering rule: `seq` is completion order, `started_at` start order.
    assert refund.started_at < check.started_at <= check.ended_at < refund.ended_at


def test_the_documented_example_is_what_the_encoders_write():
    """Re-encoding each decoded record of the page's example gives back its JSON, key for key."""
    (run_text,) = _doc_blocks("run.json")
    assert trace.run_to_json(trace.run_from_json(json.loads(run_text))) == json.loads(run_text)
    blobs = {trace.body_ref(b).sha256: b for b in EXAMPLE_BLOBS}
    for text in _doc_blocks("events.jsonl"):
        documented = json.loads(text)
        seq, event = trace.event_from_json(documented, lambda ref: blobs[ref.sha256])
        assert trace.event_to_json(seq, event, trace.body_ref) == documented


def test_a_description_is_found_anywhere_in_a_captured_value():
    """A description is found at any depth, without recursion, so a replay never returns one.

    `__irimi_repr__` and `__irimi_truncated__` say what a value was, not what it is."""
    assert {trace.REPR_KEY, trace.TRUNCATED_KEY} == trace.DESCRIPTION_KEYS
    assert trace.holds_description({"a": [1, {"b": {"__irimi_repr__": "Decimal('1')"}}]})
    assert trace.holds_description([{"__irimi_truncated__": True, "bytes": 9}])
    wholes: tuple[trace.JSONValue, ...] = (
        None,
        1,
        "__irimi_repr__",
        ["__irimi_repr__"],
        {"__irimi_bytes__": "AA=="},
    )
    for whole in wholes:
        assert not trace.holds_description(whole)
    deep: list = [{"__irimi_repr__": "x"}]
    for _ in range(100_000):
        deep = [deep]
    assert trace.holds_description(deep)


def test_the_stopped_error_types_are_how_a_cancelled_or_closed_call_is_recorded():
    stops = (asyncio.CancelledError(), GeneratorExit())
    assert {ErrorInfo.from_exception(e).type for e in stops} == trace.STOPPED_ERROR_TYPES
