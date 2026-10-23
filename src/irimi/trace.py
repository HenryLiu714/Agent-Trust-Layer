"""Trace format v2: the records a stored run is made of, and their JSON codecs.

Stored runs outlive the version of irimi that wrote them, so the shape is written down once, here
and in `docs/trace-format.md`, and versioned. This module is the shape and nothing else: frozen
dataclasses and pure encode/decode functions. It does no I/O - the store owns the files and
hands the exchange codec two callables for the bodies, which never go inline.

THE ROUND TRIP IS THE CONTRACT. Every field of `Exchange`, `RunRecord`, `ToolCall` and
`TelemetrySeen` is spelled out in its encoder and its decoder, and `tests/test_trace.py` builds
each record with a non-default value in every field and asks for it back. A field added to any of
them without codec support fails that test, which is what keeps a recording readable by a replay:
a stored run gets no second chance to repeat an L3 read.

Decoders raise `TraceFormatError` and nothing else on a malformed record - one that is not a JSON
object, lacks a field, or holds a value of the wrong type, outside its vocabulary or not finite -
refuse a `schema_version` newer than this module's or below 1, and ignore keys they do not know.
A value of the wrong type is named by its JSON type and never quoted (`json_type`), so a decoder
cannot raise while it explains itself. What a message does quote cannot recurse: a float
that is not finite, or a string - a word outside a vocabulary, a `sha256` that is no digest - cut
to MAX_QUOTED characters, so a damaged line's megabyte string is never a megabyte message.
The one exception they do not wrap is the caller's own: `exchange_from_json` and `event_from_json`
hand every body ref to `get_body`, and whatever it raises - a blob missing from the store - reaches
the caller unchanged.
"""

import hashlib
import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, assert_never, get_args

from irimi.exchange import (
    AnsweredBy,
    Door,
    Exchange,
    Headers,
    IssuedBy,
    Kind,
    OverlayFidelity,
    PreconditionOutcome,
    Request,
    Response,
    Validation,
)
from irimi.secret_shapes import cut_outside_secrets

# One integer for the whole format. `run.json` carries it and a run's events are read under
# it; `unattributed/events.jsonl`, which has no `run.json`, is read under this one. A decoder
# refuses a newer version and ignores keys it does not know. Every field of version 1 is required.
# A field added after it decodes as its dataclass default when absent, so a recording made before
# the field existed still reads; adding a field means adding that decoder path. A new value in a
# closed vocabulary (`kind`, `answered_by`, `mode`, `attribution`: every field read with
# `_one_of`) IS a bump, because an older reader refuses a value it does not
# know; `flags` is open, and a reader accepts a flag it does not know. Changing what a field means
# is a bump too. `docs/trace-format.md`, "Versioning", is the rule in full.
#
# Version 2 has `mode` `replay`, `answered_by` and `ran` `recording`, and a run record's
# `replay_of`. A version 1 run reads under it unchanged, as a run with no `replay_of`.
SCHEMA_VERSION = 2

# The run id of an exchange no run claimed: `irimi serve` with no `Irimi-Run` header.
UNATTRIBUTED = "unattributed"

# A run id becomes a directory name in the store, so this pattern is the path-traversal
# guard: no `/`, no `.` - so no `..` - no space, nothing empty, nothing unbounded.
# `pipeline.attribute_run` treats an `Irimi-Run` value it refuses as absent. Test a value with
# `is_valid_run_id`, which uses `fullmatch`, never with `RUN_ID_PATTERN.match`: `$` also matches
# before a trailing newline, so `.match` accepts `"run\n"` - a name no directory should have.
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

# A body is stored as `blobs/<sha256>`, so a `BodyRef` read back from disk is checked
# against this before a store can join it onto a path.
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")

# The longest `ErrorInfo.message` a trace keeps: a run's error is a label, not a log.
MAX_ERROR_MESSAGE = 1000
# The longest `ErrorInfo.type` a trace keeps: a class's `module.qualname` is far shorter, but
# a posted type is whatever the client sent, up to 2 MiB.
# The store cuts both, and only once it has redacted the whole of each: a key cut in two is
# no longer its shape, so redaction would not find the half a cut kept.
MAX_ERROR_TYPE = 300
# The most of an exception's message `ErrorInfo.from_exception` keeps, far past MAX_ERROR_MESSAGE:
# the SDK posts the message for the store to redact and cut, and must only keep a post under the
# control endpoint's 2 MiB.
MAX_POSTED_MESSAGE = 64 * 1024
# The most of an exception's type `ErrorInfo.from_exception` keeps: four labels' worth, as the store
# keeps of an end's while it waits (`store.MAX_QUEUED_ERROR`). A class's `module.qualname` is far
# shorter, but one made with `type()` may be any length, and one past the control endpoint's 2 MiB
# cost its run the post, and so its end.
MAX_POSTED_TYPE = 4 * MAX_ERROR_TYPE

# The most of a string a decoder's message quotes (`_quoted`). The message is read by a person, on
# `irimi runs show`'s stderr, in `list_runs`'s warning or as a control refusal, and a damaged
# line's 5 MB `kind` must not come back as a 5 MB line.
MAX_QUOTED = 64

type JSONValue = None | bool | int | float | str | list[JSONValue] | dict[str, JSONValue]

Mode = Literal["shadow", "replay"]
Attribution = Literal["process", "sdk", "header"]
Outcome = Literal["ok", "error"]
ToolKind = Literal["read", "write"]
Ran = Literal["real", "shadow", "recording"]
EventType = Literal["exchange", "tool_call", "telemetry"]


class TraceFormatError(ValueError):
    """A record this version cannot read: a newer `schema_version`, a missing field, or a value
    of the wrong type or outside its vocabulary."""


def mode_for(replay_of: str | None) -> Mode:
    """The mode of a run an engine stores: `replay` when it answers from the stored run `replay_of`,
    `shadow` when it answers from nothing. One rule for every record an engine writes itself,
    the process run's and a header run's."""
    return "shadow" if replay_of is None else "replay"


def is_valid_run_id(run_id: str) -> bool:
    """True when `run_id` may name a run - and so a directory. See RUN_ID_PATTERN."""
    return RUN_ID_PATTERN.fullmatch(run_id) is not None


@dataclass(frozen=True)
class ErrorInfo:
    type: str  # the exception's `module.qualname`; a stored one at most MAX_ERROR_TYPE
    message: str  # a stored one at most MAX_ERROR_MESSAGE characters

    @classmethod
    def from_exception(cls, exc: BaseException) -> "ErrorInfo":
        """The one spelling of "what went wrong" a run or a tool call records.

        Its message is `str(exc)`, and its type the class's `module.qualname`, cut to
        MAX_POSTED_MESSAGE and MAX_POSTED_TYPE characters outside any secret's shape: the SDK
        cannot redact, and the store redacts each whole before it cuts it to a label.

        Never raises on an exception whose `__str__` does: the SDK calls this while an
        agent's own exception is in flight, and recording it must not replace it with another."""
        kind = type(exc)
        try:
            message = str(exc)
        except Exception:
            message = f"<unprintable {kind.__qualname__}>"
        return cls(
            cut_outside_secrets(f"{kind.__module__}.{kind.__qualname__}", MAX_POSTED_TYPE),
            cut_outside_secrets(message, MAX_POSTED_MESSAGE),
        )


@dataclass(frozen=True)
class Trigger:
    """What started a run, as the SDK or the launcher saw it."""

    name: str  # the display name
    # `module:qualname` of the wrapped function; None for a context-manager run and the process run.
    entrypoint: str | None
    args: JSONValue  # the captured arguments: `irimi.sdk.capture`'s table
    replayable: bool


@dataclass(frozen=True)
class RunRecord:
    """One run: `run.json`. None in `outcome` means the run has not ended - or never will."""

    schema_version: int
    run_id: str
    mode: Mode  # `replay` is a run a replay answered from a stored one
    # `process` is the `irimi shadow -- <cmd>` run, `sdk` a run the SDK started, and `header` a
    # run id that arrived on exchanges with no start event.
    attribution: Attribution
    trigger: Trigger | None
    agent_version: str | None
    engine_version: str
    sdk_version: str | None
    started_at: float | None
    ended_at: float | None
    outcome: Outcome | None
    error: ErrorInfo | None
    exit_code: int | None  # set only for the process run
    dropped_events: int = 0
    # The run a replay answered this one from. None for every run that is not a replay, and for a
    # version 1 run.json, which has no such key.
    replay_of: str | None = None


@dataclass(frozen=True)
class ToolCall:
    """A tool call the proxy cannot see, reported by the SDK."""

    tool_call_id: str
    run_id: str
    name: str
    kind: ToolKind
    # `real` ran the function; `shadow` ran its shadow stand-in; `recording` was answered from the
    # run a replay replays.
    ran: Ran
    args: JSONValue
    result: JSONValue
    error: ErrorInfo | None
    started_at: float
    ended_at: float
    # A sync tool that returned a coroutine: the call ended when the coroutine did, and `result`
    # and `error` are the coroutine's. A replay that answers such a call from the recording hands
    # the agent a coroutine again, which it awaits as it awaited the first. Absent in an older
    # record, it reads as False.
    awaited: bool = False


@dataclass(frozen=True)
class RecordedOutcome:
    """What a recorded read tool call came to, as a replay hands it back: the control
    endpoint answers the SDK's lookup with it (`recorded_outcome_to_json`), and the SDK reads it
    (`recorded_outcome_from_json`). `result` is still captured JSON, for the SDK to revive."""

    result: JSONValue
    error: ErrorInfo | None
    awaited: bool


# What the SDK posts to ask a replay for a read tool's recorded outcome: the tool's name and its
# args as captured, and nothing else.
RECORDED_LOOKUP_FIELDS = ("name", "args")

# The markers of a captured value that describe it rather than hold it (`docs/trace-format.md`,
# "Captured values"): its repr, or its size when it was too large to keep. Trace format, not the
# SDK's alone: a replay's recording refuses a recorded result holding one, as the SDK's `revive`
# does, so neither side states the rule apart.
REPR_KEY = "__irimi_repr__"
TRUNCATED_KEY = "__irimi_truncated__"
DESCRIPTION_KEYS = frozenset({REPR_KEY, TRUNCATED_KEY})

# The `ErrorInfo.type` a tool call records when something outside it stopped it before it
# finished: its task was cancelled (an `asyncio.wait_for` that timed out), or its coroutine was
# closed unawaited. It holds no outcome of the tool's own, so a replay never answers with it: the
# agent's `except TimeoutError` would meet a RecordedToolError where the recording went on.
# `ErrorInfo.from_exception` spells them so on every supported Python.
STOPPED_ERROR_TYPES = frozenset({"asyncio.exceptions.CancelledError", "builtins.GeneratorExit"})


def holds_description(value: JSONValue) -> bool:
    """Whether a captured value holds a description of a value anywhere in it (`DESCRIPTION_KEYS`
    as a key of a JSON object at any depth), so that it cannot be rebuilt as the value. Walked
    with a stack, never recursion, so a value nested past the interpreter's stack is answered too.
    Never raises."""
    stack: list[Any] = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            if not DESCRIPTION_KEYS.isdisjoint(item):
                return True
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)
    return False


@dataclass(frozen=True)
class TelemetrySeen:
    """One telemetry exchange, recorded as having happened and nothing more: telemetry is
    forwarded and its requests and responses are never stored."""

    run_id: str
    host: str
    started_at: float


@dataclass(frozen=True)
class BodyRef:
    """Where a body went: `blobs/<sha256>`, `size` bytes of it, cut short when `truncated`."""

    sha256: str
    size: int
    truncated: bool


Event = Exchange | ToolCall | TelemetrySeen
PutBody = Callable[[bytes], BodyRef | None]
GetBody = Callable[[BodyRef], bytes]


def body_ref(body: bytes, truncated: bool = False) -> BodyRef:
    """The ref for `body` as stored: its own digest and length."""
    return BodyRef(hashlib.sha256(body).hexdigest(), len(body), truncated)


# ------------------------------------------------------------------------------------- encoders


def run_to_json(record: RunRecord) -> dict[str, Any]:
    return {
        "schema_version": record.schema_version,
        "run_id": record.run_id,
        "mode": record.mode,
        "attribution": record.attribution,
        "trigger": None if record.trigger is None else trigger_to_json(record.trigger),
        "agent_version": record.agent_version,
        "engine_version": record.engine_version,
        "sdk_version": record.sdk_version,
        "started_at": record.started_at,
        "ended_at": record.ended_at,
        "outcome": record.outcome,
        "error": error_to_json(record.error),
        "exit_code": record.exit_code,
        "dropped_events": record.dropped_events,
        "replay_of": record.replay_of,
    }


def tool_call_to_json(call: ToolCall) -> dict[str, Any]:
    return {
        "tool_call_id": call.tool_call_id,
        "run_id": call.run_id,
        "name": call.name,
        "kind": call.kind,
        "ran": call.ran,
        "args": call.args,
        "result": call.result,
        "error": error_to_json(call.error),
        "started_at": call.started_at,
        "ended_at": call.ended_at,
        "awaited": call.awaited,
    }


def recorded_outcome_to_json(call: ToolCall) -> dict[str, Any]:
    """The recorded outcome of `call`, as the control endpoint answers a lookup that hit."""
    return {"result": call.result, "error": error_to_json(call.error), "awaited": call.awaited}


def telemetry_to_json(seen: TelemetrySeen) -> dict[str, Any]:
    return {"run_id": seen.run_id, "host": seen.host, "started_at": seen.started_at}


def exchange_to_json(ex: Exchange, put_body: PutBody) -> dict[str, Any]:
    """`ex` as a JSON object. Bodies never go inline: each non-empty one is handed to `put_body`,
    and the ref it returns is what is written. An empty body is `null` and reaches no store."""
    return {
        "run_id": ex.run_id,
        "service": ex.service,
        "operation": ex.operation,
        "kind": ex.kind,
        "answered_by": ex.answered_by,
        "validation": ex.validation,
        "door": ex.door,
        "issued_by": ex.issued_by,
        "target": ex.target,
        "flags": list(ex.flags),
        "overlay": ex.overlay,
        "precondition": ex.precondition,
        "rejection_code": ex.rejection_code,
        "would_fire": list(ex.would_fire),
        "currency": ex.currency,
        "started_at": ex.started_at,
        "ended_at": ex.ended_at,
        "request": _request_to_json(ex.request, put_body),
        "response": None if ex.response is None else _response_to_json(ex.response, put_body),
        "stream_chunks": list(ex.stream_chunks),
    }


def event_to_json(seq: int, event: Event, put_body: PutBody) -> dict[str, Any]:
    """One `events.jsonl` line: `seq` and `type` first, then the record's own fields."""
    head: dict[str, Any] = {"seq": seq}
    if isinstance(event, Exchange):
        return head | {"type": "exchange"} | exchange_to_json(event, put_body)
    if isinstance(event, ToolCall):
        return head | {"type": "tool_call"} | tool_call_to_json(event)
    if isinstance(event, TelemetrySeen):
        return head | {"type": "telemetry"} | telemetry_to_json(event)
    assert_never(event)


def trigger_to_json(trigger: Trigger) -> dict[str, Any]:
    """`trigger` as `run.json` holds it, and as the SDK posts it in a run's start: one
    encoding for the file and the wire, so the endpoint reads a posted trigger with the decoder a
    stored one is read with."""
    return {
        "name": trigger.name,
        "entrypoint": trigger.entrypoint,
        "args": trigger.args,
        "replayable": trigger.replayable,
    }


def error_to_json(error: ErrorInfo | None) -> dict[str, Any] | None:
    """`error` as a run or a tool call holds it, and as the SDK posts it in a run's end."""
    return None if error is None else {"type": error.type, "message": error.message}


def _request_to_json(request: Request, put_body: PutBody) -> dict[str, Any]:
    return {
        "method": request.method,
        "scheme": request.scheme,
        "host": request.host,
        "port": request.port,
        "path": request.path,
        "query": request.query,
        "headers": _headers_to_json(request.headers),
        "body": _body_to_json(request.body, put_body),
    }


def _response_to_json(response: Response, put_body: PutBody) -> dict[str, Any]:
    return {
        "status": response.status,
        "headers": _headers_to_json(response.headers),
        "body": _body_to_json(response.body, put_body),
    }


def _headers_to_json(headers: Headers) -> list[list[str]]:
    # Pairs, not an object: order and repeats (`set-cookie`) are part of what was sent.
    return [[name, value] for name, value in headers]


def _body_to_json(body: bytes, put_body: PutBody) -> dict[str, Any] | None:
    ref = put_body(body) if body else None
    if ref is None:
        return None
    return {"sha256": ref.sha256, "size": ref.size, "truncated": ref.truncated}


# ------------------------------------------------------------------------------------- decoders


def run_from_json(d: Mapping[str, Any]) -> RunRecord:
    version = _int(d, "schema_version")
    # The version is not quoted: `str` of an integer past `sys.get_int_max_str_digits()` raises
    # ValueError, and a decoder raises only TraceFormatError.
    if version > SCHEMA_VERSION:
        raise TraceFormatError(f"schema_version is newer than this irimi reads ({SCHEMA_VERSION})")
    if version < 1:
        raise TraceFormatError("schema_version is below 1, the first version")
    trigger = _optional(d, "trigger", _object)
    record = RunRecord(
        schema_version=version,
        run_id=_str(d, "run_id"),
        mode=_one_of(d, "mode", Mode),
        attribution=_one_of(d, "attribution", Attribution),
        trigger=None if trigger is None else _trigger_from_json(trigger),
        agent_version=_optional(d, "agent_version", _str),
        engine_version=_str(d, "engine_version"),
        sdk_version=_optional(d, "sdk_version", _str),
        started_at=_optional(d, "started_at", _float),
        ended_at=_optional(d, "ended_at", _float),
        outcome=_optional(d, "outcome", lambda d, k: _one_of(d, k, Outcome)),
        error=_error_from_json(d, "error"),
        exit_code=_optional(d, "exit_code", _int),
        dropped_events=_int(d, "dropped_events"),
        replay_of=_added_later(d, "replay_of", _run_id),
    )
    # A replay's run names the run it replayed, and only a replay's: the store stamps both by one
    # rule, `mode_for`, so a record where they disagree is damage, never half a replay.
    if record.mode != mode_for(record.replay_of):
        if record.replay_of is None:
            raise TraceFormatError("'mode' is 'replay', but 'replay_of' is null")
        raise TraceFormatError(f"'replay_of' names a run, but 'mode' is {record.mode!r}")
    return record


def tool_call_from_json(d: Mapping[str, Any]) -> ToolCall:
    return ToolCall(
        tool_call_id=_str(d, "tool_call_id"),
        run_id=_str(d, "run_id"),
        name=_str(d, "name"),
        kind=_one_of(d, "kind", ToolKind),
        ran=_one_of(d, "ran", Ran),
        args=_json(d, "args"),
        result=_json(d, "result"),
        error=_error_from_json(d, "error"),
        started_at=_float(d, "started_at"),
        ended_at=_float(d, "ended_at"),
        awaited=_added_later(d, "awaited", _bool) or False,
    )


def recorded_outcome_from_json(d: Mapping[str, Any]) -> RecordedOutcome:
    """The outcome `recorded_outcome_to_json` wrote, held to the rules a stored tool call is."""
    return RecordedOutcome(
        result=_json(d, "result"),
        error=_error_from_json(d, "error"),
        awaited=_bool(d, "awaited"),
    )


def telemetry_from_json(d: Mapping[str, Any]) -> TelemetrySeen:
    return TelemetrySeen(
        run_id=_str(d, "run_id"), host=_str(d, "host"), started_at=_float(d, "started_at")
    )


def exchange_from_json(d: Mapping[str, Any], get_body: GetBody) -> Exchange:
    """The Exchange `exchange_to_json` wrote, with each body fetched back through `get_body`."""
    response = _optional(d, "response", _object)
    return Exchange(
        request=_request_from_json(_object(d, "request"), get_body),
        response=None if response is None else _response_from_json(response, get_body),
        service=_str(d, "service"),
        operation=_str(d, "operation"),
        kind=_one_of(d, "kind", Kind),
        answered_by=_one_of(d, "answered_by", AnsweredBy),
        validation=_one_of(d, "validation", Validation),
        run_id=_str(d, "run_id"),
        door=_one_of(d, "door", Door),
        flags=_strings(d, "flags"),
        target=_str(d, "target"),
        overlay=_optional(d, "overlay", lambda d, k: _one_of(d, k, OverlayFidelity)),
        precondition=_optional(d, "precondition", lambda d, k: _one_of(d, k, PreconditionOutcome)),
        rejection_code=_str(d, "rejection_code"),
        issued_by=_one_of(d, "issued_by", IssuedBy),
        would_fire=_strings(d, "would_fire"),
        currency=_str(d, "currency"),
        started_at=_float(d, "started_at"),
        ended_at=_float(d, "ended_at"),
        stream_chunks=_chunk_lengths(d, "stream_chunks"),
    )


def event_from_json(d: Mapping[str, Any], get_body: GetBody) -> tuple[int, Event]:
    """One `events.jsonl` line back: its `seq` and its record."""
    seq = _int(d, "seq")
    kind: EventType = _one_of(d, "type", EventType)
    if kind == "exchange":
        return seq, exchange_from_json(d, get_body)
    if kind == "tool_call":
        return seq, tool_call_from_json(d)
    if kind == "telemetry":
        return seq, telemetry_from_json(d)
    assert_never(kind)


def _trigger_from_json(d: Mapping[str, Any]) -> Trigger:
    return Trigger(
        name=_str(d, "name"),
        entrypoint=_optional(d, "entrypoint", _str),
        args=_json(d, "args"),
        replayable=_bool(d, "replayable"),
    )


def _error_from_json(d: Mapping[str, Any], key: str) -> ErrorInfo | None:
    error = _optional(d, key, _object)
    if error is None:
        return None
    return ErrorInfo(type=_str(error, "type"), message=_str(error, "message"))


def _request_from_json(d: Mapping[str, Any], get_body: GetBody) -> Request:
    return Request(
        method=_str(d, "method"),
        scheme=_str(d, "scheme"),
        host=_str(d, "host"),
        port=_int(d, "port"),
        path=_str(d, "path"),
        query=_str(d, "query"),
        headers=_headers_from_json(d, "headers"),
        body=_body_from_json(d, "body", get_body),
    )


def _response_from_json(d: Mapping[str, Any], get_body: GetBody) -> Response:
    return Response(
        status=_int(d, "status"),
        headers=_headers_from_json(d, "headers"),
        body=_body_from_json(d, "body", get_body),
    )


def _headers_from_json(d: Mapping[str, Any], key: str) -> Headers:
    pairs = _present(d, key)
    if not isinstance(pairs, list):
        raise TraceFormatError(f"{key!r} is {json_type(pairs)}, not a list of [name, value] pairs")
    out: list[tuple[str, str]] = []
    for index, pair in enumerate(pairs):
        if not (
            isinstance(pair, list) and len(pair) == 2 and all(isinstance(p, str) for p in pair)
        ):
            raise TraceFormatError(f"{key!r}[{index}] is not a [name, value] pair of strings")
        out.append((pair[0], pair[1]))
    return tuple(out)


def _body_from_json(d: Mapping[str, Any], key: str, get_body: GetBody) -> bytes:
    ref = _optional(d, key, _object)
    if ref is None:
        return b""
    sha256 = _str(ref, "sha256")
    if SHA256_PATTERN.fullmatch(sha256) is None:
        raise TraceFormatError(f"{_quoted(sha256)} is not a sha256 digest")
    return get_body(BodyRef(sha256, _int(ref, "size"), _bool(ref, "truncated")))


# One reader per JSON type. Each raises TraceFormatError, never KeyError, TypeError,
# OverflowError or RecursionError, so a store reading a damaged line has one exception to catch,
# and the control endpoint answers every malformed body with a 400.


def json_type(value: Any) -> str:
    """What a refused value is, by its JSON type, for a refusal's message: each decoder's, and the
    control endpoint's for a body that is no object. Never the value itself: `repr` of a list
    nested 100,000 deep raises RecursionError, which Python 3.14's `json.loads` reads and 3.12's
    refuses, and of an integer past `sys.get_int_max_str_digits()` ValueError. A decoder that
    quoted its value raised something other than TraceFormatError, on some Pythons and some stacks
    only."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "a boolean"
    if isinstance(value, int):
        return "an integer"
    if isinstance(value, float):
        return "a number"
    if isinstance(value, str):
        return "a string"
    if isinstance(value, list):
        return "a list"
    if isinstance(value, Mapping):
        return "an object"
    return f"a {type(value).__name__}"


def _quoted(text: str) -> str:
    """A string a refusal quotes, as its `repr`, so a control character is spelled and never
    printed, and cut after MAX_QUOTED characters with a `…` saying so."""
    if len(text) <= MAX_QUOTED:
        return repr(text)
    return f"{text[:MAX_QUOTED]!r}…"


def _present(d: Mapping[str, Any], key: str) -> Any:
    # Every reader comes through here, so a line that parsed as `null`, `5` or `[]` is refused
    # here rather than by a TypeError from `in`.
    if not isinstance(d, Mapping):
        raise TraceFormatError(f"{key!r} must be read from an object, not {json_type(d)}")
    if key not in d:
        raise TraceFormatError(f"missing required field {key!r}")
    return d[key]


def _optional[T](
    d: Mapping[str, Any], key: str, read: Callable[[Mapping[str, Any], str], T]
) -> T | None:
    """`null` is None; any other value is read with `read`. The key itself is still required."""
    return None if _present(d, key) is None else read(d, key)


def _str(d: Mapping[str, Any], key: str) -> str:
    value = _present(d, key)
    if not isinstance(value, str):
        raise TraceFormatError(f"{key!r} is {json_type(value)}, not a string")
    return value


def _run_id(d: Mapping[str, Any], key: str) -> str:
    """A run id one record names another run by (`replay_of`), held to `is_valid_run_id`, the rule
    a run id must keep to name a directory: `runs list` prints it in a column, and `irimi replay`
    looks it up as a run. Refused by its length, never quoted, as a decoder refuses any value."""
    value = _str(d, key)
    if not is_valid_run_id(value):
        raise TraceFormatError(f"{key!r} is not a run id ({len(value)} characters)")
    return value


def _int(d: Mapping[str, Any], key: str) -> int:
    value = _present(d, key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TraceFormatError(f"{key!r} is {json_type(value)}, not an integer")
    return value


def _float(d: Mapping[str, Any], key: str) -> float:
    # JSON has one number type, and `json.dumps(0.0)` is `0.0` but a hand-written `0` is not, so an
    # integer is read as a float. Not a non-finite one: `json.loads` reads `NaN` and
    # `Infinity` by default, and a record holding one could not be written back with
    # `allow_nan=False`. An integer too large for a float is refused, not an OverflowError.
    value = _present(d, key)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TraceFormatError(f"{key!r} is {json_type(value)}, not a number")
    try:
        number = float(value)
    except OverflowError:
        raise TraceFormatError(f"{key!r} is an integer too large to be a timestamp") from None
    if not math.isfinite(number):
        raise TraceFormatError(f"{key!r} is {value!r}, not a finite number")
    return number


def _json(d: Mapping[str, Any], key: str) -> JSONValue:
    """A free JSON value (`args`, `result`), refused when a float anywhere in it is not finite:
    `json.loads` reads `NaN`, and a record holding one could not be written back."""
    value = _present(d, key)
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, float) and not math.isfinite(item):
            raise TraceFormatError(f"{key!r} holds {item!r}, not a finite number")
        if isinstance(item, list):
            stack.extend(item)
        elif isinstance(item, dict):
            stack.extend(item.values())
    return value


def _bool(d: Mapping[str, Any], key: str) -> bool:
    value = _present(d, key)
    if not isinstance(value, bool):
        raise TraceFormatError(f"{key!r} is {json_type(value)}, not a boolean")
    return value


def _object(d: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = _present(d, key)
    if not isinstance(value, dict):
        raise TraceFormatError(f"{key!r} is {json_type(value)}, not an object")
    return value


def _strings(d: Mapping[str, Any], key: str) -> tuple[str, ...]:
    value = _present(d, key)
    if not isinstance(value, list):
        raise TraceFormatError(f"{key!r} is {json_type(value)}, not a list of strings")
    for index, item in enumerate(value):
        if not isinstance(item, str):
            raise TraceFormatError(f"{key!r}[{index}] is {json_type(item)}, not a string")
    return tuple(value)


def _added_later[T](
    d: Mapping[str, Any], key: str, read: Callable[[Mapping[str, Any], str], T]
) -> T | None:
    """An optional field version 1 lacked (`replay_of`, `awaited`): absent or `null` is None, so
    a record made before the field existed reads as one that never set it; any other value is read
    with `read`."""
    return None if key not in d else _optional(d, key, read)


def _chunk_lengths(d: Mapping[str, Any], key: str) -> tuple[int, ...]:
    """A streamed response's chunk lengths: absent in an older recording, and read then as its
    default, `()`. Present, it is a list of
    positive integers."""
    if key not in d:
        return ()
    value = _present(d, key)
    if not isinstance(value, list):
        raise TraceFormatError(f"{key!r} is {json_type(value)}, not a list")
    for index, length in enumerate(value):
        if isinstance(length, bool) or not isinstance(length, int) or length <= 0:
            raise TraceFormatError(f"{key!r}[{index}] is not a positive integer")
    return tuple(value)


def _one_of(d: Mapping[str, Any], key: str, vocabulary: Any) -> Any:
    """A string from a `Literal` vocabulary. A value this version does not know is a format error
    and not a new mode silently read as an old one."""
    value = _str(d, key)
    if value not in get_args(vocabulary):
        raise TraceFormatError(f"{key!r} is {_quoted(value)}, not one of {get_args(vocabulary)}")
    return value
