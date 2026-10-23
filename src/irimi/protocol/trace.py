"""The trace format: the records a stored run is made of, and their JSON codecs.

Frozen dataclasses and pure encode/decode functions; no I/O. Bodies never go inline: the store
passes callables that write and read them as blobs. See docs/trace-format.md.

Guarantees:
- Every record field round-trips through its encoder and decoder. Pinned by tests/test_trace.py.
- Decoders raise only `TraceFormatError` on a malformed record, refuse a `schema_version` newer
  than this module's or below 1, and ignore unknown keys. Messages never quote a whole value.
- Whatever `get_body` raises reaches the caller unchanged.
"""

import hashlib
import json
import math
import re
import secrets
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, assert_never, get_args

from irimi import __version__
from irimi.core.exchange import (
    AnsweredBy,
    Door,
    Exchange,
    Flag,
    Headers,
    IssuedBy,
    Kind,
    OverlayCoverage,
    Precondition,
    PreconditionOutcome,
    Request,
    Response,
)
from irimi.core.limits import MAX_ERROR_MESSAGE, MAX_ERROR_TYPE, MAX_POSTED_MESSAGE, MAX_POSTED_TYPE
from irimi.core.secret_shapes import cut_outside_secrets

# On-disk trace format version, written to each run.json. Bump it when a closed vocabulary gains a
# value or a field changes meaning; a new optional field that decodes to its default needs no bump.
# See docs/trace-format.md.
SCHEMA_VERSION = 3

# The run id of an exchange no run claimed: `irimi serve` with no `Irimi-Run` header.
UNATTRIBUTED = "unattributed"

# A run id becomes a directory name, so this is the path-traversal guard. Test with
# `is_valid_run_id` (`fullmatch`): `.match` lets `$` accept a trailing newline.
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

# A `BodyRef` read from disk is checked against this before it is joined onto a blob path.
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")

# The longest string a decoder's message quotes, so a damaged line never yields a huge message.
MAX_QUOTED = 64

type JSONValue = bool | int | float | str | list[JSONValue] | dict[str, JSONValue] | None

Mode = Literal["shadow", "replay"]
Attribution = Literal["process", "sdk", "header"]
RunStatus = Literal["ok", "error"]
ToolKind = Literal["read", "write"]
Ran = Literal["real", "shadow", "recording"]
EventType = Literal["exchange", "tool_call", "telemetry"]


class TraceFormatError(ValueError):
    """A record this version cannot read: newer schema, missing field, or bad value."""


def mode_for(replay_of: str | None) -> Mode:
    """Return `replay` when the run answers from stored run `replay_of`, else `shadow`."""
    return "shadow" if replay_of is None else "replay"


def is_valid_run_id(run_id: str) -> bool:
    """Return True when `run_id` may name a run, and so a directory."""
    return RUN_ID_PATTERN.fullmatch(run_id) is not None


def new_run_id() -> str:
    """Return a fresh run id: 16 lower-case hex characters.

    Lower case keeps ids distinct on a case-insensitive filesystem.
    """
    return secrets.token_hex(8)


def new_tool_call_id() -> str:
    """Return a fresh tool call id: 16 lower-case hex characters."""
    return secrets.token_hex(8)


MAIN_MODULE = "__main__"


def importable_module(module: str) -> str:
    """Return the name another process imports `module` by.

    `__main__` run with `python -m pkg.agent` becomes `pkg.agent`; a script run by path, or a
    package's `__main__.py`, stays `__main__`.
    """
    if module != MAIN_MODULE:
        return module
    name = getattr(getattr(sys.modules.get(MAIN_MODULE), "__spec__", None), "name", None)
    if not isinstance(name, str) or not name or name.rpartition(".")[2] == MAIN_MODULE:
        return module
    return name


@dataclass(frozen=True)
class ErrorInfo:
    type: str  # the exception's `module.qualname`
    message: str

    @classmethod
    def from_exception(cls, exc: BaseException) -> "ErrorInfo":
        """Return the error a run or tool call records for `exc`.

        Both fields are cut to the posted limits without splitting a secret, since the SDK cannot
        redact. Never raises, even when `exc.__str__` does.
        """
        kind = type(exc)
        try:
            message = str(exc)
        except Exception:
            message = f"<unprintable {kind.__qualname__}>"
        return cls(
            cut_outside_secrets(
                f"{importable_module(kind.__module__)}.{kind.__qualname__}", MAX_POSTED_TYPE
            ),
            cut_outside_secrets(message, MAX_POSTED_MESSAGE),
        )

    def as_label(self) -> "ErrorInfo":
        """Return the error cut to the stored limits.

        Call only on redacted text: a cut first can keep half a key that redaction misses.
        """
        return ErrorInfo(self.type[:MAX_ERROR_TYPE], self.message[:MAX_ERROR_MESSAGE])


@dataclass(frozen=True)
class Trigger:
    """What started a run, as the SDK or the launcher saw it."""

    name: str
    # `module:qualname` of the wrapped function; None for a context-manager or process run.
    entrypoint: str | None
    args: JSONValue  # captured as `irimi.sdk._capture` describes
    replayable: bool
    # A process run's command line; its `args` are then None. run.json stores it as `args.argv`.
    argv: tuple[str, ...] | None = None

    @classmethod
    def command(cls, argv: Sequence[str]) -> "Trigger":
        """Return the trigger of a process run: its command line."""
        return cls(name=argv[0], entrypoint=None, args=None, replayable=True, argv=tuple(argv))


def trigger_args(trigger: Trigger) -> JSONValue:
    """Return `trigger`'s args as run.json holds them: a command line as `{"argv": [...]}`."""
    return trigger.args if trigger.argv is None else {"argv": list(trigger.argv)}


@dataclass(frozen=True)
class RunEnd:
    """How a run ended. `exit_code` is set only for a process run."""

    ended_at: float
    outcome: RunStatus
    error: ErrorInfo | None
    exit_code: int | None


@dataclass(frozen=True)
class RunRecord:
    """One run: `run.json`. `end` is None for a run that has not ended.

    `attribution`: `process` is the `irimi shadow -- <cmd>` run, `sdk` a run the SDK started,
    `header` a run id seen on exchanges with no start event.
    """

    schema_version: int
    run_id: str
    mode: Mode
    attribution: Attribution
    trigger: Trigger | None
    agent_version: str | None
    engine_version: str
    sdk_version: str | None
    started_at: float | None
    end: RunEnd | None = None
    dropped_events: int = 0
    replay_of: str | None = None  # the stored run a replay answers from

    @classmethod
    def process(
        cls,
        run_id: str,
        argv: Sequence[str],
        agent_version: str | None,
        started_at: float,
        trigger: Trigger | None = None,
        replay_of: str | None = None,
    ) -> "RunRecord":
        """Return the run `irimi shadow` or `irimi replay` stores for its child process tree."""
        return cls(
            schema_version=SCHEMA_VERSION,
            run_id=run_id,
            mode=mode_for(replay_of),
            attribution="process",
            trigger=Trigger.command(argv) if trigger is None else trigger,
            agent_version=agent_version,
            engine_version=__version__,
            sdk_version=None,
            started_at=started_at,
            replay_of=replay_of,
        )

    @classmethod
    def sdk(
        cls,
        run_id: str,
        trigger: Trigger,
        agent_version: str | None,
        sdk_version: str,
        started_at: float,
        replay_of: str | None = None,
    ) -> "RunRecord":
        """Return a run the SDK started, as the control endpoint stores it."""
        return cls(
            schema_version=SCHEMA_VERSION,
            run_id=run_id,
            mode=mode_for(replay_of),
            attribution="sdk",
            trigger=trigger,
            agent_version=agent_version,
            engine_version=__version__,
            sdk_version=sdk_version,
            started_at=started_at,
            replay_of=replay_of,
        )

    @classmethod
    def header(
        cls, run_id: str, started_at: float | None = None, replay_of: str | None = None
    ) -> "RunRecord":
        """Return the record of a run id first seen on an event with no start.

        Pass the first event's `started_at` so the run sorts by when it was first seen.
        """
        return cls(
            schema_version=SCHEMA_VERSION,
            run_id=run_id,
            mode=mode_for(replay_of),
            attribution="header",
            trigger=None,
            agent_version=None,
            engine_version=__version__,
            sdk_version=None,
            started_at=started_at,
            replay_of=replay_of,
        )

    @property
    def argv(self) -> tuple[str, ...] | None:
        """Return the command line a process run recorded, or None."""
        if self.attribution != "process" or self.trigger is None:
            return None
        return self.trigger.argv


@dataclass(frozen=True)
class ToolCall:
    """A tool call the proxy cannot see, reported by the SDK."""

    tool_call_id: str
    run_id: str
    name: str
    kind: ToolKind
    # `real` ran the function, `shadow` ran its fake, `recording` answered from the replayed run.
    ran: Ran
    args: JSONValue
    result: JSONValue
    error: ErrorInfo | None
    started_at: float
    ended_at: float
    # A sync tool returned a coroutine; `result` and `error` are the coroutine's, and a replay
    # hands the agent a coroutine again.
    awaited: bool = False


# Keys that mark a captured value as a description (its repr, or its size when too large) rather
# than the value itself. Such a value cannot be replayed.
REPR_KEY = "__irimi_repr__"
TRUNCATED_KEY = "__irimi_truncated__"
DESCRIPTION_KEYS = frozenset({REPR_KEY, TRUNCATED_KEY})

# Error types of a tool call stopped from outside (cancelled, or closed unawaited). They hold no
# outcome of the tool's own, so a replay never answers with them.
STOPPED_ERROR_TYPES = frozenset({"asyncio.exceptions.CancelledError", "builtins.GeneratorExit"})


def holds_description(value: JSONValue) -> bool:
    """Return True if any JSON object in `value` has a `DESCRIPTION_KEYS` key. Never raises.

    Walks with a stack, so arbitrarily deep values do not hit the recursion limit.
    """
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


def canonical_json(value: Any) -> str:
    """Return `value` as JSON with sorted keys and no spacing.

    Raises ValueError for NaN or infinity, RecursionError past `json.dumps`'s depth.
    """
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True)
class TelemetrySeen:
    """A forwarded telemetry exchange; only the fact that it happened is stored."""

    run_id: str
    host: str
    started_at: float


@dataclass(frozen=True)
class BodyRef:
    """A stored body: `blobs/<sha256>`, `size` bytes, cut short when `truncated`."""

    sha256: str
    size: int
    truncated: bool


Event = Exchange | ToolCall | TelemetrySeen
PutBody = Callable[[bytes], BodyRef | None]
GetBody = Callable[[BodyRef], bytes]


@dataclass(frozen=True)
class StoredRun:
    """A run read back from the store: its record and its events in `seq` order."""

    record: RunRecord
    events: list[Event]  # bodies loaded or empty; see `StoreReader.load_run`


def body_ref(body: bytes, truncated: bool = False) -> BodyRef:
    """Return the ref for `body`: its digest and length."""
    return BodyRef(hashlib.sha256(body).hexdigest(), len(body), truncated)


# ------------------------------------------------------------------------------------- encoders


def run_to_json(record: RunRecord) -> dict[str, JSONValue]:
    end = record.end
    # On disk the end is four flat keys, each null until the run ends.
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
        "ended_at": None if end is None else end.ended_at,
        "outcome": None if end is None else end.outcome,
        "error": error_to_json(None if end is None else end.error),
        "exit_code": None if end is None else end.exit_code,
        "dropped_events": record.dropped_events,
        "replay_of": record.replay_of,
    }


def tool_call_to_json(call: ToolCall) -> dict[str, JSONValue]:
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


def telemetry_to_json(seen: TelemetrySeen) -> dict[str, JSONValue]:
    return {"run_id": seen.run_id, "host": seen.host, "started_at": seen.started_at}


def exchange_to_json(ex: Exchange, put_body: PutBody) -> dict[str, JSONValue]:
    """Return `ex` as a JSON object, each non-empty body replaced by the ref `put_body` returns."""
    return {
        "run_id": ex.run_id,
        "service": ex.service,
        "operation": ex.operation,
        "kind": ex.kind,
        "answered_by": ex.answered_by,
        "door": ex.door,
        "issued_by": ex.issued_by,
        "target": ex.target,
        "flags": list(ex.flags),
        "overlay": ex.overlay,
        "precondition": ex.precondition.outcome,
        "rejection_code": ex.precondition.rejection_code,
        "would_fire": list(ex.would_fire),
        "currency": ex.precondition.currency,
        "started_at": ex.started_at,
        "ended_at": ex.ended_at,
        "request": _request_to_json(ex.request, put_body),
        "response": None if ex.response is None else _response_to_json(ex.response, put_body),
        "stream_chunks": list(ex.stream_chunks),
    }


def event_to_json(seq: int, event: Event, put_body: PutBody) -> dict[str, JSONValue]:
    """Return one `events.jsonl` line: `seq` and `type`, then the record's fields."""
    head: dict[str, JSONValue] = {"seq": seq}
    if isinstance(event, Exchange):
        return head | {"type": "exchange"} | exchange_to_json(event, put_body)
    if isinstance(event, ToolCall):
        return head | {"type": "tool_call"} | tool_call_to_json(event)
    if isinstance(event, TelemetrySeen):
        return head | {"type": "telemetry"} | telemetry_to_json(event)
    assert_never(event)


def trigger_to_json(trigger: Trigger) -> dict[str, JSONValue]:
    """Return `trigger` as run.json holds it and as the SDK posts it in a run's start."""
    return {
        "name": trigger.name,
        "entrypoint": trigger.entrypoint,
        "args": trigger_args(trigger),
        "replayable": trigger.replayable,
    }


def error_to_json(error: ErrorInfo | None) -> dict[str, JSONValue] | None:
    """Return `error` as a run or tool call holds it and as the SDK posts it in a run's end."""
    return None if error is None else {"type": error.type, "message": error.message}


def _request_to_json(request: Request, put_body: PutBody) -> dict[str, JSONValue]:
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


def _response_to_json(response: Response, put_body: PutBody) -> dict[str, JSONValue]:
    return {
        "status": response.status,
        "headers": _headers_to_json(response.headers),
        "body": _body_to_json(response.body, put_body),
    }


def _headers_to_json(headers: Headers) -> list[JSONValue]:
    # Pairs, not an object: order and repeats (`set-cookie`) matter.
    return [[name, value] for name, value in headers]


def _body_to_json(body: bytes, put_body: PutBody) -> dict[str, JSONValue] | None:
    ref = put_body(body) if body else None
    if ref is None:
        return None
    return {"sha256": ref.sha256, "size": ref.size, "truncated": ref.truncated}


# ------------------------------------------------------------------------------------- decoders


def run_from_json(d: Mapping[str, Any]) -> RunRecord:
    version = _int(d, "schema_version")
    # The version is not quoted: `str` of a huge integer raises ValueError.
    if version > SCHEMA_VERSION:
        raise TraceFormatError(f"schema_version is newer than this irimi reads ({SCHEMA_VERSION})")
    if version < 1:
        raise TraceFormatError("schema_version is below 1, the first version")
    trigger = read_optional(d, "trigger", read_object)
    run_id = read_str(d, "run_id")
    mode: Mode = read_one_of(d, "mode", Mode)
    attribution: Attribution = read_one_of(d, "attribution", Attribution)
    stored = None if trigger is None else _stored_trigger(trigger, attribution)
    agent_version = read_optional(d, "agent_version", read_str)
    engine_version = read_str(d, "engine_version")
    sdk_version = read_optional(d, "sdk_version", read_str)
    started_at = read_optional(d, "started_at", read_float)
    ended_at = read_optional(d, "ended_at", read_float)
    outcome: RunStatus | None = read_optional(
        d, "outcome", lambda d, k: read_one_of(d, k, RunStatus)
    )
    error = read_error(d, "error")
    exit_code = read_optional(d, "exit_code", _int)
    record = RunRecord(
        schema_version=version,
        run_id=run_id,
        mode=mode,
        attribution=attribution,
        trigger=stored,
        agent_version=agent_version,
        engine_version=engine_version,
        sdk_version=sdk_version,
        started_at=started_at,
        end=_run_end(ended_at, outcome, error, exit_code),
        dropped_events=_int(d, "dropped_events"),
        replay_of=read_optional(d, "replay_of", _run_id),
    )
    # Every constructor sets `mode` from `replay_of`, so a mismatch means a damaged record.
    if record.mode != mode_for(record.replay_of):
        if record.replay_of is None:
            raise TraceFormatError("'mode' is 'replay', but 'replay_of' is null")
        raise TraceFormatError(f"'replay_of' names a run, but 'mode' is {record.mode!r}")
    return record


def _run_end(
    ended_at: float | None,
    outcome: RunStatus | None,
    error: ErrorInfo | None,
    exit_code: int | None,
) -> RunEnd | None:
    """Return the four flat end keys as a `RunEnd`, or None when the run has not ended."""
    if ended_at is not None and outcome is not None:
        return RunEnd(ended_at, outcome, error, exit_code)
    if ended_at is not None or outcome is not None:
        raise TraceFormatError("'ended_at' and 'outcome' are null together or not at all")
    if error is not None or exit_code is not None:
        raise TraceFormatError("a run that has not ended has a null 'error' and 'exit_code'")
    return None


def _stored_trigger(d: Mapping[str, Any], attribution: Attribution) -> Trigger:
    """Return a run's trigger, reading a process run's `args.argv` back into `Trigger.argv`."""
    trigger = trigger_from_json(d)
    if attribution != "process" or trigger.entrypoint is not None:
        return trigger
    args = trigger.args
    if not (isinstance(args, dict) and set(args) == {"argv"} and isinstance(args["argv"], list)):
        return trigger
    words = [word for word in args["argv"] if isinstance(word, str)]
    if not words or len(words) != len(args["argv"]):
        return trigger
    return Trigger(trigger.name, None, None, trigger.replayable, tuple(words))


def tool_call_from_json(d: Mapping[str, Any]) -> ToolCall:
    return ToolCall(
        tool_call_id=read_str(d, "tool_call_id"),
        run_id=read_str(d, "run_id"),
        name=read_str(d, "name"),
        kind=read_one_of(d, "kind", ToolKind),
        ran=read_one_of(d, "ran", Ran),
        args=read_json(d, "args"),
        result=read_json(d, "result"),
        error=read_error(d, "error"),
        started_at=read_float(d, "started_at"),
        ended_at=read_float(d, "ended_at"),
        awaited=read_bool(d, "awaited"),
    )


def telemetry_from_json(d: Mapping[str, Any]) -> TelemetrySeen:
    return TelemetrySeen(
        run_id=read_str(d, "run_id"),
        host=read_str(d, "host"),
        started_at=read_float(d, "started_at"),
    )


def exchange_from_json(d: Mapping[str, Any], get_body: GetBody) -> Exchange:
    """Return the Exchange `exchange_to_json` wrote, fetching each body through `get_body`."""
    response = read_optional(d, "response", read_object)
    return Exchange(
        request=_request_from_json(read_object(d, "request"), get_body),
        response=None if response is None else _response_from_json(response, get_body),
        service=read_str(d, "service"),
        operation=read_str(d, "operation"),
        kind=read_one_of(d, "kind", Kind),
        answered_by=read_one_of(d, "answered_by", AnsweredBy),
        run_id=read_str(d, "run_id"),
        door=read_one_of(d, "door", Door),
        flags=_flags(d, "flags"),
        target=read_str(d, "target"),
        overlay=read_optional(d, "overlay", lambda d, k: read_one_of(d, k, OverlayCoverage)),
        precondition=Precondition(
            read_optional(d, "precondition", lambda d, k: read_one_of(d, k, PreconditionOutcome)),
            read_str(d, "rejection_code"),
            read_str(d, "currency"),
        ),
        issued_by=read_one_of(d, "issued_by", IssuedBy),
        would_fire=_strings(d, "would_fire"),
        started_at=read_float(d, "started_at"),
        ended_at=read_float(d, "ended_at"),
        stream_chunks=_chunk_lengths(d, "stream_chunks"),
    )


def event_from_json(d: Mapping[str, Any], get_body: GetBody) -> tuple[int, Event]:
    """Return an `events.jsonl` line's `seq` and record."""
    seq = _int(d, "seq")
    kind: EventType = read_one_of(d, "type", EventType)
    if kind == "exchange":
        return seq, exchange_from_json(d, get_body)
    if kind == "tool_call":
        return seq, tool_call_from_json(d)
    if kind == "telemetry":
        return seq, telemetry_from_json(d)
    assert_never(kind)


def trigger_from_json(d: Mapping[str, Any]) -> Trigger:
    """Return the trigger `trigger_to_json` wrote, its args unchanged."""
    return Trigger(
        name=read_str(d, "name"),
        entrypoint=read_optional(d, "entrypoint", read_str),
        args=read_json(d, "args"),
        replayable=read_bool(d, "replayable"),
    )


def read_error(d: Mapping[str, Any], key: str) -> ErrorInfo | None:
    """Return the error `error_to_json` wrote under `key`, or None for `null`."""
    error = read_optional(d, key, read_object)
    if error is None:
        return None
    return ErrorInfo(type=read_str(error, "type"), message=read_str(error, "message"))


def _request_from_json(d: Mapping[str, Any], get_body: GetBody) -> Request:
    return Request(
        method=read_str(d, "method"),
        scheme=read_str(d, "scheme"),
        host=read_str(d, "host"),
        port=_int(d, "port"),
        path=read_str(d, "path"),
        query=read_str(d, "query"),
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
    ref = read_optional(d, key, read_object)
    if ref is None:
        return b""
    sha256 = read_str(ref, "sha256")
    if SHA256_PATTERN.fullmatch(sha256) is None:
        raise TraceFormatError(f"{_quoted(sha256)} is not a sha256 digest")
    return get_body(BodyRef(sha256, _int(ref, "size"), read_bool(ref, "truncated")))


# One reader per JSON type. Each raises only TraceFormatError, so a store has one exception to
# catch and the control endpoint answers a malformed body with a 400.


def json_type(value: Any) -> str:
    """Return a refused value's JSON type for an error message.

    Never `repr`: that can raise on a deeply nested list or a huge integer.
    """
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
    """Return `repr(text)`, cut after MAX_QUOTED characters with a trailing `…`."""
    if len(text) <= MAX_QUOTED:
        return repr(text)
    return f"{text[:MAX_QUOTED]!r}…"


def _present(d: Mapping[str, Any], key: str) -> Any:
    # Every reader calls this, so a line that parsed as `null`, `5` or `[]` is refused here.
    if not isinstance(d, Mapping):
        raise TraceFormatError(f"{key!r} must be read from an object, not {json_type(d)}")
    if key not in d:
        raise TraceFormatError(f"missing required field {key!r}")
    return d[key]


def read_optional[T](
    d: Mapping[str, Any], key: str, read: Callable[[Mapping[str, Any], str], T]
) -> T | None:
    """Return None for `null`, else `read(d, key)`. The key is still required."""
    return None if _present(d, key) is None else read(d, key)


def read_str(d: Mapping[str, Any], key: str) -> str:
    value = _present(d, key)
    if not isinstance(value, str):
        raise TraceFormatError(f"{key!r} is {json_type(value)}, not a string")
    return value


def _run_id(d: Mapping[str, Any], key: str) -> str:
    """Return a run id that names another run (`replay_of`), held to `is_valid_run_id`."""
    value = read_str(d, key)
    if not is_valid_run_id(value):
        raise TraceFormatError(f"{key!r} is not a run id ({len(value)} characters)")
    return value


def _int(d: Mapping[str, Any], key: str) -> int:
    value = _present(d, key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TraceFormatError(f"{key!r} is {json_type(value)}, not an integer")
    return value


def read_float(d: Mapping[str, Any], key: str) -> float:
    # An integer is accepted as a float. NaN and infinity are refused: a record holding one could
    # not be written back with `allow_nan=False`.
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


def read_json(d: Mapping[str, Any], key: str) -> JSONValue:
    """Return a free JSON value (`args`, `result`), refusing any non-finite float in it."""
    value: JSONValue = _present(d, key)
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


def read_bool(d: Mapping[str, Any], key: str) -> bool:
    value = _present(d, key)
    if not isinstance(value, bool):
        raise TraceFormatError(f"{key!r} is {json_type(value)}, not a boolean")
    return value


def read_object(d: Mapping[str, Any], key: str) -> Mapping[str, Any]:
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


def _flags(d: Mapping[str, Any], key: str) -> tuple[Flag, ...]:
    """Return an exchange's flags, dropping any this version does not know."""
    known = {flag.value for flag in Flag}
    return tuple(Flag(flag) for flag in _strings(d, key) if flag in known)


def _chunk_lengths(d: Mapping[str, Any], key: str) -> tuple[int, ...]:
    """Return a streamed response's chunk lengths: a list of positive integers."""
    value = _present(d, key)
    if not isinstance(value, list):
        raise TraceFormatError(f"{key!r} is {json_type(value)}, not a list")
    for index, length in enumerate(value):
        if isinstance(length, bool) or not isinstance(length, int) or length <= 0:
            raise TraceFormatError(f"{key!r}[{index}] is not a positive integer")
    return tuple(value)


def read_one_of(d: Mapping[str, Any], key: str, vocabulary: Any) -> Any:
    """Return a string from a `Literal` vocabulary; an unknown value is a format error."""
    value = read_str(d, key)
    if value not in get_args(vocabulary):
        raise TraceFormatError(f"{key!r} is {_quoted(value)}, not one of {get_args(vocabulary)}")
    return value
