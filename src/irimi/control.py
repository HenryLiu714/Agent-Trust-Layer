"""The control endpoint: `/_irimi/` on irimi's own listener, where the SDK reports what the proxy
cannot see on the wire (#73).

The SDK runs inside the agent's process and the proxy in its own. A run's start and end, what
triggered it and the tool calls it made that are not HTTP reach the proxy here, and the proxy
hands them to the trace store. THE PROXY STAYS THE ONLY WRITER TO THE STORE: an SDK writing its
own files would need a disk the agent's container shares with irimi's, and could order a tool call
against an exchange only by comparing two processes' clocks.

    POST /_irimi/runs/{run_id}/start        204   the run's record, attribution `sdk`
    POST /_irimi/runs/{run_id}/tool-calls   204   one tool call, its run id from the path
    POST /_irimi/runs/{run_id}/end          204   the run's end
    GET  /_irimi/health                     200   engine version, serve mode, store counters

A control request is answered here and nowhere else: never forwarded, never put to the policy,
never recorded as an exchange. Every answer is stamped `Irimi-Answered-By: control`, because every
response irimi decided is stamped (CLAUDE.md). Its run id is in the path, never in `Irimi-Run`.

NOTHING HERE RAISES. `ControlEndpoint.answer` runs inside mitmproxy's request hook, and a raised
hook forwards the flow - here, to irimi's own listener. A request this endpoint refuses is a 4xx
whose body names what was wrong; anything else that fails is a 500, logged with its traceback. The
store only enqueues, so an answer does no disk I/O and never blocks the event loop.

The endpoint is not a boundary. Any client that can reach the listener may call it - loopback only
unless `serve` binds wider (#77) - and that client is the agent whose run it records.
"""

import dataclasses
import json
import logging
from collections.abc import Callable, Mapping
from typing import Any, NoReturn

from irimi import __version__, trace
from irimi.exchange import CONTROL_ANSWER, CONTROL_PREFIX, Door, Headers, Request, Response
from irimi.pipeline import ANSWERED_BY_HEADER
from irimi.store import TraceStore, names_a_run_dir
from irimi.trace import (
    MAX_ERROR_MESSAGE,
    SCHEMA_VERSION,
    ErrorInfo,
    RunRecord,
    ToolCall,
    TraceFormatError,
)

logger = logging.getLogger(__name__)

# The largest body a control request may post. A trigger's arguments and a tool call's result are
# JSON the SDK captured; a body past this is answered 413 and never parsed.
MAX_CONTROL_BODY = 2 * 1024 * 1024
# The longest line a refusal's body says. A decoder's message quotes the value it refused, and a
# 2 MiB string must not come back as a 2 MiB error.
MAX_REFUSAL = 200

HEALTH_ROUTE = "health"
RUNS_ROUTE = "runs"
START_ACTION = "start"
TOOL_CALLS_ACTION = "tool-calls"
END_ACTION = "end"
RUN_ACTIONS = (START_ACTION, TOOL_CALLS_ACTION, END_ACTION)

# The `RunRecord` fields each run route takes from the posted body. Every one must be present, and
# only `agent_version` and `error` may be null. The rest of the record is irimi's to say, so a
# posted `attribution` or `engine_version` is never read.
START_FIELDS = ("trigger", "agent_version", "sdk_version", "started_at")
START_REQUIRED = ("trigger", "sdk_version", "started_at")  # `agent_version` may be null
END_FIELDS = ("ended_at", "outcome", "error")  # `error` may be null

# Called with each tool call the store accepted, for the live summary (P3-10, #76).
OnToolCall = Callable[[ToolCall], None]

_STAMP = (ANSWERED_BY_HEADER, CONTROL_ANSWER)


class _Refused(Exception):
    """A control request this endpoint will not carry out: the status to answer and the one line
    its error body says. Raised by the checks below and turned into the answer by `answer`."""

    def __init__(self, status: int, message: str, headers: Headers = ()) -> None:
        super().__init__(message)
        self.status = status
        self.message = message
        self.headers = headers


def is_control_request(request: Request, door: Door) -> bool:
    """A request addressed to irimi's own listener (`reverse_door.detect_door` says `reverse`)
    whose path starts with CONTROL_PREFIX. Asked BEFORE the reverse door rewrites the request,
    which would read `_irimi` as an upstream host and refuse it with a 403. An absolute-form
    request through the forward proxy that names the listener is `reverse` too, so it is
    handled the same."""
    return door == "reverse" and request.path.startswith(CONTROL_PREFIX)


def _json_response(status: int, doc: Mapping[str, Any], headers: Headers = ()) -> Response:
    body = json.dumps(doc, allow_nan=False).encode()
    return Response(status, (_STAMP, ("content-type", "application/json"), *headers), body)


def _error_response(status: int, message: str, headers: Headers = ()) -> Response:
    return _json_response(status, {"error": message[:MAX_REFUSAL]}, headers)


NO_CONTENT = Response(204, (_STAMP,), b"")
# The answer to a control request that failed for a reason it did not cause. The engine sends the
# same bytes if building the answer itself raised.
INTERNAL_ERROR = _error_response(500, "internal")


class ControlEndpoint:
    """Answers control requests against one trace store. Built once per engine."""

    def __init__(
        self, store: TraceStore, *, serve: bool, on_tool_call: OnToolCall | None = None
    ) -> None:
        self.store = store
        self.serve = serve
        self.on_tool_call = on_tool_call

    def answer(self, request: Request) -> Response:
        """The response to one control request. Never raises (see the module doc)."""
        try:
            return self._answer(request)
        except _Refused as refused:
            return _error_response(refused.status, refused.message, refused.headers)
        except Exception:
            logger.exception(
                "irimi: the control endpoint failed on %s %s", request.method, request.path
            )
            return INTERNAL_ERROR

    def _answer(self, request: Request) -> Response:
        """Route, then check in this order: the method (405), the run id (400), the body's size
        (413), its JSON (400) and its fields (400). Only then is the store called."""
        route = request.path.removeprefix(CONTROL_PREFIX)
        if route == HEALTH_ROUTE:
            _require_method(request, "GET")
            return _json_response(200, self._health())
        # `runs/{run_id}/{action}`. The run id is everything between `runs/` and the action, so an
        # id holding a `/` - `../x` - is refused as an id (400) rather than as a route (404).
        segments = route.split("/")
        if len(segments) < 3 or segments[0] != RUNS_ROUTE or segments[-1] not in RUN_ACTIONS:
            raise _Refused(404, f"no control route {request.path!r}")
        _require_method(request, "POST")
        run_id = "/".join(segments[1:-1])
        if not names_a_run_dir(run_id):
            # The store would only count such an event as a drop, or file it in `unattributed/`:
            # the agent is told instead, and no directory is made (#70).
            raise _Refused(400, f"{run_id!r} cannot name a run")
        posted = _posted(request)
        action = segments[-1]
        if action == START_ACTION:
            self._start(run_id, posted)
        elif action == TOOL_CALLS_ACTION:
            self._tool_call(run_id, posted)
        else:
            self._end(run_id, posted)
        return NO_CONTENT

    def _health(self) -> dict[str, Any]:
        return {
            "engine_version": __version__,
            "serve": self.serve,
            "store": dataclasses.asdict(self.store.stats()),
        }

    def _start(self, run_id: str, posted: Mapping[str, Any]) -> None:
        record = _sdk_run(run_id, posted, START_FIELDS)
        for name in START_REQUIRED:
            if getattr(record, name) is None:
                _null(name)
        self.store.start_run(record)

    def _tool_call(self, run_id: str, posted: Mapping[str, Any]) -> None:
        if "run_id" in posted:
            raise _Refused(400, "'run_id' comes from the path, not the body")
        try:
            call = trace.tool_call_from_json({**posted, "run_id": run_id})
        except TraceFormatError as exc:
            raise _Refused(400, str(exc)) from None
        call = dataclasses.replace(call, error=_label(call.error))
        self.store.record_tool_call(call)
        if self.on_tool_call is None:
            return
        # The call is stored whatever the callback does: a live summary that breaks costs the
        # summary its line, never the run its record.
        try:
            self.on_tool_call(call)
        except Exception:
            logger.exception("irimi: on_tool_call raised; the tool call is still stored")

    def _end(self, run_id: str, posted: Mapping[str, Any]) -> None:
        record = _sdk_run(run_id, posted, END_FIELDS)
        if record.ended_at is None:
            _null("ended_at")
        if record.outcome is None:
            _null("outcome")
        self.store.end_run(run_id, record.ended_at, record.outcome, _label(record.error))


def _require_method(request: Request, method: str) -> None:
    if request.method != method:
        raise _Refused(
            405, f"{request.path} takes {method}, not {request.method}", (("allow", method),)
        )


def _posted(request: Request) -> Mapping[str, Any]:
    """The body as a JSON object. `NaN` and `Infinity` are refused, as the trace decoders refuse
    a non-finite timestamp and the store writes with `allow_nan=False` (#68, #70)."""
    if len(request.body) > MAX_CONTROL_BODY:
        raise _Refused(
            413, f"the body is {len(request.body)} bytes, over the {MAX_CONTROL_BODY} allowed"
        )
    try:
        posted = json.loads(request.body, parse_constant=_refuse_constant)
    except (ValueError, RecursionError) as exc:  # UnicodeDecodeError is a ValueError
        raise _Refused(400, f"the body is not JSON: {exc}") from None
    if not isinstance(posted, dict):
        raise _Refused(400, f"the body is a JSON {type(posted).__name__}, not an object")
    return posted


def _label(error: ErrorInfo | None) -> ErrorInfo | None:
    """A posted error cut to MAX_ERROR_MESSAGE characters, as `ErrorInfo.from_exception` cuts one:
    a run's error is a label, not a log (#68). Cut rather than refused, so that a long message
    never costs a run its end."""
    if error is None or len(error.message) <= MAX_ERROR_MESSAGE:
        return error
    return dataclasses.replace(error, message=error.message[:MAX_ERROR_MESSAGE])


def _refuse_constant(name: str) -> NoReturn:
    raise ValueError(f"{name} is not a finite number")


def _null(name: str) -> NoReturn:
    raise _Refused(400, f"{name!r} may not be null")


def _sdk_run(run_id: str, posted: Mapping[str, Any], fields: tuple[str, ...]) -> RunRecord:
    """A run started by the SDK, with `fields` read from `posted` by `trace.run_from_json`: the
    decoder of the record they belong to, so a posted value is held to exactly the rules a stored
    one is - a finite timestamp, an `outcome` from its vocabulary, a trigger with all its keys.

    Every other field is irimi's and comes from the encoder, never from `posted`. A field in
    `fields` that `posted` lacks is left out, so the decoder names it missing."""
    base = trace.run_to_json(
        RunRecord(
            schema_version=SCHEMA_VERSION,
            run_id=run_id,
            mode="shadow",
            attribution="sdk",
            trigger=None,
            agent_version=None,
            engine_version=__version__,
            sdk_version=None,
            started_at=None,
            ended_at=None,
            outcome=None,
            error=None,
            exit_code=None,
        )
    )
    doc = {key: value for key, value in base.items() if key not in fields}
    doc.update({key: posted[key] for key in fields if key in posted})
    try:
        return trace.run_from_json(doc)
    except TraceFormatError as exc:
        raise _Refused(400, str(exc)) from None
