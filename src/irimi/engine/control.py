"""The control endpoint: `/_irimi/` on irimi's listener, where the SDK reports what the wire cannot
show. The proxy stays the only writer to the trace store.

    POST runs/{run_id}/start                204  the run's record, attribution `sdk`
    POST runs/{run_id}/tool-calls           204  one tool call; `unattributed` for no run
    POST runs/{run_id}/end                  204  the run's end
    POST runs/{run_id}/tool-calls/recorded  200  a replay's recorded read-tool result, else 404
    GET  health                             200  engine version, serve mode, store counters

- Answered here only: never forwarded or recorded as an exchange; every answer stamped `control`.
- Another major `Irimi-Protocol` version gets 426; decoders ignore fields they do not know.
- Never raises (fail-closed hook rule): a refusal is a 4xx naming the problem, a failure a 500.
- Not a boundary: any client that can reach the listener may call it.
"""

import dataclasses
import json
import logging
from collections.abc import Callable, Mapping
from typing import Any, NoReturn

from irimi import __version__
from irimi.core.exchange import Headers, Request, Response
from irimi.core.limits import MAX_CONTROL_BODY
from irimi.decide.policy import AnswerPolicy
from irimi.engine.wire import irimi_error
from irimi.protocol import control as protocol
from irimi.protocol import trace
from irimi.protocol.control import (
    CONTROL_ANSWER,
    CONTROL_PREFIX,
    PROTOCOL_VERSION,
    RECORDED_ACTION,
    RECORDED_LOOKUP_FIELDS,
    RUN_ACTIONS,
    RUNS_ROUTE,
    START_ACTION,
    TOOL_ACTIONS,
    TOOL_CALLS_ACTION,
    protocol_major,
)
from irimi.protocol.headers import ANSWERED_BY_HEADER, PROTOCOL_HEADER
from irimi.protocol.trace import UNATTRIBUTED, ErrorInfo, RunRecord, ToolCall, TraceFormatError
from irimi.record.store import RunStore
from irimi.record.store.layout import names_a_run_dir

logger = logging.getLogger(__name__)

# A refusal may quote the agent's path or run id; this caps the message's length.
MAX_REFUSAL = 200
# A body nested past `json.loads`'s recursion limit (which varies) is a 400, not irimi's 500.
TOO_DEEP = "the body nests too deeply to read"
OWN_RUN = "is irimi's own run, which the SDK may not start or end"

HEALTH_ROUTE = "health"
# A recorded-call miss, or any lookup outside a replay: the SDK then runs the tool for real.
NO_RECORDED_CALL = "no recorded call"

# Called with each tool call the store accepted, for the live summary.
OnToolCall = Callable[[ToolCall], None]

_STAMP = (ANSWERED_BY_HEADER, CONTROL_ANSWER)
# On every answer, so a client can tell which protocol the endpoint speaks without a 426.
_PROTOCOL = (PROTOCOL_HEADER, PROTOCOL_VERSION)


class _Refused(Exception):
    """A refused control request: the status and the one-line message `answer` returns."""

    def __init__(self, status: int, message: str, headers: Headers = ()) -> None:
        super().__init__(message)
        self.status = status
        self.message = message
        self.headers = headers


def _json_response(status: int, doc: Mapping[str, Any], headers: Headers = ()) -> Response:
    return Response.json(status, doc, (_STAMP, _PROTOCOL, *headers))


def _error_response(status: int, message: str, headers: Headers = ()) -> Response:
    """Return irimi's error stamped `control`: a 5xx is irimi's failure, else a refusal."""
    error_type = "irimi_internal_error" if status >= 500 else "irimi_control_refused"
    return irimi_error(status, error_type, message[:MAX_REFUSAL], (_STAMP, _PROTOCOL, *headers))


NO_CONTENT = Response(204, (_STAMP, _PROTOCOL), b"")
# The engine also sends these bytes if building an answer raised.
INTERNAL_ERROR = _error_response(500, "internal")


class ControlEndpoint:
    """Answers control requests against one trace store. Built once per engine."""

    def __init__(
        self,
        store: RunStore,
        *,
        serve: bool,
        process_run: str | None = None,
        on_tool_call: OnToolCall | None = None,
        policy: AnswerPolicy | None = None,
        replay_of: str | None = None,
    ) -> None:
        self.store = store
        self.serve = serve
        self.process_run = process_run
        # The stored run being replayed, if any; every run the SDK starts here replays it.
        self.replay_of = replay_of
        self.on_tool_call = on_tool_call
        # Answers `tool-calls/recorded`; the path's run id is checked, never used to choose.
        self.policy = policy

    def answer(self, request: Request, *, whole_body: bool = True) -> Response:
        """Return the response to one control request. Never raises (fail-closed hook rule).

        `whole_body` is False when the body decoded past the engine's cap; it is then refused 413.
        """
        try:
            return self._answer(request, whole_body)
        except _Refused as refused:
            return _error_response(refused.status, refused.message, refused.headers)
        except Exception:
            logger.exception(
                "irimi: the control endpoint failed on %s %s", request.method, request.path
            )
            return INTERNAL_ERROR

    def _answer(self, request: Request, whole_body: bool) -> Response:
        """Check version (426), route (404), method (405), run id, size (413), JSON and fields."""
        sent = request.header(PROTOCOL_HEADER)
        if sent is not None and protocol_major(sent) != protocol_major(PROTOCOL_VERSION):
            raise _Refused(
                426, f"this irimi speaks control protocol {PROTOCOL_VERSION}, not {sent}"
            )
        route = request.path.removeprefix(CONTROL_PREFIX)
        if route == HEALTH_ROUTE:
            _require_method(request, "GET")
            return _json_response(200, self._health())
        split = _split_run_route(route)
        if split is None:
            raise _Refused(404, f"no control route {request.path!r}")
        _require_method(request, "POST")
        run_id, action = split
        about_a_tool = action in TOOL_ACTIONS
        unattributed = run_id == UNATTRIBUTED and about_a_tool
        if not names_a_run_dir(run_id) and not unattributed:
            # The store would silently drop such an event; the agent is told instead.
            raise _Refused(400, f"{run_id!r} cannot name a run")
        if run_id == self.process_run and not about_a_tool:
            # A start would replace irimi's argv trigger; an end would precede the child's exit.
            raise _Refused(400, f"{run_id!r} {OWN_RUN}")
        posted = _posted(request, whole_body)
        if action == RECORDED_ACTION:
            return self._recorded(posted)
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
        # A posted `attribution` or `engine_version` is ignored: irimi sets those.
        start = _decode_or_400(protocol.run_start_from_json, posted)
        record = RunRecord.sdk(
            run_id,
            start.trigger,
            start.agent_version,
            start.sdk_version,
            start.started_at,
            self.replay_of,
        )
        self.store.start_run(record)

    def _tool_call(self, run_id: str, posted: Mapping[str, Any]) -> None:
        if "run_id" in posted:
            raise _Refused(400, "'run_id' comes from the path, not the body")
        call = _decode_or_400(trace.tool_call_from_json, {**posted, "run_id": run_id})
        # The store gets the error whole: it redacts before it cuts.
        self.store.record_tool_call(call)
        if self.on_tool_call is None:
            return
        # A broken live summary costs its line, never the stored call.
        try:
            self.on_tool_call(dataclasses.replace(call, error=_label(call.error)))
        except Exception:
            logger.exception("irimi: on_tool_call raised; the tool call is still stored")

    def _recorded(self, posted: Mapping[str, Any]) -> Response:
        """Return a read tool call's recorded outcome, or 404; a hit is not recorded as a call."""
        for field in RECORDED_LOOKUP_FIELDS:
            if field not in posted:  # worded as the trace decoders word it
                raise _Refused(400, f"missing required field {field!r}")
        name = posted["name"]
        if not isinstance(name, str):
            raise _Refused(400, f"'name' is {trace.json_type(name)}, not a string")
        found = (
            None if self.policy is None else self.policy.recorded_tool_call(name, posted["args"])
        )
        if found is None:
            return _error_response(404, NO_RECORDED_CALL)
        return _json_response(200, protocol.recorded_outcome_to_json(found))

    def _end(self, run_id: str, posted: Mapping[str, Any]) -> None:
        end = _decode_or_400(protocol.run_end_from_json, posted)
        self.store.end_run(run_id, end.ended_at, end.outcome, end.error)


def _split_run_route(route: str) -> tuple[str, str] | None:
    """Split `runs/{run_id}/{action}` from the end, so an id holding `/` is a 400, not a 404."""
    head = f"{RUNS_ROUTE}/"
    if not route.startswith(head):
        return None
    rest = route[len(head) :]
    for action in (RECORDED_ACTION, *RUN_ACTIONS):
        if rest.endswith(f"/{action}"):
            return rest[: -len(action) - 1], action
    return None


def _require_method(request: Request, method: str) -> None:
    if request.method != method:
        raise _Refused(
            405, f"{request.path} takes {method}, not {request.method}", (("allow", method),)
        )


def _posted(request: Request, whole_body: bool) -> Mapping[str, Any]:
    """Return the body as a JSON object, refusing `NaN` and `Infinity` (the store rejects them)."""
    if not whole_body:
        raise _Refused(413, f"the body decodes past the {MAX_CONTROL_BODY} bytes allowed")
    if len(request.body) > MAX_CONTROL_BODY:
        raise _Refused(
            413, f"the body is {len(request.body)} bytes, over the {MAX_CONTROL_BODY} allowed"
        )
    try:
        posted = json.loads(request.body, parse_constant=_refuse_constant)
    except RecursionError:
        raise _Refused(400, TOO_DEEP) from None
    except ValueError as exc:  # UnicodeDecodeError is a ValueError
        raise _Refused(400, f"the body is not JSON: {exc}") from None
    if not isinstance(posted, dict):
        raise _Refused(400, f"the body is {trace.json_type(posted)}, not an object")
    return posted


def _label(error: ErrorInfo | None) -> ErrorInfo | None:
    """Return a posted error cut for the live line; never store it (a pre-redaction cut leaks)."""
    return None if error is None else error.as_label()


def _decode_or_400[T](decode: Callable[[Mapping[str, Any]], T], doc: Mapping[str, Any]) -> T:
    """Return `doc` decoded, turning a TraceFormatError into a 400."""
    try:
        return decode(doc)
    except TraceFormatError as exc:
        raise _Refused(400, str(exc)) from None


def _refuse_constant(name: str) -> NoReturn:
    raise ValueError(f"{name} is not a finite number")
