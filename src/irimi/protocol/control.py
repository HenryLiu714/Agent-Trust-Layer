"""The control endpoint's routes and bodies; a refused body raises `TraceFormatError`.

POST /_irimi/runs/{run_id}/start                  a `RunStart`
POST /_irimi/runs/{run_id}/tool-calls             a `trace.ToolCall`, its run id the path's
POST /_irimi/runs/{run_id}/end                    a `trace.RunEnd` with no exit code
POST /_irimi/runs/{run_id}/tool-calls/recorded    a lookup, answered a `RecordedOutcome`
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, NoReturn, get_args

from irimi.protocol.trace import (
    ErrorInfo,
    JSONValue,
    RunEnd,
    RunStatus,
    ToolCall,
    TraceFormatError,
    Trigger,
    error_to_json,
    read_bool,
    read_error,
    read_float,
    read_json,
    read_object,
    read_one_of,
    read_optional,
    read_str,
    trigger_from_json,
    trigger_to_json,
)

# The SDK reports under this path; such a request is never forwarded or recorded as an `Exchange`.
CONTROL_PREFIX = "/_irimi/"
# Not an `AnsweredBy`: control requests never appear in a trace.
CONTROL_ANSWER = "control"
RUNS_ROUTE = "runs"
RunAction = Literal["start", "tool-calls", "end"]
START_ACTION: RunAction = "start"
TOOL_CALLS_ACTION: RunAction = "tool-calls"
END_ACTION: RunAction = "end"
RUN_ACTIONS: tuple[RunAction, ...] = get_args(RunAction)
# A replay's lookup of a read tool's recorded result; it reports nothing, so is no run action.
RECORDED_ACTION = f"{TOOL_CALLS_ACTION}/recorded"
# Tool-call routes, usable by a run with no SDK start (the process run, or `unattributed`).
TOOL_ACTIONS = frozenset({TOOL_CALLS_ACTION, RECORDED_ACTION})
PROTOCOL_VERSION = "1"  # another major version gets a 426, so a mismatched SDK fails loudly
RECORDED_LOOKUP_FIELDS = ("name", "args")


def protocol_major(version: str) -> str:
    """Return the major part of a control protocol version: `1` of `1.2`."""
    return version.strip().partition(".")[0]


@dataclass(frozen=True)
class RunStart:
    """What the SDK posts when it starts a run."""

    trigger: Trigger
    agent_version: str | None
    sdk_version: str
    started_at: float


@dataclass(frozen=True)
class RecordedOutcome:
    """A recorded read tool call's outcome for a replay lookup; `result` is still captured JSON."""

    result: JSONValue
    error: ErrorInfo | None
    awaited: bool


def run_start_to_json(start: RunStart) -> dict[str, JSONValue]:
    return {
        "trigger": trigger_to_json(start.trigger),
        "agent_version": start.agent_version,
        "sdk_version": start.sdk_version,
        "started_at": start.started_at,
    }


def run_start_from_json(d: Mapping[str, Any]) -> RunStart:
    """Return a posted start. Every field is required; only `agent_version` may be null."""
    trigger = read_optional(d, "trigger", read_object)
    agent_version = read_optional(d, "agent_version", read_str)
    sdk_version = read_optional(d, "sdk_version", read_str)
    started_at = read_optional(d, "started_at", read_float)
    if trigger is None:
        _null("trigger")
    if sdk_version is None:
        _null("sdk_version")
    if started_at is None:
        _null("started_at")
    return RunStart(trigger_from_json(trigger), agent_version, sdk_version, started_at)


def run_end_to_json(end: RunEnd) -> dict[str, JSONValue]:
    """Return `end` as the SDK posts it, without the exit code (only a process run has one)."""
    return {"ended_at": end.ended_at, "outcome": end.outcome, "error": error_to_json(end.error)}


def run_end_from_json(d: Mapping[str, Any]) -> RunEnd:
    """Return a posted end. Every field is required; only `error` may be null."""
    ended_at = read_optional(d, "ended_at", read_float)
    outcome: RunStatus | None = read_optional(
        d, "outcome", lambda d, k: read_one_of(d, k, RunStatus)
    )
    error = read_error(d, "error")
    if ended_at is None:
        _null("ended_at")
    if outcome is None:
        _null("outcome")
    return RunEnd(ended_at, outcome, error, None)


def recorded_outcome_to_json(call: ToolCall) -> dict[str, JSONValue]:
    """Return the recorded outcome of `call`, as the endpoint answers a lookup that hit."""
    return {"result": call.result, "error": error_to_json(call.error), "awaited": call.awaited}


def recorded_outcome_from_json(d: Mapping[str, Any]) -> RecordedOutcome:
    """Return the outcome `recorded_outcome_to_json` wrote."""
    return RecordedOutcome(
        result=read_json(d, "result"),
        error=read_error(d, "error"),
        awaited=read_bool(d, "awaited"),
    )


def _null(name: str) -> NoReturn:
    raise TraceFormatError(f"{name!r} may not be null")
