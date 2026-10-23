"""The control endpoint's routes and the bodies posted to them: what the SDK sends and what
`irimi.engine.control` reads, spelled once for both.

    POST /_irimi/runs/{run_id}/start                  a `RunStart`
    POST /_irimi/runs/{run_id}/tool-calls             a `trace.ToolCall`, its run id the path's
    POST /_irimi/runs/{run_id}/end                    a `trace.RunEnd` with no exit code
    POST /_irimi/runs/{run_id}/tool-calls/recorded    a lookup, answered a `RecordedOutcome`

Each body is decoded by the trace format's own readers, so a posted value is held to exactly the
rules a stored one is, and each refusal is a `TraceFormatError`.
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

# A request addressed to irimi's own listener whose path starts here is the SDK reporting a run's
# start, end or tool calls. It is never forwarded and never an `Exchange`.
CONTROL_PREFIX = "/_irimi/"
# The `Irimi-Answered-By` value every control answer carries. Not an `AnsweredBy`: that vocabulary
# is the trace's, and a control request never appears in a trace.
CONTROL_ANSWER = "control"
# The run routes under CONTROL_PREFIX: `runs/{run_id}/{action}`.
RUNS_ROUTE = "runs"
RunAction = Literal["start", "tool-calls", "end"]
START_ACTION: RunAction = "start"
TOOL_CALLS_ACTION: RunAction = "tool-calls"
END_ACTION: RunAction = "end"
RUN_ACTIONS: tuple[RunAction, ...] = get_args(RunAction)
# The one control route that answers with something: `runs/{run_id}/tool-calls/recorded`, where the
# SDK asks a replay for a read tool's recorded result. Not a run action: it reports nothing.
RECORDED_ACTION = f"{TOOL_CALLS_ACTION}/recorded"
# The routes about a tool call, which a run with no SDK start may use: the process run, or
# `unattributed` for a call made in no run.
TOOL_ACTIONS = frozenset({TOOL_CALLS_ACTION, RECORDED_ACTION})
# The control protocol this irimi speaks. The endpoint answers 426 to another major version, so an
# SDK and a proxy upgraded apart say so instead of losing runs.
PROTOCOL_VERSION = "1"
# What the SDK posts to ask a replay for a read tool's recorded outcome: the tool's name and its
# args as captured, and nothing else.
RECORDED_LOOKUP_FIELDS = ("name", "args")


def protocol_major(version: str) -> str:
    """The major part of a control protocol version: `1` of `1.2`."""
    return version.strip().partition(".")[0]


@dataclass(frozen=True)
class RunStart:
    """What the SDK posts when it starts a run. The rest of the run's record is irimi's to say."""

    trigger: Trigger
    agent_version: str | None
    sdk_version: str
    started_at: float


@dataclass(frozen=True)
class RecordedOutcome:
    """What a recorded read tool call came to, as a replay hands it back: the control endpoint
    answers the SDK's lookup with it, and the SDK reads it. `result` is still captured JSON, for
    the SDK to revive."""

    result: JSONValue
    error: ErrorInfo | None
    awaited: bool


def run_start_to_json(start: RunStart) -> dict[str, Any]:
    return {
        "trigger": trigger_to_json(start.trigger),
        "agent_version": start.agent_version,
        "sdk_version": start.sdk_version,
        "started_at": start.started_at,
    }


def run_start_from_json(d: Mapping[str, Any]) -> RunStart:
    """A posted start. Every field must be present, and only `agent_version` may be null."""
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


def run_end_to_json(end: RunEnd) -> dict[str, Any]:
    """`end` as the SDK posts it. Its exit code is not posted: only the process run has one."""
    return {"ended_at": end.ended_at, "outcome": end.outcome, "error": error_to_json(end.error)}


def run_end_from_json(d: Mapping[str, Any]) -> RunEnd:
    """A posted end. Every field must be present, and only `error` may be null."""
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


def recorded_outcome_to_json(call: ToolCall) -> dict[str, Any]:
    """The recorded outcome of `call`, as the control endpoint answers a lookup that hit."""
    return {"result": call.result, "error": error_to_json(call.error), "awaited": call.awaited}


def recorded_outcome_from_json(d: Mapping[str, Any]) -> RecordedOutcome:
    """The outcome `recorded_outcome_to_json` wrote, held to the rules a stored tool call is."""
    return RecordedOutcome(
        result=read_json(d, "result"),
        error=read_error(d, "error"),
        awaited=read_bool(d, "awaited"),
    )


def _null(name: str) -> NoReturn:
    raise TraceFormatError(f"{name!r} may not be null")
