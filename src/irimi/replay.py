"""What `irimi replay` runs, and what it says of a replay once it has run (#84).

Pure, like `compare`: nothing here runs a process or prints. `cli.cmd_replay` asks `plan` what to
run for a stored run, runs it under the replay's engine, and prints `outcome`'s lines.

WHAT RUNS, in order of precedence (`plan`):

1. `-- CMD` given: that command, as `irimi shadow -- CMD` would run it.
2. `--call` given, or the run's trigger has an entrypoint: a call of that function with the args,
   `--args` or the trigger's, in `python -m irimi.sdk.replay_child`. Args the trigger did not
   record replayably are refused (`not_replayable`).
3. A process run (`irimi shadow -- <cmd>`): its recorded argv, run again.

Anything else is refused: there is nothing to run. So is a call `replay_child` could never make:
an entrypoint in `__main__`, which is `replay_child` itself in the child, and a process run's
`{"argv": [...]}` given to a function as its parameters; and `--args` with no function to give them
to.

WHAT IT SAYS (`Outcome.lines`): which run was replayed as which; how the agent ended, its exit code
printed and never deciding the exit, or the call it could not make (`replay_child.CallOutcome`),
which makes the replay one that could not run; where the replay diverged; how many of the agent's
calls the recording answered and how many it missed; then the body of `irimi compare` between the
two runs (#79), whose exit code is the replay's. A divergence is the first of the replayed run's
events, in the order stored (`seq`, which the store numbers from 1 with no gap), that the recording
did not answer: an exchange the agent sent flagged `replay-miss` (a precondition read irimi issued
never is), or a read tool that ran for real (#83). A write tool's stand-in runs in a replay as it
does under shadow, and is neither (#83). It is not `recording.Recording.diverged`, the engine's
state after a missed write, which a missed llm call never sets (#82).

A replay that missed nothing says so only when it made every call the recording holds; one that
made fewer, an agent that stopped early, says how many it never made. A call recorded within a
read tool's span is that tool's body's (`recording.made_within`, #83), which a replay answering the
tool never makes: it is the tool's, and not counted. A run the store dropped events of is never
called the same: what it lacks is not compared.
"""

from collections.abc import Sequence
from dataclasses import dataclass

from irimi import compare, redact, report
from irimi.exchange import FROM_RECORDING_FLAG, REPLAY_MISS_FLAG, Exchange
from irimi.recording import made_within
from irimi.sdk.replay_child import CallOutcome
from irimi.sdk.replays import ReplayResult
from irimi.servicemap import MapIndex
from irimi.store import StoredRun
from irimi.trace import JSONValue, RunRecord, ToolCall, Trigger, holds_description

NO_DIVERGENCE = "no divergence: every call was answered from the recording"
CHILD_MODULE = "irimi.sdk.replay_child"
# Where a run recorded under `python -m agent` or `python agent.py` says its trigger is (#89).
MAIN_MODULE = "__main__"


@dataclass(frozen=True)
class Call:
    """A trigger to call: its `module:qualname`, and its captured args, `{parameter: value}`."""

    entrypoint: str
    args: dict[str, JSONValue]


@dataclass(frozen=True)
class Plan:
    """What a replay runs: a `Call`, or a command (`argv`), and the trigger its run records - the
    replayed run's, with the call's entrypoint and args when they were given in its place."""

    call: Call | None
    argv: tuple[str, ...]
    trigger: Trigger | None


class Refused(Exception):
    """A replay that cannot run: the one line saying why, without `error: `."""


def plan(
    source: RunRecord,
    command: list[str],
    call: str | None,
    args: dict[str, JSONValue] | None,
) -> Plan:
    """What replaying `source` runs, given `irimi replay`'s `-- CMD`, `--call` and `--args` (see the
    module doc), or Refused."""
    trigger = source.trigger
    if command:
        return Plan(None, tuple(command), trigger)
    entrypoint = call or (trigger.entrypoint if trigger is not None else None)
    if entrypoint is not None:
        if entrypoint.partition(":")[0] == MAIN_MODULE:
            raise Refused(
                f"run {source.run_id}'s entrypoint {entrypoint} is in __main__, which is "
                "irimi's own child when it is called; start the agent by its module name, or "
                "pass --call or -- CMD"
            )
        # A process run's args are its argv, which no function takes; a replay's run is a process
        # run too, but keeps the trigger it replayed, entrypoint and args (#84).
        if args is None and call is not None and _argv(source) is not None:
            raise Refused(
                f"run {source.run_id} is a process run, whose args are its argv; pass --args "
                "with --call"
            )
        if args is None:
            reason = not_replayable(trigger)
            if reason is not None:
                raise Refused(
                    f"run {source.run_id}'s trigger arguments were not recorded replayably "
                    f"({reason}); pass --args"
                )
            assert trigger is not None and isinstance(trigger.args, dict)
            args = trigger.args
        elif holds_description(args):
            raise Refused("--args holds a value's description, not the value")
        name = trigger.name if trigger is not None else entrypoint
        return Plan(Call(entrypoint, args), (), Trigger(name, entrypoint, args, True))
    if args is not None:
        raise Refused(f"run {source.run_id} has no entrypoint to give --args to; pass --call too")
    argv = _argv(source)
    if argv is None:
        raise Refused(f"run {source.run_id} has no entrypoint and no argv; pass --call or -- CMD")
    return Plan(None, argv, trigger)


def not_replayable(trigger: Trigger | None) -> str | None:
    """Why a trigger's recorded args cannot be given to its function again, or None when they can:
    no trigger at all; args capture said were not replayable (a value stored by its description, a
    call that did not bind, a class `revive` could not find); args the store could not redact,
    stored as `<redaction-failed>` with `replayable` still true (#70); or args that are not an
    object of parameters, as `sdk.run(trigger=...)` may record."""
    if trigger is None:
        return "the run has no trigger"
    if trigger.args == redact.REDACTION_FAILED:
        return "the store could not redact them"
    if holds_description(trigger.args):
        return "a value was recorded as its description"
    if not trigger.replayable:
        return "the SDK could not capture them as values"
    if not isinstance(trigger.args, dict):
        return "they are not an object of parameters"
    return None


def _argv(source: RunRecord) -> tuple[str, ...] | None:
    """The command line a process run recorded (`runner.process_run`), or None."""
    trigger = source.trigger
    if source.attribution != "process" or trigger is None or not isinstance(trigger.args, dict):
        return None
    argv = trigger.args.get("argv")
    if not isinstance(argv, list) or not argv or not all(isinstance(a, str) for a in argv):
        return None
    return tuple(a for a in argv if isinstance(a, str))


@dataclass(frozen=True)
class Ended:
    """How a replay's agent ended: its exit code; what `replay_child` said of its call, None for a
    command, or a child that said nothing; and `stopped`, the exit code of an `irimi replay` that
    was itself interrupted (SIGINT, SIGTERM) while the agent ran, which is then the replay's."""

    code: int
    call: CallOutcome | None = None
    stopped: int | None = None


@dataclass(frozen=True)
class Outcome:
    """A replay, as `irimi replay` reports it: `source` replayed as `replayed`."""

    source: RunRecord
    replayed: RunRecord
    ended: Ended
    diverged_at: str | None  # the divergence point's line, `#<n>: <event line>`, or None
    answered: int  # the agent's exchanges and read tools the recording answered
    missed: int  # and those it did not
    # How many of the recording's calls the replay never made: those of the kinds a replay looks
    # up (`_looked_up`), less those it answered, and less those the replay made that no policy
    # was asked of (a write refused for its cut body before any was, #94).
    never_made: int
    comparison: compare.Comparison

    @property
    def called(self) -> bool:
        return self.ended.call is None or self.ended.call.called

    @property
    def exit_code(self) -> int:
        """The comparison's, but `compare.TROUBLE` for a call the child could not make, the signal
        exit code of a replay that was stopped, and never `compare.SAME` for a run the store
        dropped events of."""
        if self.ended.stopped is not None:
            return self.ended.stopped
        if not self.called:
            return compare.TROUBLE
        dropped = self.source.dropped_events or self.replayed.dropped_events
        return max(self.comparison.exit_code, compare.DIFFERENT if dropped else compare.SAME)

    def lines(self) -> list[str]:
        a = self.source.agent_version or report.NOT_SET
        b = self.replayed.agent_version or report.NOT_SET
        header = (
            f"irimi replay · {self.source.run_id} → {self.replayed.run_id} · "
            f"{report.printable(a)} → {report.printable(b)}"
        )
        lines = [header, f"  agent {self._agent()}"]
        if self.ended.stopped is not None:
            lines.append("  stopped: irimi replay was interrupted before the agent ended")
        if compare.args_differ(self.source, self.replayed):
            lines.append(f"  {compare.ARGS_DIFFER}")
        lines.append(f"  {self._diverged()}")
        lines.append(f"  {self.answered} from the recording · {self.missed} missed")
        for record, which in ((self.source, "recording"), (self.replayed, "replay")):
            if record.dropped_events:
                lines.append(
                    f"  the store dropped {record.dropped_events} of the {which}'s events, "
                    "which are not compared"
                )
        return [*lines, "", *self.comparison.lines()]

    def result(self) -> ReplayResult:
        """What `--result-file` writes, and `sdk.replay` returns."""
        return ReplayResult(
            self.source.run_id,
            self.replayed.run_id,
            self.exit_code,
            self.diverged_at,
            tuple(self.lines()),
        )

    def _agent(self) -> str:
        call = self.ended.call
        if call is not None and not call.called and call.error is not None:
            return f"could not be called: {report.printable(call.error.message)}"
        return f"exited {self.ended.code}"

    def _diverged(self) -> str:
        if self.diverged_at is not None:
            return f"diverged at {self.diverged_at}"
        if self.never_made > 0:
            were = "was" if self.never_made == 1 else "were"
            return (
                f"no divergence, but {self.never_made} of the recording's calls {were} never made"
            )
        return NO_DIVERGENCE


def refused(run_id: str, lines: Sequence[str], replay_run_id: str | None = None) -> ReplayResult:
    """The result of a replay that could not run, or whose run could not be read back: exit
    `compare.TROUBLE`, and what irimi said of why."""
    return ReplayResult(run_id, replay_run_id, compare.TROUBLE, None, tuple(lines))


def outcome(
    source: StoredRun, replayed: StoredRun, ended: Ended, index: MapIndex | None
) -> Outcome:
    """What replaying `source` as `replayed` did. `index` is the loaded maps, for each write's
    sentence, as `compare.compare_runs` takes it."""
    diverged = None
    answered = missed = unasked = 0
    for n, event in enumerate(replayed.events, start=1):
        hit = _answered(event)
        if hit is None:
            unasked += _looked_up(event)
            continue
        if hit:
            answered += 1
            continue
        missed += 1
        if diverged is None:
            diverged = f"#{n}: {report.event_line(event)}"
    tools = [e for e in source.events if isinstance(e, ToolCall) and e.kind == "read"]
    recorded = sum(_looked_up(e) and not _in_a_tool(e, tools) for e in source.events)
    never_made = recorded - answered - unasked
    comparison = compare.compare_runs(source, replayed, index)
    return Outcome(
        source.record,
        replayed.record,
        ended,
        diverged,
        answered,
        missed,
        max(never_made, 0),
        comparison,
    )


def _looked_up(event: object) -> bool:
    """Whether a replay asks the recording for `event`'s kind of call: an exchange the agent sent,
    but telemetry and a delegated answer, which a replay never looks up, and a read tool (#82,
    #83)."""
    if isinstance(event, Exchange):
        return (
            event.issued_by == "agent"
            and event.kind != "telemetry"
            and event.answered_by != "delegated"
        )
    return isinstance(event, ToolCall) and event.kind == "read"


def _in_a_tool(event: object, tools: list[ToolCall]) -> bool:
    """Whether `event` was recorded within one of the read `tools`' spans, its body's: an exchange,
    or a read tool its body called (#83)."""
    if isinstance(event, Exchange):
        return any(made_within(event.started_at, event.ended_at, tool) for tool in tools)
    if not isinstance(event, ToolCall):
        return False
    span = (event.started_at, event.ended_at)
    return any(
        tool is not event and span != (tool.started_at, tool.ended_at) and made_within(*span, tool)
        for tool in tools
    )


def _answered(event: object) -> bool | None:
    """Whether the recording answered `event`: True, False for a miss, None for an event no
    recording answers - telemetry, a delegated answer, a precondition read irimi issued, a write
    tool's stand-in, a tool the replay's SDK did not look up."""
    if isinstance(event, Exchange):
        if event.issued_by != "agent":
            return None
        if FROM_RECORDING_FLAG in event.flags:
            return True
        return False if REPLAY_MISS_FLAG in event.flags else None
    if isinstance(event, ToolCall) and event.kind == "read":
        return {"recording": True, "real": False}.get(event.ran)
    return None
