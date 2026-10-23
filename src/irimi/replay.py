"""What `irimi replay` runs, and what it says of a replay once it has run.

Pure, like `compare`: nothing here runs a process or prints. `cli.cmd_replay` asks `plan` what to
run for a stored run, runs it under the replay's engine, and prints `outcome`'s lines.

WHAT RUNS, in order of precedence (`plan`):

1. `-- CMD` given: that command, as `irimi shadow -- CMD` would run it.
2. `--call` given, or the run's trigger has an entrypoint: a call of that function with the args,
   `--args` or the trigger's, in `python -m irimi.sdk.replay_child`, under irimi's own Python or
   `--python`'s. Args the trigger did not record replayably are refused
   (`not_replayable`).
3. A process run (`irimi shadow -- <cmd>`): its recorded argv, run again.

Anything else is refused: there is nothing to run. So is a call `replay_child` could never make:
an entrypoint in `__main__`, which is `replay_child` itself in the child, and a process run's
`{"argv": [...]}` given to a function as its parameters; and `--args` with no function to give them
to.

WHAT IT SAYS (`Outcome.lines`): which run was replayed as which; for a process run, that only its
argv runs again; how the agent ended, its exit code printed and never deciding the exit; where the
replay diverged; how many of the agent's calls the recording answered and how many it missed; then
the body of `irimi compare` between the two runs, whose exit code is the replay's. A replay whose
call the child could not make (`replay_child.CallOutcome`) has no outcome: `irimi replay` says why
and stops. A divergence is the first of the agent's calls the recording was asked for, in the
order stored (`seq`), that it did not answer: an exchange the agent sent flagged `replay-miss` (a
precondition read irimi issued never is), or a read tool that ran for real. It is numbered among
those calls, so the agent's 4th call is the 4th the totals count. A write tool's stand-in runs in
a replay as it does under shadow, and is neither. It is not `recording.Recording.diverged`, the
engine's state after a missed write, which a missed llm call never sets.

A replay that missed nothing says so only when it made every call the recording holds; one that
made fewer, an agent that stopped early, says how many it never made. A call recorded within a
read tool's span is that tool's body's (`recording.made_within`), which a replay answering the
tool never makes: it is the tool's, and not counted. A run the store dropped events of is never
called the same: what it lacks is not compared.
"""

import os
import shlex
from collections.abc import Sequence
from dataclasses import dataclass

from irimi import compare, redact, report
from irimi.exchange import Exchange, Flag
from irimi.recording import made_within
from irimi.sdk import replay_child
from irimi.sdk.replay_child import CallOutcome
from irimi.sdk.replays import ReplayResult
from irimi.servicemap import MapIndex
from irimi.store import StoredRun
from irimi.trace import MAIN_MODULE, JSONValue, RunRecord, ToolCall, Trigger, holds_description

# A version a run did not record, beside one the other run did.
UNSET = "(unset)"
NO_DIVERGENCE = "no divergence: every call was answered from the recording"
# What a process run's replay does not reproduce, said under its header.
ARGV_ONLY = "argv only: environment, cwd and stdin are not recorded"
# What `python -m` runs to call a trigger: the child's own module, by its name.
CHILD_MODULE = replay_child.__name__


@dataclass(frozen=True)
class Call:
    """A trigger to call: its `module:qualname`, and its captured args, `{parameter: value}`."""

    entrypoint: str
    args: dict[str, JSONValue]


@dataclass(frozen=True)
class Plan:
    """What a replay runs: a `Call`, or a command (`argv`), and the trigger its run records - the
    replayed run's, with the call's entrypoint and args when they were given in its place, and
    named after another function `--call` names. None for a command run for a run that recorded no
    trigger: its run records the command line, as a process run does. `rerun` is set when `argv`
    is the process run's own recorded command line."""

    call: Call | None
    argv: tuple[str, ...]
    trigger: Trigger | None
    rerun: bool = False


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
            raise Refused(_started_as_script(source.run_id, entrypoint, recorded_argv(source)))
        # A process run's args are its argv, which no function takes.
        if args is None and call is not None and recorded_argv(source) is not None:
            raise Refused(
                f"run {source.run_id} recorded a command line, not a call's arguments; pass "
                "--args with --call"
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
        # The run is named after the function it calls, by its `module:qualname`, as
        # `@sdk.trigger` names one; the recorded entrypoint called again keeps the recorded name,
        # which `name=` may have given.
        if trigger is not None and entrypoint == trigger.entrypoint:
            name = trigger.name
        else:
            name = entrypoint
        return Plan(Call(entrypoint, args), (), Trigger(name, entrypoint, args, True))
    if args is not None:
        raise Refused(f"run {source.run_id} has no entrypoint to give --args to; pass --call too")
    argv = recorded_argv(source)
    if argv is None:
        raise Refused(f"run {source.run_id} has no entrypoint and no argv; pass --call or -- CMD")
    return Plan(None, argv, trigger, rerun=True)


def _started_as_script(run_id: str, entrypoint: str, launched_as: Sequence[str] | None) -> str:
    """The refusal of a trigger in `__main__`, which in a replay's child is `replay_child` itself,
    ending with the command that replays it. Only a process run records the command line that
    names the trigger's module; an SDK run's record does not."""
    module = _script_module(launched_as) if launched_as else None
    replay_with = (
        f"irimi replay {run_id} --call {module or '<module>'}:{entrypoint.partition(':')[2]} "
        "--python <your venv python>"
    )
    if not launched_as:
        return report.printable(
            f"run {run_id} was started as a script, so irimi cannot import its trigger "
            f"{entrypoint}; replay it with: {replay_with}"
        )
    words = [os.path.basename(launched_as[0]) or launched_as[0], *launched_as[1:]]
    return report.printable(
        f"run {run_id} was started as a script ({shlex.join(words)}), so irimi cannot import "
        f"its trigger; replay it with: {replay_with}"
    )


def _script_module(argv: Sequence[str]) -> str | None:
    """The module a `python path/name.py` or `python -m name` command line runs, or None."""
    program = os.path.basename(argv[0])
    if program.endswith(".py"):
        return program[: -len(".py")]
    if not program.startswith("python") or len(argv) < 2:
        return None
    if argv[1] == "-m" and len(argv) > 2:
        return argv[2]
    if argv[1].endswith(".py"):
        return os.path.basename(argv[1])[: -len(".py")]
    return None


def not_replayable(trigger: Trigger | None) -> str | None:
    """Why a trigger's recorded args cannot be given to its function again, or None when they can:
    no trigger at all; args capture said were not replayable (a value stored by its description, a
    call that did not bind, a class `revive` could not find); args the store could not redact,
    stored as `<redaction-failed>` with `replayable` still true; or args that are not an
    object of parameters, as `sdk.run(name, inputs=...)` may record."""
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


def recorded_argv(source: RunRecord) -> tuple[str, ...] | None:
    """The command line a process run recorded (`runner.process_run`), or None. A replay's run is
    a process run too, but one that keeps the trigger it called, whose entrypoint says its args
    are a function's, even one with a parameter named `argv`."""
    trigger = source.trigger
    if source.attribution != "process" or trigger is None or trigger.entrypoint is not None:
        return None
    argv = trigger.args.get("argv") if isinstance(trigger.args, dict) else None
    if not isinstance(argv, list):
        return None
    words = [a for a in argv if isinstance(a, str)]
    return tuple(words) if words and len(words) == len(argv) else None


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
    # The divergence point, `the agent's <n>th call: <event line>`, or None.
    diverged_at: str | None
    answered: int  # the agent's exchanges and read tools the recording answered
    missed: int  # and those it did not
    # How many of the recording's calls the replay never made: those of the kinds a replay looks
    # up (`_looked_up`), less those it answered, and less those the replay made that no policy
    # was asked of (a write refused for its cut body before any policy saw it).
    never_made: int
    comparison: compare.Comparison
    # The recorded command line a process run's replay ran again, or None.
    rerun: tuple[str, ...] | None = None

    @property
    def exit_code(self) -> int:
        """The comparison's, but the signal exit code of a replay that was stopped, and never
        `compare.SAME` for a run the store dropped events of."""
        if self.ended.stopped is not None:
            return self.ended.stopped
        dropped = self.source.dropped_events or self.replayed.dropped_events
        return max(self.comparison.exit_code, compare.DIFFERENT if dropped else compare.SAME)

    def lines(self) -> list[str]:
        header = f"irimi replay · {self.source.run_id} -> {self.replayed.run_id}"
        a, b = self.source.agent_version, self.replayed.agent_version
        if a or b:
            header += f" · agent {report.printable(a or UNSET)} -> {report.printable(b or UNSET)}"
        lines = [header]
        if self.rerun is not None:
            lines.append(f"  re-ran {report.printable(shlex.join(self.rerun))} ({ARGV_ONLY})")
        lines.append(f"  agent exited {self.ended.code}")
        if self.ended.stopped is not None:
            lines.append("  stopped: irimi replay was interrupted before the agent ended")
        # Only of a trigger the source recorded: a run with none, replayed by its command, records
        # that command line as its trigger, and was given no args.
        if self.source.trigger is not None and compare.args_differ(self.source, self.replayed):
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
    source: StoredRun,
    replayed: StoredRun,
    ended: Ended,
    index: MapIndex | None,
    rerun: tuple[str, ...] | None = None,
) -> Outcome:
    """What replaying `source` as `replayed` did. `index` is the loaded maps, for each write's
    sentence, as `compare.compare_runs` takes it; `rerun` the recorded command line a process
    run's replay ran again."""
    diverged = None
    answered = missed = unasked = 0
    for event in replayed.events:
        hit = _answered(event)
        if hit is None:
            unasked += _looked_up(event)
            continue
        if hit:
            answered += 1
            continue
        missed += 1
        if diverged is None:
            n = answered + missed
            diverged = f"the agent's {ordinal(n)} call: {report.event_line(event)}"
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
        rerun,
    )


def ordinal(n: int) -> str:
    """`1st`, `2nd`, `3rd`, `4th`, `11th`, `21st`."""
    suffix = "th" if 10 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _looked_up(event: object) -> bool:
    """Whether a replay asks the recording for `event`'s kind of call: an exchange the agent sent,
    and a read tool. Not telemetry, a delegated answer or one whose target irimi could not reach,
    which a replay never looks up, nor a request irimi refused before asking the policy."""
    if isinstance(event, Exchange):
        return (
            event.issued_by == "agent"
            and event.kind != "telemetry"
            and event.answered_by != "delegated"
            and Flag.TARGET_FAILED not in event.flags
            and Flag.REFUSED not in event.flags
        )
    return isinstance(event, ToolCall) and event.kind == "read"


def _in_a_tool(event: object, tools: list[ToolCall]) -> bool:
    """Whether `event` was recorded within one of the read `tools`' spans, its body's: an exchange,
    or a read tool its body called."""
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
        if Flag.FROM_RECORDING in event.flags:
            return True
        return False if Flag.REPLAY_MISS in event.flags else None
    if isinstance(event, ToolCall) and event.kind == "read":
        return {"recording": True, "real": False}.get(event.ran)
    return None
