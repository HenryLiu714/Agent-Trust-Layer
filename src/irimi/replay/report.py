"""What `irimi replay` says of a replay once it has run.

Pure, like `compare`: nothing here runs a process or prints.

WHAT IT SAYS (`ReplayReport.lines`): which run was replayed as which; for a process run, that
only its argv runs again; how the agent ended, its exit code printed and never deciding the exit;
where the replay diverged; how many of the agent's calls the recording answered and how many it
missed; then the body of `irimi compare` between the two runs, whose exit code is the replay's. A
replay whose call the child could not make (`CallOutcome`) has no report: `irimi replay` says why
and stops. A divergence is the first of the agent's calls the recording was asked for, in the
order stored (`seq`), that it did not answer: an exchange the agent sent flagged `replay-miss` (a
precondition read irimi issued never is), or a read tool that ran for real. It is numbered among
those calls, so the agent's 4th call is the 4th the totals count. A write tool's fake runs in a
replay as it does under shadow, and is neither. It is not `replay.index.ReplayIndex.diverged`, the
engine's state after a missed write, which a missed llm call never sets.

A replay that missed nothing says so only when it made every call the recording holds; one that
made fewer, an agent that stopped early, says how many it never made. A call recorded within a
read tool's span is that tool's body's (`replay.index.made_within`, `nested_in`), which a replay
answering the tool never makes: it is the tool's, and not counted. A run the store dropped events
of is never called the same: what it lacks is not compared.
"""

import shlex
from collections.abc import Sequence
from dataclasses import dataclass

from irimi import compare
from irimi.exchange import Exchange, Flag
from irimi.protocol.replay_files import CallOutcome, ReplayResult
from irimi.replay.index import made_within, nested_in
from irimi.report.runs import event_line
from irimi.report.terminal import printable
from irimi.servicemap import MapIndex
from irimi.trace import RunRecord, StoredRun, ToolCall

# A version a run did not record, beside one the other run did.
UNSET = "(unset)"
NO_DIVERGENCE = "no divergence: every call was answered from the recording"
# What a process run's replay does not reproduce, said under its header.
ARGV_ONLY = "argv only: environment, cwd and stdin are not recorded"


@dataclass(frozen=True)
class Ended:
    """How a replay's agent ended: its exit code; what `replay_child` said of its call, None for a
    command, or a child that said nothing; and `stopped`, the exit code of an `irimi replay` that
    was itself interrupted (SIGINT, SIGTERM) while the agent ran, which is then the replay's."""

    code: int
    call: CallOutcome | None = None
    stopped: int | None = None


@dataclass(frozen=True)
class ReplayReport:
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
            header += f" · agent {printable(a or UNSET)} -> {printable(b or UNSET)}"
        lines = [header]
        if self.rerun is not None:
            lines.append(f"  re-ran {printable(shlex.join(self.rerun))} ({ARGV_ONLY})")
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
) -> ReplayReport:
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
            diverged = f"the agent's {ordinal(n)} call: {event_line(event)}"
    tools = [e for e in source.events if isinstance(e, ToolCall) and e.kind == "read"]
    recorded = sum(_looked_up(e) and not _in_a_tool(e, tools) for e in source.events)
    never_made = recorded - answered - unasked
    comparison = compare.compare_runs(source, replayed, index)
    return ReplayReport(
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
    return any(tool is not event and nested_in(event, tool) for tool in tools)


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
