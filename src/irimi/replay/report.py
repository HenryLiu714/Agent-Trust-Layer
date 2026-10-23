"""Build what `irimi replay` reports once a replay has run. Pure: runs nothing, prints nothing.

- The divergence point is the first agent call the recording did not answer (a `replay-miss`
  exchange or a read tool that ran for real); only a missed write sets `ReplayIndex.diverged`.
- Calls within a read tool's span belong to its body and are not counted (`made_within`).
- The exit code is `irimi compare`'s, never the agent's; with dropped events it is never OK.
"""

import shlex
from collections.abc import Sequence
from dataclasses import dataclass

from irimi.core import exitcodes
from irimi.core.exchange import Exchange, Flag
from irimi.protocol.replay_files import CallResult, ReplayResult
from irimi.protocol.trace import RunRecord, StoredRun, ToolCall
from irimi.replay import compare
from irimi.replay.index import made_within, nested_in
from irimi.report.runs import event_line
from irimi.report.terminal import printable
from irimi.servicemaps import MapIndex

# A version a run did not record, beside one the other run did.
UNSET = "(unset)"
NO_DIVERGENCE = "no divergence: every call was answered from the recording"
# What a process run's replay does not reproduce, said under its header.
ARGV_ONLY = "argv only: environment, cwd and stdin are not recorded"


@dataclass(frozen=True)
class Ended:
    """How a replay's agent ended.

    `call` is what the child reported, None for a command or a silent child. `stopped` is the exit
    code of an `irimi replay` interrupted while the agent ran, which then becomes the replay's."""

    code: int
    call: CallResult | None = None
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
    # Recorded calls the replay looks up (`_looked_up`), less those it answered and those it made
    # without asking the policy (a write refused for its cut body).
    never_made: int
    comparison: compare.Comparison
    rerun: tuple[str, ...] | None = None  # the command line a process run's replay ran again

    @property
    def exit_code(self) -> int:
        """Return the comparison's exit code, or the stop signal's; never OK with dropped events."""
        if self.ended.stopped is not None:
            return self.ended.stopped
        dropped = self.source.dropped_events or self.replayed.dropped_events
        return max(self.comparison.exit_code, exitcodes.DIFFERENT if dropped else exitcodes.OK)

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
        # A source with no trigger was replayed by its command, which has no args to compare.
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
        """Return what `--result-file` writes and `testing.replay` returns."""
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
    """Return an `exitcodes.ERROR` result for a replay that could not run or be read back."""
    return ReplayResult(run_id, replay_run_id, exitcodes.ERROR, None, tuple(lines))


def outcome(
    source: StoredRun,
    replayed: StoredRun,
    ended: Ended,
    index: MapIndex | None,
    rerun: tuple[str, ...] | None = None,
) -> ReplayReport:
    """Return the report of replaying `source` as `replayed`.

    `index` is the loaded maps; `rerun` the recorded command line a process run's replay ran."""
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
    """Return `n` as an English ordinal: `1st`, `2nd`, `11th`, `21st`."""
    suffix = "th" if 10 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _looked_up(event: object) -> bool:
    """Return True if a replay asks the recording for `event`: an agent exchange or a read tool.

    Not telemetry, a target's answer or failure, or a request refused before the policy."""
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
    """Return True if `event` (an exchange or read tool) lies within a read tool's span."""
    if isinstance(event, Exchange):
        return any(made_within(event.started_at, event.ended_at, tool) for tool in tools)
    if not isinstance(event, ToolCall):
        return False
    return any(tool is not event and nested_in(event, tool) for tool in tools)


def _answered(event: object) -> bool | None:
    """Return whether the recording answered `event`: True, False for a miss, None if not asked.

    Not asked: telemetry, a target's answer, irimi's check read, a write tool's fake, and a tool
    the replay's SDK did not look up."""
    if isinstance(event, Exchange):
        if event.issued_by != "agent":
            return None
        if Flag.FROM_RECORDING in event.flags:
            return True
        return False if Flag.REPLAY_MISS in event.flags else None
    if isinstance(event, ToolCall) and event.kind == "read":
        return {"recording": True, "real": False}.get(event.ran)
    return None
