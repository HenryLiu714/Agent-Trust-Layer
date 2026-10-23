"""What `irimi replay` runs for a stored run.

Pure: nothing here runs a process or prints. `cli.cmd_replay` asks `plan` what to run, runs it
under the replay's engine, and prints `replay.report`'s lines.

WHAT RUNS, in order of precedence (`plan`):

1. `-- CMD` given: that command, as `irimi shadow -- CMD` would run it.
2. `--call` given, or the run's trigger has an entrypoint: a call of that function with the args,
   `--args` or the trigger's, in `python -m irimi.sdk.replay_child`, under irimi's own Python or
   `--python`'s. Args the trigger did not record replayably are refused
   (`not_replayable`).
3. A process run (`irimi shadow -- <cmd>`): its recorded argv, run again.

Anything else is refused: there is nothing to run. So is a call `replay_child` could never make:
an entrypoint in `__main__`, which is `replay_child` itself in the child, and a process run's
command line given to a function as its parameters; and `--args` with no function to give them
to.
"""

import os
import shlex
from collections.abc import Sequence
from dataclasses import dataclass

from irimi import redact
from irimi.report.terminal import printable
from irimi.trace import MAIN_MODULE, JSONValue, RunRecord, Trigger, holds_description

# What `python -m` runs to call a trigger: the child's own module, by its name. Named, not
# imported: the engine never imports the SDK.
CHILD_MODULE = "irimi.sdk.replay_child"


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
            raise Refused(_started_as_script(source.run_id, entrypoint, source.argv))
        # A process run's args are its argv, which no function takes.
        if args is None and call is not None and source.argv is not None:
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
    argv = source.argv
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
        return printable(
            f"run {run_id} was started as a script, so irimi cannot import its trigger "
            f"{entrypoint}; replay it with: {replay_with}"
        )
    words = [os.path.basename(launched_as[0]) or launched_as[0], *launched_as[1:]]
    return printable(
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
