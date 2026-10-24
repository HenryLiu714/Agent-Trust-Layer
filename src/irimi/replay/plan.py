"""Decide what `irimi replay` runs for a stored run. Pure: runs no process and prints nothing.

In order of precedence:

1. `-- CMD`: that command, as `irimi shadow -- CMD` would run it.
2. `--entrypoint`, or the trigger's: a call with `--args` or the trigger's replayable args, in
   `python -m irimi.replay.child`.
3. A process run: its recorded argv, run again.

Anything else is refused, as is an entrypoint in `__main__` and `--args` with no function.
"""

import os
import shlex
from collections.abc import Sequence
from dataclasses import dataclass

from irimi.protocol.trace import MAIN_MODULE, JSONValue, RunRecord, Trigger, holds_description
from irimi.record import redact
from irimi.report.terminal import printable

# Named, not imported: the child imports the SDK, which the engine never does.
CHILD_MODULE = "irimi.replay.child"


@dataclass(frozen=True)
class Call:
    """A trigger to call: its `module:qualname`, and its captured args, `{parameter: value}`."""

    entrypoint: str
    args: dict[str, JSONValue]


@dataclass(frozen=True)
class Plan:
    """What a replay runs: a `Call` or a command (`argv`), and the trigger its run records.

    `rerun` is set when `argv` is the process run's own recorded command line."""

    call: Call | None
    argv: tuple[str, ...]
    trigger: Trigger | None
    rerun: bool = False


class Refused(Exception):
    """A replay that cannot run; the message is one line saying why, without `error: `."""


def plan(
    source: RunRecord,
    command: list[str],
    call: str | None,
    args: dict[str, JSONValue] | None,
) -> Plan:
    """Return what replaying `source` runs, or raise Refused; see the module docstring."""
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
                "--args with --entrypoint"
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
        # Keep the recorded name if its entrypoint runs again: `name=` may have set it.
        if trigger is not None and entrypoint == trigger.entrypoint:
            name = trigger.name
        else:
            name = entrypoint
        return Plan(Call(entrypoint, args), (), Trigger(name, entrypoint, args, True))
    if args is not None:
        raise Refused(
            f"run {source.run_id} has no entrypoint to give --args to; pass --entrypoint too"
        )
    argv = source.argv
    if argv is None:
        raise Refused(
            f"run {source.run_id} has no entrypoint and no argv; pass --entrypoint or -- CMD"
        )
    return Plan(None, argv, trigger, rerun=True)


def _started_as_script(run_id: str, entrypoint: str, launched_as: Sequence[str] | None) -> str:
    """Return the refusal for a trigger in `__main__`, ending with a command that replays it.

    In the child, `__main__` is the child itself. Only a process run's argv names the module."""
    module = _script_module(launched_as) if launched_as else None
    replay_with = (
        f"irimi replay {run_id} --entrypoint {module or '<module>'}:{entrypoint.partition(':')[2]} "
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
    """Return the module a `python path/name.py` or `python -m name` command line runs, or None."""
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
    """Return why a trigger's recorded args cannot be passed to its function again, or None."""
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
