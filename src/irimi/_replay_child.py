"""The child `irimi replay` starts to call a stored run's trigger again.

`irimi replay` runs `python -m irimi._replay_child` with `IRIMI_REPLAY_CALL`,
`IRIMI_REPLAY_ARGS_FILE` and `IRIMI_REPLAY_OUTCOME_FILE` set (`runner.child_env`). It imports the
trigger (`_entrypoints.resolve`), revives its args, calls it (running a coroutine to its end), and
writes how the call ended as a `CallOutcome`, which `irimi replay` ends the run with. It exits 0
when the call returns, 1 when it raises, and `CANNOT_CALL` with one `error:` line for a call it
could not make. Imports only the stdlib and the SDK's layers, so it runs under the agent's Python.
"""

import asyncio
import inspect
import json
import os
import sys
import traceback
from pathlib import Path
from typing import Any

from irimi import exitcodes, trace
from irimi._calls import call_arguments
from irimi._entrypoints import NotCallable, resolve
from irimi.protocol.env import REPLAY_ARGS_FILE_ENV, REPLAY_CALL_ENV, REPLAY_OUTCOME_FILE_ENV
from irimi.protocol.replay_files import CallOutcome, call_outcome_to_json
from irimi.sdk._capture import NotReplayable, revive
from irimi.sdk._errors import error_info

# How the child exits when it could not call the trigger at all: a shell's "found, but cannot be
# run".
CANNOT_CALL = exitcodes.CANNOT_EXEC


def main() -> int:
    """Call the trigger the environment names. The trigger is normally the run's `@sdk.trigger`,
    which the SDK adopts as the replay's run, so every request and tool call lands in it and the
    run posts no end of its own. What the call raised is recorded as a run records it
    (`_errors.error_info`). The outcome is written on every path: an interruption before the
    call says the call was never made, and then ends the process."""
    entrypoint = os.environ.get(REPLAY_CALL_ENV)
    args_file = os.environ.get(REPLAY_ARGS_FILE_ENV)
    outcome_file = os.environ.get(REPLAY_OUTCOME_FILE_ENV)
    if not entrypoint or not args_file or not outcome_file:
        print(
            f"error: {REPLAY_CALL_ENV}, {REPLAY_ARGS_FILE_ENV} and "
            f"{REPLAY_OUTCOME_FILE_ENV} name no call; irimi replay starts this module",
            file=sys.stderr,
        )
        return CANNOT_CALL
    try:
        fn = resolve(entrypoint)
        args, kwargs = _arguments(fn, _revived(args_file))
    except BaseException as exc:
        # Whatever stops the child before the call is a call never made, never an agent that ran
        # and raised: a KeyboardInterrupt as the module loads too, which then ends this
        # process as it would have.
        why = _why_not_called(exc)
        print(f"error: cannot call {entrypoint}: {why}", file=sys.stderr)
        cause = exc.cause if isinstance(exc, NotCallable) else None
        _write(outcome_file, CallOutcome(False, trace.ErrorInfo("cannot-call", why), cause))
        if not isinstance(exc, Exception):
            raise
        return CANNOT_CALL
    try:
        result = fn(*args, **kwargs)
        if inspect.iscoroutine(result):
            asyncio.run(result)
    except BaseException as exc:
        # Whatever ended the call is what its run records, a SystemExit or a KeyboardInterrupt as
        # a recorded run records it; those two then end this process as they would have.
        _write(outcome_file, CallOutcome(True, error_info(exc)))
        if isinstance(exc, SystemExit | KeyboardInterrupt):
            raise
        traceback.print_exc()
        return 1
    _write(outcome_file, CallOutcome(True, None))
    return 0


def _why_not_called(exc: BaseException) -> str:
    """The reason a call was never made: a NotCallable's own, else what stopped the child."""
    if isinstance(exc, NotCallable):
        return str(exc)
    said = str(exc)
    return f"{type(exc).__name__} before the call" + (f": {said}" if said else "")


def _arguments(fn: Any, revived: dict[str, Any]) -> tuple[list[Any], dict[str, Any]]:
    try:
        return call_arguments(fn, revived)
    except TypeError as exc:
        raise NotCallable(f"its args do not bind: {exc}") from None


def _write(path: str, outcome: CallOutcome) -> None:
    """Never raises: a child that cannot say how it ended still exits as it ended, and `irimi
    replay` then ends the run by its exit code alone."""
    try:
        Path(path).write_text(json.dumps(call_outcome_to_json(outcome)), encoding="utf-8")
    except Exception:
        pass


def _revived(args_file: str) -> dict[str, Any]:
    try:
        with open(args_file, encoding="utf-8") as fh:
            captured = json.load(fh)
    except (OSError, ValueError) as exc:
        raise NotCallable(f"its args could not be read: {exc}") from None
    if not isinstance(captured, dict):
        raise NotCallable("its args are not an object of parameters")
    try:
        revived = revive(captured)
    except NotReplayable as exc:
        raise NotCallable(f"its args do not revive: {exc}") from None
    assert isinstance(revived, dict)
    return revived


if __name__ == "__main__":
    sys.exit(main())
