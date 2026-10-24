"""The child process `irimi replay` starts to call a stored run's trigger again.

It reads the call, args file and outcome file from the environment (`runner.child_env`), revives
the args, calls the trigger, and writes a `CallResult`. Exits 0 when the call returns, 1 when it
raises, `CANNOT_CALL` when it cannot call. Imports only the stdlib and the SDK's layers, so it
runs under the agent's Python.
"""

import asyncio
import contextlib
import inspect
import json
import os
import sys
import traceback
from pathlib import Path
from typing import Any

from irimi.core import exitcodes
from irimi.core.calls import call_arguments
from irimi.protocol import trace
from irimi.protocol.entrypoints import NotCallable, resolve
from irimi.protocol.env import REPLAY_ARGS_FILE_ENV, REPLAY_CALL_ENV, REPLAY_OUTCOME_FILE_ENV
from irimi.protocol.replay_files import CallResult, call_outcome_to_json
from irimi.sdk._capture import NotReplayable, revive
from irimi.sdk._errors import error_info

# A shell's "found, but cannot be run".
CANNOT_CALL = exitcodes.CANNOT_EXEC


def main() -> int:
    """Call the trigger the environment names and write its outcome, on every path.

    The SDK adopts an `@sdk.trigger` as the replay's run, so the run posts no end of its own."""
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
        # Anything that stops the child before the call, a KeyboardInterrupt included, is a call
        # never made, not an agent that raised. A non-Exception still ends the process.
        why = _why_not_called(exc)
        print(f"error: cannot call {entrypoint}: {why}", file=sys.stderr)
        cause = exc.cause if isinstance(exc, NotCallable) else None
        _write(outcome_file, CallResult(False, trace.ErrorInfo("cannot-call", why), cause))
        if not isinstance(exc, Exception):
            raise
        return CANNOT_CALL
    try:
        result = fn(*args, **kwargs)
        if inspect.iscoroutine(result):
            asyncio.run(result)
    except BaseException as exc:
        # The run records whatever ended the call; SystemExit and KeyboardInterrupt still end
        # the process.
        _write(outcome_file, CallResult(True, error_info(exc)))
        if isinstance(exc, SystemExit | KeyboardInterrupt):
            raise
        traceback.print_exc()
        return 1
    _write(outcome_file, CallResult(True, None))
    return 0


def _why_not_called(exc: BaseException) -> str:
    """Return why a call was never made: a NotCallable's message, else what stopped the child."""
    if isinstance(exc, NotCallable):
        return str(exc)
    said = str(exc)
    return f"{type(exc).__name__} before the call" + (f": {said}" if said else "")


def _arguments(fn: Any, revived: dict[str, Any]) -> tuple[list[Any], dict[str, Any]]:
    try:
        return call_arguments(fn, revived)
    except TypeError as exc:
        raise NotCallable(f"its args do not bind: {exc}") from None


def _write(path: str, outcome: CallResult) -> None:
    """Write the outcome file. Never raises: `irimi replay` falls back to the exit code."""
    with contextlib.suppress(Exception):
        Path(path).write_text(json.dumps(call_outcome_to_json(outcome)), encoding="utf-8")


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
