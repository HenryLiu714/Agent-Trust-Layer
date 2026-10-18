"""The child `irimi replay` starts to call a stored run's trigger again (#84).

    IRIMI_REPLAY_CALL=module:qualname IRIMI_REPLAY_ARGS_FILE=args.json \\
        IRIMI_REPLAY_OUTCOME_FILE=outcome.json python -m irimi.sdk.replay_child

`irimi replay` sets the three variables (`runner.child_env`), with the rest of what `irimi shadow`
gives its child, so this process's requests go through the replay's engine. It:

1. imports the function `module:qualname` names (`api.resolve`): the module, then each name in
   the qualname. This is the one place a replay imports the agent's code, as running it would;
   `capture.revive` never imports, so the args are revived after it, when the classes they name
   are loaded;
2. revives the args, the JSON object `{parameter: captured value}` the file holds;
3. calls the function with them (`capture.call_arguments`), and when the call returns a coroutine,
   an `async def` trigger's or a run a sync trigger handed off (#74), runs it to its end with
   `asyncio.run`, so the run's body runs and the run ends.

The function is normally the `@sdk.trigger` the run recorded, so the call is a run, which the SDK
adopts as the replay's (`runs.RunScope`): every request and tool call lands in the replay's run,
and posts no end of its own. So the child says how the call ended in the outcome file
(`CallOutcome`), and `irimi replay` ends the run with it: what the call raised, recorded as a run
records it (`errors.error_info`), so a replay that ends on a recorded `KeyError` stores
`builtins.KeyError`, as its recording did (#83).

It exits 0 when the call returns and 1 when it raises, printing the traceback to stderr, as an
uncaught exception would. A call it cannot make - a module that does not import, a qualname that
names nothing or whose lookup raises, a method, whose instance a run does not record, a function
in `__main__`, args that do not revive or do not bind to its parameters - exits `CANNOT_CALL`
with one `error:` line, and its outcome says why: `irimi replay` reports that as a replay that
could not run, never as an agent that ran and differed. The outcome is written on every path: an
interruption before the call says the call was never made, and then ends the process.
"""

import asyncio
import inspect
import json
import os
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from irimi import paths, trace
from irimi.sdk.api import NotCallable, resolve
from irimi.sdk.capture import NotReplayable, call_arguments, revive
from irimi.sdk.errors import error_info

# How the child exits when it could not call the trigger at all: a shell's "found, but cannot be
# run".
CANNOT_CALL = 126


@dataclass(frozen=True)
class CallOutcome:
    """How the child's call ended, as it writes it for `irimi replay`: `called` False when the
    call could not be made, `error` why; otherwise what it raised, None when it returned."""

    called: bool
    error: trace.ErrorInfo | None

    def to_json(self) -> dict[str, Any]:
        return {"called": self.called, "error": trace.error_to_json(self.error)}

    @classmethod
    def read(cls, path: Path) -> "CallOutcome | None":
        """The outcome the child wrote at `path`, or None when it wrote none that reads: it was
        killed, or never started, or the file is not one this module writes. Never raises: the
        agent can write the file too, since its environment names it, and JSON nested deeper than
        the stack raises RecursionError, which would end `irimi replay` before it ends the run
        (#84)."""
        try:
            d = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, RecursionError):
            return None
        if not isinstance(d, dict) or set(d) != {"called", "error"}:
            return None
        called, error = d["called"], d["error"]
        if not isinstance(called, bool):
            return None
        if error is None:
            return cls(called, None)
        if not (isinstance(error, dict) and set(error) == {"type", "message"}):
            return None
        if not all(isinstance(v, str) for v in error.values()):
            return None
        return cls(called, trace.ErrorInfo(error["type"], error["message"]))


def main() -> int:
    entrypoint = os.environ.get(paths.REPLAY_CALL_ENV)
    args_file = os.environ.get(paths.REPLAY_ARGS_FILE_ENV)
    outcome_file = os.environ.get(paths.REPLAY_OUTCOME_FILE_ENV)
    if not entrypoint or not args_file or not outcome_file:
        print(
            f"error: {paths.REPLAY_CALL_ENV}, {paths.REPLAY_ARGS_FILE_ENV} and "
            f"{paths.REPLAY_OUTCOME_FILE_ENV} name no call; irimi replay starts this module",
            file=sys.stderr,
        )
        return CANNOT_CALL
    try:
        fn = resolve(entrypoint)
        args, kwargs = _arguments(fn, _revived(args_file))
    except BaseException as exc:
        # Whatever stops the child before the call is a call never made, never an agent that ran
        # and raised (#84): a KeyboardInterrupt as the module loads too, which then ends this
        # process as it would have.
        why = _why_not_called(exc)
        print(f"error: cannot call {entrypoint}: {why}", file=sys.stderr)
        _write(outcome_file, CallOutcome(False, trace.ErrorInfo("cannot-call", why)))
        if not isinstance(exc, Exception):
            raise
        return CANNOT_CALL
    try:
        result = fn(*args, **kwargs)
        if inspect.iscoroutine(result):
            asyncio.run(result)
    except BaseException as exc:
        # Whatever ended the call is what its run records, a SystemExit or a KeyboardInterrupt as
        # a recorded run records it (#74); those two then end this process as they would have.
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
        Path(path).write_text(json.dumps(outcome.to_json()), encoding="utf-8")
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
