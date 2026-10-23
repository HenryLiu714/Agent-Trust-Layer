"""Replay a stored run from a test: `irimi replay` as a Python function.

    result = testing.replay(run_id)
    assert result.same, result

This module starts the proxy in a subprocess, so it needs `irimi` installed with the proxy in the
interpreter that runs the test. Agent code imports `irimi.sdk`, never this.
"""

import json
import os
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from irimi.core.calls import bind
from irimi.core.limits import MAX_CAPTURED_BYTES
from irimi.protocol.entrypoints import NotCallable, entrypoint, resolve
from irimi.protocol.replay_files import ReplayResult, replay_result_from_json
from irimi.sdk._capture import capture, capture_call

__all__ = ["ReplayError", "ReplayResult", "replay"]


def _run_irimi(argv: list[str], env: Mapping[str, str]) -> int:
    """Run `irimi <argv>` with `env` in a subprocess of this interpreter, and return its exit
    code; replaced in tests."""
    return subprocess.run(
        [sys.executable, "-m", "irimi", *argv], env=dict(env), check=False
    ).returncode


class ReplayError(Exception):
    """A replay could not be started as asked, or exited without writing a result.

    Example:
        try:
            result = testing.replay(run_id)
        except testing.ReplayError as e:
            pytest.fail(str(e))
    """


def replay(
    run_id: str,
    call: Callable[..., Any] | None = None,
    *,
    args: tuple[Any, ...] = (),
    kwargs: Mapping[str, Any] | None = None,
    store: str | os.PathLike[str] | None = None,
) -> ReplayResult:
    """Replay a stored run through irimi, and return how the replay compared with it.

    The agent runs in a fresh process under a replay engine that answers from the recording, as
    `irimi replay` runs it. With no `call`, the run's recorded trigger is called with its recorded
    args; given `call`, that function is called instead with `args` and `kwargs`, for example to
    replay an edited version. A replay that ran and differed is a result, not an error.

    Args:
        run_id: The stored run to replay.
        call: A function to call in place of the recorded trigger. It must be importable as
            `module:qualname`, as a decorated trigger normally is.
        args: Positional arguments for `call`. They must be ones a run can record and revive.
        kwargs: Keyword arguments for `call`, under the same rule.
        store: The trace store. Defaults to `$IRIMI_STORE`, else `$IRIMI_HOME/store`.

    Returns:
        The `ReplayResult`; `result.same` is true when nothing diverged.

    Raises:
        ReplayError: When `args` or `kwargs` are given without `call`, `call` cannot be imported
            by its name, its arguments do not bind or cannot be recorded, or the replay exited
            without writing a result.

    Example:
        result = testing.replay(run_id, handle, args=(Ticket("T-1"),))
        assert result.same, result
    """
    kwargs = {} if kwargs is None else kwargs
    if call is None and (args or kwargs):
        raise ReplayError("testing.replay takes args and kwargs only for a call it is given")
    planned = None if call is None else (_importable(call), _captured(call, tuple(args), kwargs))
    # A directory only this user can enter: the args are the caller's, unredacted.
    with tempfile.TemporaryDirectory(prefix="irimi-replay-") as tmp:
        result_file = Path(tmp) / "result.json"
        argv = ["replay", f"--run={run_id}", "--port", "0", "--result-file", str(result_file)]
        if planned is not None:
            args_file = Path(tmp) / "args.json"
            args_file.write_text(json.dumps(planned[1]), encoding="utf-8")
            argv += ["--entrypoint", planned[0], "--args-file", str(args_file)]
        if store is not None:
            argv += ["--store", os.fspath(store)]
        env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
        code = _run_irimi(argv, env)
        try:
            return replay_result_from_json(json.loads(result_file.read_text(encoding="utf-8")))
        except (OSError, ValueError) as exc:
            raise ReplayError(f"irimi replay exited {code} and wrote no result: {exc}") from None


def _importable(fn: Callable[..., Any]) -> str:
    """`fn`'s `module:qualname`, which the child imports it by, or ReplayError naming why it
    would import something else, or nothing: `resolve`'s refusals, `__main__` among them, are
    the child's, so they are asked of it here, the one rule."""
    try:
        name = entrypoint(fn, "testing.replay")
    except TypeError as exc:
        raise ReplayError(str(exc)) from None
    try:
        found = resolve(name)
    except NotCallable as exc:
        raise ReplayError(f"{name} cannot be called by its name: {exc}") from None
    if found != fn:  # a classmethod is bound afresh on each lookup, and compares equal
        raise ReplayError(f"{name} names another function than the one given")
    return name


def _captured(fn: Callable[..., Any], args: tuple[Any, ...], kwargs: Mapping[str, Any]) -> Any:
    """`args` and `kwargs` bound to `fn` and captured as a trigger's are, or ReplayError naming
    why the child could not be given them: they do not bind, as calling `fn` would say, or which
    of them does not capture replayably."""
    try:
        bound = bind(fn, args, kwargs)
    except TypeError as exc:
        raise ReplayError(f"the arguments do not bind to {fn.__qualname__}: {exc}") from None
    captured, replayable = capture_call(fn, args, kwargs)
    if replayable:
        return captured
    unreplayable = [name for name, value in bound.arguments.items() if not capture(value)[1]]
    if not unreplayable:
        raise ReplayError(
            f"the arguments to {fn.__qualname__} are larger than a run records "
            f"({MAX_CAPTURED_BYTES} bytes)"
        )
    if len(unreplayable) == 1:
        raise ReplayError(
            f"the argument {unreplayable[0]} to {fn.__qualname__} does not capture replayably"
        )
    raise ReplayError(
        f"the arguments {', '.join(unreplayable)} to {fn.__qualname__} do not capture replayably"
    )
