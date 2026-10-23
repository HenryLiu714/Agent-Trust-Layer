"""`sdk.replay()`: `irimi replay` as a Python function, for a test.

    result = sdk.replay(run_id)                         # the run's own trigger and args
    result = sdk.replay(run_id, handle, Ticket(...))    # a function of yours, with its args

It runs `python -m irimi replay --call ... --args-file ... --result-file ... -- RUN_ID` as a
subprocess and returns what that wrote, as a `ReplayResult`. The agent gets a fresh process, whose
HTTP clients read `HTTPS_PROXY` and the CA when they are built, as every `irimi shadow` child's do:
rewiring the environment of this process, whose clients may already exist, would not reach them.
Its `PYTHONPATH` is this process's `sys.path`, so it imports what this process can. The args go in
a file only this user can read, never on the command line, where `ps` shows them and the kernel
caps one argument's length. The run id goes after `--`, so one that starts with `-` is no flag.

`ReplayResult` and its JSON are declared here, the one codec: `irimi replay --result-file` writes
it with `to_json`, and this reads it back with `from_json`.

Imports only the stdlib and irimi's lowest layers, as the rest of the SDK does: no engine.
"""

import json
import os
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from irimi.sdk.api import NotCallable, entrypoint, resolve
from irimi.sdk.capture import MAX_CAPTURED_BYTES, bind, capture, capture_call

RESULT_FIELDS = ("source_run_id", "replay_run_id", "exit_code", "diverged_at", "lines")


def _run_irimi(argv: list[str], env: Mapping[str, str]) -> int:
    """`irimi <argv>` with `env`, in a subprocess of this interpreter: its exit code. The one seam
    a test harness that cannot let `irimi` leave its process replaces (the sample workflows', whose
    fake internet resolves names only there)."""
    return subprocess.run(
        [sys.executable, "-m", "irimi", *argv], env=dict(env), check=False
    ).returncode


@dataclass(frozen=True)
class ReplayResult:
    """What one `irimi replay` said. `replay_run_id` is the run the replay stored, None when
    it was refused before it ran (`exit_code` 2, the reason in `lines`). `exit_code` is the
    comparison's, as `irimi compare`'s: 0 the same writes, 1 any differ, 2 trouble (a replay that
    could not run, or call its trigger); and 130 or 143 for one SIGINT or SIGTERM stopped.
    `diverged_at` is the line of the first call the recording did not answer, None when every call
    was."""

    source_run_id: str
    replay_run_id: str | None
    exit_code: int
    diverged_at: str | None
    lines: tuple[str, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            "source_run_id": self.source_run_id,
            "replay_run_id": self.replay_run_id,
            "exit_code": self.exit_code,
            "diverged_at": self.diverged_at,
            "lines": list(self.lines),
        }

    @classmethod
    def from_json(cls, d: Mapping[str, Any]) -> "ReplayResult":
        """Raises ValueError for anything `to_json` did not write: a missing or unknown field, or
        one of the wrong type."""
        if not isinstance(d, Mapping) or set(d) != set(RESULT_FIELDS):
            raise ValueError(f"a replay result has the fields {', '.join(RESULT_FIELDS)}")
        source, replayed, code = d["source_run_id"], d["replay_run_id"], d["exit_code"]
        diverged, lines = d["diverged_at"], d["lines"]
        if (
            not isinstance(source, str)
            or not (replayed is None or isinstance(replayed, str))
            or not isinstance(code, int)
            or isinstance(code, bool)
            or not (diverged is None or isinstance(diverged, str))
            or not isinstance(lines, list)
            or not all(isinstance(line, str) for line in lines)
        ):
            raise ValueError("a replay result field has the wrong type")
        return cls(source, replayed, code, diverged, tuple(lines))


def replay(
    run_id: str,
    fn: Callable[..., Any] | None = None,
    /,
    *args: Any,
    store: str | os.PathLike[str] | None = None,
    **kwargs: Any,
) -> ReplayResult:
    """Replay the stored run `run_id` through irimi, and return what changed.

    `fn=None` replays the run's recorded entrypoint with its recorded args. Given `fn`, it is
    called instead, with `args` and `kwargs`: it must be importable as `module:qualname`, as a
    decorated trigger normally is, and its arguments must capture replayably (`capture`), or this
    raises ValueError naming why. `store` is the trace store, `$IRIMI_HOME/store` by default.

    Never raises on the replay's own exit code: a refused replay is a result with `exit_code` 2.
    RuntimeError when `irimi replay` exited without writing a result at all."""
    if fn is None and (args or kwargs):
        raise ValueError("sdk.replay takes arguments only for a function it is given")
    call = None if fn is None else (_importable(fn), _captured(fn, args, kwargs))
    # A directory only this user can enter: the args are the caller's, unredacted.
    with tempfile.TemporaryDirectory(prefix="irimi-replay-") as tmp:
        result_file = Path(tmp) / "result.json"
        argv = ["replay", "--port", "0", "--result-file", str(result_file)]
        if call is not None:
            args_file = Path(tmp) / "args.json"
            args_file.write_text(json.dumps(call[1]), encoding="utf-8")
            argv += ["--call", call[0], "--args-file", str(args_file)]
        if store is not None:
            argv += ["--store", os.fspath(store)]
        env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
        code = _run_irimi([*argv, "--", run_id], env)
        try:
            return ReplayResult.from_json(json.loads(result_file.read_text(encoding="utf-8")))
        except (OSError, ValueError) as exc:
            raise RuntimeError(f"irimi replay exited {code} and wrote no result: {exc}") from None


def _importable(fn: Callable[..., Any]) -> str:
    """`fn`'s `module:qualname`, which the child imports it by, or ValueError naming why it would
    import something else, or nothing: `api.resolve`'s refusals, `__main__` among them, are the
    child's, so they are asked of it here, the one rule."""
    try:
        name = entrypoint(fn, "sdk.replay")
    except TypeError as exc:
        raise ValueError(str(exc)) from None
    try:
        found = resolve(name)
    except NotCallable as exc:
        raise ValueError(f"{name} cannot be called by its name: {exc}") from None
    if found != fn:  # a classmethod is bound afresh on each lookup, and compares equal
        raise ValueError(f"{name} names another function than the one given")
    return name


def _captured(fn: Callable[..., Any], args: tuple[Any, ...], kwargs: Mapping[str, Any]) -> Any:
    """`args` and `kwargs` bound to `fn` and captured as a trigger's are, or ValueError naming
    why the child could not be given them: they do not bind, as calling `fn` would say, or which
    of them does not capture replayably."""
    try:
        bound = bind(fn, args, kwargs)
    except TypeError as exc:
        raise ValueError(f"the arguments do not bind to {fn.__qualname__}: {exc}") from None
    captured, replayable = capture_call(fn, args, kwargs)
    if replayable:
        return captured
    unreplayable = [name for name, value in bound.arguments.items() if not capture(value)[1]]
    if not unreplayable:
        raise ValueError(
            f"the arguments to {fn.__qualname__} are larger than a run records "
            f"({MAX_CAPTURED_BYTES} bytes)"
        )
    if len(unreplayable) == 1:
        raise ValueError(
            f"the argument {unreplayable[0]} to {fn.__qualname__} does not capture replayably"
        )
    raise ValueError(
        f"the arguments {', '.join(unreplayable)} to {fn.__qualname__} do not capture replayably"
    )
