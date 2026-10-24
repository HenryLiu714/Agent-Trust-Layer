"""A replay's two files: the child's `CallResult` and `irimi replay --result-file`'s result."""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from irimi.core import exitcodes
from irimi.protocol.trace import ErrorInfo, JSONValue, error_to_json

RESULT_FIELDS = ("source_run_id", "replay_run_id", "exit_code", "diverged_at", "lines")


@dataclass(frozen=True)
class CallResult:
    """How the child's call ended."""

    called: bool  # False: the call could not be made, and `error` says why
    error: ErrorInfo | None  # otherwise what the call raised, or None
    cause: str | None = None  # `NotCallable.cause`


@dataclass(frozen=True)
class ReplayResult:
    """One replay's result, as `irimi.testing.replay` returns it; `str()` gives the printed lines.

    Attributes:
        source_run_id: The stored run that was replayed.
        replay_run_id: The run the replay stored, or None when it was refused before it ran.
        exit_code: 0 same writes, 1 any differ, 2 could not run, 130/143 stopped by a signal.
        diverged_at: The line naming the first call the recording did not answer, or None.
        lines: What `irimi replay` printed, one string per line.

    Example:
        result = testing.replay(run_id)
        assert result.same, result
    """

    source_run_id: str
    replay_run_id: str | None
    exit_code: int
    diverged_at: str | None
    lines: tuple[str, ...]

    @property
    def same(self) -> bool:
        """Return True if the replay made the same writes as the recording."""
        return self.exit_code == exitcodes.OK

    def __str__(self) -> str:
        return "\n".join(self.lines)


def call_outcome_to_json(outcome: CallResult) -> dict[str, JSONValue]:
    d: dict[str, JSONValue] = {"called": outcome.called, "error": error_to_json(outcome.error)}
    if outcome.cause is not None:
        d["cause"] = outcome.cause
    return d


def read_call_outcome(path: Path) -> CallResult | None:
    """Return the child's outcome at `path`, or None; never raises, since the agent can write it."""
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, RecursionError):
        return None
    if not isinstance(d, dict) or set(d) - {"cause"} != {"called", "error"}:
        return None
    called, error, cause = d["called"], d["error"], d.get("cause")
    if not isinstance(called, bool) or not (cause is None or isinstance(cause, str)):
        return None
    if error is None:
        return CallResult(called, None, cause)
    if not (isinstance(error, dict) and set(error) == {"type", "message"}):
        return None
    if not all(isinstance(v, str) for v in error.values()):
        return None
    return CallResult(called, ErrorInfo(error["type"], error["message"]), cause)


def replay_result_to_json(result: ReplayResult) -> dict[str, JSONValue]:
    return {
        "source_run_id": result.source_run_id,
        "replay_run_id": result.replay_run_id,
        "exit_code": result.exit_code,
        "diverged_at": result.diverged_at,
        "lines": list(result.lines),
    }


def replay_result_from_json(d: Mapping[str, Any]) -> ReplayResult:
    """Return the result `replay_result_to_json` wrote; raise ValueError for anything else."""
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
    return ReplayResult(source, replayed, code, diverged, tuple(lines))
