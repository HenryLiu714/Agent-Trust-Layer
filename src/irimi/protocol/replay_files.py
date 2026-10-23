"""The two files a replay hands between processes.

- The outcome file: the replay's child (`irimi._replay_child`) writes how its call of a trigger
  ended (`CallOutcome`), and `irimi replay` ends the replay's run with it.
- The result file: `irimi replay --result-file` writes what the replay said (`ReplayResult`), and
  `irimi.testing.replay` reads it back.
"""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from irimi import exitcodes
from irimi.trace import ErrorInfo, error_to_json

RESULT_FIELDS = ("source_run_id", "replay_run_id", "exit_code", "diverged_at", "lines")


@dataclass(frozen=True)
class CallOutcome:
    """How the child's call ended: `called` False when the call could not be made, `error` why;
    otherwise what it raised, None when it returned. `cause` is, for a call not made because
    importing the trigger's module raised, that exception's type (`NotCallable.cause`)."""

    called: bool
    error: ErrorInfo | None
    cause: str | None = None


@dataclass(frozen=True)
class ReplayResult:
    """What one replay said, as `irimi.testing.replay` returns it.

    `same` is true when the replay made the same writes as the recording, and `str(result)` is
    what `irimi replay` printed, so `assert result.same, result` shows the difference.

    Attributes:
        source_run_id: The stored run that was replayed.
        replay_run_id: The run the replay stored, or None when it was refused before it ran.
        exit_code: `irimi replay`'s exit code: 0 the same writes, 1 any differ, 2 a replay that
            could not run or call its trigger, 130 or 143 one SIGINT or SIGTERM stopped.
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
        """The replay ran and made the same writes as the recording: exit code 0."""
        return self.exit_code == exitcodes.OK

    def __str__(self) -> str:
        return "\n".join(self.lines)


def call_outcome_to_json(outcome: CallOutcome) -> dict[str, Any]:
    d: dict[str, Any] = {"called": outcome.called, "error": error_to_json(outcome.error)}
    if outcome.cause is not None:
        d["cause"] = outcome.cause
    return d


def read_call_outcome(path: Path) -> CallOutcome | None:
    """The outcome the child wrote at `path`, or None when it wrote none that reads: it was
    killed, or never started, or the file is not one `call_outcome_to_json` writes. Never raises:
    the agent can write the file too, since its environment names it, and JSON nested deeper than
    the stack raises RecursionError, which would end `irimi replay` before it ends the run."""
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
        return CallOutcome(called, None, cause)
    if not (isinstance(error, dict) and set(error) == {"type", "message"}):
        return None
    if not all(isinstance(v, str) for v in error.values()):
        return None
    return CallOutcome(called, ErrorInfo(error["type"], error["message"]), cause)


def replay_result_to_json(result: ReplayResult) -> dict[str, Any]:
    return {
        "source_run_id": result.source_run_id,
        "replay_run_id": result.replay_run_id,
        "exit_code": result.exit_code,
        "diverged_at": result.diverged_at,
        "lines": list(result.lines),
    }


def replay_result_from_json(d: Mapping[str, Any]) -> ReplayResult:
    """Raises ValueError for anything `replay_result_to_json` did not write: a missing or unknown
    field, or one of the wrong type."""
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
