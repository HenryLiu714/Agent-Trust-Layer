"""`irimi compare` over two corpus runs, and the output rows that neither run's summary printed."""

from __future__ import annotations

from tests.e2e.workflows.harness.result import Result


def compare(a: Result, b: Result) -> tuple[int, list[str], list[str]]:
    """Run `irimi compare` over the SDK runs `a` and `b` started, in their shared store."""
    ids = [result.one("run.start")["run"] for result in (a, b)]
    return b.compare_runs(*ids)


# Whose summary printed each kind of `irimi compare` row: both for `=`, A for `-` and a pair's A
# line, B for `+` and a pair's B line (printed under its A line).
ROW_SIDES = {"  = ": "AB", "  - ": "A", "  ~ A ": "A", "  ? A ": "A", "  + ": "B", "    B ": "B"}


# How a `~` or `?` pair's A row starts, and its B row under it.
PAIR_A_PREFIXES = ("  ~ A ", "  ? A ")
PAIR_B_PREFIX = "    B "
# How a differing field starts, printed under a `~` pair whose two lines read the same.
FIELD_DIFF_PREFIX = "      "


def _under_a_pair(rows: list[str], i: int) -> bool:
    """Return whether row `i` is a field line under a `~` pair's A and B rows."""
    while i > 0 and rows[i - 1].startswith(FIELD_DIFF_PREFIX):
        i -= 1
    return i >= 2 and rows[i - 1].startswith(PAIR_B_PREFIX) and rows[i - 2].startswith("  ~ A ")


def rows_not_shown(result: Result, run_a: str, run_b: str, out: list[str]) -> list[str]:
    """Return the rows of compare's output `out` that its runs' summaries did not print.

    Empty when compare showed each run's own writes and nothing else, except a pair it told apart
    (`_told_apart`)."""
    shown = {"A": result.shown_writes(run_a)[1], "B": result.shown_writes(run_b)[1]}
    rows = out[out.index("") + 1 : -2] if out[-2:-1] == [""] else []
    missing = []
    for i, row in enumerate(rows):
        if row.startswith(FIELD_DIFF_PREFIX) and _under_a_pair(rows, i):
            continue
        prefix = next((p for p in ROW_SIDES if row.startswith(p)), None)
        if prefix is not None and all(row[len(prefix) :] in shown[s] for s in ROW_SIDES[prefix]):
            continue
        pair = rows[i : i + 2] if row.startswith(PAIR_A_PREFIXES) else rows[max(i - 1, 0) : i + 1]
        if not (len(pair) == 2 and _told_apart(pair[0], pair[1], shown)):
            missing.append(row)
    return missing


def _told_apart(a_row: str, b_row: str, shown: dict[str, list[str]]) -> bool:
    """Return whether `a_row` over `b_row` is a pair both summaries print as one line.

    `irimi compare` escapes backslashes so the two rows differ; unescaped, both are that line."""
    if not (a_row.startswith(PAIR_A_PREFIXES) and b_row.startswith(PAIR_B_PREFIX)):
        return False
    a, b = a_row[len(PAIR_A_PREFIXES[0]) :], b_row[len(PAIR_B_PREFIX) :]
    line = a.replace("\\\\", "\\")
    return a != b and b.replace("\\\\", "\\") == line and line in shown["A"] and line in shown["B"]
