"""`irimi compare` over two runs of the corpus, and the rows of its output that no summary of
either run printed."""

from __future__ import annotations

from tests.e2e.workflows.harness.result import Result


def compare(a: Result, b: Result) -> tuple[int, list[str], list[str]]:
    """`irimi compare` over the SDK run each of `a` and `b` started, as `b`'s user would type it,
    naming the store both recorded into: `run`'s `store`, given to both."""
    ids = [result.one("run.start")["run"] for result in (a, b)]
    return b.compare_runs(*ids)


# Whose summary printed each kind of `irimi compare` row: A's for a write both made the same
# (B's too), for one only A made, and for a pair's A line; B's for one only B made and for a pair's
# B line, which a `~` or a `?` prints under its A line.
ROW_SIDES = {"  = ": "AB", "  - ": "A", "  ~ A ": "A", "  ? A ": "A", "  + ": "B", "    B ": "B"}


# How a `~` or `?` pair's A row starts, and its B row under it.
PAIR_A_PREFIXES = ("  ~ A ", "  ? A ")
PAIR_B_PREFIX = "    B "
# How a field that differs starts, which compare prints under a `~` pair whose lines are one.
FIELD_DIFF_PREFIX = "      "


def _under_a_pair(rows: list[str], i: int) -> bool:
    """Whether row `i` is one of the field lines under a `~` pair: the rows above it, past any
    other field lines, are the pair's B row and its A row."""
    while i > 0 and rows[i - 1].startswith(FIELD_DIFF_PREFIX):
        i -= 1
    return i >= 2 and rows[i - 1].startswith(PAIR_B_PREFIX) and rows[i - 2].startswith("  ~ A ")


def rows_not_shown(result: Result, run_a: str, run_b: str, out: list[str]) -> list[str]:
    """Every row of `irimi compare <run_a> <run_b>`'s output `out` that is not a write line the
    summary of its run printed (`irimi runs show`), or that no row kind starts. Empty
    when compare showed each run's own writes and nothing else, but for the one exception compare
    makes: a pair it told apart (`_told_apart`)."""
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
    """Whether `a_row` over `b_row` is a `~` or `?` pair whose writes the two summaries print as
    one line, which `irimi compare` prints with each backslash escaped too so the two differ:
    the rows differ, and with each doubled backslash made one again, both are that line,
    and each run's summary printed it."""
    if not (a_row.startswith(PAIR_A_PREFIXES) and b_row.startswith(PAIR_B_PREFIX)):
        return False
    a, b = a_row[len(PAIR_A_PREFIXES[0]) :], b_row[len(PAIR_B_PREFIX) :]
    line = a.replace("\\\\", "\\")
    return a != b and b.replace("\\\\", "\\") == line and line in shown["A"] and line in shown["B"]
