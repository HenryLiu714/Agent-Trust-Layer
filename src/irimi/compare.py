"""Two stored runs side by side, write by write, with no verdict (#79, design D12).

A run's actions are the writes it attempted, in `seq` order: each exchange its summary prints a
write line for (`report.is_intercepted`) and each write tool whose stand-in ran
(`report.is_stood_in`). An action's text is that summary line, so a comparison and the two
summaries cannot disagree about what either run did.

The two runs' actions are lined up in two passes. The first lines up the actions that are the
same, key and text, so they anchor the rest. The second lines up what lies between them by what
each action is: an exchange by its service and operation, a tool call by its name. A refund of a
different amount is then one write changed, not one removed and another added; and a run that adds
a second, refused refund after the first shows that one added, where lining up by key alone would
pair the first refund with the refused one and show the first as added.

Pure, like `report`: `compare_runs` returns a `Comparison` and prints nothing. `irimi compare`
prints `header_lines` over the comparison's lines, and `irimi replay` (#84) prints its own header
over the same lines, so there is one alignment.
"""

import difflib
import json
from collections.abc import Callable, Hashable, Iterator, Sequence
from dataclasses import dataclass
from typing import Literal

from irimi import report
from irimi.exchange import BODY_TRUNCATED_FLAG, Exchange
from irimi.servicemap import MapIndex
from irimi.store import StoredRun
from irimi.trace import JSONValue, RunRecord, ToolCall

# The exit codes, as diff(1)'s: the same, different, or trouble (a run that cannot be read).
SAME = 0
DIFFERENT = 1
TROUBLE = 2

# The first item of an action's key. An exchange's key is three items long and a tool call's two,
# so no service a map names, `tool` included, can line a write up with a tool call.
EXCHANGE_KEY = "exchange"
TOOL_KEY = "tool"
NOT_A_VERDICT = "a comparison, not a verdict: nothing was replayed"
ARGS_DIFFER = "(the triggers' args differ)"
# A pair whose texts differ when either side is flagged `body-truncated` (#70, #94): its text may
# have been rendered from part of a request, so the difference may be the cut, not the run. The
# flag does not say which body was cut, so a cut response counts too.
CUT_NOTE = "a body was cut when stored"

Mark = Literal["same", "changed", "cut", "only_a", "only_b"]
MARKERS: dict[Mark, str] = {"same": "=", "changed": "~", "cut": "?", "only_a": "-", "only_b": "+"}


@dataclass(frozen=True)
class Action:
    """One write a run attempted, as `irimi compare` lines it up and compares it."""

    key: tuple[str, ...]  # (EXCHANGE_KEY, service, operation), or (TOOL_KEY, the tool's name)
    text: str  # the summary's line for it, without the indent
    cut: bool = False  # its exchange is flagged `body-truncated`


@dataclass(frozen=True)
class Row:
    """One line of a comparison: a pair lined up, or an action only one run has."""

    mark: Mark
    a: Action | None
    b: Action | None


@dataclass(frozen=True)
class Comparison:
    rows: tuple[Row, ...]

    def count(self, mark: Mark) -> int:
        return sum(1 for row in self.rows if row.mark == mark)

    @property
    def exit_code(self) -> int:
        """`SAME` when every row is `same`, and `DIFFERENT` otherwise: a pair it could not compare
        is not called the same."""
        return SAME if all(row.mark == "same" for row in self.rows) else DIFFERENT

    def lines(self) -> list[str]:
        """Every row, then a blank line and the totals; the totals alone when neither run wrote."""
        rows = [line for row in self.rows for line in _row_lines(row)]
        return [*rows, "", self._totals()] if rows else [self._totals()]

    def _totals(self) -> str:
        totals = (
            f"  {self.count('same')} same · {self.count('changed')} changed · "
            f"{self.count('only_a')} only in A · {self.count('only_b')} only in B"
        )
        cut = self.count("cut")
        return f"{totals} · {cut} not compared ({CUT_NOTE})" if cut else totals


def actions(run: StoredRun, index: MapIndex | None) -> list[Action]:
    """The writes `run` attempted, in `seq` order, each with its summary line's text. Reads, LLM
    calls, telemetry and read tools are not actions, and neither is a retry the idempotency store
    answered as the write it repeats (#46)."""
    out: list[Action] = []
    for event in run.events:
        if isinstance(event, Exchange) and report.is_intercepted(event):
            cut = BODY_TRUNCATED_FLAG in event.flags
            key = (EXCHANGE_KEY, event.service, event.operation)
            out.append(Action(key, report.write_text(event, index), cut))
        elif isinstance(event, ToolCall) and report.is_stood_in(event):
            out.append(Action((TOOL_KEY, event.name), report.tool_write_text(event)))
    return out


def align(a: Sequence[Action], b: Sequence[Action]) -> tuple[Row, ...]:
    """`a` and `b` lined up, every action in exactly one row: the same actions first, by key and
    text, then what lies between them by key alone."""
    rows: list[Row] = []
    for tag, x, y in _opcodes(a, b, lambda action: (action.key, action.text)):
        if tag == "equal":
            rows += [Row("same", one, other) for one, other in zip(x, y, strict=True)]
        else:
            rows += _by_key(x, y)
    return tuple(rows)


def _by_key(a: Sequence[Action], b: Sequence[Action]) -> list[Row]:
    """Actions no same action lined up, lined up by key. A stretch whose keys differ is all of
    `a`'s as `only_a`, then all of `b`'s as `only_b`."""
    rows: list[Row] = []
    for tag, x, y in _opcodes(a, b, lambda action: action.key):
        if tag == "equal":
            rows += [_pair(one, other) for one, other in zip(x, y, strict=True)]
        else:
            rows += [Row("only_a", one, None) for one in x]
            rows += [Row("only_b", None, other) for other in y]
    return rows


def _opcodes(
    a: Sequence[Action], b: Sequence[Action], by: Callable[[Action], Hashable]
) -> Iterator[tuple[str, Sequence[Action], Sequence[Action]]]:
    """`difflib`'s opcodes over `a` and `b` compared `by`, each with the two stretches it spans.
    No junk heuristic: a run of forty identical writes is forty writes, not noise."""
    matcher = difflib.SequenceMatcher(a=[by(x) for x in a], b=[by(y) for y in b], autojunk=False)
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        yield tag, a[i1:i2], b[j1:j2]


def _pair(x: Action, y: Action) -> Row:
    if x.text == y.text:
        return Row("same", x, y)
    return Row("cut" if x.cut or y.cut else "changed", x, y)


def compare_runs(a: StoredRun, b: StoredRun, index: MapIndex | None) -> Comparison:
    """`a`'s writes against `b`'s. `index` is the loaded maps, for each write's `human:` sentence,
    as `report.stored_summary_lines` takes it."""
    return Comparison(align(actions(a, index), actions(b, index)))


def header_lines(a: RunRecord, b: RunRecord) -> list[str]:
    """`irimi compare`'s header: which two runs, that no verdict follows, and each run's trigger.
    Says so when the triggers' args differ, since then the runs were not given the same work."""
    lines = [
        f"irimi compare · {a.run_id} vs {b.run_id} · {NOT_A_VERDICT}",
        f"  A  {report.run_label(a)}",
        f"  B  {report.run_label(b)}",
    ]
    if _args_differ(a, b):
        lines.append(f"  {ARGS_DIFFER}")
    return lines


def _args_differ(a: RunRecord, b: RunRecord) -> bool:
    """Compared as the JSON they are stored as, keys sorted: in Python `True == 1`, and the
    triggers' args `{"n": 1}` and `{"n": true}` differ. Args nested deeper than `json.dumps`
    writes are called different, since irimi cannot say they are the same (#100)."""
    try:
        return _canonical(a) != _canonical(b)
    except RecursionError:
        return True


def _canonical(record: RunRecord) -> str:
    args: JSONValue = record.trigger.args if record.trigger is not None else None
    return json.dumps(args, sort_keys=True)


def _row_lines(row: Row) -> list[str]:
    """A pair that differs prints both sides, A over B; every other row prints its one action."""
    marker = MARKERS[row.mark]
    if row.mark in ("changed", "cut") and row.a is not None and row.b is not None:
        return [f"  {marker} A {row.a.text}", f"    B {row.b.text}"]
    action = row.a if row.a is not None else row.b
    assert action is not None, row
    return [f"  {marker} {action.text}"]
