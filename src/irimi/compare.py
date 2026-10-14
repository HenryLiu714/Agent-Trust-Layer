"""Two stored runs side by side, write by write, with no verdict (#79, design D12).

A run's actions are the writes it attempted, in `seq` order: each exchange its summary prints a
write line for (`report.is_intercepted`) and each write tool whose stand-in ran
(`report.is_stood_in`). An action is that summary line, so a comparison and the two summaries
cannot disagree about what either run did. Two actions are the same by the line before it is
escaped for printing, since escaping prints a newline and a typed `\n` alike; a pair that differs
only so prints both its lines with each backslash escaped too, the one place a comparison does not
print the summary's line (`_row_lines`).

The two runs' actions are lined up in two passes. The first lines up the actions that are the
same, key and text, so they anchor the rest. The second lines up what lies between them by what
each action is: an exchange by its service and operation, a tool call by its name. A refund of a
different amount is then one write changed, not one removed and another added; and a run that adds
a second, refused refund after the first shows that one added, where lining up by key alone would
pair the first refund with the refused one and show the first as added.

An action whose body was cut when stored is never the same as another, whatever its text: the
first pass anchors none, and the second pairs it by key into a `cut` row, `?` (#79, #94).

Pure, like `report`: `compare_runs` returns a `Comparison` and prints nothing. `irimi compare`
prints `header_lines` over the comparison's lines, and `irimi replay` (#84) prints its own header
over the same lines, so there is one alignment.
"""

import difflib
import json
import unicodedata
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
# A pair either side of which is flagged `body-truncated` (#70, #94), whatever its two texts: a
# text rendered from part of a request, or from none (#94 stores an over-cap body empty), says
# nothing of what the cut part held, so two cut refunds of different amounts print alike and a
# pair irimi could not read is never called the same (#79). The flag does not say which body was
# cut, so a cut response counts too.
CUT_NOTE = "a body was cut when stored"

Mark = Literal["same", "changed", "cut", "only_a", "only_b"]
MARKERS: dict[Mark, str] = {"same": "=", "changed": "~", "cut": "?", "only_a": "-", "only_b": "+"}


@dataclass(frozen=True)
class Action:
    """One write a run attempted, as `irimi compare` lines it up and compares it."""

    key: tuple[str, ...]  # (EXCHANGE_KEY, service, operation), or (TOOL_KEY, the tool's name)
    # The summary's line for it, without the indent and before it is escaped. Two actions are the
    # same by this and not by `text`: escaping leaves a backslash as it is, so a Slack post holding
    # a newline and one holding a typed `\n` printed alike and compared `=` (#79).
    sentence: str
    cut: bool = False  # its exchange is flagged `body-truncated`

    @property
    def text(self) -> str:
        """The summary's line for it, without the indent, as the summary prints it (#72)."""
        return report.printable(self.sentence)

    @property
    def escaped(self) -> str:
        """`text` with each backslash escaped too, so a newline prints as `\\n` and a typed one as
        `\\\\n`, and each character that prints as nothing or as a space spelled as its escape:
        printed for a pair whose texts are alike though its sentences are not (#79)."""
        escaped = report.printable(self.sentence.replace("\\", "\\\\"))
        return "".join(_spelled(char) for char in escaped)

    @property
    def seen(self) -> str:
        """`text` as a reader sees it: each character that prints as nothing dropped, and each that
        prints as a space a space (#79)."""
        return "".join(_seen(char) for char in self.text)


# The Unicode categories that print as nothing or as a space: format characters (a zero-width
# space, a right-to-left override), separators, private-use and unassigned code points. Two posts
# that differ only by one print alike, and `report.printable` leaves them as they are (#79).
_UNSEEN = frozenset({"Cf", "Zs", "Zl", "Zp", "Co", "Cn"})


def _spelled(char: str) -> str:
    """`char`, or its `\\u`/`\\U` escape when a reader cannot see it; an ASCII space stays."""
    if char == " " or unicodedata.category(char) not in _UNSEEN:
        return char
    code = ord(char)
    return f"\\u{code:04x}" if code <= 0xFFFF else f"\\U{code:08x}"


def _seen(char: str) -> str:
    category = unicodedata.category(char)
    if category not in _UNSEEN:
        return char
    return " " if category.startswith("Z") else ""


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
    """The writes `run` attempted, in `seq` order, each with its summary line. Reads, LLM
    calls, telemetry and read tools are not actions, and neither is a retry the idempotency store
    answered as the write it repeats (#46)."""
    out: list[Action] = []
    for event in run.events:
        if isinstance(event, Exchange) and report.is_intercepted(event):
            cut = BODY_TRUNCATED_FLAG in event.flags
            key = (EXCHANGE_KEY, event.service, event.operation)
            out.append(Action(key, report.write_sentence(event, index), cut))
        elif isinstance(event, ToolCall) and report.is_stood_in(event):
            out.append(Action((TOOL_KEY, event.name), report.tool_write_sentence(event)))
    return out


def align(a: Sequence[Action], b: Sequence[Action]) -> tuple[Row, ...]:
    """`a` and `b` lined up, every action in exactly one row: the same actions first, by key and
    text, then what lies between them by key alone."""
    rows: list[Row] = []
    for tag, x, y in _opcodes(a, b, _anchor):
        if tag == "equal":
            rows += [Row("same", one, other) for one, other in zip(x, y, strict=True)]
        else:
            rows += _by_key(x, y)
    return tuple(rows)


def _anchor(action: Action) -> Hashable:
    """What the first pass lines `action` up by. A cut action's is a new object, equal to nothing,
    so it anchors nothing as the same and the second pass pairs it by key as `cut` (#79, #94)."""
    return object() if action.cut else (action.key, action.sentence)


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
    """Two actions of one key: `cut` when either was cut, whatever their texts (#79, #94)."""
    if x.cut or y.cut:
        return Row("cut", x, y)
    return Row("same" if x.sentence == y.sentence else "changed", x, y)


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
    """A pair that differs prints both sides, A over B; every other row prints its one action.

    Escaping leaves a backslash as it is, so a Slack post holding a newline and one holding a
    typed `\\n` print one line, and a `~` over two identical lines tells its reader nothing (#79).
    A pair whose sentences differ but print alike to a reader prints both with each backslash
    escaped first, the newline as `\\n` and the typed one as `\\\\n`, and each character a reader
    cannot see spelled as its escape: a zero-width space, a no-break space."""
    marker = MARKERS[row.mark]
    if row.mark in ("changed", "cut") and row.a is not None and row.b is not None:
        a, b = row.a.text, row.b.text
        if row.a.seen == row.b.seen and row.a.sentence != row.b.sentence:
            a, b = row.a.escaped, row.b.escaped
        return [f"  {marker} A {a}", f"    B {b}"]
    action = row.a if row.a is not None else row.b
    assert action is not None, row
    return [f"  {marker} {action.text}"]
