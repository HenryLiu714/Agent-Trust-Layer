"""Two stored runs side by side, write by write.

A run's actions are the writes it attempted, in `seq` order: each exchange its summary prints a
write line for (`exchange.is_intercepted`) and each write tool whose fake ran
(`exchange.fake_ran`). An action is compared by its data, never by the line it prints: its key,
what irimi did with it (`outcome`), and what it sent (`fields`), the request less what says who
asked rather than what, as a replay matches it (`recording.compared_body`). A map's `human:`
template names a few of a write's fields, so two customer updates that print one line may still
differ; such a pair prints the fields that differ under its two lines.

The summary's line is still what each action prints (`sentence`). A pair whose lines differ but
print alike, since escaping prints a newline and a typed `\\n` alike, prints both with each
backslash escaped too, the one place a comparison does not print the summary's line
(`_row_lines`).

The two runs' actions are lined up in two passes. The first lines up the actions that are the
same, so they anchor the rest. The second lines up what lies between them by what each action is:
an exchange by its service and operation, a tool call by its name. A refund of a different amount
is then one write changed, not one removed and another added; and a run that adds a second,
refused refund after the first shows that one added, where lining up by key alone would pair the
first refund with the refused one and show the first as added.

An action whose body was cut when stored is never the same as another, whatever its data: the
first pass anchors none, and the second pairs it by key into a `cut` row, `?`.

Pure, like `report`: `compare_runs` returns a `Comparison` and prints nothing. `irimi compare`
prints `header_lines` over the comparison's lines, and `irimi replay` prints its own header over
the same lines (`replay.Outcome`), with `args_differ`'s note too, so there is one alignment.
"""

import difflib
import hashlib
import json
import unicodedata
from collections.abc import Callable, Hashable, Iterator, Sequence
from dataclasses import dataclass
from typing import Literal
from urllib.parse import parse_qsl

from irimi import report, services
from irimi.exchange import Exchange, Flag, fake_ran, is_intercepted
from irimi.recording import compared_body
from irimi.servicemap import MapIndex, path_params
from irimi.store import StoredRun
from irimi.trace import JSONValue, RunRecord, ToolCall, canonical_json

# The exit codes, as diff(1)'s: the same, different, or trouble (a run that cannot be read).
SAME = 0
DIFFERENT = 1
TROUBLE = 2

# The first item of an action's key. An exchange's key is three items long and a tool call's two,
# so no service a map names, `tool` included, can line a write up with a tool call.
EXCHANGE_KEY = "exchange"
TOOL_KEY = "tool"
ARGS_DIFFER = "(the triggers' args differ)"
# A pair either side of which is flagged `body-truncated`, whatever its data: a body cut when
# stored says nothing of what the cut part held, so two cut refunds of different amounts look
# alike and a pair irimi could not read is never called the same. The flag does not say which
# body was cut, so a cut response counts too.
CUT_NOTE = "a body was cut when stored"
# The flags that say what irimi did with a write, beside `answered_by` and the precondition. Not
# `from-recording` or `replay-miss`, which say how a replay answered and would set every replayed
# write apart from its recording.
OUTCOME_FLAGS = frozenset(
    {
        Flag.DECISION_FAILED,
        Flag.REFUSED,
        Flag.BODY_TRUNCATED,
        Flag.TARGET_FAILED,
        Flag.IDEMPOTENCY_CONFLICT,
    }
)
# How many differing fields a pair prints under its lines, how much of each value, and how many
# levels into an object it names the member that differs.
MAX_FIELDS_SHOWN = 5
MAX_VALUE_SHOWN = 60
MAX_DEPTH_SHOWN = 4
ABSENT = "(absent)"

Mark = Literal["same", "changed", "cut", "only_a", "only_b"]
MARKERS: dict[Mark, str] = {"same": "=", "changed": "~", "cut": "?", "only_a": "-", "only_b": "+"}

# A named value of an action, its value as canonical JSON.
Field = tuple[str, str]


@dataclass(frozen=True)
class Action:
    """One write a run attempted, as `irimi compare` lines it up and compares it."""

    key: tuple[str, ...]  # (EXCHANGE_KEY, service, operation), or (TOOL_KEY, the tool's name)
    # The summary's line for it, without the indent and before it is escaped: what it prints.
    sentence: str
    cut: bool = False  # its exchange is flagged `body-truncated`
    # What irimi did with it, and what it sent: two actions are the same by these and their key.
    outcome: tuple[Field, ...] = ()
    # None for data JSON cannot hold, nested too deep or not finite, which is the same as nothing.
    fields: tuple[Field, ...] | None = ()

    @property
    def text(self) -> str:
        """The summary's line for it, without the indent, as the summary prints it."""
        return report.printable(self.sentence)

    @property
    def escaped(self) -> str:
        """`text` with each backslash escaped too, so a newline prints as `\\n` and a typed one as
        `\\\\n`, and each character that prints as nothing or as a space spelled as its escape:
        printed for a pair whose texts are alike though its sentences are not."""
        escaped = report.printable(self.sentence.replace("\\", "\\\\"))
        return "".join(_spelled(char) for char in escaped)

    @property
    def seen(self) -> str:
        """`text` as a reader sees it: each character that prints as nothing dropped, and each that
        prints as a space a space."""
        return "".join(_seen(char) for char in self.text)

    @property
    def data(self) -> Hashable:
        """What it is compared by. A cut action's, and one whose fields JSON cannot hold, is a new
        object, equal to nothing."""
        if self.cut or self.fields is None:
            return object()
        return (self.key, self.outcome, self.fields)


# The Unicode categories that print as nothing or as a space: format characters (a zero-width
# space, a right-to-left override), separators, private-use and unassigned code points. Two posts
# that differ only by one print alike, and `report.printable` leaves them as they are.
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
    answered as the write it repeats."""
    out: list[Action] = []
    checks = report.check_reads([event for event in run.events if isinstance(event, Exchange)])
    for event in run.events:
        if isinstance(event, Exchange) and is_intercepted(event):
            out.append(_exchange_action(event, index, checks.get(id(event))))
        elif isinstance(event, ToolCall) and fake_ran(event):
            error = event.error.type if event.error is not None else None
            out.append(
                Action(
                    (TOOL_KEY, event.name),
                    report.tool_write_sentence(event),
                    outcome=_named({"raised": error}),
                    fields=_fields(_spread("args", event.args)),
                )
            )
    return out


def _spread(name: str, value: JSONValue) -> dict[str, JSONValue]:
    """An object's members as fields of their own, `name.member` in member order, so a diff names
    the one that differs; anything else as the one field `name`."""
    if isinstance(value, dict):
        return {f"{name}.{member}": value[member] for member in sorted(value)}
    return {name: value}


def _exchange_action(
    exchange: Exchange, index: MapIndex | None, check_read: Exchange | None
) -> Action:
    request = exchange.request
    route = report.route_of(exchange, index)
    pairs = parse_qsl(request.query, keep_blank_values=True)
    sent: dict[str, JSONValue] = {}
    if route is not None:
        # The objects its path names, by the route's holes. A path the store redacted, a webhook's,
        # is a placeholder only the key of the install that stored it spells, and names none.
        named: dict[str, JSONValue] = dict(path_params(route.path, request.path))
        sent.update(_spread("path", named))
    else:
        sent["request"] = f"{request.method} {request.host}{request.path}"
    sent["query"] = [list(pair) for pair in sorted(pairs, key=lambda pair: pair[0])]
    sent["scope"] = list(services.scope_of(exchange.service, request))
    sent.update(_spread("body", compared_body(request, exchange.kind, route)))
    fields = _fields(sent)
    if fields is None and request.body:
        # A body JSON cannot hold is still the same as its own bytes.
        sent = {k: v for k, v in sent.items() if not k.startswith("body")}
        sent["body"] = "sha256:" + hashlib.sha256(request.body).hexdigest()
        fields = _fields(sent)
    flags: list[JSONValue] = [flag for flag in sorted(exchange.flags) if flag in OUTCOME_FLAGS]
    outcome: dict[str, JSONValue] = {
        "answered by": exchange.answered_by,
        "precondition": exchange.precondition,
        "rejection code": exchange.rejection_code,
        "target": exchange.target,
        "currency": exchange.currency,
        "flags": flags,
    }
    return Action(
        (EXCHANGE_KEY, exchange.service, exchange.operation),
        report.write_sentence(exchange, index, check_read),
        Flag.BODY_TRUNCATED in exchange.flags,
        _named(outcome),
        fields,
    )


def _named(values: dict[str, JSONValue]) -> tuple[Field, ...]:
    """Each value that is set, as canonical JSON, by name. Every value here is a string or a list
    of strings, which JSON always holds."""
    return tuple((name, canonical_json(value)) for name, value in values.items() if value)


def _fields(values: dict[str, JSONValue]) -> tuple[Field, ...] | None:
    """Every value, empty ones too, as canonical JSON, by name; None when one is not JSON."""
    try:
        return tuple((name, canonical_json(value)) for name, value in values.items())
    except (ValueError, RecursionError):
        return None


def align(a: Sequence[Action], b: Sequence[Action]) -> tuple[Row, ...]:
    """`a` and `b` lined up, every action in exactly one row: the same actions first, by their
    data, then what lies between them by key alone."""
    rows: list[Row] = []
    for tag, x, y in _opcodes(a, b, lambda action: action.data):
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
    """Two actions of one key: `cut` when either was cut, whatever their data."""
    if x.cut or y.cut:
        return Row("cut", x, y)
    return Row("same" if x.data == y.data else "changed", x, y)


def compare_runs(a: StoredRun, b: StoredRun, index: MapIndex | None) -> Comparison:
    """`a`'s writes against `b`'s. `index` is the loaded maps, for each write's `human:` sentence
    and its route's `compare_ignores:` fields, as `report.stored_summary_lines` takes it."""
    return Comparison(align(actions(a, index), actions(b, index)))


def header_lines(a: RunRecord, b: RunRecord) -> list[str]:
    """`irimi compare`'s header: which two runs, and each run's trigger. Says so when the
    triggers' args differ, since then the runs were not given the same work."""
    lines = [
        f"irimi compare · {a.run_id} vs {b.run_id}",
        f"  A  {report.run_label(a)}".rstrip(),
        f"  B  {report.run_label(b)}".rstrip(),
    ]
    if args_differ(a, b):
        lines.append(f"  {ARGS_DIFFER}")
    return lines


def args_differ(a: RunRecord, b: RunRecord) -> bool:
    """Compared as canonical JSON: in Python `True == 1`, and the triggers' args `{"n": 1}` and
    `{"n": true}` differ. Args JSON cannot write, nested too deep or not finite, are called
    different, since irimi cannot say they are the same."""
    try:
        return _args(a) != _args(b)
    except (ValueError, RecursionError):
        return True


def _args(record: RunRecord) -> str:
    return canonical_json(record.trigger.args if record.trigger is not None else None)


def _row_lines(row: Row) -> list[str]:
    """A pair that differs prints both sides, A over B; every other row prints its one action.

    Escaping leaves a backslash as it is, so a Slack post holding a newline and one holding a
    typed `\\n` print one line, and a `~` over two identical lines tells its reader nothing.
    A pair whose sentences differ but print alike to a reader prints both with each backslash
    escaped first, the newline as `\\n` and the typed one as `\\\\n`, and each character a reader
    cannot see spelled as its escape: a zero-width space, a no-break space. A pair whose sentences
    are one string prints the fields that differ under them."""
    marker = MARKERS[row.mark]
    if row.mark in ("changed", "cut") and row.a is not None and row.b is not None:
        a, b = row.a.text, row.b.text
        if row.a.seen == row.b.seen and row.a.sentence != row.b.sentence:
            a, b = row.a.escaped, row.b.escaped
        lines = [f"  {marker} A {a}", f"    B {b}"]
        if row.mark == "changed" and row.a.sentence == row.b.sentence:
            lines += field_diff(row.a, row.b)
        return lines
    action = row.a if row.a is not None else row.b
    assert action is not None, row
    return [f"  {marker} {action.text}"]


def field_diff(a: Action, b: Action) -> list[str]:
    """The fields and outcome parts of `a` and `b` that differ, one line each, `A -> B`, at most
    `MAX_FIELDS_SHOWN` of them, each value cut to `MAX_VALUE_SHOWN` characters and escaped."""
    differ: list[str] = []
    for one, other in ((a.fields or (), b.fields or ()), (a.outcome, b.outcome)):
        x, y = dict(one), dict(other)
        # Where it went first, then the body's fields, each in name order.
        for name in sorted(x.keys() | y.keys(), key=lambda n: (n.startswith("body"), n)):
            if x.get(name) != y.get(name):
                differ += _differences(name, _parsed(x.get(name)), _parsed(y.get(name)))
    shown = [f"      {report.printable(line)}" for line in differ[:MAX_FIELDS_SHOWN]]
    more = len(differ) - MAX_FIELDS_SHOWN
    return [*shown, f"      and {more} more"] if more > 0 else shown


# A field one side of a pair does not have.
_NONE = object()


def _parsed(value: str | None) -> object:
    return _NONE if value is None else json.loads(value)


def _differences(name: str, x: object, y: object, depth: int = 0) -> list[str]:
    """`name: A -> B` for a field whose values differ, and for each member that differs of two
    objects, `name.member`, a few levels down, so a long object names the part that changed."""
    if _json(x) == _json(y):
        return []
    if isinstance(x, dict) and isinstance(y, dict) and depth < MAX_DEPTH_SHOWN:
        return [
            line
            for member in sorted(x.keys() | y.keys())
            for line in _differences(
                f"{name}.{member}", x.get(member, _NONE), y.get(member, _NONE), depth + 1
            )
        ]
    return [f"{name}: {_shown(x)} -> {_shown(y)}"]


def _json(value: object) -> str | None:
    return None if value is _NONE else canonical_json(value)


def _shown(value: object) -> str:
    text = _json(value)
    if text is None:
        return ABSENT
    return text if len(text) <= MAX_VALUE_SHOWN else f"{text[:MAX_VALUE_SHOWN]}…"
