"""Compare two stored runs write by write, for `irimi compare` and `irimi replay`.

Actions (`RunSummary.actions`) compare by data: key, `outcome` and `fields`, never printed text.
Identical actions anchor first; the stretches between pair by key, so a refund of another amount
is one changed write. An action whose body was cut when stored is never `same`.
"""

import difflib
import hashlib
import json
import unicodedata
from collections.abc import Callable, Hashable, Iterator, Sequence
from dataclasses import dataclass
from typing import Literal
from urllib.parse import parse_qsl

from irimi import services
from irimi.core import exitcodes
from irimi.core.exchange import Flag
from irimi.protocol.trace import JSONValue, RunRecord, StoredRun, canonical_json, trigger_args
from irimi.replay.index import compared_body
from irimi.report import model
from irimi.report.model import RunView, WriteEntry
from irimi.report.runs import run_label
from irimi.report.summary import tool_write_sentence, write_sentence
from irimi.report.terminal import printable
from irimi.servicemaps import MapIndex, path_params

# The first item of an action's key. Exchange keys have three items and tool keys two, so a
# service named `tool` still never lines up with a tool call.
EXCHANGE_KEY = "exchange"
TOOL_KEY = "tool"
ARGS_DIFFER = "(the triggers' args differ)"
# Noted for a pair either side of which is flagged `body-truncated`; the flag does not say which
# body was cut, so a cut response counts too.
CUT_NOTE = "a body was cut when stored"
# Flags that say what irimi did with a write. `from-recording` and `replay-miss` are left out:
# they would set every replayed write apart from its recording.
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
    sentence: str  # the summary's line for it, without the indent and unescaped
    cut: bool = False  # its exchange is flagged `body-truncated`
    outcome: tuple[Field, ...] = ()
    # None for data JSON cannot hold (nested too deep or not finite); such an action equals nothing.
    fields: tuple[Field, ...] | None = ()

    @property
    def text(self) -> str:
        """Return the summary's line for it, without the indent, as the summary prints it."""
        return printable(self.sentence)

    @property
    def escaped(self) -> str:
        """Return `text` with backslashes and invisible characters escaped."""
        escaped = printable(self.sentence.replace("\\", "\\\\"))
        return "".join(_spelled(char) for char in escaped)

    @property
    def seen(self) -> str:
        """Return `text` as a reader sees it: invisible characters dropped, spaces as spaces."""
        return "".join(_seen(char) for char in self.text)

    @property
    def data(self) -> Hashable:
        """Return what it is compared by; a fresh object, equal to nothing, if cut or not JSON."""
        if self.cut or self.fields is None:
            return object()
        return (self.key, self.outcome, self.fields)


# Unicode categories that print as nothing or as a space (format, separator, private-use and
# unassigned). `printable` leaves them, so two posts differing only by one print alike.
_UNSEEN = frozenset({"Cf", "Zs", "Zl", "Zp", "Co", "Cn"})


def _spelled(char: str) -> str:
    """Return `char`, or its `\\u`/`\\U` escape when invisible; an ASCII space stays."""
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
        """Return `exitcodes.OK` when every row is `same`, else `DIFFERENT` (a `cut` row too)."""
        same = all(row.mark == "same" for row in self.rows)
        return exitcodes.OK if same else exitcodes.DIFFERENT

    def lines(self) -> list[str]:
        """Return every row, a blank line and the totals; the totals alone if neither run wrote."""
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
    """Return the writes `run` attempted, in `seq` order; idempotent retries are not actions."""
    out: list[Action] = []
    for action in model.build(RunView(run.record, run.events), index).actions:
        if isinstance(action, WriteEntry):
            out.append(_exchange_action(action))
            continue
        error = action.error.type if action.error is not None else None
        out.append(
            Action(
                (TOOL_KEY, action.name),
                tool_write_sentence(action),
                outcome=_named({"raised": error}),
                fields=_fields(_spread("args", action.args)),
            )
        )
    return out


def _spread(name: str, value: JSONValue) -> dict[str, JSONValue]:
    """Return an object's members as fields `name.member`, sorted; anything else as field `name`."""
    if isinstance(value, dict):
        return {f"{name}.{member}": value[member] for member in sorted(value)}
    return {name: value}


def _exchange_action(entry: WriteEntry) -> Action:
    exchange, route = entry.exchange, entry.route
    request = exchange.request
    pairs = parse_qsl(request.query, keep_blank_values=True)
    sent: dict[str, JSONValue] = {}
    if route is not None:
        # A redacted path (a webhook's) holds a placeholder that depends on the install's key, so
        # its params name nothing comparable across installs.
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
        "precondition": exchange.precondition.outcome,
        "rejection code": exchange.precondition.rejection_code,
        "target": exchange.target,
        "currency": exchange.precondition.currency,
        "flags": flags,
    }
    return Action(
        (EXCHANGE_KEY, exchange.service, exchange.operation),
        write_sentence(entry),
        Flag.BODY_TRUNCATED in exchange.flags,
        _named(outcome),
        fields,
    )


def _named(values: dict[str, JSONValue]) -> tuple[Field, ...]:
    """Return each set value as canonical JSON, by name. Values are strings or lists of strings."""
    return tuple((name, canonical_json(value)) for name, value in values.items() if value)


def _fields(values: dict[str, JSONValue]) -> tuple[Field, ...] | None:
    """Return every value, empty ones too, as canonical JSON by name; None if one is not JSON."""
    try:
        return tuple((name, canonical_json(value)) for name, value in values.items())
    except (ValueError, RecursionError):
        return None


def align(a: Sequence[Action], b: Sequence[Action]) -> tuple[Row, ...]:
    """Line up `a` and `b`, each action in one row: identical actions by data, the rest by key."""
    rows: list[Row] = []
    for tag, x, y in _opcodes(a, b, lambda action: action.data):
        if tag == "equal":
            rows += [Row("same", one, other) for one, other in zip(x, y, strict=True)]
        else:
            rows += _by_key(x, y)
    return tuple(rows)


def _by_key(a: Sequence[Action], b: Sequence[Action]) -> list[Row]:
    """Line up unanchored actions by key; an unmatched stretch is all `only_a`, then `only_b`."""
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
    """Yield `difflib` opcodes over `a` and `b` compared `by`, with the stretches each spans.

    Autojunk is off: forty identical writes are forty writes, not noise."""
    matcher = difflib.SequenceMatcher(a=[by(x) for x in a], b=[by(y) for y in b], autojunk=False)
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        yield tag, a[i1:i2], b[j1:j2]


def _pair(x: Action, y: Action) -> Row:
    """Return the row for two actions of one key: `cut` when either was cut, whatever the data."""
    if x.cut or y.cut:
        return Row("cut", x, y)
    return Row("same" if x.data == y.data else "changed", x, y)


def compare_runs(a: StoredRun, b: StoredRun, index: MapIndex | None) -> Comparison:
    """Compare `a`'s writes against `b`'s; `index` supplies `human:` and `compare_ignores:`."""
    return Comparison(align(actions(a, index), actions(b, index)))


def header_lines(a: RunRecord, b: RunRecord) -> list[str]:
    """Return `irimi compare`'s header: the two runs, their triggers, and whether args differ."""
    lines = [
        f"irimi compare · {a.run_id} vs {b.run_id}",
        f"  A  {run_label(a)}".rstrip(),
        f"  B  {run_label(b)}".rstrip(),
    ]
    if args_differ(a, b):
        lines.append(f"  {ARGS_DIFFER}")
    return lines


def args_differ(a: RunRecord, b: RunRecord) -> bool:
    """Return True if the triggers' args differ as canonical JSON, or cannot be written as it."""
    try:
        return _args(a) != _args(b)
    except (ValueError, RecursionError):
        return True


def _args(record: RunRecord) -> str:
    return canonical_json(trigger_args(record.trigger) if record.trigger is not None else None)


def _row_lines(row: Row) -> list[str]:
    """Return a row's lines: a differing pair as A over B, any other row as its one action.

    A pair whose sentences differ but look alike prints `escaped` text (`printable` prints a
    newline and a typed `\\n` alike). A pair with one sentence prints its differing fields."""
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
    """Return the differing fields and outcome parts as `A -> B` lines, capped by `MAX_*_SHOWN`."""
    differ: list[str] = []
    for one, other in ((a.fields or (), b.fields or ()), (a.outcome, b.outcome)):
        x, y = dict(one), dict(other)
        # Non-body fields sort first, then the body's, each in name order.
        for name in sorted(x.keys() | y.keys(), key=lambda n: (n.startswith("body"), n)):
            if x.get(name) != y.get(name):
                differ += _differences(name, _parsed(x.get(name)), _parsed(y.get(name)))
    shown = [f"      {printable(line)}" for line in differ[:MAX_FIELDS_SHOWN]]
    more = len(differ) - MAX_FIELDS_SHOWN
    return [*shown, f"      and {more} more"] if more > 0 else shown


# A field one side of a pair does not have.
_NONE = object()


def _parsed(value: str | None) -> object:
    return _NONE if value is None else json.loads(value)


def _differences(name: str, x: object, y: object, depth: int = 0) -> list[str]:
    """Return `name: A -> B` lines, descending into objects up to `MAX_DEPTH_SHOWN` levels."""
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
