"""How the SDK captures a trigger's arguments as JSON (#74).

A run's trigger is stored as `Trigger.args` in its `run.json`, and replay (#84) re-runs the
trigger with those args turned back into values by `revive`, this module's inverse, which #84
adds. So capture says two things about every value: its JSON, and whether that JSON is enough to
rebuild an equal value (`replayable`). The rules are a table, each row has a test, and
`docs/trace-format.md` ("Captured arguments") is the reference:

    None, bool, int, str, finite float  itself                                   replayable
    non-finite float                    {"__irimi_repr__": "nan"}, "inf", "-inf" not
    list, tuple                         a JSON list                              AND of items
    dict, every key a str               a JSON object                            AND of values
    dict with any other key             {"__irimi_repr__": repr(v)[:1000]}       not
    dataclass instance                  {"__irimi_type__": "module:qualname",    AND of fields
                                         "value": <its init fields, captured>}
    object with a callable model_dump   {"__irimi_type__": "module:qualname",    same
                                         "value": <v.model_dump(mode="json"), captured>}
    bytes                               {"__irimi_bytes__": <base64>}            replayable
    anything else                       {"__irimi_repr__": repr(v)[:1000]}       not

A type is matched exactly, never by `isinstance`: an IntEnum, a namedtuple or an OrderedDict is
"anything else", because `revive` would build the base type, not the value the trigger was given.
A dict holding one of the four keys capture writes (`RESERVED_KEYS`) is captured by its repr, so a
revived marker always means capture wrote it. A dataclass or model whose class `revive` could not
import - defined inside a function (`<locals>` in its qualname), or in `__main__`, which is a
different module in the process that replays it - is not replayable either.

Two bounds keep the walk short whatever the agent passes. A value inside more than `MAX_DEPTH`
containers is `{"__irimi_repr__": "<too deep>"}`, not replayable; for a trigger, depth counts from
each parameter's value. And args whose JSON is larger than `MAX_CAPTURED_BYTES` are replaced as a
whole by `{"__irimi_truncated__": true, "bytes": n}`, not replayable. The walk counts the bytes it
is producing as it goes and stops as soon as they pass the limit, so a value that holds one large
object many times, or itself, is walked at most that far; when it stops early, `n` is the count
so far, already past the limit. A `repr` is the value's own cost: it is cut to 1000 characters
after it is made.

CAPTURE NEVER RAISES. It runs inside the agent's call, before the agent's own function: a `repr`
or a `model_dump` that raises makes its value `{"__irimi_repr__": ...}`, never the agent's error.
"""

import base64
import dataclasses
import inspect
import json
import math
import sys
from collections.abc import Callable, Mapping
from typing import Any

from irimi.trace import JSONValue

REPR_KEY = "__irimi_repr__"
TYPE_KEY = "__irimi_type__"
BYTES_KEY = "__irimi_bytes__"
TRUNCATED_KEY = "__irimi_truncated__"
RESERVED_KEYS = frozenset({REPR_KEY, TYPE_KEY, BYTES_KEY, TRUNCATED_KEY})

# The deepest a captured value nests: a value inside more containers than this is not walked.
MAX_DEPTH = 32
# The most of a `repr` a capture keeps.
MAX_REPR = 1000
# The largest a trigger's captured args may be, as the SDK posts them. The control endpoint takes
# a start of up to 2 MiB (`control.MAX_CONTROL_BODY`), so the args fit with room for the rest.
MAX_CAPTURED_BYTES = 1024 * 1024
TOO_DEEP = "<too deep>"
# A first parameter with one of these names is the receiver of a method, not an argument.
RECEIVER_NAMES = ("self", "cls")

type Captured = tuple[JSONValue, bool]


def capture(value: object) -> Captured:
    """`value` as JSON, and whether that JSON rebuilds it. See the module doc. Never raises."""
    return _capture(value, 0)


def _capture(value: object, depth: int) -> Captured:
    walk = _Walk()
    try:
        encoded, replayable = walk.encode(value, depth)
        size = len(serialized(encoded))
    except _TooBig:
        return {TRUNCATED_KEY: True, "bytes": walk.counted}, False
    except Exception:  # every value is guarded in the walk; this is the last line of defence
        return {REPR_KEY: _repr(value)}, False
    if size > MAX_CAPTURED_BYTES:
        return {TRUNCATED_KEY: True, "bytes": size}, False
    return encoded, replayable


def capture_call(
    fn: Callable[..., Any], args: tuple[Any, ...], kwargs: Mapping[str, Any]
) -> Captured:
    """A trigger's arguments as `{parameter: value}`, bound against `fn`'s signature, captured.

    A first parameter named `self` or `cls` is the receiver and is left out: it says nothing about
    the run, and leaving it in would make every method's run unreplayable. Defaults are not
    applied: the args are what the caller passed, so a replay of edited code takes the new
    defaults. When the call does not bind - the agent called the function wrongly, and Python is
    about to say so - the args are captured as `{"args": [...], "kwargs": {...}}`, not
    replayable: the call raised TypeError the first time, and the shape is the one a function
    whose parameters are named `args` and `kwargs` binds to."""
    try:
        signature = _signature(fn)
        bound = signature.bind(*args, **kwargs)
    except Exception:
        encoded, _ = capture({"args": list(args), "kwargs": dict(kwargs)})
        return encoded, False
    arguments = dict(bound.arguments)
    first = next(iter(signature.parameters), None)
    if first in RECEIVER_NAMES:
        arguments.pop(first, None)
    # One level less than the dict: each parameter's value may nest MAX_DEPTH deep, as a value
    # given to `sdk.run(trigger=...)` may.
    return _capture(arguments, -1)


def _signature(fn: Callable[..., Any]) -> inspect.Signature:
    """`fn`'s parameters, its annotations left unevaluated. Only the names are needed, and on
    Python 3.14, where annotations are evaluated lazily, `inspect.signature` evaluates them: a
    function annotated with a name imported under `TYPE_CHECKING` would raise NameError, and its
    every call be captured as one that does not bind."""
    if sys.version_info >= (3, 14):
        import annotationlib

        return inspect.signature(fn, annotation_format=annotationlib.Format.FORWARDREF)
    return inspect.signature(fn)


def serialized(value: JSONValue) -> bytes:
    """`value` as the SDK sends it: the one encoding the size limit is measured in."""
    return json.dumps(value, allow_nan=False).encode()


class _TooBig(Exception):
    """The walk passed MAX_CAPTURED_BYTES: the args are replaced as a whole."""


class _Walk:
    """One capture's walk over a value. `counted` is a lower bound on the bytes of JSON written
    so far: every value adds at least one, so the walk visits at most MAX_CAPTURED_BYTES values."""

    def __init__(self) -> None:
        self.counted = 0

    def encode(self, value: object, depth: int) -> Captured:
        if depth > MAX_DEPTH:
            return self._marker(REPR_KEY, TOO_DEEP), False
        try:
            return self._encode(value, depth)
        except _TooBig:
            raise
        except Exception:
            return self._opaque(value), False

    def _encode(self, value: object, depth: int) -> Captured:
        kind = type(value)
        if value is None:
            self._count(4)
            return None, True
        if kind is bool:
            assert isinstance(value, bool)
            self._count(4)
            return value, True
        if kind is int:
            assert isinstance(value, int)
            text = json.dumps(value)  # ValueError past Python's digit limit: then it is opaque
            self._count(len(text))
            return value, True
        if kind is float:
            assert isinstance(value, float)
            if not math.isfinite(value):
                return self._marker(REPR_KEY, repr(value)), False
            self._count(len(repr(value)))
            return value, True
        if kind is str:
            assert isinstance(value, str)
            self._count(len(value) + 2)
            return value, True
        if kind is bytes:
            assert isinstance(value, bytes)
            self._count(4 * ((len(value) + 2) // 3))  # before encoding: base64 grows it a third
            return {BYTES_KEY: base64.b64encode(value).decode("ascii")}, True
        if kind is list or kind is tuple:
            assert isinstance(value, list | tuple)
            self._count(2)
            items: list[JSONValue] = []
            replayable = True
            for item in value:
                encoded, ok = self.encode(item, depth + 1)
                items.append(encoded)
                replayable = replayable and ok
            return items, replayable
        if kind is dict:
            assert isinstance(value, dict)
            if any(type(key) is not str for key in value) or RESERVED_KEYS.intersection(value):
                return self._opaque(value), False
            self._count(2)
            fields: dict[str, JSONValue] = {}
            replayable = True
            for key, item in value.items():
                self._count(len(key) + 3)
                fields[key], ok = self.encode(item, depth + 1)
                replayable = replayable and ok
            return fields, replayable
        if dataclasses.is_dataclass(value) and not isinstance(value, type):
            init = {f.name: getattr(value, f.name) for f in dataclasses.fields(value) if f.init}
            return self._typed(kind, init, depth)
        if callable(getattr(kind, "model_dump", None)) and not isinstance(value, type):
            return self._typed(kind, value.model_dump(mode="json"), depth)  # type: ignore[attr-defined]
        return self._opaque(value), False

    def _typed(self, kind: type, fields: object, depth: int) -> Captured:
        """A dataclass or a pydantic model: its class, and its fields captured at its own depth."""
        name = f"{kind.__module__}:{kind.__qualname__}"
        self._count(len(name) + len(TYPE_KEY) + 15)
        encoded, replayable = self.encode(fields, depth)
        importable = "<locals>" not in name and kind.__module__ != "__main__"
        return {TYPE_KEY: name, "value": encoded}, replayable and importable

    def _opaque(self, value: object) -> JSONValue:
        return self._marker(REPR_KEY, _repr(value))

    def _marker(self, key: str, text: str) -> JSONValue:
        self._count(len(key) + len(text) + 8)
        return {key: text}

    def _count(self, size: int) -> None:
        self.counted += size
        if self.counted > MAX_CAPTURED_BYTES:
            raise _TooBig


def _repr(value: object) -> str:
    try:
        return repr(value)[:MAX_REPR]
    except Exception:
        return f"<unrepresentable {type(value).__qualname__}>"
