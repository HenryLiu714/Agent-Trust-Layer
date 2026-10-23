"""Capture a trigger's arguments or a tool's result as JSON, and revive that JSON into values.

`capture` says two things about every value: its JSON, and whether that JSON rebuilds an equal
value (`replayable`). `revive` is its inverse, which a replay uses for a trigger's args and a read
tool's recorded result. The rules are the table in `docs/trace-format.md` ("Captured arguments"),
each row with a test. Capture never raises into the agent: a value it cannot encode, or whose
`repr` or `model_dump` raises, is captured by its description. Revive never imports a module.
"""

import base64
import dataclasses
import inspect
import json
import math
import sys
from collections.abc import Callable, Mapping
from typing import Any

from irimi._calls import receiver, signature
from irimi.limits import MAX_CAPTURED_BYTES
from irimi.secret_shapes import cut_outside_secrets
from irimi.trace import (
    MAIN_MODULE,
    REPR_KEY,
    TRUNCATED_KEY,
    JSONValue,
    holds_description,
    importable_module,
)

# The two markers that describe a value, `REPR_KEY` and `TRUNCATED_KEY`, are the trace format's,
# so a replay's recording and `revive` refuse one rule; these two are capture's own.
TYPE_KEY = "__irimi_type__"
BYTES_KEY = "__irimi_bytes__"
RESERVED_KEYS = frozenset({REPR_KEY, TYPE_KEY, BYTES_KEY, TRUNCATED_KEY})

# The deepest a captured value nests: a value inside more containers than this is not walked.
MAX_DEPTH = 32
# The most of a `repr` a capture keeps.
MAX_REPR = 1000
TOO_DEEP = "<too deep>"

type Captured = tuple[JSONValue, bool]


def capture(value: object) -> Captured:
    """`value` as JSON, and whether that JSON rebuilds it, by the table in `docs/trace-format.md`.

    A type is matched exactly, never by `isinstance`, because `revive` builds the base type. A
    value inside more than `MAX_DEPTH` containers is `{"__irimi_repr__": "<too deep>"}`, and
    args whose JSON passes `MAX_CAPTURED_BYTES` are replaced whole by the truncation marker; the
    walk counts bytes as it goes, so a value that holds itself is walked at most that far. A
    `repr` is cut to `MAX_REPR` outside any secret's shape (`secret_shapes.cut_outside_secrets`),
    since the store redacts it only after this cut. Never raises."""
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

    A method's or a classmethod's first parameter named `self` or `cls` is the receiver and is
    left out (`_calls.receiver`): it says nothing about the run, and leaving it in would make every
    method's run unreplayable. Defaults are not applied: the args are what the caller passed, so
    a replay of edited code takes the new defaults. When the call does not bind - the agent
    called the function wrongly, and Python is about to say so - the args are captured as
    `{"args": [...], "kwargs": {...}}`, not replayable: the call raised TypeError the first time,
    and the shape is the one a function whose parameters are named `args` and `kwargs` binds to."""
    try:
        sig = signature(fn)
        bound = sig.bind(*args, **kwargs)
    except Exception:
        encoded, _ = capture({"args": list(args), "kwargs": dict(kwargs)})
        return encoded, False
    arguments = dict(bound.arguments)
    self_name = receiver(fn, sig)
    if self_name is not None:
        arguments.pop(self_name, None)
    # One level less than the dict: each parameter's value may nest MAX_DEPTH deep, as a value
    # given to `sdk.run(name, inputs=...)` may.
    return _capture(arguments, -1)


class NotReplayable(Exception):
    """A captured value `revive` cannot turn back into the value it was: the reason."""


def revive(value: JSONValue) -> Any:
    """The value `value` was captured from, or NotReplayable naming why not.
    The inverse of the capture table, row by row:

        JSON scalars, lists, objects            themselves; a tuple was captured as a list, and
                                                comes back as one
        {"__irimi_bytes__": <base64>}           bytes
        {"__irimi_type__": "m:q", "value": v}   the class `m:q` names, in a module this process
                                                has imported: a dataclass is `cls(**revive(v))`,
                                                a class with a callable `model_validate` is
                                                `cls.model_validate(revive(v))`
        {"__irimi_repr__": ...}, anywhere       NotReplayable: its JSON describes the value, it
        {"__irimi_truncated__": ...}, anywhere  is not the value

    Revive never imports. A stored run names its classes, and importing a module runs its code, so
    a class is looked up only in a module already in `sys.modules`, and only in what it already
    holds, never through its `__getattr__`: a tool that returned one has imported it, as has the
    agent a replay re-runs. A class it cannot find there, that is neither,
    or whose constructor raises is NotReplayable naming it. Never raises anything else."""
    # One walk for the refusal, the trace format's (`trace.holds_description`), before any class is
    # built: the engine refuses the same recorded results.
    if holds_description(value):
        raise NotReplayable("the value was recorded as its description, not as itself")
    try:
        return _revive(value)
    except NotReplayable:
        raise
    except Exception as exc:  # a stored value deeper than the stack, or a class that misbehaves
        raise NotReplayable(f"{type(exc).__name__} while reviving") from None


def _revive(value: JSONValue) -> Any:
    if isinstance(value, list):
        return [_revive(item) for item in value]
    if not isinstance(value, dict):
        return value
    marker = RESERVED_KEYS.intersection(value)
    if not marker:
        return {key: _revive(item) for key, item in value.items()}
    if BYTES_KEY in marker:
        encoded = value[BYTES_KEY]
        if not isinstance(encoded, str):
            raise NotReplayable("a bytes value is not base64 text")
        try:
            return base64.b64decode(encoded, validate=True)
        except ValueError:  # binascii.Error, or text that is not ASCII
            raise NotReplayable("a bytes value is not base64 text") from None
    return _revive_typed(value.get(TYPE_KEY), value.get("value"))


def _revive_typed(name: JSONValue, fields: JSONValue) -> Any:
    """A dataclass or a model, from its `module:qualname` and its captured fields."""
    if not isinstance(name, str) or ":" not in name:
        raise NotReplayable(f"{TYPE_KEY} names no class")
    module, _, qualname = name.partition(":")
    found: Any = sys.modules.get(module)
    if found is None and module == importable_module(MAIN_MODULE):
        found = sys.modules.get(MAIN_MODULE)  # this process was started as `python -m module`
    if found is None:
        raise NotReplayable(f"{name} is in a module this process has not imported")
    try:
        for part in qualname.split("."):
            # Looked up statically: `getattr` would run a module's `__getattr__` (PEP 562), which a
            # lazy package uses to import on first use, or a metaclass's, so a name in a stored
            # run could make revive import after all.
            found = inspect.getattr_static(found, part)
    except AttributeError:
        raise NotReplayable(f"{name} is not in its module") from None
    if not isinstance(found, type):
        raise NotReplayable(f"{name} is not a class")
    revived = _revive(fields)
    try:
        if dataclasses.is_dataclass(found):
            if not isinstance(revived, dict):
                raise NotReplayable(f"{name}'s fields are not an object")
            return found(**revived)
        if callable(getattr(found, "model_validate", None)):
            return found.model_validate(revived)  # type: ignore[attr-defined]
    except NotReplayable:
        raise
    except Exception:
        raise NotReplayable(f"{name} could not be built from its fields") from None
    raise NotReplayable(f"{name} is neither a dataclass nor a model")


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
        module = importable_module(kind.__module__)
        name = f"{module}:{kind.__qualname__}"
        self._count(len(name) + len(TYPE_KEY) + 15)
        encoded, replayable = self.encode(fields, depth)
        importable = "<locals>" not in name and module != MAIN_MODULE
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
        return cut_outside_secrets(repr(value), MAX_REPR)
    except Exception:
        return f"<unrepresentable {type(value).__qualname__}>"
