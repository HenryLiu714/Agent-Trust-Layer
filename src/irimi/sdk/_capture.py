"""Capture a trigger's arguments or a tool's result as JSON, and revive that JSON into values.

`capture` returns a value's JSON and whether it rebuilds an equal value (`replayable`); a replay
uses `revive` to invert it. The rules are the table in `docs/trace-format.md`.

- Capture never raises (the SDK never raises): a value it cannot encode is captured by its repr.
- Revive never imports a module.
"""

import base64
import dataclasses
import inspect
import json
import math
import sys
from collections.abc import Callable, Mapping
from typing import Any, Protocol, cast

from irimi.core.calls import receiver, signature
from irimi.core.limits import MAX_CAPTURED_BYTES
from irimi.core.secret_shapes import cut_outside_secrets
from irimi.protocol.trace import (
    MAIN_MODULE,
    REPR_KEY,
    TRUNCATED_KEY,
    JSONValue,
    holds_description,
    importable_module,
)

# `REPR_KEY` and `TRUNCATED_KEY` come from the trace format because the engine also refuses them.
TYPE_KEY = "__irimi_type__"
BYTES_KEY = "__irimi_bytes__"
RESERVED_KEYS = frozenset({REPR_KEY, TYPE_KEY, BYTES_KEY, TRUNCATED_KEY})

MAX_DEPTH = 32
MAX_REPR = 1000
TOO_DEEP = "<too deep>"

type Captured = tuple[JSONValue, bool]


def capture(value: object) -> Captured:
    """Return `value` as JSON and whether that JSON rebuilds it.

    Types match exactly, not by `isinstance`, because `revive` builds the base type. Past
    `MAX_DEPTH` a value becomes a "<too deep>" repr; past `MAX_CAPTURED_BYTES` the whole value
    becomes the truncation marker, which also bounds a self-referencing value. A repr is cut to
    `MAX_REPR` outside any secret's shape, since the store redacts only after this cut.
    Never raises (the SDK never raises).
    """
    return _capture(value, 0)


def _capture(value: object, depth: int) -> Captured:
    walk = _Walk()
    try:
        encoded, replayable = walk.encode(value, depth)
        size = len(serialized(encoded))
    except _TooBig:
        return {TRUNCATED_KEY: True, "bytes": walk.counted}, False
    except Exception:  # the walk guards each value; this catches anything it missed
        return {REPR_KEY: _repr(value)}, False
    if size > MAX_CAPTURED_BYTES:
        return {TRUNCATED_KEY: True, "bytes": size}, False
    return encoded, replayable


def capture_call(
    fn: Callable[..., Any], args: tuple[Any, ...], kwargs: Mapping[str, Any]
) -> Captured:
    """Capture a trigger's arguments as `{parameter: value}`, bound against `fn`'s signature.

    The receiver (`self`/`cls`) is left out, else every method's run would be unreplayable.
    Defaults are not applied, so a replay of edited code takes the new defaults. A call that does
    not bind is captured as `{"args": [...], "kwargs": {...}}`, not replayable.
    """
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
    # Start one level up so each parameter may nest MAX_DEPTH deep, as `sdk.run` inputs may.
    return _capture(arguments, -1)


class NotReplayable(Exception):
    """Raised when `revive` cannot rebuild a captured value; the message says why."""


def revive(value: JSONValue) -> Any:
    """Return the value `value` was captured from.

        JSON scalars, lists, objects            themselves (a tuple comes back as a list)
        {"__irimi_bytes__": <base64>}           bytes
        {"__irimi_type__": "m:q", "value": v}   dataclass `cls(**revive(v))`, or
                                                `cls.model_validate(revive(v))`
        {"__irimi_repr__": ...}, anywhere       NotReplayable: a description, not the value
        {"__irimi_truncated__": ...}, anywhere  NotReplayable

    A class is looked up only in a module already in `sys.modules`, never through `__getattr__`,
    because importing runs code a stored run names.

    Raises:
        NotReplayable: the value cannot be rebuilt. Nothing else is raised.
    """
    # Refuse descriptions before building any class; the engine refuses the same results.
    if holds_description(value):
        raise NotReplayable("the value was recorded as its description, not as itself")
    try:
        return _revive(value)
    except NotReplayable:
        raise
    except Exception as exc:  # e.g. a value deeper than the stack, or a misbehaving class
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
    """Build a dataclass or model from its `module:qualname` and captured fields."""
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
            # `getattr` could run a module's or metaclass's `__getattr__`, which lazy packages use
            # to import on first use.
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
            return cast(type[_Model], found).model_validate(revived)
    except NotReplayable:
        raise
    except Exception:
        raise NotReplayable(f"{name} could not be built from its fields") from None
    raise NotReplayable(f"{name} is neither a dataclass nor a model")


class _Model(Protocol):
    """Type the pydantic methods capture and revive call, so the SDK never imports pydantic."""

    @classmethod
    def model_validate(cls, obj: object) -> object: ...

    def model_dump(self, *, mode: str) -> object: ...


def serialized(value: JSONValue) -> bytes:
    """Encode `value` as the SDK sends it; the size limit is measured in this encoding."""
    return json.dumps(value, allow_nan=False).encode()


class _TooBig(Exception):
    """Raised when the walk passes MAX_CAPTURED_BYTES, so the whole value is replaced."""


class _Walk:
    """Walk one value for capture, counting a lower bound of JSON bytes in `counted`.

    Each value adds at least one byte, so the walk visits at most MAX_CAPTURED_BYTES values.
    """

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
            text = json.dumps(value)  # ValueError past Python's digit limit makes it opaque
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
            return self._typed(kind, cast(_Model, value).model_dump(mode="json"), depth)
        return self._opaque(value), False

    def _typed(self, kind: type, fields: object, depth: int) -> Captured:
        """Capture a dataclass or pydantic model as its class name and its fields."""
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
