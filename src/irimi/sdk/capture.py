"""How the SDK captures a trigger's arguments as JSON.

A run's trigger is stored as `Trigger.args` in its `run.json`, and replay re-runs the
trigger with those args turned back into values by `revive`, this module's inverse, which a
replay also uses to hand a read tool its recorded result. So capture says two things about
every value: its JSON, and whether that JSON is enough to rebuild an equal value (`replayable`).
The rules are a table, each row has a test, and `docs/trace-format.md` ("Captured arguments") is
the reference:

    None, bool, int, str, finite float  itself                                   replayable
    non-finite float                    {"__irimi_repr__": "nan"}, "inf", "-inf" not
    list, tuple                         a JSON list                              AND of items
    dict, every key a str               a JSON object                            AND of values
    dict with any other key             {"__irimi_repr__": <repr(v), cut>}       not
    dataclass instance                  {"__irimi_type__": "module:qualname",    AND of fields
                                         "value": <its init fields, captured>}
    object with a callable model_dump   {"__irimi_type__": "module:qualname",    same
                                         "value": <v.model_dump(mode="json"), captured>}
    bytes                               {"__irimi_bytes__": <base64>}            replayable
    anything else                       {"__irimi_repr__": <repr(v), cut>}       not

A type is matched exactly, never by `isinstance`: an IntEnum, a namedtuple or an OrderedDict is
"anything else", because `revive` would build the base type, not the value the trigger was given.
A dict holding one of the four keys capture writes (`RESERVED_KEYS`) is captured by its repr, so a
revived marker always means capture wrote it. A dataclass or model whose class `revive` could not
import - defined inside a function (`<locals>` in its qualname), or in a script run by path,
whose `__main__` is a different module in the process that replays it - is not replayable either.
A class in a module started with `python -m` is named by the module's real name
(`importable_module`).

Two bounds keep the walk short whatever the agent passes. A value inside more than `MAX_DEPTH`
containers is `{"__irimi_repr__": "<too deep>"}`, not replayable; for a trigger, depth counts from
each parameter's value. And args whose JSON is larger than `MAX_CAPTURED_BYTES` are replaced as a
whole by `{"__irimi_truncated__": true, "bytes": n}`, not replayable. The walk counts the bytes it
is producing as it goes and stops as soon as they pass the limit, so a value that holds one large
object many times, or itself, is walked at most that far; when it stops early, `n` is the count
so far, already past the limit. A `repr` is the value's own cost: it is cut to 1000 characters
after it is made, outside any secret's shape (`secret_shapes.cut_outside_secrets`), since the
store redacts it only after this cut, and the kept half of a key would no longer be its shape.

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
# The largest a trigger's captured args may be, as the SDK posts them. The control endpoint takes
# a start of up to 2 MiB (`control.MAX_CONTROL_BODY`), so the args fit with room for the rest.
MAX_CAPTURED_BYTES = 1024 * 1024
TOO_DEEP = "<too deep>"
# A method's first parameter with one of these names is its receiver, not an argument (`_receiver`).
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

    A method's or a classmethod's first parameter named `self` or `cls` is the receiver and is
    left out (`_receiver`): it says nothing about the run, and leaving it in would make every
    method's run unreplayable. Defaults are not applied: the args are what the caller passed, so
    a replay of edited code takes the new defaults. When the call does not bind - the agent
    called the function wrongly, and Python is about to say so - the args are captured as
    `{"args": [...], "kwargs": {...}}`, not replayable: the call raised TypeError the first time,
    and the shape is the one a function whose parameters are named `args` and `kwargs` binds to."""
    try:
        signature = _signature(fn)
        bound = signature.bind(*args, **kwargs)
    except Exception:
        encoded, _ = capture({"args": list(args), "kwargs": dict(kwargs)})
        return encoded, False
    arguments = dict(bound.arguments)
    receiver = _receiver(fn, signature)
    if receiver is not None:
        arguments.pop(receiver, None)
    # One level less than the dict: each parameter's value may nest MAX_DEPTH deep, as a value
    # given to `sdk.run(name, inputs=...)` may.
    return _capture(arguments, -1)


def bind(
    fn: Callable[..., Any], args: tuple[Any, ...], kwargs: Mapping[str, Any]
) -> inspect.BoundArguments:
    """`args` and `kwargs` bound to `fn`'s parameters, as `capture_call` binds a trigger's call:
    TypeError when they do not bind, as calling `fn` would raise."""
    return _signature(fn).bind(*args, **kwargs)


def _receiver(fn: Callable[..., Any], signature: inspect.Signature) -> str | None:
    """The name of `fn`'s first parameter when it is the receiver of a method or a classmethod,
    else None. It is when it is named `self` or `cls` and `fn` is defined in a class body - its
    qualname says so - unless the class holds it as a `staticmethod`. A plain function or a static
    method receives no instance, so its first parameter is an argument whatever its name, and
    leaving it out would make a run replay could not call again. A bound method's receiver is
    already bound, and not in its signature. A class this cannot look up statically in a module
    already imported - one defined in a function, or not yet built - is taken by the name alone.
    Never raises."""
    first = next(iter(signature.parameters), None)
    qualname = getattr(fn, "__qualname__", None)
    if first not in RECEIVER_NAMES or inspect.ismethod(fn) or not isinstance(qualname, str):
        return None
    owner, _, name = qualname.rpartition(".")
    if not owner or owner.endswith("<locals>"):
        return None  # defined at a module's top level or in a function, not in a class body
    found: Any = sys.modules.get(getattr(fn, "__module__", None) or "")
    try:
        for part in (*owner.split("."), name):
            found = inspect.getattr_static(found, part)
    except AttributeError:
        return first
    return None if isinstance(found, staticmethod) else first


def _signature(fn: Callable[..., Any]) -> inspect.Signature:
    """`fn`'s parameters, its annotations left unevaluated. Only the names are needed, and on
    Python 3.14, where annotations are evaluated lazily, `inspect.signature` evaluates them: a
    function annotated with a name imported under `TYPE_CHECKING` would raise NameError, and its
    every call be captured as one that does not bind."""
    if sys.version_info >= (3, 14):
        import annotationlib

        return inspect.signature(fn, annotation_format=annotationlib.Format.FORWARDREF)
    return inspect.signature(fn)


# The parameters a keyword argument binds to (`call_arguments`).
_KEYWORDS = (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)


def call_arguments(
    fn: Callable[..., Any], arguments: Mapping[str, Any]
) -> tuple[list[Any], dict[str, Any]]:
    """`capture_call`'s binding undone: the positional and keyword arguments that call `fn`
    with `arguments`, `{parameter: value}` as a trigger's args are captured, revived. Each value
    goes where its parameter takes it: the leading positional parameters in their places, up to the
    first one the call left to its default, `*args` spread out after them, `**kwargs` spread in,
    and every other by its name. TypeError when they do not bind to `fn`'s parameters, as calling
    it would raise: a parameter missing or unknown. TypeError too for what no call binds, which
    args given by hand (`--args`) can say and Python would bind otherwise than they say: a
    `*args` after a positional parameter left out, whose values would fill that parameter; a
    `*args` that is no list or a `**kwargs` that is no object, which would be spread as
    characters or keys; and a `**kwargs` naming a parameter a keyword binds to, whose value it
    would replace."""
    positional: list[Any] = []
    named = dict(arguments)
    left_out: str | None = None  # the first positional parameter the call left to its default
    signature = _signature(fn)
    for name, parameter in signature.parameters.items():
        kind = parameter.kind
        is_positional = kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
        if name not in named:
            if is_positional and left_out is None:
                left_out = name
            continue
        if is_positional and left_out is None:
            positional.append(named.pop(name))
        elif kind is inspect.Parameter.VAR_POSITIONAL:
            spread = named.pop(name)
            if not isinstance(spread, list):
                raise TypeError(f"{name} must be a list, not {type(spread).__name__}")
            if left_out is not None:
                raise TypeError(
                    f"{name} takes its values after {left_out}, which was left to its default"
                )
            positional.extend(spread)
        elif kind is inspect.Parameter.VAR_KEYWORD:
            spread = named.pop(name)
            if not isinstance(spread, dict):
                raise TypeError(f"{name} must be an object, not {type(spread).__name__}")
            # Only a name a keyword binds to is taken. A positional-only parameter's, or `*args`'
            # or `**kwargs`' own, is free for `**kwargs`: `f(1, a=2)` and `f(kwargs=2)` bind so.
            taken = sorted(
                key
                for key in spread
                if key in signature.parameters and signature.parameters[key].kind in _KEYWORDS
            )
            if taken:
                raise TypeError(f"{name} names {', '.join(taken)}, which is a parameter of its own")
            named.update(spread)
    signature.bind(*positional, **named)
    return positional, named


class NotReplayable(Exception):
    """A captured value `revive` cannot turn back into the value it was: the reason."""


def revive(value: JSONValue) -> Any:
    """The value `value` was captured from (see the module doc), or NotReplayable naming why not.
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

    REVIVE NEVER IMPORTS. A stored run names its classes, and importing a module runs its code, so
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
