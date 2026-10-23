"""Bind a trigger's arguments to its parameters, and undo the binding for a replay.

The SDK binds a trigger's call to record `{parameter: value}`; a replay turns that object back
into the positional and keyword arguments that call the function again. Both read a function's
parameters through `signature`, and both leave a method's receiver out (`receiver`). Stdlib only,
so the SDK and a replay's child share it.
"""

import inspect
import sys
from collections.abc import Callable, Mapping
from typing import Any

# A method's first parameter with one of these names is its receiver, not an argument (`receiver`).
RECEIVER_NAMES = ("self", "cls")


def bind(
    fn: Callable[..., Any], args: tuple[Any, ...], kwargs: Mapping[str, Any]
) -> inspect.BoundArguments:
    """`args` and `kwargs` bound to `fn`'s parameters, as `capture_call` binds a trigger's call:
    TypeError when they do not bind, as calling `fn` would raise."""
    return signature(fn).bind(*args, **kwargs)


def receiver(fn: Callable[..., Any], sig: inspect.Signature) -> str | None:
    """The name of `fn`'s first parameter when it is the receiver of a method or a classmethod,
    else None. It is when it is named `self` or `cls` and `fn` is defined in a class body - its
    qualname says so - unless the class holds it as a `staticmethod`. A plain function or a static
    method receives no instance, so its first parameter is an argument whatever its name, and
    leaving it out would make a run replay could not call again. A bound method's receiver is
    already bound, and not in its signature. A class this cannot look up statically in a module
    already imported - one defined in a function, or not yet built - is taken by the name alone.
    Never raises."""
    first = next(iter(sig.parameters), None)
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


def signature(fn: Callable[..., Any]) -> inspect.Signature:
    """`fn`'s parameters, its annotations left unevaluated. Only the names are needed, and on
    Python 3.14, where annotations are evaluated lazily, `inspect.signature` evaluates them: a
    function annotated with a name imported under `TYPE_CHECKING` would raise NameError, and its
    every call be captured as one that does not bind."""
    if sys.version_info >= (3, 14):
        import annotationlib  # noqa: PLC0415  # exists from 3.14 only

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
    sig = signature(fn)
    for name, parameter in sig.parameters.items():
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
            params = sig.parameters
            taken = sorted(key for key in spread if key in params and params[key].kind in _KEYWORDS)
            if taken:
                raise TypeError(f"{name} names {', '.join(taken)}, which is a parameter of its own")
            named.update(spread)
    sig.bind(*positional, **named)
    return positional, named
