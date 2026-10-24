"""Bind a trigger's arguments to its parameters and back for a replay, leaving receivers out."""

import inspect
import sys
from collections.abc import Callable, Mapping
from typing import Any

RECEIVER_NAMES = ("self", "cls")


def bind(
    fn: Callable[..., Any], args: tuple[Any, ...], kwargs: Mapping[str, Any]
) -> inspect.BoundArguments:
    """Bind `args` and `kwargs` to `fn`'s parameters. Raises TypeError when they do not bind."""
    return signature(fn).bind(*args, **kwargs)


def receiver(fn: Callable[..., Any], sig: inspect.Signature) -> str | None:
    """Return the name of `fn`'s receiver parameter, or None. Never raises.

    That is a first parameter named `self` or `cls` of a non-static function in a class body.
    """
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
    """Return `fn`'s signature with annotations left unevaluated.

    On Python 3.14 `inspect.signature` evaluates them, and a `TYPE_CHECKING`-only name would raise.
    """
    if sys.version_info >= (3, 14):
        import annotationlib  # noqa: PLC0415  # exists from 3.14 only

        return inspect.signature(fn, annotation_format=annotationlib.Format.FORWARDREF)
    return inspect.signature(fn)


_KEYWORDS = (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)


def call_arguments(
    fn: Callable[..., Any], arguments: Mapping[str, Any]
) -> tuple[list[Any], dict[str, Any]]:
    """Return the positional and keyword arguments that call `fn` with recorded `arguments`.

    Positionals bind by position up to the first one left to its default, the rest by name.

    Raises:
        TypeError: a parameter is missing or unknown, or Python would bind the arguments otherwise.
    """
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
            # Positional-only names and the `*args`/`**kwargs` names stay free for `**kwargs`.
            params = sig.parameters
            taken = sorted(key for key in spread if key in params and params[key].kind in _KEYWORDS)
            if taken:
                raise TypeError(f"{name} names {', '.join(taken)}, which is a parameter of its own")
            named.update(spread)
    sig.bind(*positional, **named)
    return positional, named
