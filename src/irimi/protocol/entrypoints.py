"""Name a trigger as `module:qualname`, and import it back from that name for a replay."""

import importlib
import inspect
import types
from collections.abc import Callable
from typing import Any

from irimi.protocol.trace import MAIN_MODULE, ErrorInfo, importable_module


def entrypoint(f: Callable[..., Any], decorator: str = "@sdk.trigger") -> str:
    """Return `f`'s importable `module:qualname`.

    Raises TypeError naming `decorator` for a class (wrapping breaks `isinstance`) or a callable
    with no module or qualname.
    """
    module = getattr(f, "__module__", None)
    qualname = getattr(f, "__qualname__", None)
    if isinstance(f, type) or not isinstance(module, str) or not isinstance(qualname, str):
        if isinstance(f, type):
            what = f"the class {qualname}"
        elif isinstance(qualname, str):
            what = f"{qualname}, which names no module"
        else:
            what = type(f).__qualname__
        raise TypeError(
            f"{decorator} wraps a function or a method, not {what}: decorate the function itself"
        )
    return f"{importable_module(module)}:{qualname}"


class NotCallable(Exception):
    """The trigger cannot be called; `cause` is the error type a failed import raised, else None."""

    def __init__(self, reason: str, cause: str | None = None) -> None:
        super().__init__(reason)
        self.cause = cause


def resolve(entrypoint: str) -> Callable[..., Any]:
    """Return the function `entrypoint` (`module:qualname`) names, importing its module.

    Raises NotCallable for a plain method, anything in `__main__`, or a raising import or lookup.
    """
    module_name, sep, qualname = entrypoint.partition(":")
    if not sep or not module_name or not qualname:
        raise NotCallable("it is not module:qualname")
    if module_name == MAIN_MODULE:
        raise NotCallable("it is in __main__, which in a replay's child is the child itself")
    try:
        found: Any = importlib.import_module(module_name)
    except (Exception, SystemExit) as exc:
        # A module that parses argv on import exits on irimi's arguments.
        raise NotCallable(
            f"importing {module_name} raised {type(exc).__name__}: {exc}",
            ErrorInfo.from_exception(exc).type,
        ) from None
    owner: Any = None
    for part in qualname.split("."):
        owner = found
        try:
            found = getattr(found, part)
        except AttributeError:
            raise NotCallable(f"{module_name} has no {qualname}") from None
        except Exception as exc:
            # A lazy module `__getattr__` or a descriptor can raise anything.
            raise NotCallable(
                f"looking up {qualname} in {module_name} raised {type(exc).__name__}: {exc}"
            ) from None
    if isinstance(owner, type) and isinstance(
        inspect.getattr_static(owner, qualname.rsplit(".", 1)[-1], None), types.FunctionType
    ):
        raise NotCallable("it is a method, and a run does not record the instance it was called on")
    if not callable(found):
        raise NotCallable(f"{qualname} is not callable")
    call: Callable[..., Any] = found
    return call
