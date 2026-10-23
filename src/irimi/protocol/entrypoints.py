"""Name a function by its `module:qualname`, and import the function such a name names.

A trigger records its entrypoint with `entrypoint`, and a replay calls it again through
`resolve`. Shared by the SDK, `irimi.testing` and a replay's child, so the three agree on which
functions can be named and found again.
"""

import importlib
import inspect
import types
from collections.abc import Callable
from typing import Any

from irimi.protocol.trace import MAIN_MODULE, ErrorInfo, importable_module


def entrypoint(f: Callable[..., Any], decorator: str = "@sdk.trigger") -> str:
    """`f`'s `module:qualname`, which replay imports it by: a function in a module started with
    `python -m` is named by that module's real name (`trace.importable_module`). Refused with
    TypeError for what has none: a `functools.partial` or an instance with `__call__`, a function
    `exec` defined with no `__name__` in its namespace, and a class, which decorated would be
    replaced by a function, so that `isinstance` against it would break even in production.
    `decorator` names the one refusing: the tool decorators refuse the same things, for their
    default name."""
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
    """The trigger cannot be called from what `irimi replay` gave this process: the reason.
    `cause` is the type of what importing its module raised, as `trace.ErrorInfo` names a type
    (`builtins.ModuleNotFoundError`), and None when the import was not the trouble."""

    def __init__(self, reason: str, cause: str | None = None) -> None:
        super().__init__(reason)
        self.cause = cause


def resolve(entrypoint: str) -> Callable[..., Any]:
    """Return the function `entrypoint` (`module:qualname`) names, importing its module.

    A method defined on a class, called without the instance it was bound to, is refused: the run
    recorded its args without `self`. A `staticmethod` and a `classmethod` are what their class
    gives. A function in `__main__` is refused too: in a replay's child `__main__` is the child
    itself, whose `main` would call itself until the stack ran out. An Exception or a SystemExit
    the import or a lookup raises is NotCallable."""
    module_name, sep, qualname = entrypoint.partition(":")
    if not sep or not module_name or not qualname:
        raise NotCallable("it is not module:qualname")
    if module_name == MAIN_MODULE:
        raise NotCallable("it is in __main__, which in a replay's child is the child itself")
    try:
        found: Any = importlib.import_module(module_name)
    except (Exception, SystemExit) as exc:
        # SystemExit too: a module that parses its argv as it is imported exits on irimi's.
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
            # A module's `__getattr__` that imports lazily, or a class attribute's descriptor,
            # raising: the call is never made, as when the import raises.
            raise NotCallable(
                f"looking up {qualname} in {module_name} raised {type(exc).__name__}: {exc}"
            ) from None
    if isinstance(owner, type) and isinstance(
        inspect.getattr_static(owner, qualname.rsplit(".", 1)[-1], None), types.FunctionType
    ):
        raise NotCallable("it is a method, and a run does not record the instance it was called on")
    if not callable(found):
        raise NotCallable(f"{qualname} is not callable")
    return found
