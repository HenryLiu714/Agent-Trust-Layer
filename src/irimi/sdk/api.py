"""The two ways an agent marks a run: `@sdk.trigger` around a function, `sdk.run()` around a block.
Both are thin: what a run is, is `runs.RunScope`, and what its args are, is `capture`."""

import contextvars
import functools
import importlib
import inspect
import logging
import threading
import types
from collections.abc import Callable
from dataclasses import dataclass
from types import TracebackType
from typing import Any, overload

from irimi.limits import MAX_NAME
from irimi.sdk.capture import capture, capture_call
from irimi.sdk.client import LOGGER_NAME
from irimi.sdk.context import is_active
from irimi.sdk.runs import RunScope
from irimi.secret_shapes import cut_outside_secrets
from irimi.trace import MAIN_MODULE, ErrorInfo, importable_module

logger = logging.getLogger(LOGGER_NAME)

# Set once a trigger defined in a script run by path has been warned of.
_warned_script = False
_warn_lock = threading.Lock()


@overload
def trigger[F: Callable[..., Any]](fn: F, /) -> F: ...
@overload
def trigger[F: Callable[..., Any]](*, name: str | None = None) -> Callable[[F], F]: ...
def trigger(fn: Callable[..., Any] | None = None, *, name: str | None = None) -> Any:
    """Mark `fn` as where a run starts: each call that is not already inside a run is one.

    Usable bare (`@sdk.trigger`) or called (`@sdk.trigger(name="refund")`); `name` defaults to the
    function's `module:qualname` (`label`). It wraps sync functions, `async def` functions and
    methods - a `staticmethod` or `classmethod` in either decorator order, which stays one - and
    keeps their signature and their nature: the wrapper is a coroutine function exactly when `fn`
    is. The run's trigger records the function's `module:qualname` as its entrypoint and the
    call's arguments, captured (`capture.capture_call`). A trigger in a script run by path records
    `__main__`, which no replay can import, and is warned of once under irimi.

    A sync function that returns a coroutine - a plain decorator over an `async def`, or
    `return handle_async(x)` - has not run its body when it returns, so its run goes with the
    coroutine and ends when the coroutine does (`RunScope.hand_off`). Anything else it returns is
    its result: a sync entrypoint that drives a coroutine to completion itself keeps returning
    its value.

    Refused at decoration time, with TypeError, so the mistake shows when the agent loads rather
    than as a lost run: a generator or async-generator function, whose call returns before its
    body has run; what has no importable `module:qualname` to record (`entrypoint`); a `name`
    that is not a string; and a name passed positionally."""
    if fn is not None and not callable(fn) and not isinstance(fn, staticmethod | classmethod):
        raise TypeError(f"@sdk.trigger takes its name as name=, not {fn!r}")
    check_name(name, "@sdk.trigger")

    def decorate(f: Callable[..., Any]) -> Callable[..., Any]:
        if isinstance(f, staticmethod | classmethod):
            # The function inside is the trigger, and the descriptor stays: a static method
            # returned as a plain function would be bound, and given the instance as its first
            # argument, with the SDK inactive too.
            return type(f)(decorate(f.__func__))
        entry = entrypoint(f)
        if inspect.isgeneratorfunction(f) or inspect.isasyncgenfunction(f):
            raise TypeError(
                f"@sdk.trigger cannot wrap the generator function {f.__qualname__}: a run ends "
                "when the call returns, before a generator's body has run"
            )
        run_name = label(f, name, "@sdk.trigger")
        if entry.startswith(f"{MAIN_MODULE}:"):
            _warn_script(f)

        if inspect.iscoroutinefunction(f):

            @functools.wraps(f)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                async with RunScope(run_name, entry, lambda: capture_call(f, args, kwargs)):
                    result = f(*args, **kwargs)
                    return await result if inspect.isawaitable(result) else result

            return async_wrapper

        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            scope = RunScope(run_name, entry, lambda: capture_call(f, args, kwargs))
            with scope:
                result = f(*args, **kwargs)
                return scope.hand_off(result) if inspect.iscoroutine(result) else result

        return wrapper

    return decorate if fn is None else decorate(fn)


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


def label(f: Callable[..., Any], name: object, api: str) -> str:
    """The name a trigger's runs or a tool's calls record: `name`, or `f`'s `module:qualname`.
    The one naming rule, which `sdk.run` follows too (`check_name`). `f` must have a
    `module:qualname` even when `name` is given, so a named tool is refused what a bare one is."""
    check_name(name, api)
    default = entrypoint(f, api)
    return cut_outside_secrets(default if name is None else str(name), MAX_NAME)


def check_name(name: object, api: str) -> None:
    """TypeError for a name that is not a string, at decoration rather than as a lost run. Cut
    outside any secret's shape (`secret_shapes.cut_outside_secrets`) by `label` and `RunScope`."""
    if name is not None and not isinstance(name, str):
        raise TypeError(f"{api}(name=...) must be a string, not {name!r}")


def _warn_script(f: Callable[..., Any]) -> None:
    """A trigger in a script run by path records `__main__:fn`, which no replay can import. Warned
    once per process, and only under irimi, so production stays quiet."""
    global _warned_script
    if not is_active():
        return
    with _warn_lock:
        if _warned_script:
            return
        _warned_script = True
    logger.warning(
        "irimi: trigger %s is in a script run by path, so its runs cannot be replayed by "
        "entrypoint; start the agent with python -m <module> instead",
        f.__qualname__,
    )


class NotCallable(Exception):
    """The trigger cannot be called from what `irimi replay` gave this process: the reason.
    `cause` is the type of what importing its module raised, as `trace.ErrorInfo` names a type
    (`builtins.ModuleNotFoundError`), and None when the import was not the trouble."""

    def __init__(self, reason: str, cause: str | None = None) -> None:
        super().__init__(reason)
        self.cause = cause


def resolve(entrypoint: str) -> Callable[..., Any]:
    """The function `entrypoint`, `module:qualname`, names, imported: `entrypoint`'s inverse, which
    a replay calls a trigger by. A method defined on a class, called without the instance it
    was bound to, is refused: the run recorded its args without `self`. A `staticmethod` and
    a `classmethod` are what their class gives. A function in `__main__` is refused too: in a
    replay's child `__main__` is `replay_child` itself, whose `main` would call itself until the
    stack ran out. An Exception or a SystemExit the import or a lookup raises is
    NotCallable."""
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


@dataclass(frozen=True)
class Run:
    """The run a `with sdk.run(...) as r` block started: its id and its name."""

    id: str
    name: str


class run:
    """A block that is one run, with `with` or `async with`. `name` is the run's display name,
    a string. `inputs` is any value, captured as the run's args (`capture.capture`); the run has
    no entrypoint, so replay needs one named for it. The block gets a `Run`, or None while the
    SDK is inactive or the block joins a run already current.

    One object may be entered again - nested in itself, on several threads, in several tasks at
    once, or as a module-level constant - and each entry is one `RunScope` of its own (nested
    ones join the outer run, as nested triggers do). The scopes are kept per context, in a
    context variable of this object's, so an exit leaves the entry its own thread or task made.
    An exit in a context that made no entry - an async generator closed from another task - ends
    nothing, and that run is stored incomplete."""

    def __init__(self, name: str, *, inputs: object = None) -> None:
        if not isinstance(name, str):
            raise TypeError(f"sdk.run(name) must be a string, not {name!r}")
        self._inputs = inputs
        self._name = cut_outside_secrets(name, MAX_NAME)
        self._entries: contextvars.ContextVar[tuple[_Entry, ...]] = contextvars.ContextVar(
            "irimi_sdk_run_entries", default=()
        )

    def __enter__(self) -> Run | None:
        scope = self._push()
        if scope is None:
            return None
        try:
            run_id = scope.__enter__()
        except BaseException:
            self._pop()
            raise
        return None if run_id is None else Run(run_id, self._name)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        scope = self._pop()
        if scope is not None:
            scope.__exit__(exc_type, exc, tb)

    async def __aenter__(self) -> Run | None:
        scope = self._push()
        if scope is None:
            return None
        try:
            run_id = await scope.__aenter__()
        except BaseException:
            self._pop()
            raise
        return None if run_id is None else Run(run_id, self._name)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        scope = self._pop()
        if scope is not None:
            await scope.__aexit__(exc_type, exc, tb)

    def _push(self) -> RunScope | None:
        """A new entry's scope, on this context's stack. None, and nothing pushed, while the SDK
        is inactive and the stack is empty: an inactive SDK sets no context variable, and that
        holds for this object's own bookkeeping too. An exit then finds the stack as empty as the
        entry did, so it pops nothing, whatever `is_active()` says by then. A stack that holds
        entries is pushed onto whatever `is_active()` says, so an exit always takes off the entry it
        pairs with."""
        entries = self._entries.get()
        if not entries and not is_active():
            return None
        entry = _Entry(RunScope(self._name, None, lambda: capture(self._inputs)))
        entry.token = self._entries.set((*entries, entry))
        return entry.scope

    def _pop(self) -> RunScope | None:
        """The scope this context's innermost entry made, taken off the stack. None when this
        context made none: its stack is empty, or it holds only entries a parent context made,
        whose tokens cannot be reset here - ValueError while the maker is still inside, and
        RuntimeError once the maker has left and spent the token."""
        entries = self._entries.get()
        if not entries:
            return None
        try:
            self._entries.reset(entries[-1].token)
        except (ValueError, RuntimeError):
            return None
        return entries[-1].scope


@dataclass
class _Entry:
    """One entry into a `run`: its scope, and the token that takes it off the stack again. The
    token is reset rather than the stack set back, so a worker that enters a new `sdk.run` per
    message leaves nothing behind in its context. Set by `_push` as soon as the entry is on the
    stack."""

    scope: RunScope
    token: contextvars.Token[tuple["_Entry", ...]] = None  # type: ignore[assignment]
