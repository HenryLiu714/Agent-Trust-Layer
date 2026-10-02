"""The two ways an agent marks a run: `@sdk.trigger` around a function, `sdk.run()` around a block
(#74). Both are thin: what a run is, is `runs.RunScope`, and what its args are, is `capture`."""

import contextvars
import functools
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from types import TracebackType
from typing import Any, overload

from irimi.sdk.capture import capture, capture_call
from irimi.sdk.runs import RunScope, active


@overload
def trigger[F: Callable[..., Any]](fn: F, /) -> F: ...
@overload
def trigger[F: Callable[..., Any]](*, name: str | None = None) -> Callable[[F], F]: ...
def trigger(fn: Callable[..., Any] | None = None, *, name: str | None = None) -> Any:
    """Mark `fn` as where a run starts: each call that is not already inside a run is one.

    Usable bare (`@sdk.trigger`) or called (`@sdk.trigger(name="refund")`); `name` defaults to the
    function's `__qualname__`. It wraps sync functions, `async def` functions and methods - a
    `staticmethod` or `classmethod` in either decorator order, which stays one - and keeps their
    signature and their nature: the wrapper is a coroutine function exactly when `fn` is. The
    run's trigger records the function's `module:qualname` as its entrypoint and the call's
    arguments, captured (`capture.capture_call`).

    A sync function that returns a coroutine - a plain decorator over an `async def`, or
    `return handle_async(x)` - has not run its body when it returns, so its run goes with the
    coroutine and ends when the coroutine does (`RunScope.hand_off`). Anything else it returns is
    its result: a sync entrypoint that drives a coroutine to completion itself keeps returning
    its value (#74).

    Refused at decoration time, with TypeError, so the mistake shows when the agent loads rather
    than as a lost run: a generator or async-generator function, whose call returns before its
    body has run; what has no importable `module:qualname` to record (`_entrypoint`); a `name`
    that is not a string; and a name passed positionally."""
    if fn is not None and not callable(fn) and not isinstance(fn, staticmethod | classmethod):
        raise TypeError(f"@sdk.trigger takes its name as name=, not {fn!r}")
    if name is not None and not isinstance(name, str):
        raise TypeError(f"@sdk.trigger(name=...) must be a string, not {name!r}")

    def decorate(f: Callable[..., Any]) -> Callable[..., Any]:
        if isinstance(f, staticmethod | classmethod):
            # The function inside is the trigger, and the descriptor stays: a static method
            # returned as a plain function would be bound, and given the instance as its first
            # argument, with the SDK inactive too (#74).
            return type(f)(decorate(f.__func__))
        entrypoint = _entrypoint(f)
        if inspect.isgeneratorfunction(f) or inspect.isasyncgenfunction(f):
            raise TypeError(
                f"@sdk.trigger cannot wrap the generator function {f.__qualname__}: a run ends "
                "when the call returns, before a generator's body has run"
            )
        run_name = f.__qualname__ if name is None else name

        if inspect.iscoroutinefunction(f):

            @functools.wraps(f)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                async with RunScope(run_name, entrypoint, lambda: capture_call(f, args, kwargs)):
                    result = f(*args, **kwargs)
                    return await result if inspect.isawaitable(result) else result

            return async_wrapper

        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            scope = RunScope(run_name, entrypoint, lambda: capture_call(f, args, kwargs))
            with scope:
                result = f(*args, **kwargs)
                return scope.hand_off(result) if inspect.iscoroutine(result) else result

        return wrapper

    return decorate if fn is None else decorate(fn)


def _entrypoint(f: Callable[..., Any]) -> str:
    """`f`'s `module:qualname`, which replay (#84) imports it by. Refused with TypeError for what
    has none: a `functools.partial` or an instance with `__call__`, a function `exec` defined with
    no `__name__` in its namespace, and a class, which decorated would be replaced by a function,
    so that `isinstance` against it broke even in production."""
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
            f"@sdk.trigger wraps a function or a method, not {what}: decorate the function that "
            "handles the run"
        )
    return f"{module}:{qualname}"


class run:  # noqa: N801 - the SDK's spelling: `with sdk.run(...)`
    """A block that is one run, with `with` or `async with`. `trigger` is any value, captured as
    the run's args (`capture.capture`); the run has no entrypoint, so replay (#84) needs one named
    for it. `name` is the run's display name.

    One object may be entered again - nested in itself, on several threads, in several tasks at
    once, or as a module-level constant - and each entry is one `RunScope` of its own (nested
    ones join the outer run, as nested triggers do). The scopes are kept per context, in a
    context variable of this object's, so an exit leaves the entry its own thread or task made.
    An exit in a context that made no entry - an async generator closed from another task - ends
    nothing, and that run is stored incomplete."""

    def __init__(self, trigger: object = None, name: str = "run") -> None:
        self._trigger = trigger
        self._name = str(name)  # never a lost run over a mistyped name: it is a label
        self._entries: contextvars.ContextVar[tuple[_Entry, ...]] = contextvars.ContextVar(
            "irimi_sdk_run_entries", default=()
        )

    def __enter__(self) -> None:
        scope = self._push()
        if scope is None:
            return
        try:
            scope.__enter__()
        except BaseException:
            self._pop()
            raise

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        scope = self._pop()
        if scope is not None:
            scope.__exit__(exc_type, exc, tb)

    async def __aenter__(self) -> None:
        scope = self._push()
        if scope is None:
            return
        try:
            await scope.__aenter__()
        except BaseException:
            self._pop()
            raise

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
        is inactive and the stack is empty: the issue's "set no context variable" holds for this
        object's own bookkeeping too (#74). An exit then finds the stack as empty as the entry
        did, so it pops nothing, whatever `active()` says by then. A stack that holds entries is
        pushed onto whatever `active()` says, so an exit always takes off the entry it pairs
        with."""
        entries = self._entries.get()
        if not entries and not active():
            return None
        entry = _Entry(RunScope(self._name, None, lambda: capture(self._trigger)))
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
