"""Mark where a run begins and ends: `@sdk.trigger`, `sdk.run()` and their shared `RunScope`."""

import asyncio
import contextlib
import contextvars
import functools
import inspect
import os
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, replace
from types import TracebackType
from typing import Any, overload

from irimi import __version__
from irimi.core.limits import MAX_NAME
from irimi.core.secret_shapes import cut_outside_secrets
from irimi.protocol import trace
from irimi.protocol.control import RunStart, run_end_to_json, run_start_to_json
from irimi.protocol.entrypoints import entrypoint
from irimi.protocol.env import AGENT_VERSION_ENV
from irimi.protocol.trace import MAIN_MODULE
from irimi.sdk import _context as context
from irimi.sdk._capture import Captured, capture, capture_call
from irimi.sdk._errors import error_info
from irimi.sdk._http_labels import instrument
from irimi.sdk._state import state


@dataclass(frozen=True)
class _Opened:
    """Hold a run this entry made current; `irimi replay` posts an `adopted` run's start and end."""

    run_id: str
    token: contextvars.Token[str | None]
    start: dict[str, Any]
    adopted: bool = False


class RunScope:
    """Enter a run once, with `with` or `async with`; entering gives the run id or None.

        a run is current       join it: no capture, no post, returns None
        in a replay's child    adopt the replay's run: no capture, no posts
        otherwise              capture, mint an id, post `start`

    On exit a new run posts `end`. Posts are synchronous, so they bracket the run's requests.
    """

    def __init__(self, name: str, entrypoint: str | None, capture: Callable[[], Captured]) -> None:
        self._name = name
        self._entrypoint = entrypoint
        self._capture = capture
        self._opened: _Opened | None = None

    def __enter__(self) -> str | None:
        opened = self._open()
        if opened is None:
            return None
        try:
            _report_start(opened)
        except BaseException:
            self._close()  # `with` does not call `__exit__` when `__enter__` raises
            raise
        return opened.run_id

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            if self._opened is not None:
                _report_end(self._opened, exc)
        finally:
            self._close()

    async def __aenter__(self) -> str | None:
        opened = self._open()
        if opened is None:
            return None
        try:
            await off_the_loop(_report_start, opened)
        except BaseException:
            self._close()  # `async with` does not call `__aexit__` when `__aenter__` raises
            raise
        return opened.run_id

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if isinstance(exc, GeneratorExit):
            # A closing coroutine may not await, so the end is posted inline.
            self.__exit__(exc_type, exc, tb)
            return
        try:
            if self._opened is not None:
                await off_the_loop(_report_end, self._opened, exc)
        finally:
            self._close()

    def hand_off[T](self, coroutine: Coroutine[Any, Any, T]) -> Coroutine[Any, Any, T]:
        """Move this entry's run into `coroutine`; whichever task awaits it ends the run."""
        opened = self._opened
        if opened is None:
            return coroutine
        self._close()  # so the enclosing `with` ends nothing
        return self._carry(opened, coroutine)

    async def _carry[T](self, opened: _Opened, coroutine: Coroutine[Any, Any, T]) -> T:
        self._opened = replace(opened, token=context.enter(opened.run_id))
        try:
            result = await coroutine
        except BaseException as exc:
            await self.__aexit__(type(exc), exc, exc.__traceback__)
            raise
        await self.__aexit__(None, None, None)
        return result

    def _open(self) -> _Opened | None:
        """Adopt or start a run and make it current; None if inactive or joining a run."""
        if not context.is_active() or context.current_run_id() is not None:
            return None
        adopted = context.replay_run_id()
        if adopted is not None:
            self._opened = _Opened(adopted, context.enter(adopted), {}, adopted=True)
            return self._opened
        args, replayable = self._capture()
        trigger = trace.Trigger(self._name, self._entrypoint, args, replayable)
        start = run_start_to_json(
            RunStart(trigger, os.environ.get(AGENT_VERSION_ENV), __version__, time.time())
        )
        run_id = trace.new_run_id()
        self._opened = _Opened(run_id, context.enter(run_id), start)
        return self._opened

    def _close(self) -> None:
        opened, self._opened = self._opened, None
        if opened is None:
            return
        # A coroutine the garbage collector closes runs in another context, with nothing to restore.
        with contextlib.suppress(ValueError):
            context.leave(opened.token)


async def off_the_loop[T](fn: Callable[..., T], *args: Any) -> T:
    """Return `fn(*args)` run in a worker thread, or inline with no asyncio loop or executor."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return fn(*args)
    try:
        waiting = loop.run_in_executor(None, contextvars.copy_context().run, fn, *args)
    except RuntimeError:
        return fn(*args)
    return await waiting


def _report_start(opened: _Opened) -> None:
    """Post the run's start (unless adopted), then call `instrument()`."""
    if not opened.adopted:
        state.reporter.post(opened.run_id, "start", opened.start)
    try:
        instrument()
    except Exception:
        state.warn_once(
            "instrument",
            "irimi: could not label this process's requests with their run; the runs go on "
            "and are recorded, but their requests may be recorded under the process's run "
            "instead of their own",
            exc_info=True,
        )


def _report_end(opened: _Opened, exc: BaseException | None) -> None:
    """Post the run's end, unless adopted (`irimi replay` ends that run)."""
    if opened.adopted:
        return
    error = None if exc is None else error_info(exc)
    end = trace.RunEnd(time.time(), "ok" if exc is None else "error", error, None)
    state.reporter.post(opened.run_id, "end", run_end_to_json(end))


@overload
def trigger[F: Callable[..., Any]](fn: F, /) -> F: ...
@overload
def trigger[F: Callable[..., Any]](*, name: str | None = None) -> Callable[[F], F]: ...
def trigger(fn: Callable[..., Any] | None = None, *, name: str | None = None) -> Any:
    """Mark a function as the start of a run.

    Each call outside a run is one run, recorded with its arguments; a call inside a run joins it.
    Works on sync and async functions and methods. Inert unless irimi is running the process.

    Args:
        fn: The function, when used bare as `@sdk.trigger`.
        name: The run's display name. Defaults to the function's `module:qualname`.

    Returns:
        The function wrapped, with its signature and its sync or async nature kept.

    Raises:
        TypeError: At decoration, for a generator function, a callable with no importable
            `module:qualname`, or a `name` that is not a string or is passed positionally.

    Example:
        @sdk.trigger
        def handle(ticket: Ticket) -> None: ...
    """
    if fn is not None and not callable(fn) and not isinstance(fn, staticmethod | classmethod):
        raise TypeError(f"@sdk.trigger takes its name as name=, not {fn!r}")
    check_name(name, "@sdk.trigger")

    def decorate(f: Callable[..., Any]) -> Callable[..., Any]:
        if isinstance(f, staticmethod | classmethod):
            # Keep the descriptor, or a static method would be bound to the instance.
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


def label(f: Callable[..., Any], name: object, api: str) -> str:
    """Return `name`, or `f`'s `module:qualname` (required either way), cut to MAX_NAME."""
    check_name(name, api)
    default = entrypoint(f, api)
    return cut_outside_secrets(default if name is None else str(name), MAX_NAME)


def check_name(name: object, api: str) -> None:
    """Raise TypeError at decoration for a name that is not a string, not later as a lost run."""
    if name is not None and not isinstance(name, str):
        raise TypeError(f"{api}(name=...) must be a string, not {name!r}")


def _warn_script(f: Callable[..., Any]) -> None:
    """Warn once, under irimi only, that a trigger in a script run by path cannot be replayed."""
    if not context.is_active():
        return
    state.warn_once(
        "script-trigger",
        "irimi: trigger %s is in a script run by path, so its runs cannot be replayed by "
        "entrypoint; start the agent with python -m <module> instead",
        f.__qualname__,
    )


@dataclass(frozen=True)
class Run:
    """The run a `with sdk.run(...) as r` block started.

    Attributes:
        id: The run's id, as `irimi runs show` and `irimi replay` take it.
        name: The run's display name.

    Example:
        with sdk.run("nightly") as r:
            log.info("irimi run %s", r and r.id)
    """

    id: str
    name: str


class run:
    """Mark a block of code as one run, with `with` or `async with`.

    Use it where there is no single function to decorate, such as one message in a worker loop.
    A block inside a run joins it; each entry, on any thread or task, is its own run.

    Args:
        name: The run's display name.
        inputs: Any value, recorded as the run's arguments. A replay needs `--entrypoint`.

    Returns:
        Entering gives a `Run`, or None while the SDK is inactive or the block joins a run.

    Raises:
        TypeError: When `name` is not a string.

    Example:
        with sdk.run("nightly", inputs={"date": "2026-09-29"}) as r:
            reconcile()
    """

    def __init__(self, name: str, *, inputs: object = None) -> None:
        if not isinstance(name, str):
            raise TypeError(f"sdk.run(name) must be a string, not {name!r}")
        self._inputs = inputs
        self._name = cut_outside_secrets(name, MAX_NAME)
        # A stack per context, so an exit closes the entry its own thread or task made.
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
        """Push and return a new entry's scope; None if inactive, unless entries are open."""
        entries = self._entries.get()
        if not entries and not context.is_active():
            return None
        entry = _Entry(RunScope(self._name, None, lambda: capture(self._inputs)))
        entry.token = self._entries.set((*entries, entry))
        return entry.scope

    def _pop(self) -> RunScope | None:
        """Pop this context's innermost scope; None if none, or exiting from another task."""
        entries = self._entries.get()
        token = entries[-1].token if entries else None
        if token is None:
            return None
        try:
            self._entries.reset(token)
        except (ValueError, RuntimeError):
            return None
        return entries[-1].scope


@dataclass
class _Entry:
    """Hold one entry into a `run`: its scope and the token that pops it."""

    scope: RunScope
    token: contextvars.Token[tuple["_Entry", ...]] | None = None
