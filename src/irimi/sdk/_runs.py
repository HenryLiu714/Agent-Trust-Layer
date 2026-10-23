"""Where a run begins and ends: `@sdk.trigger`, `sdk.run()` and the `RunScope` both share.

A trigger makes a fresh `RunScope` per call, and `sdk.run` one per entry. The scope joins a run
already current, adopts a replay's run in a replay's child, or starts a new one: it mints the id,
posts the start, and posts the end with how the body ended. While the SDK is inactive it does
nothing at all, and a post never raises into the agent.
"""

import asyncio
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
    """A run this entry made current: its id, the token that undoes it, and the start to post.
    `adopted` is a replay's run, whose start and end are `irimi replay`'s: it posts neither."""

    run_id: str
    token: contextvars.Token[str | None]
    start: dict[str, Any]
    adopted: bool = False


class RunScope:
    """One entry into a run, used once, as a context manager for both `with` and `async with`.

    `name` is already cut to MAX_NAME. `capture` is called only if a new run starts, and returns
    the trigger's args and whether they are replayable. Entering it gives the id of the run it
    opened or adopted, or None. Entering it:

    1. Nesting. If a run is already current, this entry joins it: no new run, no capture, no post.
    2. A replay's run. Otherwise, in a replay's child (`_context.replay_run_id`), it adopts the
       run the replay is: makes that id current, calls `instrument()`, and captures and posts
       nothing, since `irimi replay` owns that run's record, start and end alike. So everything
       the child does lands in the one run, however many triggers it reaches.
    3. A new run. Otherwise it captures the trigger, mints an id (`trace.new_run_id`), makes it
       current, posts the run's `start` and calls `instrument()`.
    4. The body runs.
    5. It posts the run's `end`, unless it adopted the run: outcome `ok`, or `error` with what
       was raised - any BaseException, KeyboardInterrupt and CancelledError included - which then
       propagates, the same object.
    6. It makes current again whatever was current before.

    An entry interrupted while its start is posted never ran its body: it leaves the run at once
    and posts no end, and if the start arrived the run is stored incomplete. One interrupted while
    its end is posted propagates the interruption, with the body's own exception as its
    `__context__`, and still leaves the run.

    Posts are synchronous, so the start reaches irimi before the run's first request and the end
    after its last: `with` posts inline, and `async with` in a worker thread (`off_the_loop`). A
    run whose posts failed goes on, current and labelled, and irimi only lacks its record. While
    `is_active()` is false an entry sets no context variable, captures nothing, makes no network
    call and patches nothing."""

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
            self._close()  # interrupted while the start was posted: `with` will not call `__exit__`
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
            # Cancelled while the start was posted: `async with` will not call `__aexit__`. The
            # post finishes in its thread.
            self._close()
            raise
        return opened.run_id

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if isinstance(exc, GeneratorExit):
            # The coroutine is being closed, not awaited: a pending task garbage-collected, or a
            # `.close()` by hand. Nothing may be awaited now - Python would raise "coroutine
            # ignored GeneratorExit" into whoever closed it - so the end is posted inline.
            self.__exit__(exc_type, exc, tb)
            return
        try:
            if self._opened is not None:
                await off_the_loop(_report_end, self._opened, exc)
        finally:
            self._close()

    def hand_off[T](self, coroutine: Coroutine[Any, Any, T]) -> Coroutine[Any, Any, T]:
        """The run this entry opened, handed to `coroutine`: called inside a sync `with`, by a sync
        function that returned a coroutine instead of running its body. The run leaves this
        context now, with no end, and is current again in whatever task awaits the coroutine that
        is returned, which ends it as `async with` would, with the coroutine's outcome.
        `coroutine` itself when this entry opened no run: inactive, or joined."""
        opened = self._opened
        if opened is None:
            return coroutine
        self._close()  # so the `with` this is called in ends nothing on its way out
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
        """Steps 1 to 3 but the post: decide, and when this entry adopts a replay's run, make it
        current, or when it starts a run, capture its trigger, make it current and build its
        start. None when the SDK is inactive or this entry joins a run. Captured before the id is
        current, so nothing past this point can leave the id set without `_close` to undo it."""
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
        try:
            context.leave(opened.token)
        except ValueError:
            # Closed from another context than the one that entered it - a coroutine the garbage
            # collector closes runs in whatever context collected it. The entering context is
            # abandoned with the coroutine, so there is nothing to make current again there.
            pass


async def off_the_loop[T](fn: Callable[..., T], *args: Any) -> T:
    """`fn(*args)`, and what it returns, from a worker thread, so the event loop goes on serving
    other tasks while it waits: a post, or a replay's lookup. Inline wherever
    `asyncio.to_thread` would raise into the agent: when no asyncio loop is running - a coroutine
    driven by trio, or by hand - and when the loop's executor takes no more work, shut down at the
    end of `asyncio.run` or as the interpreter exits.

    This is `asyncio.to_thread` spelled out, so that a refusal to start the thread, which raises
    before the future exists, is told apart from the wait."""
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
    """The run's start, posted, then `instrument()`, before the run's first request. An adopted run
    posts none, and is instrumented all the same."""
    if not opened.adopted:
        state.reporter.post(opened.run_id, "start", opened.start)
    try:
        instrument()
    except Exception:
        # The SDK never raises into the agent, its HTTP patching included: the run goes on
        # unlabelled.
        state.warn_once(
            "instrument",
            "irimi: could not label this process's requests with their run; the runs go on "
            "and are recorded, but their requests may be recorded under the process's run "
            "instead of their own",
            exc_info=True,
        )


def _report_end(opened: _Opened, exc: BaseException | None) -> None:
    """The run's end, posted. An adopted run posts none: `irimi replay` ends it."""
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

    Each call made outside an existing run becomes one run, recorded with the function's name and
    arguments; a call inside a run joins it. Works on sync and async functions, methods, static
    methods and class methods. Inert unless irimi is running the process.

    Args:
        fn: The function, when used bare as `@sdk.trigger`.
        name: The run's display name. Defaults to the function's `module:qualname`.

    Returns:
        The function wrapped, with its signature and its sync or async nature kept.

    Raises:
        TypeError: At decoration, for a generator function, a class or another callable with no
            importable `module:qualname`, or a `name` that is not a string or is passed
            positionally.

    Example:
        @sdk.trigger
        def handle(ticket: Ticket) -> None: ...
    """
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

        # A sync function that returns a coroutine has not run its body when it returns, so its
        # run goes with the coroutine and ends when the coroutine does (`RunScope.hand_off`).
        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            scope = RunScope(run_name, entry, lambda: capture_call(f, args, kwargs))
            with scope:
                result = f(*args, **kwargs)
                return scope.hand_off(result) if inspect.iscoroutine(result) else result

        return wrapper

    return decorate if fn is None else decorate(fn)


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
            if r is not None:
                log.info("irimi run %s", r.id)
    """

    id: str
    name: str


class run:
    """Mark a block of code as one run, with `with` or `async with`.

    Use it where a run has no single function to decorate, such as one message in a worker loop.
    A block entered inside a run joins it. One object may be entered again, on several threads or
    tasks at once, and each entry is a run of its own. Inert unless irimi is running the process.

    Args:
        name: The run's display name.
        inputs: Any value, recorded as the run's arguments. The run has no entrypoint, so a
            replay of it needs one named with `--entrypoint`.

    Returns:
        Entering it gives a `Run`, or None while the SDK is inactive or the block joins a run
        already current.

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
        # One stack of entries per context, so an exit leaves the entry its own thread or task
        # made, and nested entries join the outer run as nested triggers do.
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
        if not entries and not context.is_active():
            return None
        entry = _Entry(RunScope(self._name, None, lambda: capture(self._inputs)))
        entry.token = self._entries.set((*entries, entry))
        return entry.scope

    def _pop(self) -> RunScope | None:
        """The scope this context's innermost entry made, taken off the stack. None when this
        context made none: its stack is empty, or it holds only entries a parent context made,
        whose tokens cannot be reset here - ValueError while the maker is still inside, and
        RuntimeError once the maker has left and spent the token. An exit in such a context - an
        async generator closed from another task - ends nothing, and that run is stored
        incomplete."""
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
