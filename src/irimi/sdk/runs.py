"""Where a run begins and ends: the one place `@sdk.trigger` and `sdk.run` share.

`RunScope` is ONE entry into a run, as a context manager for both `with` and `async with`: a
trigger makes a fresh one for each call, and `sdk.run` one for each block it is entered for.
Entering it:

1. NESTING. If a run is already current, this entry joins it: no new run, no capture, no post.
2. A REPLAY'S RUN. Otherwise, in a replay's child (`context.replay_run_id`), it ADOPTS the run the
   replay is: makes that id current, calls `instrument()`, and captures and posts nothing, since
   `irimi replay` owns that run's record, start and end alike. So everything the child does
   lands in the one run: a trigger `replay_child` calls, a trigger an argv reaches, or several.
3. A NEW RUN. Otherwise it captures the trigger, mints an id (`trace.new_run_id`), makes it current,
   posts the run's `start` and calls `instrument()`.
4. The body runs.
5. It posts the run's `end`, unless it adopted the run: outcome `ok`, or `error` with what was
   raised - ANY BaseException, KeyboardInterrupt and CancelledError included - which then
   propagates, the same object.
6. It makes current again whatever was current before.

An entry interrupted while its start is posted (a KeyboardInterrupt, a cancelled task) never ran
its body: it leaves the run at once and posts no end, and if the start arrived the run is stored
incomplete. One interrupted while its end is posted propagates the interruption, with the body's
own exception as its `__context__`, as any other line would, and still leaves the run.

A sync entry whose function returned a coroutine hands the run to it (`hand_off`): the run leaves
the caller's context unended, and the task that awaits the coroutine makes it current again and
ends it when the coroutine ends. Its start was posted inline, as every sync entry's is, even on an
event loop's thread: until the function returned, nothing said its body was not already running.

INACTIVE MEANS INERT. While `is_active()` is false - production, with no irimi in front - an
entry does nothing at all: it sets no context variable, captures nothing, makes no network call and
patches nothing. That is what makes the SDK safe to leave in production code.

POSTS ARE SYNCHRONOUS, so the start reaches irimi before the run's first request and the end after
its last: `with` posts inline, and `async with` in a worker thread (`off_the_loop`), so the
event loop keeps serving other tasks meanwhile. A post never raises (`client.ControlClient`); a
run whose posts failed goes on, current and labelled, and irimi only lacks its record.

The id and the start and end posts live here and nowhere else, and so does adopting a replay's id:
one branch in `_open`.
"""

import asyncio
import contextvars
import logging
import os
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, replace
from types import TracebackType
from typing import Any

from irimi import __version__, trace
from irimi.protocol.control import RunStart, run_end_to_json, run_start_to_json
from irimi.protocol.env import AGENT_VERSION_ENV
from irimi.sdk import context, errors
from irimi.sdk.capture import Captured
from irimi.sdk.client import LOGGER_NAME, ControlClient, Reporter
from irimi.sdk.instrumentation import instrument

logger = logging.getLogger(LOGGER_NAME)


# Every post the SDK makes goes through this one client, so each kind of failure is logged once
# per process. Read at each post, so a test can put a fresh one in its place.
reporter: Reporter = ControlClient()
# Set once `instrument()` has raised and been logged, so a broken one is logged once, not per run.
_instrument_failed = False


@dataclass(frozen=True)
class _Opened:
    """A run this entry made current: its id, the token that undoes it, and the start to post.
    `adopted` is a replay's run, whose start and end are `irimi replay`'s: it posts neither."""

    run_id: str
    token: contextvars.Token[str | None]
    start: dict[str, Any]
    adopted: bool = False


class RunScope:
    """One entry into a run (see the module doc), used once. `name` is already cut to MAX_NAME.
    `capture` is called only if a new run starts, and returns the trigger's args and whether
    they are replayable. Entering it gives the id of the run it opened or adopted, or None."""

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
    global _instrument_failed
    if not opened.adopted:
        reporter.post(opened.run_id, "start", opened.start)
    try:
        instrument()
    except Exception:
        # The SDK never raises into the agent, its HTTP patching included: the run goes on
        # unlabelled.
        if not _instrument_failed:
            _instrument_failed = True
            logger.warning(
                "irimi: could not label this process's requests with their run; the runs go on "
                "and are recorded, but their requests may be recorded under the process's run "
                "instead of their own",
                exc_info=True,
            )


def _report_end(opened: _Opened, exc: BaseException | None) -> None:
    """The run's end, posted. An adopted run posts none: `irimi replay` ends it."""
    if opened.adopted:
        return
    error = None if exc is None else errors.error_info(exc)
    end = trace.RunEnd(time.time(), "ok" if exc is None else "error", error, None)
    reporter.post(opened.run_id, "end", run_end_to_json(end))
