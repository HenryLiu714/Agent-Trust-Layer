"""Where a run begins and ends: the one place `@sdk.trigger` and `sdk.run` share (#74).

`RunScope` is ONE entry into a run, as a context manager for both `with` and `async with`: a
trigger makes a fresh one for each call, and `sdk.run` one for each block it is entered for.
Entering it:

1. NESTING. If a run is already current, this entry joins it: no new run, no capture, no post.
2. A NEW RUN. Otherwise it captures the trigger, mints an id (`new_run_id`) and makes it current,
   posts the run's `start` and calls `instrument()`.
3. The body runs.
4. It posts the run's `end`: outcome `ok`, or `error` with what was raised - ANY BaseException,
   KeyboardInterrupt and CancelledError included - which then propagates, the same object.
5. It makes current again whatever was current before.

An entry interrupted while its start is posted (a KeyboardInterrupt, a cancelled task) never ran
its body: it leaves the run at once and posts no end, and if the start arrived the run is stored
incomplete. One interrupted while its end is posted propagates the interruption, with the body's
own exception as its `__context__`, as any other line would, and still leaves the run.

A sync entry whose function returned a coroutine hands the run to it (`hand_off`): the run leaves
the caller's context unended, and the task that awaits the coroutine makes it current again and
ends it when the coroutine ends. Its start was posted inline, as every sync entry's is, even on an
event loop's thread: until the function returned, nothing said its body was not already running.

INACTIVE MEANS INERT. While `active()` is false - production, with no irimi in front - an entry
does nothing at all: it sets no context variable, captures nothing, makes no network call and
patches nothing. That is what makes the SDK safe to leave in production code.

POSTS ARE SYNCHRONOUS, so the start reaches irimi before the run's first request and the end after
its last: `with` posts inline, and `async with` in a worker thread (`_off_the_loop`), so the
event loop keeps serving other tasks meanwhile. A post never raises (`client.ControlClient`); a
run whose posts failed goes on, current and labelled, and irimi only lacks its record.

The id and the start and end posts live here and nowhere else. Replay (#84) adds its rule here, as
one branch in `_open`: a top-level run under `IRIMI_REPLAY_RUN` adopts that id and posts nothing.
"""

import asyncio
import contextvars
import logging
import os
import secrets
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, replace
from types import TracebackType
from typing import Any

from irimi import __version__, paths, trace
from irimi.sdk import context
from irimi.sdk.capture import Captured
from irimi.sdk.client import LOGGER_NAME, ControlClient, Reporter
from irimi.sdk.instrumentation import instrument

logger = logging.getLogger(LOGGER_NAME)

# The longest trigger name a run records. A name is a label, as an error message is (#68), and a
# start past `control.MAX_CONTROL_BODY` would be refused whole.
MAX_NAME = 1000

# Every post the SDK makes goes through this one client, so each kind of failure is logged once
# per process. Read at each post, so a test can put a fresh one in its place.
reporter: Reporter = ControlClient()
# Set once `instrument()` has raised and been logged, so a broken one is logged once, not per run.
_instrument_failed = False


def new_run_id() -> str:
    """A fresh run id: 16 lower-case hex characters. It passes `trace.is_valid_run_id`, and being
    lower-case it never differs only in case from another, which a case-insensitive filesystem
    would file in `unattributed/` (#70, #73)."""
    return secrets.token_hex(8)


@dataclass(frozen=True)
class _Opened:
    """A run this entry started: its id, the token that undoes it, and the start to post."""

    run_id: str
    token: contextvars.Token[str | None]
    start: dict[str, Any]


class RunScope:
    """One entry into a run (see the module doc), used once. `capture` is called only if a new
    run starts, and returns the trigger's args and whether they are replayable."""

    def __init__(self, name: str, entrypoint: str | None, capture: Callable[[], Captured]) -> None:
        self._name = name[:MAX_NAME]
        self._entrypoint = entrypoint
        self._capture = capture
        self._opened: _Opened | None = None

    def __enter__(self) -> None:
        opened = self._open()
        if opened is None:
            return
        try:
            _report_start(opened)
        except BaseException:
            self._close()  # interrupted while the start was posted: `with` will not call `__exit__`
            raise

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

    async def __aenter__(self) -> None:
        opened = self._open()
        if opened is None:
            return
        try:
            await _off_the_loop(_report_start, opened)
        except BaseException:
            # Cancelled while the start was posted: `async with` will not call `__aexit__`. The
            # post finishes in its thread.
            self._close()
            raise

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if isinstance(exc, GeneratorExit):
            # The coroutine is being closed, not awaited: a pending task garbage-collected, or a
            # `.close()` by hand. Nothing may be awaited now - Python would raise "coroutine
            # ignored GeneratorExit" into whoever closed it - so the end is posted inline (#74).
            self.__exit__(exc_type, exc, tb)
            return
        try:
            if self._opened is not None:
                await _off_the_loop(_report_end, self._opened, exc)
        finally:
            self._close()

    def hand_off[T](self, coroutine: Coroutine[Any, Any, T]) -> Coroutine[Any, Any, T]:
        """The run this entry opened, handed to `coroutine`: called inside a sync `with`, by a sync
        function that returned a coroutine instead of running its body. The run leaves this
        context now, with no end, and is current again in whatever task awaits the coroutine that
        is returned, which ends it as `async with` would, with the coroutine's outcome (#74).
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
        """Steps 1 and 2 but the post: decide, and when this entry starts a run, capture its
        trigger, make it current and build its start. None when the SDK is inactive or this entry
        joins a run. Captured before the id is current, so nothing past this point can leave the
        id set without `_close` to undo it."""
        if not context.active() or context.current_run_id() is not None:
            return None
        args, replayable = self._capture()
        trigger = trace.Trigger(self._name, self._entrypoint, args, replayable)
        start = {
            "trigger": trace.trigger_to_json(trigger),
            "agent_version": os.environ.get(paths.AGENT_VERSION_ENV),
            "sdk_version": __version__,
            "started_at": time.time(),
        }
        run_id = new_run_id()
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
            # abandoned with the coroutine, so there is nothing to make current again there (#74).
            pass


async def _off_the_loop(fn: Callable[..., None], *args: Any) -> None:
    """`fn(*args)` in a worker thread, so the event loop goes on serving other tasks while it
    waits. Inline wherever `asyncio.to_thread` would raise into the agent: when no asyncio loop is
    running - a coroutine driven by trio, or by hand - and when the loop's executor takes no more
    work, shut down at the end of `asyncio.run` or as the interpreter exits (#74).

    This is `asyncio.to_thread` spelled out, so that a refusal to start the thread, which raises
    before the future exists, is told apart from the wait."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        fn(*args)
        return
    try:
        waiting = loop.run_in_executor(None, contextvars.copy_context().run, fn, *args)
    except RuntimeError:
        fn(*args)
        return
    await waiting


def _report_start(opened: _Opened) -> None:
    global _instrument_failed
    reporter.post(opened.run_id, "start", opened.start)
    try:
        instrument()
    except Exception:
        # The SDK never raises into the agent, #75's patching included: the run goes on unlabelled.
        if not _instrument_failed:
            _instrument_failed = True
            logger.warning(
                "irimi could not label this process's requests with their run; the runs go on "
                "and are recorded, but their requests may land in the process run.",
                exc_info=True,
            )


def _report_end(opened: _Opened, exc: BaseException | None) -> None:
    error = None if exc is None else trace.ErrorInfo.from_exception(exc)
    reporter.post(
        opened.run_id,
        "end",
        {
            "ended_at": time.time(),
            "outcome": "ok" if exc is None else "error",
            "error": trace.error_to_json(error),
        },
    )
