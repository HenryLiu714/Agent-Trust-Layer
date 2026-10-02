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
incomplete.

INACTIVE MEANS INERT. While `active()` is false - production, with no irimi in front - an entry
does nothing at all: it sets no context variable, captures nothing, makes no network call and
patches nothing. That is what makes the SDK safe to leave in production code.

POSTS ARE SYNCHRONOUS, so the start reaches irimi before the run's first request and the end after
its last: `with` posts inline, and `async with` in a worker thread (`asyncio.to_thread`), so the
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
from collections.abc import Callable
from dataclasses import dataclass
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


def active() -> bool:
    """True when irimi is in front of this process: `IRIMI_ENGINE_ACTIVE` is `1`, as `irimi
    shadow` sets it for its child (#73). Read at each call, never cached."""
    return os.environ.get(paths.ENGINE_ACTIVE_ENV) == "1"


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
        try:
            if self._opened is not None:
                await _off_the_loop(_report_end, self._opened, exc)
        finally:
            self._close()

    def _open(self) -> _Opened | None:
        """Steps 1 and 2 but the post: decide, and when this entry starts a run, capture its
        trigger, make it current and build its start. None when the SDK is inactive or this entry
        joins a run. Captured before the id is current, so nothing past this point can leave the
        id set without `_close` to undo it."""
        if not active() or context.current_run_id() is not None:
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
        if opened is not None:
            context.leave(opened.token)


async def _off_the_loop(fn: Callable[..., None], *args: Any) -> None:
    """`fn(*args)` in a worker thread, so the event loop goes on serving other tasks while it
    waits. Inline when no asyncio loop is running - a coroutine driven by trio, or by hand - where
    `asyncio.to_thread` would raise into the agent."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        fn(*args)
        return
    await asyncio.to_thread(fn, *args)


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
