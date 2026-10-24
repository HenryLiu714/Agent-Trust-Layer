"""Track the current run's id in a context variable; only `_runs.RunScope` sets it."""

import contextvars
import functools
import os
from collections.abc import Callable

from irimi.protocol.env import ENGINE_ACTIVE_ENV, REPLAY_RUN_ENV, RUN_ENV

_RUN_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar("irimi_run_id", default=None)


def is_active() -> bool:
    """Return whether irimi is running this process (`IRIMI_ENGINE_ACTIVE=1`, read at each call).

    Returns:
        True under irimi; when False, triggers and tools call straight through, unrecorded.

    Example:
        if sdk.is_active():
            log.info("running under irimi")
    """
    return os.environ.get(ENGINE_ACTIVE_ENV) == "1"


def replay_run_id() -> str | None:
    """Return the replayed run if `IRIMI_REPLAY_RUN` matches `IRIMI_RUN` (a stale export won't)."""
    if not is_active():
        return None
    replaying = os.environ.get(REPLAY_RUN_ENV) or None
    if replaying is None or replaying != os.environ.get(RUN_ENV):
        return None
    return replaying


def current_run_id() -> str | None:
    """Return the id of the run this code is running in.

    Returns:
        The run's id, or None outside a run or while the SDK is inactive. It follows asyncio
        tasks and `asyncio.to_thread`; a thread needs `propagate`.

    Example:
        log.info("handling ticket", extra={"irimi_run": sdk.current_run_id()})
    """
    return _RUN_ID.get()


def enter(run_id: str) -> contextvars.Token[str | None]:
    """Make `run_id` current in this context, until `leave` is given the token."""
    return _RUN_ID.set(run_id)


def leave(token: contextvars.Token[str | None]) -> None:
    """Restore the previous run; ValueError unless called in the context `enter` ran in."""
    _RUN_ID.reset(token)


def propagate[**P, R](fn: Callable[P, R]) -> Callable[P, R]:
    """Keep work handed to another thread in the current run.

    Each call runs `fn` in a fresh copy of the context captured now, so it may run on many threads.

    Args:
        fn: The callable to run in the current run.

    Returns:
        A callable with `fn`'s signature that runs it in the current run.

    Example:
        executor.submit(sdk.propagate(refund_one), charge_id)
    """
    context = contextvars.copy_context()

    @functools.wraps(fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        return context.copy().run(fn, *args, **kwargs)

    return wrapper
