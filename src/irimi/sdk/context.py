"""Which run is current: the SDK's one piece of per-call state, a context variable.

A run's id lives in a `contextvars.ContextVar`, so it follows the code a run runs rather than the
thread it runs on. An `asyncio` task created inside a run and a call through `asyncio.to_thread`
copy the context, and stay in the run. A `threading.Thread` and `ThreadPoolExecutor.submit` start
from an empty context, and lose it, unless the callable they are given went through `propagate`.
Nothing else in the SDK holds a run id; `runs.RunScope` is the only code that sets one.
"""

import contextvars
import functools
import os
from collections.abc import Callable

from irimi import paths

_RUN_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar("irimi_run_id", default=None)


def active() -> bool:
    """True when irimi is in front of this process: `IRIMI_ENGINE_ACTIVE` is `1`, as `irimi
    shadow` sets it for its child. Read at each call, never cached. Here rather than in
    `runs`, so `instrumentation`, which `runs` imports, reads the same rule."""
    return os.environ.get(paths.ENGINE_ACTIVE_ENV) == "1"


def replay_run_id() -> str | None:
    """The run a replay's engine names in `IRIMI_REPLAY_RUN` for its child, or None when irimi is
    not replaying this process: the one rule `tools` asks before it looks a read tool up,
    and `runs.RunScope` before it adopts the id. Never while the SDK is inactive, as
    `current_run_id`: irimi is not in front, whatever the environment says. Read at each call,
    never cached.

    irimi names a replay's run twice, as the child's process run (`IRIMI_RUN`) and as the run it
    replays as, so the two must agree. A variable a shell exported and irimi did not set - a serve
    agent is not irimi's child, and serve sets neither - says nothing: adopting it would merge
    every run the agent starts into one that was never started."""
    if not active():
        return None
    replaying = os.environ.get(paths.REPLAY_RUN_ENV) or None
    if replaying is None or replaying != os.environ.get(paths.RUN_ENV):
        return None
    return replaying


def current_run_id() -> str | None:
    """The id of the run this code is running in. None outside every run, and always while the
    SDK is inactive."""
    return _RUN_ID.get()


def enter(run_id: str) -> contextvars.Token[str | None]:
    """Make `run_id` current in this context, until `leave` is given the token."""
    return _RUN_ID.set(run_id)


def leave(token: contextvars.Token[str | None]) -> None:
    """Make current again what was current before `enter` returned `token`. Called in the context
    `enter` was: a `RunScope` is entered and left by one call or one block, and `sdk.run` leaves
    only the entries it made in the context it is leaving (`api.run`). The one exception is a
    coroutine closed by the garbage collector, whose ValueError `RunScope._close` absorbs."""
    _RUN_ID.reset(token)


def propagate[**P, R](fn: Callable[P, R]) -> Callable[P, R]:
    """`fn`, run inside a copy of the context as it is when `propagate` is CALLED, so work handed
    to a thread stays in the run that handed it over: `executor.submit(sdk.propagate(work))`.

    Each call runs in a fresh copy of that context, because one `Context` can be entered by one
    thread at a time and the same wrapper may be running on several."""
    context = contextvars.copy_context()

    @functools.wraps(fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        return context.copy().run(fn, *args, **kwargs)

    return wrapper
