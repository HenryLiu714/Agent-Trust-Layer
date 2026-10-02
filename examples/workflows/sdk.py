"""The irimi SDK as the sample workflows use it, before and after it exists.

Agents write `from examples.workflows import sdk` where a real agent would write
`from irimi import sdk`, and use the API #74 and #76 specify: `@sdk.trigger`, `sdk.run()`,
`sdk.current_run_id()`, `sdk.propagate()`, `sdk.active()` and `@sdk.tool(kind=..., shadow=...)`.

- **When `irimi.sdk` exists**, as it has since #74, this module re-exports it, name by name: each
  name `irimi.sdk` has is the real one, and each it does not have yet is the stand-in's. #74
  shipped `trigger`, `run`, `current_run_id`, `propagate` and `active`; until #76 ships `tool`,
  `tool` is the stand-in's, so an agent that uses `@sdk.tool` still imports.
  Names this module does not define (`sdk.instrument`, `sdk.replay`, `ReplayResult`, ...) are
  forwarded to `irimi.sdk` by the module `__getattr__`.
  Three real names are still wrapped, for the observation log the corpus pins:
  - `tool` wraps the real function and its stand-in so each logs which of them ran
    (`agentkit.obs("tool", ...)`), because that is the harness's proof that no write tool ran for
    real under shadow. The real `tool` does its own decoration-time checks on what the agent wrote.
  - `trigger` and `run` log `run.start` and `run.end` from inside the real run, with the real
    run id, so the pins on them hold in both modes.
- **Until then**, it implements the same API with no control endpoint and no recording:
  - a context-variable run id that nested triggers join;
  - `agentkit.http` labelling each request with `Irimi-Run` (which irimi strips, #67);
  - #76's call-time table, so an active write tool runs its stand-in and never the real function.

  It raises #76's decoration-time `TypeError`s too, so an agent that is wrong today is wrong in
  the same way once the SDK lands.

Swapping this module for `irimi.sdk` must never let a write tool run for real under shadow. That
is why the fallback's table is not simplified.
"""

from __future__ import annotations

import contextlib
import contextvars
import functools
import importlib
import inspect
import os
import secrets
from collections.abc import Callable, Iterator
from types import ModuleType
from typing import Any, TypeVar

from examples.workflows import agentkit

F = TypeVar("F", bound=Callable[..., Any])


def _load_real() -> ModuleType | None:
    """`irimi.sdk`, or None when it does not exist yet. Only its absence falls back: an
    ImportError raised inside a broken `irimi.sdk` (or a dependency it lacks) propagates, so the
    corpus never silently tests the stand-in in place of an SDK that fails to import."""
    try:
        return importlib.import_module("irimi.sdk")
    except ModuleNotFoundError as exc:
        if exc.name in ("irimi", "irimi.sdk"):
            return None
        raise


_real = _load_real()


def _from_real(name: str) -> Any:
    """`irimi.sdk.<name>`, or None when the real SDK does not have it (yet)."""
    return getattr(_real, name, None) if _real is not None else None


def __getattr__(name: str) -> Any:
    """Forward a public name this module does not define to `irimi.sdk`, so an agent can use a
    later issue's API (`sdk.replay`, #84) without an edit here."""
    if not name.startswith("_") and _real is not None and hasattr(_real, name):
        return getattr(_real, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


ENGINE_ACTIVE_ENV = "IRIMI_ENGINE_ACTIVE"
_run_id: contextvars.ContextVar[str | None] = contextvars.ContextVar("irimi_run", default=None)
# Under the real SDK: the real runs whose `run.start` this module has logged and whose `run.end`
# it has not, so a nested trigger, which joins one, logs nothing. A set of ids rather than a context
# variable, because a run handed to a coroutine (#74) is logged as ended from the task that awaits
# it, not the context that logged its start.
_logged_runs: set[str] = set()


def active() -> bool:
    real = _from_real("active")
    if real is not None:
        return bool(real())
    return os.environ.get(ENGINE_ACTIVE_ENV) == "1"


def current_run_id() -> str | None:
    real = _from_real("current_run_id")
    if real is not None:
        return real()  # type: ignore[no-any-return]
    return _run_id.get()


def propagate(fn: Callable[..., Any]) -> Callable[..., Any]:
    real = _from_real("propagate")
    if real is not None:
        return real(fn)  # type: ignore[no-any-return]
    ctx = contextvars.copy_context()

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return ctx.copy().run(fn, *args, **kwargs)

    return wrapper


@contextlib.contextmanager
def _logged_run(name: str, run_id: str) -> Iterator[None]:
    """Log `run.start`, then `run.end` with how the block ended. The one place both modes log a
    run, so the corpus's pins on these events mean the same thing in each."""
    agentkit.obs("run.start", name=name, run=run_id)
    try:
        yield
    except BaseException as exc:
        agentkit.obs("run.end", name=name, run=run_id, outcome="error", error=type(exc).__name__)
        raise
    else:
        agentkit.obs("run.end", name=name, run=run_id, outcome="ok")


@contextlib.contextmanager
def _entered(name: str) -> Iterator[None]:
    """A run while the fallback is in use. A nested entry joins the current run."""
    if not active() or _run_id.get() is not None:
        yield
        return
    run_id = secrets.token_hex(8)
    token = _run_id.set(run_id)
    try:
        with _logged_run(name, run_id):
            yield
    finally:
        _run_id.reset(token)


@contextlib.contextmanager
def _observed(name: str) -> Iterator[None]:
    """Entered inside a real trigger or `run`: log the real run it started. The real SDK decides
    whether there is a run (inactive: none) and whether this entry joined an outer one (the same
    id this module already logged), so this only reads its id."""
    run_id = current_run_id()
    if run_id is None or run_id in _logged_runs:
        yield
        return
    _logged_runs.add(run_id)
    try:
        with _logged_run(name, run_id):
            yield
    finally:
        _logged_runs.discard(run_id)


def _around(f: Callable[..., Any], enter: Callable[[], Any]) -> Callable[..., Any]:
    """`f` inside the context manager `enter()` makes, keeping `f`'s signature (which #74 binds
    the captured args against) and its sync/async nature."""
    if inspect.iscoroutinefunction(f):

        @functools.wraps(f)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            with enter():
                return await f(*args, **kwargs)

        return async_wrapper

    @functools.wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with contextlib.ExitStack() as stack:
            stack.enter_context(enter())
            result = f(*args, **kwargs)
            if _real is not None and inspect.iscoroutine(result):
                # The real trigger hands its run to the coroutine and ends it when the coroutine
                # ends (#74), so the logged run goes with it: `run.end` is the coroutine's outcome.
                return _carried(stack.pop_all(), result)
            return result

    return wrapper


async def _carried(observed: contextlib.ExitStack, coroutine: Any) -> Any:
    """`coroutine`, awaited inside the observation a sync trigger opened before it returned it."""
    with observed:
        return await coroutine


def trigger(fn: Callable[..., Any] | None = None, *, name: str | None = None) -> Any:
    real = _from_real("trigger")

    def decorate(f: Callable[..., Any]) -> Callable[..., Any]:
        if isinstance(f, staticmethod | classmethod):
            # The function inside is the trigger and the descriptor stays one, as the real
            # trigger keeps it (#74): this module's wrapper cannot call a descriptor.
            return type(f)(decorate(f.__func__))
        is_generator = inspect.isgeneratorfunction(f) or inspect.isasyncgenfunction(f)
        run_name = f.__qualname__ if name is None else name
        if real is not None:
            # A generator goes to the real decorator as it is, for its own TypeError (#74).
            inner = f if is_generator else _around(f, lambda: _observed(run_name))
            return real(inner) if name is None else real(name=name)(inner)  # type: ignore[no-any-return]
        if is_generator:
            raise TypeError(f"@sdk.trigger cannot wrap a generator: {f.__qualname__}")
        return _around(f, lambda: _entered(run_name))

    return decorate(fn) if fn is not None else decorate


class run:  # noqa: N801 - the SDK's spelling
    """`with sdk.run(trigger=..., name=...)` and `async with` alike."""

    def __init__(self, trigger: Any = None, name: str = "run") -> None:
        real = _from_real("run")
        self._inner: Any = real(trigger=trigger, name=name) if real is not None else None
        self._name = name
        self._cm: Any = None

    def __enter__(self) -> run:
        if self._inner is not None:
            self._inner.__enter__()
            self._cm = _observed(self._name)
        else:
            self._cm = _entered(self._name)
        self._cm.__enter__()
        return self

    def __exit__(self, *exc: Any) -> Any:
        # The log's `run.end` first, while the real run is still current.
        suppressed = self._cm.__exit__(*exc)
        if self._inner is not None:
            return self._inner.__exit__(*exc)
        return suppressed

    async def __aenter__(self) -> run:
        if self._inner is not None:
            await self._inner.__aenter__()
            self._cm = _observed(self._name)
            self._cm.__enter__()
            return self
        return self.__enter__()

    async def __aexit__(self, *exc: Any) -> Any:
        if self._inner is not None:
            self._cm.__exit__(*exc)
            return await self._inner.__aexit__(*exc)
        return self.__exit__(*exc)


def _logged(fn: Callable[..., Any], tool_name: str, kind: str, ran: str) -> Callable[..., Any]:
    """`fn`, logging that it ran. Keeps `fn`'s signature and its sync/async nature, which #76
    checks and binds against. What #76 refuses (a stand-in that is not callable, a generator) is
    returned as it is, so the real `tool`'s check sees what the agent wrote."""
    if not callable(fn) or inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
        return fn
    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            agentkit.obs("tool", name=tool_name, kind=kind, ran=ran, run=current_run_id())
            return await fn(*args, **kwargs)

        return async_wrapper

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        agentkit.obs("tool", name=tool_name, kind=kind, ran=ran, run=current_run_id())
        return fn(*args, **kwargs)

    return wrapper


def tool(*, kind: str, shadow: Callable[..., Any] | None = None, name: str | None = None) -> Any:
    real = _from_real("tool")

    def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
        tool_name = name or f"{fn.__module__}.{fn.__qualname__}"
        if real is None:
            _check_tool(fn, kind, shadow)
        real_fn = _logged(fn, tool_name, kind, "real")
        stand_in = _logged(shadow, tool_name, kind, "shadow") if shadow is not None else None
        if real is not None:
            # The real decorator's own decoration-time checks decide (#76); this one does not
            # pre-empt them, so W6's `decoration_errors` tests the SDK that ships.
            return real(kind=kind, shadow=stand_in, name=tool_name)(real_fn)  # type: ignore[no-any-return]
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                if kind == "write" and active():
                    assert stand_in is not None
                    return await stand_in(*args, **kwargs)
                return await real_fn(*args, **kwargs)

            return async_wrapper

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if kind == "write" and active():
                assert stand_in is not None
                return stand_in(*args, **kwargs)
            return real_fn(*args, **kwargs)

        return wrapper

    return decorate


def _check_tool(fn: Callable[..., Any], kind: str, shadow: Callable[..., Any] | None) -> None:
    """#76's decoration-time rules: nothing unnamed runs live, and the error comes at load time."""
    if kind not in ("read", "write"):
        raise TypeError(f"@sdk.tool kind must be 'read' or 'write', not {kind!r}")
    if kind == "write" and shadow is None:
        raise TypeError(f"@sdk.tool(kind='write') on {fn.__qualname__} must name a shadow stand-in")
    if kind == "read" and shadow is not None:
        raise TypeError(f"@sdk.tool(kind='read') on {fn.__qualname__} cannot have a stand-in")
    if shadow is not None:
        if not callable(shadow):
            raise TypeError(f"the stand-in for {fn.__qualname__} is not callable")
        if inspect.iscoroutinefunction(shadow) != inspect.iscoroutinefunction(fn):
            raise TypeError(f"the stand-in for {fn.__qualname__} must be sync or async as it is")
    if inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
        raise TypeError(f"@sdk.tool cannot wrap a generator: {fn.__qualname__}")
