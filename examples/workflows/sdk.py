"""The irimi SDK as the sample workflows use it, before and after it exists.

Agents write `from examples.workflows import sdk` where a real agent would write
`from irimi import sdk`, and use the API #74 and #76 specify: `@sdk.trigger`, `sdk.run()`,
`sdk.current_run_id()`, `sdk.propagate()`, `sdk.active()` and `@sdk.tool(kind=..., shadow=...)`.

- **When `irimi.sdk` exists**, this module re-exports it. `tool` still wraps the real function and
  its stand-in so each logs which of them ran (`agentkit.obs("tool", ...)`), because that is the
  harness's proof that no write tool ran for real under shadow.
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
import inspect
import os
import secrets
from collections.abc import Callable, Iterator
from typing import Any, TypeVar

from examples.workflows import agentkit

F = TypeVar("F", bound=Callable[..., Any])

try:  # pragma: no cover - which branch runs depends on whether #74 has landed
    from irimi import sdk as _real  # type: ignore[attr-defined]
except ImportError:
    _real = None

ENGINE_ACTIVE_ENV = "IRIMI_ENGINE_ACTIVE"
_run_id: contextvars.ContextVar[str | None] = contextvars.ContextVar("irimi_run", default=None)


def active() -> bool:
    if _real is not None:
        return bool(_real.active())
    return os.environ.get(ENGINE_ACTIVE_ENV) == "1"


def current_run_id() -> str | None:
    if _real is not None:
        return _real.current_run_id()  # type: ignore[no-any-return]
    return _run_id.get()


def propagate(fn: Callable[..., Any]) -> Callable[..., Any]:
    if _real is not None:
        return _real.propagate(fn)  # type: ignore[no-any-return]
    ctx = contextvars.copy_context()

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return ctx.copy().run(fn, *args, **kwargs)

    return wrapper


@contextlib.contextmanager
def _entered(name: str) -> Iterator[None]:
    """A run while the fallback is in use. A nested entry joins the current run."""
    if not active() or _run_id.get() is not None:
        yield
        return
    run_id = secrets.token_hex(8)
    token = _run_id.set(run_id)
    agentkit.obs("run.start", name=name, run=run_id)
    try:
        yield
    except BaseException as exc:
        agentkit.obs("run.end", name=name, run=run_id, outcome="error", error=type(exc).__name__)
        raise
    else:
        agentkit.obs("run.end", name=name, run=run_id, outcome="ok")
    finally:
        _run_id.reset(token)


def trigger(fn: Callable[..., Any] | None = None, *, name: str | None = None) -> Any:
    if _real is not None:
        return _real.trigger(fn, name=name) if fn is not None else _real.trigger(name=name)

    def decorate(f: Callable[..., Any]) -> Callable[..., Any]:
        if inspect.isgeneratorfunction(f) or inspect.isasyncgenfunction(f):
            raise TypeError(f"@sdk.trigger cannot wrap a generator: {f.__qualname__}")
        run_name = name or f.__qualname__
        if inspect.iscoroutinefunction(f):

            @functools.wraps(f)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                with _entered(run_name):
                    return await f(*args, **kwargs)

            return async_wrapper

        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with _entered(run_name):
                return f(*args, **kwargs)

        return wrapper

    return decorate(fn) if fn is not None else decorate


class run:  # noqa: N801 - the SDK's spelling
    """`with sdk.run(trigger=..., name=...)` and `async with` alike."""

    def __init__(self, trigger: Any = None, name: str = "run") -> None:
        self._inner: Any = _real.run(trigger=trigger, name=name) if _real is not None else None
        self._name = name
        self._cm: Any = None

    def __enter__(self) -> run:
        if self._inner is not None:
            self._inner.__enter__()
        else:
            self._cm = _entered(self._name)
            self._cm.__enter__()
        return self

    def __exit__(self, *exc: Any) -> Any:
        target = self._inner if self._inner is not None else self._cm
        return target.__exit__(*exc)

    async def __aenter__(self) -> run:
        if self._inner is not None:
            await self._inner.__aenter__()
            return self
        return self.__enter__()

    async def __aexit__(self, *exc: Any) -> Any:
        if self._inner is not None:
            return await self._inner.__aexit__(*exc)
        return self.__exit__(*exc)


def _logged(fn: Callable[..., Any], tool_name: str, kind: str, ran: str) -> Callable[..., Any]:
    """`fn`, logging that it ran. Keeps `fn`'s signature and its sync/async nature, which #76
    checks and binds against."""
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
    def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
        tool_name = name or f"{fn.__module__}.{fn.__qualname__}"
        _check_tool(fn, kind, shadow)
        real_fn = _logged(fn, tool_name, kind, "real")
        stand_in = _logged(shadow, tool_name, kind, "shadow") if shadow is not None else None
        if _real is not None:
            return _real.tool(kind=kind, shadow=stand_in, name=tool_name)(real_fn)  # type: ignore[no-any-return]
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
