"""The SDK's run and tool decorators, wrapped to log `run.start`, `run.end` and `tool` events.

The `tool` event's `ran` (`real`, `shadow` or `recording`) proves no write tool ran for real."""

from __future__ import annotations

import contextlib
import contextvars
import functools
import inspect
import sys
from collections.abc import Callable, Iterator
from typing import Any, TypeVar, overload

from irimi import sdk
from tests.e2e.workflows.agents import agentkit

F = TypeVar("F", bound=Callable[..., Any])

# Runs with a logged `run.start` and no `run.end` yet, so a nested trigger logs nothing. A set, not
# a context variable: a run handed to a coroutine ends in the task that awaits it.
_logged_runs: set[str] = set()


def _default_name(f: Callable[..., Any]) -> str:
    """Return the SDK's default name for a trigger or tool: `module:qualname`, `-m` aware."""
    module = f.__module__
    if module == "__main__":
        name = getattr(getattr(sys.modules[module], "__spec__", None), "name", None)
        if isinstance(name, str) and name and name.rpartition(".")[2] != "__main__":
            module = name
    return f"{module}:{f.__qualname__}"


@contextlib.contextmanager
def _logged_run(
    name: str, run_id: str, called_with: dict[str, str] | None
) -> Iterator[dict[str, Any]]:
    """Log `run.start`, then `run.end` with how the block ended and what it put in the dict."""
    if called_with is None:
        agentkit.obs("run.start", name=name, run=run_id)
    else:
        agentkit.obs("run.start", name=name, run=run_id, called_with=called_with)
    ended: dict[str, Any] = {}
    try:
        yield ended
    except BaseException as exc:
        agentkit.obs("run.end", name=name, run=run_id, outcome="error", error=type(exc).__name__)
        raise
    else:
        agentkit.obs("run.end", name=name, run=run_id, outcome="ok", **ended)


@contextlib.contextmanager
def _observed(name: str, called_with: dict[str, str] | None = None) -> Iterator[dict[str, Any]]:
    """Log the run a real trigger or `run` started, unless the SDK joined it to an outer one."""
    run_id = sdk.current_run_id()
    if run_id is None or run_id in _logged_runs:
        yield {}
        return
    _logged_runs.add(run_id)
    try:
        with _logged_run(name, run_id, called_with) as ended:
            yield ended
    finally:
        _logged_runs.discard(run_id)


def _called_with(f: Callable[..., Any], args: Any, kwargs: Any) -> dict[str, str]:
    """Return `{parameter: type name}` for a call of `f`, or empty if it does not bind.

    Annotations stay unevaluated, as the SDK leaves them."""
    try:
        if sys.version_info >= (3, 14):
            import annotationlib  # exists from 3.14 only

            signature = inspect.signature(f, annotation_format=annotationlib.Format.FORWARDREF)
        else:
            signature = inspect.signature(f)
        bound = signature.bind(*args, **kwargs)
    except Exception:
        return {}
    return {name: type(value).__qualname__ for name, value in bound.arguments.items()}


def _around(f: Callable[..., Any], enter: Callable[[dict[str, str]], Any]) -> Callable[..., Any]:
    """Wrap `f` in `enter(called_with)`, keeping its signature and its sync or async nature."""
    if inspect.iscoroutinefunction(f):

        @functools.wraps(f)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            with enter(_called_with(f, args, kwargs)) as ended:
                ended["returned"] = result = await f(*args, **kwargs)
                return result

        return async_wrapper

    @functools.wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with contextlib.ExitStack() as stack:
            ended = stack.enter_context(enter(_called_with(f, args, kwargs)))
            result = f(*args, **kwargs)
            if inspect.iscoroutine(result):
                # The SDK ends a sync trigger's run when the coroutine it returned ends.
                return _carried(stack.pop_all(), ended, result)
            ended["returned"] = result
            return result

    return wrapper


async def _carried(observed: contextlib.ExitStack, ended: dict[str, Any], coroutine: Any) -> Any:
    """Await `coroutine` inside the observation a sync trigger opened before returning it."""
    with observed:
        ended["returned"] = result = await coroutine
        return result


@overload
def trigger[F: Callable[..., Any]](fn: F, /) -> F: ...
@overload
def trigger(*, name: str | None = None) -> Callable[[F], F]: ...
def trigger(fn: Callable[..., Any] | None = None, *, name: str | None = None) -> Any:
    def decorate(f: Callable[..., Any]) -> Callable[..., Any]:
        if isinstance(f, staticmethod | classmethod):
            # The function inside is the trigger, and the descriptor stays one.
            return type(f)(decorate(f.__func__))
        if inspect.isgeneratorfunction(f) or inspect.isasyncgenfunction(f):
            inner = f  # for the SDK's own TypeError
        else:
            run_name = _default_name(f) if name is None else name
            inner = _around(f, lambda types: _observed(run_name, types))
        return sdk.trigger(inner) if name is None else sdk.trigger(name=name)(inner)

    return decorate(fn) if fn is not None else decorate


def _run_name(entered: Any, name: str) -> str:
    """Return the name the SDK's run goes by, cut as irimi stores it, or `name` with no run."""
    return str(entered.name) if entered is not None else name


class run:
    """`with observe.run(name, inputs=...)` and `async with` alike: the SDK's `run`, logged."""

    def __init__(self, name: str, *, inputs: Any = None) -> None:
        self._inner: Any = sdk.run(name, inputs=inputs)
        self._name = name
        self._cm: Any = None

    def __enter__(self) -> Any:
        entered = self._inner.__enter__()
        self._cm = _observed(_run_name(entered, self._name))
        self._cm.__enter__()
        return entered

    def __exit__(self, *exc: Any) -> Any:
        # The log's `run.end` first, while the run is still current.
        self._cm.__exit__(*exc)
        return self._inner.__exit__(*exc)

    async def __aenter__(self) -> Any:
        entered = await self._inner.__aenter__()
        self._cm = _observed(_run_name(entered, self._name))
        self._cm.__enter__()
        return entered

    async def __aexit__(self, *exc: Any) -> Any:
        self._cm.__exit__(*exc)
        return await self._inner.__aexit__(*exc)


# Within one call of a tool `_answered` wraps: a cell `_logged` sets once the function or its fake
# has run, so a call in which neither did logs `ran="recording"`.
_ran_a_body: contextvars.ContextVar[list[bool] | None] = contextvars.ContextVar(
    "ran_a_body", default=None
)


def _log_tool(tool_name: str, kind: str, ran: str) -> None:
    cell = _ran_a_body.get()
    if cell is not None:
        cell[0] = True
    agentkit.obs("tool", name=tool_name, kind=kind, ran=ran, run=sdk.current_run_id())


def _logged(fn: Callable[..., Any], tool_name: str, kind: str, ran: str) -> Callable[..., Any]:
    """Wrap `fn` to log that it ran; what the SDK refuses is returned as is, for its own check."""
    if not callable(fn) or inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
        return fn
    if _is_async(fn):

        @functools.wraps(fn)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            _log_tool(tool_name, kind, ran)
            return await fn(*args, **kwargs)

        return async_wrapper

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        _log_tool(tool_name, kind, ran)
        return fn(*args, **kwargs)

    return wrapper


def _answered(decorated: Callable[..., Any], tool_name: str, kind: str) -> Callable[..., Any]:
    """Wrap the SDK's tool wrapper to log `ran="recording"` for a call in which neither body ran."""

    def answered(cell: list[bool]) -> None:
        if not cell[0] and sdk.is_active():
            agentkit.obs(
                "tool", name=tool_name, kind=kind, ran="recording", run=sdk.current_run_id()
            )

    if inspect.iscoroutinefunction(decorated):

        @functools.wraps(decorated)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            cell = [False]
            token = _ran_a_body.set(cell)
            try:
                result = await decorated(*args, **kwargs)
            except Exception:
                answered(cell)
                raise
            finally:
                _ran_a_body.reset(token)
            answered(cell)
            return result

        return async_wrapper

    @functools.wraps(decorated)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        cell = [False]
        token = _ran_a_body.set(cell)
        try:
            result = decorated(*args, **kwargs)
        except Exception:
            answered(cell)
            raise
        finally:
            _ran_a_body.reset(token)
        answered(cell)
        return result

    return wrapper


def _is_async(f: Callable[..., Any]) -> bool:
    """Return whether calling `f` gives a coroutine, as the SDK tests a fake's nature."""
    if inspect.iscoroutinefunction(f):
        return True
    return not inspect.isroutine(f) and inspect.iscoroutinefunction(type(f).__call__)


@overload
def read_tool[F: Callable[..., Any]](fn: F, /) -> F: ...
@overload
def read_tool(*, name: str | None = None) -> Callable[[F], F]: ...
def read_tool(fn: Callable[..., Any] | None = None, /, *, name: str | None = None) -> Any:
    decorate = _tool("read", None, name, lambda _: sdk.read_tool(name=name))
    return decorate if fn is None else decorate(fn)


def write_tool(*, fake: Callable[..., Any], name: str | None = None) -> Callable[[F], F]:
    return _tool("write", fake, name, lambda logged: sdk.write_tool(fake=logged, name=name))


def _tool(
    kind: str, fake: Any, name: str | None, real: Callable[[Any], Callable[[Any], Any]]
) -> Callable[[Any], Any]:
    """Return `read_tool`'s or `write_tool`'s decorator; `real` builds the SDK's from the fake."""

    def decorate(fn: Any) -> Any:
        # A `staticmethod` or `classmethod` stays one, the wrapper going around the function inside.
        descriptor = type(fn) if isinstance(fn, staticmethod | classmethod) else None
        inner = fn.__func__ if descriptor is not None else fn
        # The agent's own `name` goes to the SDK as written, so the name irimi stores is the
        # SDK's default and the log's this module's, which `test_stored_runs.py` holds equal.
        tool_name = _default_name(inner) if name is None else name
        real_fn = _logged(inner, tool_name, kind, "real")
        logged_fake = _logged_fake(fake, inner, real_fn, tool_name, kind)
        decorated = real(logged_fake)(real_fn if descriptor is None else descriptor(real_fn))
        if descriptor is None:
            return _answered(decorated, tool_name, kind)
        return descriptor(_answered(decorated.__func__, tool_name, kind))

    return decorate


def _logged_fake(
    fake: Any, fn: Callable[..., Any], logged_fn: Callable[..., Any], tool_name: str, kind: str
) -> Any:
    """Wrap `fake` to log that it ran, in the shape the agent wrote it.

    A fake that is the function itself reuses `fn`'s wrapper, so the SDK still refuses it."""
    if fake is None:
        return None
    static = isinstance(fake, staticmethod)
    inner = fake.__func__ if static else fake
    logged = logged_fn if inner is fn else _logged(inner, tool_name, kind, "shadow")
    return staticmethod(logged) if static else logged
