"""The SDK's run and tool decorators, logging what ran to the observation log the corpus pins.

An agent takes `trigger`, `run`, `read_tool` and `write_tool` from here and the rest of the SDK
from `irimi.sdk`. Each wrapper hands the real decorator what the agent wrote, so the real SDK's
decoration-time checks and default names decide, and logs (`agentkit.obs`):

- `run.start` and `run.end` from inside the real run, with its id: a trigger's start carries the
  type of each argument it was `called_with`, and its end what it `returned`, so a replay that
  calls the trigger alone is held to what the recording's trigger was given and returned;
- `tool` for each tool call, with which body ran: `real`, `shadow` (the fake) or `recording` (a
  read a replay answered, in which neither body ran). It is the corpus's proof that no write
  tool ran for real under shadow.
"""

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

# The runs whose `run.start` this module has logged and whose `run.end` it has not, so a nested
# trigger, which joins one, logs nothing. A set of ids rather than a context variable, because a
# run handed to a coroutine ends in the task that awaits it, not the context that started it.
_logged_runs: set[str] = set()


def _default_name(f: Callable[..., Any]) -> str:
    """The SDK's default name for a trigger or a tool: `module:qualname`, the module's real name
    when the agent was started with `python -m`."""
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
    """Log `run.start`, then `run.end` with how the block ended and what it put in the dict it is
    given as `returned`."""
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
    """Entered inside a real trigger or `run`: log the run it started. The SDK decides whether
    there is a run and whether this entry joined an outer one, so this only reads its id."""
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
    """`{parameter: type name}` for a call of `f`, or empty for a call that does not bind. The
    annotations stay unevaluated, as the SDK leaves them, so a name imported only for type
    checkers raises nothing here."""
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
    """`f` inside the context manager `enter(called_with)` makes, keeping `f`'s signature, which
    the SDK binds captured args against, and its sync or async nature."""
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
    """`coroutine`, awaited inside the observation a sync trigger opened before it returned it."""
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


class run:
    """`with observe.run(name, inputs=...)` and `async with` alike: the SDK's `run`, logged."""

    def __init__(self, name: str, *, inputs: Any = None) -> None:
        self._inner: Any = sdk.run(name, inputs=inputs)
        self._name = name
        self._cm: Any = None

    def __enter__(self) -> Any:
        entered = self._inner.__enter__()
        self._cm = _observed(self._name)
        self._cm.__enter__()
        return entered

    def __exit__(self, *exc: Any) -> Any:
        # The log's `run.end` first, while the run is still current.
        self._cm.__exit__(*exc)
        return self._inner.__exit__(*exc)

    async def __aenter__(self) -> Any:
        entered = await self._inner.__aenter__()
        self._cm = _observed(self._name)
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
    """`fn`, logging that it ran, with its signature and sync or async nature. What the SDK
    refuses (a fake that is not callable, a generator) is returned as it is, for the SDK's check."""
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
    """`decorated`, the SDK's tool wrapper, logging `ran="recording"` for a call that returned or
    raised an `Exception` while neither body ran. A call interrupted by anything else ran nothing,
    is stored by no one, and logs nothing."""

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
    """`f` is called to get a coroutine: an `async def`, or an object whose `__call__` is one, as
    the SDK tests a fake's nature."""
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
    """The decorator `read_tool` and `write_tool` return. `real` makes the SDK's decorator from
    the logged fake (None for a read)."""

    def decorate(fn: Any) -> Any:
        # A `staticmethod` or `classmethod` stays one: the logging wrapper goes around the function
        # inside, which the descriptor then supplies its receiver to.
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
    """`fake`, logging that it ran, in the shape the agent wrote it: a `staticmethod` stays one,
    and a fake that is the function itself is `fn`'s own wrapper, which the SDK refuses as itself.
    Two wrappers of one function would let the real write run under irimi, logged as its fake."""
    if fake is None:
        return None
    static = isinstance(fake, staticmethod)
    inner = fake.__func__ if static else fake
    logged = logged_fn if inner is fn else _logged(inner, tool_name, kind, "shadow")
    return staticmethod(logged) if static else logged
