"""The irimi SDK as the sample workflows use it, with a fallback for when it does not import.

Agents write `from examples.workflows import sdk` where a real agent would write
`from irimi import sdk`, and use the SDK's API: `@sdk.trigger`, `sdk.run()`,
`sdk.current_run_id()`, `sdk.propagate()`, `sdk.is_active()`, `@sdk.read_tool` and
`@sdk.write_tool(fake=...)`.

- **When `irimi.sdk` imports**, this module re-exports it, name by name: each name `irimi.sdk`
  has is the real one, and each it lacks is the stand-in's. The real SDK has `trigger`, `run`,
  `current_run_id`, `propagate`, `is_active`, `read_tool` and `write_tool`.
  Names this module does not define (`sdk.instrument`, `sdk.RecordedToolError`, ...) are
  forwarded to `irimi.sdk` by the module `__getattr__`.
  The real SDK's `instrument` labels a run's requests, so the corpus labels no request itself
  whenever the real SDK imports (`labels_requests`): a run's requests are labelled by the SDK
  that ships, or not at all.
  Four real names are still wrapped, for the observation log the corpus pins:
  - `read_tool` and `write_tool` wrap the real function and its fake so each logs which of them
    ran (`agentkit.obs("tool", ...)`), because that is the harness's proof that no write tool ran
    for real under shadow. The real decorators do their own decoration-time checks on what the
    agent wrote, and are given it as written: a `staticmethod` or `classmethod`, the function's or
    the fake's, stays one, the logging wrapper inside it; a fake that is the function itself
    stays the function's own wrapper; and the agent's own `name`, so the name irimi stores is the
    real default. A read tool a replay answers from its recording runs neither, so the
    decorated tool is wrapped once more, outside the real one, to log `ran="recording"` for a call
    that returned or raised and in which neither logged.
  - `trigger` and `run` log `run.start` and `run.end` from inside the real run, with the real
    run id, so the pins on them hold in both modes. A trigger's `run.end` carries what it
    `returned`, so a replay that calls the trigger alone, with no `main()` to log a result,
    is held to what its recording's trigger returned, and its `run.start` the type of each
    argument it was `called_with`, so the replay is held to the values `replay_child` revived.
- **When it does not import**, this module implements the same API with no control endpoint and
  no recording:
  - a context-variable run id that nested triggers join;
  - `agentkit.http`, and W3's clients, labelling each request with `Irimi-Run` (which irimi
    strips), as the real SDK does;
  - the SDK's call-time table, so an active write tool runs its stand-in and never the real
    function.

  It raises the SDK's decoration-time `TypeError`s too, so an agent that is wrong under the
  fallback is wrong in the same way under the real SDK.

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
import sys
from collections.abc import Callable, Iterator
from types import ModuleType
from typing import Any, TypeVar, overload

from examples.workflows import agentkit

F = TypeVar("F", bound=Callable[..., Any])


def _load_real() -> ModuleType | None:
    """`irimi.sdk`, or None when it is not installed. Only its absence falls back: an
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
    """`irimi.sdk.<name>`, or None when the real SDK does not have it."""
    return getattr(_real, name, None) if _real is not None else None


def __getattr__(name: str) -> Any:
    """Forward a public name this module does not define to `irimi.sdk`, so an agent can use
    the rest of the SDK's API (`sdk.instrument`) without an edit here."""
    if not name.startswith("_") and _real is not None and hasattr(_real, name):
        return getattr(_real, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


ENGINE_ACTIVE_ENV = "IRIMI_ENGINE_ACTIVE"
_run_id: contextvars.ContextVar[str | None] = contextvars.ContextVar("irimi_run", default=None)
# Under the real SDK: the real runs whose `run.start` this module has logged and whose `run.end`
# it has not, so a nested trigger, which joins one, logs nothing. A set of ids rather than a context
# variable, because a run handed to a coroutine is logged as ended from the task that awaits
# it, not the context that logged its start.
_logged_runs: set[str] = set()


def is_active() -> bool:
    real = _from_real("is_active")
    if real is not None:
        return bool(real())
    return os.environ.get(ENGINE_ACTIVE_ENV) == "1"


def _default_name(f: Callable[..., Any]) -> str:
    """The real SDK's default name for a trigger or a tool: `module:qualname`, the module's real
    name when the agent was started with `python -m`."""
    module = f.__module__
    if module == "__main__":
        name = getattr(getattr(sys.modules[module], "__spec__", None), "name", None)
        if isinstance(name, str) and name and name.rpartition(".")[2] != "__main__":
            module = name
    return f"{module}:{f.__qualname__}"


def labels_requests() -> bool:
    """True when the real `irimi.sdk` imports, False when only the stand-in is here. The real
    SDK's `instrument` puts `Irimi-Run` on a run's requests. So `agentkit.http` and W3's `call`
    label by hand only with no real SDK at all; with one, they must not, or the corpus would pass
    with or without the SDK's patch."""
    return _from_real("instrument") is not None


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
def _logged_run(
    name: str, run_id: str, called_with: dict[str, str] | None
) -> Iterator[dict[str, Any]]:
    """Log `run.start`, then `run.end` with how the block ended, and what the block put in the dict
    it is given as `returned`. The one place both modes log a run, so the corpus's pins on
    these events mean the same thing in each. A trigger's `run.start` says what it was
    `called_with`, each parameter's type, so a replay that calls the trigger alone is held to
    the values `replay_child` revived, a `bytes` as `bytes`; an `sdk.run` block has none."""
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
def _entered(name: str, called_with: dict[str, str] | None = None) -> Iterator[dict[str, Any]]:
    """A run while the fallback is in use. A nested entry joins the current run."""
    if not is_active() or _run_id.get() is not None:
        yield {}
        return
    run_id = secrets.token_hex(8)
    token = _run_id.set(run_id)
    try:
        with _logged_run(name, run_id, called_with) as ended:
            yield ended
    finally:
        _run_id.reset(token)


@contextlib.contextmanager
def _observed(name: str, called_with: dict[str, str] | None = None) -> Iterator[dict[str, Any]]:
    """Entered inside a real trigger or `run`: log the real run it started. The real SDK decides
    whether there is a run (inactive: none) and whether this entry joined an outer one (the same
    id this module already logged), so this only reads its id."""
    run_id = current_run_id()
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
    """`{parameter: type name}` for a call of `f`, as it binds; empty for a call that does not bind,
    which then raises its own TypeError. Its annotations are left unevaluated, as the real
    SDK leaves them (`capture._signature`): on Python 3.14 a name imported only for type checkers
    would raise NameError into the agent, where the real trigger runs it, and this logs nothing
    rather than raise."""
    try:
        if sys.version_info >= (3, 14):
            import annotationlib  # noqa: PLC0415  # exists from 3.14 only

            signature = inspect.signature(f, annotation_format=annotationlib.Format.FORWARDREF)
        else:
            signature = inspect.signature(f)
        bound = signature.bind(*args, **kwargs)
    except Exception:
        return {}
    return {name: type(value).__qualname__ for name, value in bound.arguments.items()}


def _around(f: Callable[..., Any], enter: Callable[[dict[str, str]], Any]) -> Callable[..., Any]:
    """`f` inside the context manager `enter(called_with)` makes, given the types `f` is called
    with, keeping `f`'s signature (which the SDK binds the captured args against) and its sync/async
    nature. What `f` returns is logged with the run's end."""
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
            if _real is not None and inspect.iscoroutine(result):
                # The real trigger hands its run to the coroutine and ends it when the coroutine
                # ends, so the logged run goes with it: `run.end` is the coroutine's outcome.
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
    real = _from_real("trigger")

    def decorate(f: Callable[..., Any]) -> Callable[..., Any]:
        if isinstance(f, staticmethod | classmethod):
            # The function inside is the trigger and the descriptor stays one, as the real
            # trigger keeps it: this module's wrapper cannot call a descriptor.
            return type(f)(decorate(f.__func__))
        is_generator = inspect.isgeneratorfunction(f) or inspect.isasyncgenfunction(f)
        run_name = _default_name(f) if name is None else name
        if real is not None:
            # A generator goes to the real decorator as it is, for its own TypeError.
            inner = f if is_generator else _around(f, lambda types: _observed(run_name, types))
            return real(inner) if name is None else real(name=name)(inner)  # type: ignore[no-any-return]
        if is_generator:
            raise TypeError(f"@sdk.trigger cannot wrap a generator: {f.__qualname__}")
        return _around(f, lambda types: _entered(run_name, types))

    return decorate(fn) if fn is not None else decorate


class run:
    """`with sdk.run(name, inputs=...)` and `async with` alike. The block gets what the real
    SDK's gives it, a `Run` or None, and None under the fallback."""

    def __init__(self, name: str, *, inputs: Any = None) -> None:
        real = _from_real("run")
        self._inner: Any = real(name, inputs=inputs) if real is not None else None
        self._name = name
        self._cm: Any = None

    def __enter__(self) -> Any:
        entered = None
        if self._inner is not None:
            entered = self._inner.__enter__()
            self._cm = _observed(self._name)
        else:
            self._cm = _entered(self._name)
        self._cm.__enter__()
        return entered

    def __exit__(self, *exc: Any) -> Any:
        # The log's `run.end` first, while the real run is still current.
        suppressed = self._cm.__exit__(*exc)
        if self._inner is not None:
            return self._inner.__exit__(*exc)
        return suppressed

    async def __aenter__(self) -> Any:
        if self._inner is not None:
            entered = await self._inner.__aenter__()
            self._cm = _observed(self._name)
            self._cm.__enter__()
            return entered
        return self.__enter__()

    async def __aexit__(self, *exc: Any) -> Any:
        if self._inner is not None:
            self._cm.__exit__(*exc)
            return await self._inner.__aexit__(*exc)
        return self.__exit__(*exc)


# Within one call of a tool `_answered` wraps: a cell `_logged` sets once the function or the
# stand-in has run, so a call in which neither did logs `ran="recording"`.
_ran_a_body: contextvars.ContextVar[list[bool] | None] = contextvars.ContextVar(
    "ran_a_body", default=None
)


def _log_tool(tool_name: str, kind: str, ran: str) -> None:
    cell = _ran_a_body.get()
    if cell is not None:
        cell[0] = True
    agentkit.obs("tool", name=tool_name, kind=kind, ran=ran, run=current_run_id())


def _logged(fn: Callable[..., Any], tool_name: str, kind: str, ran: str) -> Callable[..., Any]:
    """`fn`, logging that it ran. Keeps `fn`'s signature and its sync/async nature, which the
    real `tool` checks and binds against. What it refuses (a stand-in that is not callable, a
    generator) is returned as it is, so the real `tool`'s check sees what the agent wrote."""
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
    """`decorated`, the real `tool`'s wrapper, logging `ran="recording"` for a call in which
    neither the function nor its stand-in ran: a read tool a replay answered from its recording,
    returning or raising what it was recorded doing. Keeps the wrapper's sync/async nature.

    A recorded call raises again as `sdk.RecordedToolError`, an `Exception`. A call interrupted
    by anything else (a task cancelled while it asked the recording, Ctrl-C) ran nothing and was
    answered by nothing, and irimi stores no call of it, so it logs nothing either."""

    def answered(cell: list[bool]) -> None:
        if not cell[0] and is_active():
            agentkit.obs("tool", name=tool_name, kind=kind, ran="recording", run=current_run_id())

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
    """`f` is called to get a coroutine: an `async def`, or an object whose `__call__` is one. The
    real `tool`'s test of a stand-in's nature, so the logging wrapper keeps the nature the
    real `tool` would have seen."""
    if inspect.iscoroutinefunction(f):
        return True
    return not inspect.isroutine(f) and inspect.iscoroutinefunction(type(f).__call__)


@overload
def read_tool[F: Callable[..., Any]](fn: F, /) -> F: ...
@overload
def read_tool(*, name: str | None = None) -> Callable[[F], F]: ...
def read_tool(fn: Callable[..., Any] | None = None, /, *, name: str | None = None) -> Any:
    real = _from_real("read_tool")
    decorate = _tool("read", None, name, None if real is None else real(name=name))
    return decorate if fn is None else decorate(fn)


def write_tool(*, fake: Callable[..., Any], name: str | None = None) -> Callable[[F], F]:
    real = _from_real("write_tool")
    return _tool(
        "write",
        fake,
        name,
        None if real is None else lambda stand_in: real(fake=stand_in, name=name),
    )


def _tool(kind: str, fake: Any, name: str | None, real: Any) -> Callable[[Any], Any]:
    """The decorator `read_tool` and `write_tool` return. `real` is the real SDK's decorator for a
    read, or for a write the function that makes it from a fake; None under the fallback."""

    def decorate(fn: Any) -> Any:
        # A `staticmethod` or `classmethod` stays one, as the real decorators keep it: this
        # module's logging wrapper goes around the function inside, never around the descriptor,
        # which it could not call with the receiver the descriptor supplies.
        descriptor = type(fn) if isinstance(fn, staticmethod | classmethod) else None
        if descriptor is not None and real is None:
            return descriptor(decorate(fn.__func__))
        inner = fn.__func__ if descriptor is not None else fn
        # The real SDK's default, and its test: `name=""` is a name.
        tool_name = _default_name(inner) if name is None else name
        if real is None:
            _check_tool(fn, kind, fake)
        real_fn = _logged(inner, tool_name, kind, "real")
        stand_in = _logged_stand_in(fake, inner, real_fn, tool_name, kind)
        if real is not None:
            # The real decorator's own decoration-time checks decide; this one does not
            # pre-empt them, so W6's `decoration_errors` tests the SDK that ships. It is given
            # what the agent wrote: the descriptor, and the agent's own `name`, None included, so
            # the name irimi stores is the real SDK's default and the log's is this module's, and
            # the corpus holds the two equal (`test_stored_runs.py`).
            wrapped = real_fn if descriptor is None else descriptor(real_fn)
            decorated = (real if kind == "read" else real(stand_in))(wrapped)
            if descriptor is None:
                return _answered(decorated, tool_name, kind)
            return descriptor(_answered(decorated.__func__, tool_name, kind))
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                if kind == "write" and is_active():
                    assert stand_in is not None
                    return await stand_in(*args, **kwargs)
                return await real_fn(*args, **kwargs)

            return async_wrapper

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if kind == "write" and is_active():
                assert stand_in is not None
                return stand_in(*args, **kwargs)
            return real_fn(*args, **kwargs)

        return wrapper

    return decorate


def _logged_stand_in(
    fake: Any, fn: Callable[..., Any], logged_fn: Callable[..., Any], tool_name: str, kind: str
) -> Any:
    """`fake`, logging that it ran, in the shape the agent wrote it, so the real decorator's checks
    judge what they would without this module: a `staticmethod` stays one, the logging
    wrapper inside it, and a fake that is the function itself is the very wrapper `fn` got,
    which the real `write_tool` refuses as itself. Two wrappers of one function would let the real
    write run under irimi, logged as its fake."""
    if fake is None:
        return None
    static = isinstance(fake, staticmethod)
    inner = fake.__func__ if static else fake
    logged = logged_fn if inner is fn else _logged(inner, tool_name, kind, "shadow")
    return staticmethod(logged) if static else logged


def _check_tool(fn: Callable[..., Any], kind: str, fake: Callable[..., Any] | None) -> None:
    """The SDK's decoration-time rules: nothing unnamed runs live, and the error comes at load."""
    if isinstance(fake, staticmethod):
        fake = fake.__func__  # judged as the function inside, as the real `write_tool` judges it
    if kind == "write":
        if not callable(fake):
            raise TypeError(f"the fake for {fn.__qualname__} is not callable")
        if fake is fn:
            raise TypeError(f"the fake for {fn.__qualname__} is the function itself")
        if inspect.isgeneratorfunction(fake) or inspect.isasyncgenfunction(fake):
            raise TypeError(f"the fake for {fn.__qualname__} is a generator function")
        if _is_async(fake) != inspect.iscoroutinefunction(fn):
            raise TypeError(f"the fake for {fn.__qualname__} must be sync or async as it is")
    if inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
        raise TypeError(f"the tool decorators cannot wrap a generator: {fn.__qualname__}")
