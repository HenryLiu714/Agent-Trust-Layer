"""Label tools the proxy cannot see (database or file writes, library calls) as reads or writes."""

import functools
import inspect
import os
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, overload

from irimi.core.limits import MAX_CAPTURED_BYTES
from irimi.protocol import trace
from irimi.protocol.env import ENGINE_ACTIVE_ENV, PROXY_ENVS, RUN_ENV
from irimi.protocol.trace import STOPPED_ERROR_TYPES, TRUNCATED_KEY
from irimi.sdk import _context as context
from irimi.sdk._capture import capture, capture_call, revive, serialized
from irimi.sdk._errors import error_info, recorded_error
from irimi.sdk._runs import check_name, label, off_the_loop
from irimi.sdk._state import state

PROXY_HINTS = tuple(name for name in PROXY_ENVS if name.upper() == "HTTPS_PROXY")


@overload
def read_tool[F: Callable[..., Any]](fn: F, /) -> F: ...
@overload
def read_tool[F: Callable[..., Any]](*, name: str | None = None) -> Callable[[F], F]: ...
def read_tool(fn: Callable[..., Any] | None = None, /, *, name: str | None = None) -> Any:
    """Label a function as a read the proxy cannot see, such as a database query.

    It runs for real; under irimi each call and its result are recorded in the current run, and
    a replay answers the call from the recording when it can.

    Args:
        fn: The function, when used bare as `@sdk.read_tool`.
        name: The name calls are recorded under. Defaults to the function's `module:qualname`.

    Returns:
        The function wrapped, with its signature and its sync or async nature kept.

    Raises:
        TypeError: At decoration, for a generator function, a callable with no importable
            `module:qualname`, or a `name` that is not a string or is passed positionally.

    Example:
        @sdk.read_tool
        def load_customer(customer_id: str) -> Customer: ...
    """
    if fn is not None and not callable(fn) and not isinstance(fn, staticmethod | classmethod):
        raise TypeError(f"@sdk.read_tool takes its name as name=, not {fn!r}")
    decorate = _decorator("read", None, name, "@sdk.read_tool")
    return decorate if fn is None else decorate(fn)


def write_tool[F: Callable[..., Any]](
    *, fake: Callable[..., Any], name: str | None = None
) -> Callable[[F], F]:
    """Label a function as a write the proxy cannot see, and name the fake that replaces it.

    Under irimi only `fake` runs, with the same arguments (the instance first for a method), and
    each call is recorded in the current run. Without irimi the real function runs.

    Args:
        fake: The function that runs instead under irimi; sync or async as the real one is.
        name: The name calls are recorded under. Defaults to the function's `module:qualname`.

    Returns:
        A decorator that wraps the function, keeping its signature and its sync or async nature.

    Raises:
        TypeError: At decoration, for a fake that is not callable, is a generator function, is
            the decorated function itself or differs from it in sync or async nature; for a
            decorated generator function or a callable with no importable `module:qualname`;
            or for a `name` that is not a string.

    Example:
        @sdk.write_tool(fake=lambda row: None)
        def insert_row(row: Row) -> None: ...
    """
    if isinstance(fake, staticmethod):
        # `inspect` cannot see sync/async or generator nature through a staticmethod.
        fake = fake.__func__
    if not callable(fake):
        raise TypeError(f"@sdk.write_tool(fake=...) must be callable, not {fake!r}")
    if inspect.isgeneratorfunction(fake) or inspect.isasyncgenfunction(fake):
        raise TypeError(
            f"@sdk.write_tool(fake=...) cannot be the generator function {fake.__qualname__}"
        )
    return _decorator("write", fake, name, "@sdk.write_tool")


def _decorator(
    kind: trace.ToolKind, fake: Callable[..., Any] | None, name: str | None, api: str
) -> Callable[[Any], Any]:
    """Return a decorator that checks a tool function and wraps it."""
    check_name(name, api)

    def decorate(fn: Callable[..., Any]) -> Any:
        if isinstance(fn, staticmethod | classmethod):
            return type(fn)(decorate(fn.__func__))
        tool_name = label(fn, name, api)
        if inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
            raise TypeError(
                f"{api} cannot wrap the generator function {fn.__qualname__}: a call is "
                "recorded when it returns, before a generator's body has run"
            )
        if fake is fn:
            # Reading `fake=` as "the function to fake" would run the write for real.
            raise TypeError(
                f"{api}: the fake for {fn.__qualname__} is the function itself: fake= names "
                "the function that runs in its place under irimi"
            )
        is_async = inspect.iscoroutinefunction(fn)
        if fake is not None and _is_async(fake) != is_async:
            nature = "an async def" if is_async else "a sync function"
            raise TypeError(
                f"{api}: the fake for {fn.__qualname__} must be {nature}, as the function it "
                "replaces is"
            )
        spec = _Tool(tool_name, kind, fn, fake)

        if is_async:

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                return await _call_async(spec, args, kwargs)

            return async_wrapper

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return _call(spec, args, kwargs)

        return wrapper

    return decorate


@dataclass(frozen=True)
class _Tool:
    """Describe one decorated tool: its name, kind, real function and fake."""

    name: str
    kind: trace.ToolKind
    fn: Callable[..., Any]
    fake: Callable[..., Any] | None


def _chosen(spec: _Tool, recorded: "_Recorded | None") -> tuple[Callable[..., Any], trace.Ran]:
    """Return the body an active call runs and its `ran`, given `_replay_answer`'s `recorded`.

    | tool  | recorded                          | runs                            | ran       |
    | write | (never asked, even in a replay)   | only the fake                   | shadow    |
    | read  | holds the call, revivable         | nothing: the revived result, or | recording |
    |       |                                   | RecordedToolError if it raised  |           |
    | read  | None (miss, unrevivable, stopped) | the real function               | real      |
    """
    if spec.kind == "write":
        assert spec.fake is not None  # `write_tool` requires one
        return spec.fake, "shadow"
    if recorded is not None:
        return recorded.body(inspect.iscoroutinefunction(spec.fn)), "recording"
    return spec.fn, "real"


def _call(spec: _Tool, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    if not context.is_active():
        _warn_if_real_write(spec)
        return spec.fn(*args, **kwargs)
    # Bind against the real function, which is what the agent called, whatever runs.
    captured, _ = capture_call(spec.fn, args, kwargs)
    recorded = _replay_answer(spec, captured) if _asks_the_recording(spec) else None
    body, ran = _chosen(spec, recorded)
    call = _Call(spec, ran, captured)
    try:
        result = body(*args, **kwargs)
    except BaseException as exc:
        call.send(call.doc(None, exc))
        raise
    if inspect.iscoroutine(result):
        # The body has not run yet; the call is recorded when the coroutine ends, if ever.
        call.awaited = True
        return _finished_with(call, result)
    call.send(call.doc(result, None))
    return result


async def _call_async(spec: _Tool, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    if not context.is_active():
        _warn_if_real_write(spec)
        return await spec.fn(*args, **kwargs)
    captured, _ = capture_call(spec.fn, args, kwargs)
    recorded = None
    if _asks_the_recording(spec):
        recorded = await off_the_loop(_replay_answer, spec, captured)
    body, ran = _chosen(spec, recorded)
    call = _Call(spec, ran, captured)
    try:
        coroutine = body(*args, **kwargs)
    except BaseException as exc:  # raised before awaiting, e.g. wrong arguments
        await off_the_loop(call.send, call.doc(None, exc))
        raise
    return await _finished_with(call, coroutine)


async def _finished_with[T](call: "_Call", coroutine: Coroutine[Any, Any, T]) -> T:
    """Await `coroutine` and record its outcome; capture is inline, only the post is off-loop."""
    try:
        result = await coroutine
    except GeneratorExit as exc:
        # A closed coroutine may not await, so the post is made inline.
        call.send(call.doc(None, exc))
        raise
    except BaseException as exc:
        await off_the_loop(call.send, call.doc(None, exc))
        raise
    await off_the_loop(call.send, call.doc(result, None))
    return result


def _asks_the_recording(spec: _Tool) -> bool:
    """Return whether this call asks the recording first: only a read tool in a replay does."""
    return spec.kind == "read" and context.replay_run_id() is not None


@dataclass(frozen=True)
class _Recorded:
    """Hold a read tool call's recorded outcome: the revived result or the error, and `awaited`."""

    result: Any
    error: trace.ErrorInfo | None
    awaited: bool

    def body(self, is_async: bool) -> Callable[..., Any]:
        """Return a body giving the recorded outcome; async if the tool or recorded call was."""

        def answer(*args: Any, **kwargs: Any) -> Any:
            if self.error is not None:
                raise recorded_error(self.error)
            return self.result

        async def answer_async(*args: Any, **kwargs: Any) -> Any:
            return answer()

        return answer_async if is_async or self.awaited else answer


def _replay_answer(spec: _Tool, args: trace.JSONValue) -> _Recorded | None:
    """Return this call's recorded outcome, or None (miss, unrevivable, stopped) to run it."""
    try:
        outcome = state.reporter.recorded(_run_id(), spec.name, args)
        if outcome is None:
            return None
        if outcome.error is not None and outcome.error.type in STOPPED_ERROR_TYPES:
            # The engine refuses these too; checking here means one is never raised again.
            return None
        result = None if outcome.error is not None else revive(outcome.result)
        return _Recorded(result, outcome.error, outcome.awaited)
    except Exception:  # NotReplayable: the tool runs for real
        return None


class _Call:
    """Record one tool call; args are captured before the body runs, in case it mutates them."""

    def __init__(self, spec: _Tool, ran: trace.Ran, args: trace.JSONValue) -> None:
        self.spec = spec
        self.ran = ran
        self.args = args
        self.awaited = False
        self.run_id = _run_id()
        self.started_at = time.time()

    def doc(self, result: object, exc: BaseException | None) -> dict[str, Any] | None:
        """Return the call with its outcome as posted, or None (warned once) if building fails."""
        try:
            captured: trace.JSONValue = None
            if exc is None:
                captured = _bounded(self.args, capture(result)[0])
            call = trace.ToolCall(
                tool_call_id=trace.new_tool_call_id(),
                run_id=self.run_id,
                name=self.spec.name,
                kind=self.spec.kind,
                ran=self.ran,
                args=self.args,
                result=captured,
                error=None if exc is None else error_info(exc),
                started_at=self.started_at,
                ended_at=time.time(),
                awaited=self.awaited,
            )
            # The route names the run; the endpoint refuses a body that names one.
            doc = trace.tool_call_to_json(call)
            del doc["run_id"]
            return doc
        except Exception:
            state.warn_once(
                "record-tool-call",
                "irimi: could not record a call of tool %s; the call goes on, and later "
                "failures of this kind are not logged",
                self.spec.name,
                exc_info=True,
            )
            return None

    def send(self, doc: dict[str, Any] | None) -> None:
        """Post `doc` to this call's run. Never raises (the SDK never raises)."""
        if doc is not None:
            state.reporter.post(self.run_id, "tool-calls", doc)


def _is_async(f: Callable[..., Any]) -> bool:
    """Return whether `f` is an `async def` or an object whose `__call__` is one."""
    if inspect.iscoroutinefunction(f):
        return True
    return not inspect.isroutine(f) and inspect.iscoroutinefunction(type(f).__call__)


def _run_id() -> str:
    """Return the current run, else the process run (`IRIMI_RUN`), else `trace.UNATTRIBUTED`."""
    return context.current_run_id() or os.environ.get(RUN_ENV) or trace.UNATTRIBUTED


def _bounded(args: trace.JSONValue, result: trace.JSONValue) -> trace.JSONValue:
    """Return `result`, or the truncation marker if args and result together pass the limit."""
    size = len(serialized(result))
    if len(serialized(args)) + size <= MAX_CAPTURED_BYTES:
        return result
    return {TRUNCATED_KEY: True, "bytes": size}


def _warn_if_real_write(spec: _Tool) -> None:
    """Warn once if a write tool runs for real while a proxy is set but the SDK is inactive."""
    if spec.kind != "write" or not any(os.environ.get(var) for var in PROXY_HINTS):
        return
    state.warn_once(
        "real-write",
        "irimi: write tool %s ran for real: HTTPS_PROXY is set but %s is not",
        spec.name,
        ENGINE_ACTIVE_ENV,
    )
