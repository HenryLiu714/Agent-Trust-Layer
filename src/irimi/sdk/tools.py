"""`@sdk.read_tool` and `@sdk.write_tool`: tool calls the proxy cannot see, labelled.

The proxy sees only HTTP. A database write, a file write or a call into another library is
invisible to it, so the agent labels each such tool, and the SDK does for it what shadow mode does
for a write on the wire:

    | `is_active()` | tool  | Runs                              | Records    |
    | false         | read  | the real function                 | nothing    |
    | false         | write | the real function (WARNING, once) | nothing    |
    | true          | read  | the real function, `ran="real"`   | a ToolCall |
    | true          | write | ONLY the fake, `ran="shadow"`     | a ToolCall |

In a replay (`IRIMI_REPLAY_RUN` set), a read tool first asks the recording, over the control
endpoint, for the result the recorded run's same call got:

    | tool  | the recording                | Runs                                 | Records     |
    | read  | holds it, revivable          | nothing: the revived result, or      | `recording` |
    |       |                              | RecordedToolError if it raised       |             |
    | read  | misses, cannot revive it, or | the real function                    | `real`      |
    |       | was stopped from outside     |                                      |             |
    | write | is not asked                 | ONLY the fake, as in shadow          | `shadow`    |

A read tool's `ran="real"` in a replay is that replay's miss. A result is revivable when
`capture.revive` can rebuild it: one recorded as `__irimi_repr__` or `__irimi_truncated__` is a
description of the value, not the value. A result redaction replaced comes back with its
placeholder. A write tool's fake runs even when the recorded run made the same call: it is the
developer's substitute, and nothing recorded can stand for it.

THE SCOPE RULE, APPLIED TO THE SDK: nothing unnamed runs live. `write_tool` cannot be declared
without its fake, `read_tool` cannot be given one, and every other mistake is a TypeError when the
agent loads, never at the call.

A recorded call is posted to `{IRIMI_CONTROL}/runs/{run_id}/tool-calls` through `runs.reporter`
once it has returned or raised, so a post fails as a run's does: it never raises, and each kind of
failure is warned once per process. The call's run is the current one, or the process run
`irimi shadow` names in `IRIMI_RUN`, or none at all (`trace.UNATTRIBUTED`). An exception from the
function or the fake propagates unchanged, the same object, after the post.

HTTP made inside a tool is recorded as ordinary exchanges of the run, with no link to the call.

Which body runs is decided in one place, `_chosen`.
"""

import functools
import inspect
import logging
import os
import threading
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, overload

from irimi import trace
from irimi.limits import MAX_CAPTURED_BYTES
from irimi.protocol.env import ENGINE_ACTIVE_ENV, PROXY_ENVS, RUN_ENV
from irimi.sdk import context, runs
from irimi.sdk.api import check_name, label
from irimi.sdk.capture import capture, capture_call, revive, serialized
from irimi.sdk.client import LOGGER_NAME
from irimi.sdk.errors import error_info, recorded_error
from irimi.trace import STOPPED_ERROR_TYPES, TRUNCATED_KEY

logger = logging.getLogger(LOGGER_NAME)

# The variables whose presence says this process meant to go through a proxy: an inactive SDK
# with either set ran a write tool for real where irimi was probably meant to be in front.
# Both spellings of `HTTPS_PROXY`, from the one list of them.
PROXY_HINTS = tuple(name for name in PROXY_ENVS if name.upper() == "HTTPS_PROXY")

# Set once the "ran for real" WARNING has been logged: once per process, not per call or per tool.
_warned_real_write = False
_warn_lock = threading.Lock()
# Set once building a call's record has raised and been logged.
_record_failed = False


@overload
def read_tool[F: Callable[..., Any]](fn: F, /) -> F: ...
@overload
def read_tool[F: Callable[..., Any]](*, name: str | None = None) -> Callable[[F], F]: ...
def read_tool(fn: Callable[..., Any] | None = None, /, *, name: str | None = None) -> Any:
    """Label the decorated function as a read the proxy cannot see: it always runs for real, and
    under irimi each call is recorded in its run (see the module doc).

    Usable bare (`@sdk.read_tool`) or called (`@sdk.read_tool(name="lookup")`); `name` defaults
    to the function's `module:qualname` (`api.label`). It keeps the function's signature and its
    nature - the wrapper is a coroutine function exactly when the function is - and keeps a
    `staticmethod` or `classmethod` one, in either decorator order, as `@sdk.trigger` does.

    Refused at decoration time, with TypeError: a generator or async-generator function; what has
    no importable `module:qualname` (`api.entrypoint`); a `name` that is not a string; and a name
    passed positionally."""
    if fn is not None and not callable(fn) and not isinstance(fn, staticmethod | classmethod):
        raise TypeError(f"@sdk.read_tool takes its name as name=, not {fn!r}")
    decorate = _decorator("read", None, name, "@sdk.read_tool")
    return decorate if fn is None else decorate(fn)


def write_tool[F: Callable[..., Any]](
    *, fake: Callable[..., Any], name: str | None = None
) -> Callable[[F], F]:
    """Label the decorated function as a write the proxy cannot see: under irimi only `fake`
    runs in its place, called with the same arguments, and each call is recorded in its run (see
    the module doc).

    `name` defaults to the function's `module:qualname` (`api.label`). The decorator keeps the
    function's signature, nature and descriptor as `read_tool` does, so a fake is never handed an
    instance it does not expect. The fake of a plain method is called as the method is, with the
    instance first; a `staticmethod` fake is its function.

    Refused at decoration time, with TypeError: a fake that is not callable, is a generator
    function, is the function itself, or is sync where the function is `async def` or the
    reverse; a generator or async-generator function; what has no importable `module:qualname`;
    and a `name` that is not a string."""
    if isinstance(fake, staticmethod):
        # A fake written as a static method in its tool's class body is callable, but `inspect`
        # sees neither its nature nor a generator through it: it is judged, and called, as the
        # function inside it.
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
    """The decorator both tool decorators return: checks the function and wraps it."""
    check_name(name, api)

    def decorate(fn: Callable[..., Any]) -> Any:
        if isinstance(fn, staticmethod | classmethod):
            # The function inside is the tool, and the descriptor stays, as for a trigger.
            return type(fn)(decorate(fn.__func__))
        tool_name = label(fn, name, api)
        if inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
            raise TypeError(
                f"{api} cannot wrap the generator function {fn.__qualname__}: a call is "
                "recorded when it returns, before a generator's body has run"
            )
        if fake is fn:
            # Read as "the function to fake", `fake=` would run the write for real.
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
    """One decorated tool: what it is called, what it does, and the two bodies it may run."""

    name: str
    kind: trace.ToolKind
    fn: Callable[..., Any]
    fake: Callable[..., Any] | None


def _chosen(spec: _Tool, recorded: "_Recorded | None") -> tuple[Callable[..., Any], trace.Ran]:
    """Which body an active call runs, and what the record says ran, given what a replay's
    recording answered (`_replay_answer`, None outside a replay). THE ONE DISPATCH: a write runs
    its fake, in a replay too; a read the recording answered runs the recorded outcome; any
    other read runs the real function. An inactive SDK never gets here: the real function runs,
    recorded by nobody."""
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
    # Bound against the REAL function, which is what the agent called, whatever runs.
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
        # A sync function that returned a coroutine has not run its body yet: the call ends when
        # the coroutine does, as a sync trigger's run does (`RunScope.hand_off`). One that
        # is never awaited is never recorded. The record says so, for a replay.
        call.awaited = True
        return _finished_with(call, result)
    # Inline, as a sync trigger's posts are, even on an event loop's thread.
    call.send(call.doc(result, None))
    return result


async def _call_async(spec: _Tool, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    if not context.is_active():
        _warn_if_real_write(spec)
        return await spec.fn(*args, **kwargs)
    captured, _ = capture_call(spec.fn, args, kwargs)
    recorded = None
    if _asks_the_recording(spec):
        # Only a read in a replay waits on the endpoint, and off the event loop, as a post does:
        # any other call goes straight to its body.
        recorded = await runs.off_the_loop(_replay_answer, spec, captured)
    body, ran = _chosen(spec, recorded)
    call = _Call(spec, ran, captured)
    try:
        coroutine = body(*args, **kwargs)
    except BaseException as exc:  # raised before it awaited anything: called with wrong args
        await runs.off_the_loop(call.send, call.doc(None, exc))
        raise
    return await _finished_with(call, coroutine)


async def _finished_with[T](call: "_Call", coroutine: Coroutine[Any, Any, T]) -> T:
    """`coroutine`'s outcome, recorded as `call`'s once it is known. The result is captured here,
    on the caller's thread, as the coroutine left it; only the post waits off the event loop, so
    no other task runs between the return and the capture."""
    try:
        result = await coroutine
    except GeneratorExit as exc:
        # Closed, not awaited: nothing may be awaited now, so the post is made inline.
        call.send(call.doc(None, exc))
        raise
    except BaseException as exc:
        await runs.off_the_loop(call.send, call.doc(None, exc))
        raise
    await runs.off_the_loop(call.send, call.doc(result, None))
    return result


def _asks_the_recording(spec: _Tool) -> bool:
    """A read tool in a replay asks the recording before it runs; nothing else does."""
    return spec.kind == "read" and context.replay_run_id() is not None


@dataclass(frozen=True)
class _Recorded:
    """A read tool call's recorded outcome, as a replay hands it back: the revived result,
    or the error the recorded call raised, and whether a coroutine the call returned gave it
    (`trace.ToolCall.awaited`)."""

    result: Any
    error: trace.ErrorInfo | None
    awaited: bool

    def body(self, is_async: bool) -> Callable[..., Any]:
        """A body that gives the recorded outcome in place of the tool's, whatever arguments it is
        given: from a coroutine for an `async def` tool, and for a sync one whose recorded call
        returned a coroutine, so the agent's `await` gets what it got the first time."""

        def answer(*args: Any, **kwargs: Any) -> Any:
            if self.error is not None:
                raise recorded_error(self.error)
            return self.result

        async def answer_async(*args: Any, **kwargs: Any) -> Any:
            return answer()

        return answer_async if is_async or self.awaited else answer


def _replay_answer(spec: _Tool, args: trace.JSONValue) -> _Recorded | None:
    """The recorded outcome of this read tool called with `args`, as captured, asked of the
    control endpoint, or None, and the tool runs for real: a miss, a lookup that failed, a result
    `revive` cannot rebuild, or a recorded call something outside it stopped
    (`trace.STOPPED_ERROR_TYPES`), which has no outcome to give. Never raises."""
    try:
        outcome = runs.reporter.recorded(_run_id(), spec.name, args)
        if outcome is None:
            return None
        if outcome.error is not None and outcome.error.type in STOPPED_ERROR_TYPES:
            # The engine refuses these entries itself, so the HTTP the recorded body made is
            # left for the body that runs now; checked here too, so one is never raised again.
            return None
        result = None if outcome.error is not None else revive(outcome.result)
        return _Recorded(result, outcome.error, outcome.awaited)
    except Exception:  # NotReplayable: the tool runs for real
        return None


class _Call:
    """One recorded call: what ran, its arguments as captured before its body ran, and its
    clock, started then too, so a body that changes them is recorded with what it was given."""

    def __init__(self, spec: _Tool, ran: trace.Ran, args: trace.JSONValue) -> None:
        self.spec = spec
        self.ran = ran
        self.args = args
        # A sync tool that returned a coroutine (`trace.ToolCall.awaited`).
        self.awaited = False
        self.run_id = _run_id()
        self.started_at = time.time()

    def doc(self, result: object, exc: BaseException | None) -> dict[str, Any] | None:
        """This call with its outcome, as it is posted: built on the caller's thread, the result
        captured as the body left it. None, logged once per process, if building it failed: the
        agent's own result or exception goes on whatever happens here."""
        global _record_failed
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
            # The run is the route's, never the body's: a body naming one is refused.
            doc = trace.tool_call_to_json(call)
            del doc["run_id"]
            return doc
        except Exception:
            if not _record_failed:
                _record_failed = True
                logger.warning(
                    "irimi: could not record a call of tool %s; the call goes on, and later "
                    "failures of this kind are not logged",
                    self.spec.name,
                    exc_info=True,
                )
            return None

    def send(self, doc: dict[str, Any] | None) -> None:
        """Post `doc` to this call's run. Never raises (`client.ControlClient`)."""
        if doc is not None:
            runs.reporter.post(self.run_id, "tool-calls", doc)


def _is_async(f: Callable[..., Any]) -> bool:
    """`f` is called to get a coroutine: an `async def`, or an object whose `__call__` is one."""
    if inspect.iscoroutinefunction(f):
        return True
    return not inspect.isroutine(f) and inspect.iscoroutinefunction(type(f).__call__)


def _run_id() -> str:
    """The run a call belongs to: the current one, else the process run `irimi shadow` names for
    its child, else none. irimi files a call for no run in `unattributed/`, as it does an
    exchange that names none."""
    return context.current_run_id() or os.environ.get(RUN_ENV) or trace.UNATTRIBUTED


def _bounded(args: trace.JSONValue, result: trace.JSONValue) -> trace.JSONValue:
    """`result`, or the truncation marker in its place when the call's args and result together
    pass MAX_CAPTURED_BYTES. Each is already bounded by it alone (`capture`), and a post must stay
    under the control endpoint's `MAX_CONTROL_BODY`, twice that, or the whole call is refused."""
    size = len(serialized(result))
    if len(serialized(args)) + size <= MAX_CAPTURED_BYTES:
        return result
    return {TRUNCATED_KEY: True, "bytes": size}


def _warn_if_real_write(spec: _Tool) -> None:
    """A write tool about to run for real with the SDK inactive, in a process that names a proxy:
    probably an agent meant to run under irimi whose launcher dropped `IRIMI_ENGINE_ACTIVE`. Logged
    once per process, so a batch of a thousand writes logs one line."""
    global _warned_real_write
    if spec.kind != "write" or _warned_real_write:
        return
    if not any(os.environ.get(var) for var in PROXY_HINTS):
        return
    with _warn_lock:
        if _warned_real_write:
            return
        _warned_real_write = True
    logger.warning(
        "irimi: write tool %s ran for real: HTTPS_PROXY is set but %s is not",
        spec.name,
        ENGINE_ACTIVE_ENV,
    )
