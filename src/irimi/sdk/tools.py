"""`@sdk.tool`: a tool call the proxy cannot see, labelled read or write (#76).

The proxy sees only HTTP. A database write, a file write or a call into another library is
invisible to it, so the agent labels each such tool, and the SDK does for it what shadow mode does
for a write on the wire:

    | `active()` | kind  | Runs                              | Records    |
    | false      | read  | the real function                 | nothing    |
    | false      | write | the real function (WARNING, once) | nothing    |
    | true       | read  | the real function, `ran="real"`   | a ToolCall |
    | true       | write | ONLY the stand-in, `ran="shadow"` | a ToolCall |

THE SCOPE RULE, APPLIED TO THE SDK: nothing unnamed runs live. A write must name the stand-in that
replaces it, a read may not name one, and every mistake is a TypeError when the agent loads, never
at the call (`tool`).

A recorded call is posted to `{IRIMI_CONTROL}/runs/{run_id}/tool-calls` through `runs.reporter`
once it has returned or raised, so a post fails as a run's does: it never raises, and each kind of
failure is warned once per process. The call's run is the current one, or the process run
`irimi shadow` names in `IRIMI_RUN`, or none at all (`trace.UNATTRIBUTED`). An exception from the
function or the stand-in propagates unchanged, the same object, after the post.

HTTP made inside a tool is recorded as ordinary exchanges of the run, with no link to the call.

Which body runs is decided in one place, `_chosen`, so replay (#83) adds its branch there.
"""

import functools
import inspect
import logging
import os
import secrets
import threading
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, get_args

from irimi import paths, trace
from irimi.sdk import context, runs
from irimi.sdk.api import entrypoint
from irimi.sdk.capture import (
    MAX_CAPTURED_BYTES,
    TRUNCATED_KEY,
    capture,
    capture_call,
    serialized,
)
from irimi.sdk.client import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)

KINDS: tuple[trace.ToolKind, ...] = get_args(trace.ToolKind)
# The variables whose presence says this process meant to go through a proxy: an inactive SDK
# with either set ran a write tool for real where irimi was probably meant to be in front (#76).
# Both spellings of `HTTPS_PROXY`, from the one list of them (#75).
PROXY_HINTS = tuple(name for name in paths.PROXY_ENVS if name.upper() == "HTTPS_PROXY")

# Set once the "ran for real" WARNING has been logged: once per process, not per call or per tool.
_warned_real_write = False
_warn_lock = threading.Lock()
# Set once building a call's record has raised and been logged.
_record_failed = False


def tool(
    *, kind: trace.ToolKind, shadow: Callable[..., Any] | None = None, name: str | None = None
) -> Callable[[Callable[..., Any]], Any]:
    """Label the decorated function as a tool the proxy cannot see (see the module doc).

    `kind` is `"read"` or `"write"`. A write names `shadow`, the stand-in that runs in its place
    while irimi is in front, called with the same arguments; a read names none, because it runs
    for real and a stand-in would hide a real read. `name` defaults to the function's
    `module.qualname`.

    It keeps the function's signature and its nature - the wrapper is a coroutine function exactly
    when the function is - and keeps a `staticmethod` or `classmethod` one, in either decorator
    order, as `@sdk.trigger` does (#74), so a stand-in is never handed an instance it does not
    expect. The stand-in of a plain method is called as the method is, with the instance first.

    Refused at decoration time, with TypeError: a `kind` that is neither; a write with no stand-in;
    a read with one; a stand-in that is not callable, is a generator function, or is sync where
    the function is `async def` or the reverse; a generator or async-generator function; what has
    no importable `module.qualname` (`api.entrypoint`); and a `name` that is not a string."""
    if kind not in KINDS:
        raise TypeError(f"@sdk.tool(kind=...) must be 'read' or 'write', not {kind!r}")
    if name is not None and not isinstance(name, str):
        raise TypeError(f"@sdk.tool(name=...) must be a string, not {name!r}")
    if kind == "write" and shadow is None:
        raise TypeError(
            "@sdk.tool(kind='write') must name its stand-in as shadow=: a write the proxy cannot "
            "see never runs for real under irimi"
        )
    if kind == "read" and shadow is not None:
        raise TypeError(
            "@sdk.tool(kind='read') takes no shadow=: a read runs for real, and a stand-in would "
            "hide it"
        )
    if shadow is not None and not callable(shadow):
        raise TypeError(f"@sdk.tool(shadow=...) must be callable, not {shadow!r}")
    if shadow is not None and (
        inspect.isgeneratorfunction(shadow) or inspect.isasyncgenfunction(shadow)
    ):
        raise TypeError(
            f"@sdk.tool(shadow=...) cannot be the generator function {shadow.__qualname__}"
        )

    def decorate(fn: Callable[..., Any]) -> Any:
        if isinstance(fn, staticmethod | classmethod):
            # The function inside is the tool, and the descriptor stays (#74's rule for triggers).
            return type(fn)(decorate(fn.__func__))
        entrypoint(fn, "@sdk.tool")
        if inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
            raise TypeError(
                f"@sdk.tool cannot wrap the generator function {fn.__qualname__}: a call is "
                "recorded when it returns, before a generator's body has run"
            )
        is_async = inspect.iscoroutinefunction(fn)
        if shadow is not None and _is_async(shadow) != is_async:
            nature = "an async def" if is_async else "a sync function"
            raise TypeError(
                f"@sdk.tool: the stand-in for {fn.__qualname__} must be {nature}, as the "
                "function it stands in for is"
            )
        label = f"{fn.__module__}.{fn.__qualname__}" if name is None else name
        spec = _Tool(label[: runs.MAX_NAME], kind, fn, shadow)

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
    shadow: Callable[..., Any] | None


def _chosen(spec: _Tool) -> tuple[Callable[..., Any], trace.Ran] | None:
    """Which body this call runs, and what the record says ran. None while the SDK is inactive:
    the real function, recorded by nobody. THE ONE DISPATCH: replay (#83) adds its branch here,
    the stand-in for a write and the recording for a read."""
    if not context.active():
        return None
    if spec.kind == "write":
        assert spec.shadow is not None  # `tool` refused a write with none
        return spec.shadow, "shadow"
    return spec.fn, "real"


def _call(spec: _Tool, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    chosen = _chosen(spec)
    if chosen is None:
        _warn_if_real_write(spec)
        return spec.fn(*args, **kwargs)
    body, ran = chosen
    call = _Call(spec, ran, args, kwargs)
    try:
        result = body(*args, **kwargs)
    except BaseException as exc:
        call.send(call.doc(None, exc))
        raise
    if inspect.iscoroutine(result):
        # A sync function that returned a coroutine has not run its body yet: the call ends when
        # the coroutine does, as a sync trigger's run does (`RunScope.hand_off`, #74). One that
        # is never awaited is never recorded.
        return _finished_with(call, result)
    # Inline, as a sync trigger's posts are, even on an event loop's thread (#74).
    call.send(call.doc(result, None))
    return result


async def _call_async(spec: _Tool, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    chosen = _chosen(spec)
    if chosen is None:
        _warn_if_real_write(spec)
        return await spec.fn(*args, **kwargs)
    body, ran = chosen
    call = _Call(spec, ran, args, kwargs)
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
        # Closed, not awaited: nothing may be awaited now, so the post is made inline (#74).
        call.send(call.doc(None, exc))
        raise
    except BaseException as exc:
        await runs.off_the_loop(call.send, call.doc(None, exc))
        raise
    await runs.off_the_loop(call.send, call.doc(result, None))
    return result


class _Call:
    """One recorded call: its arguments are captured, and its clock started, before its body
    runs, so a body that changes them is recorded with what it was given."""

    def __init__(
        self, spec: _Tool, ran: trace.Ran, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> None:
        self.spec = spec
        self.ran = ran
        # Bound against the REAL function, which is what the agent called, whatever ran (#76).
        self.args, _ = capture_call(spec.fn, args, kwargs)
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
                tool_call_id=secrets.token_hex(8),
                run_id=self.run_id,
                name=self.spec.name,
                kind=self.spec.kind,
                ran=self.ran,
                args=self.args,
                result=captured,
                error=None if exc is None else trace.ErrorInfo.from_exception(exc),
                started_at=self.started_at,
                ended_at=time.time(),
            )
            # The run is the route's, never the body's: a body naming one is refused (#73).
            doc = trace.tool_call_to_json(call)
            del doc["run_id"]
            return doc
        except Exception:
            if not _record_failed:
                _record_failed = True
                logger.warning(
                    "irimi could not record a call of tool %s; the call goes on, and later "
                    "failures of this kind are not logged.",
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
    exchange that names none (#70, #77)."""
    return context.current_run_id() or os.environ.get(paths.RUN_ENV) or trace.UNATTRIBUTED


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
        paths.ENGINE_ACTIVE_ENV,
    )
