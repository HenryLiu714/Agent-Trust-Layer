"""What a read tool raised when the recorded run called it, raised again in a replay.

A replay answers a read tool from its recording, and a recorded call that raised raises again. The
recording holds what it raised as `trace.ErrorInfo`, a type's `module.qualname` and a message,
never the exception, and the SDK never imports a class a stored run names. So it raises
`RecordedToolError`, which carries both; for a builtin `Exception`, which needs no import, the
class is also a subclass of that builtin, named as it is, so the agent's own `except KeyError`
catches it as it caught the original. Its `args` are `(message,)`: the original's are not stored.

Here, below `runs` and `tools`, so a run that ends on one records the recorded type too
(`error_info`).
"""

import builtins
import threading

from irimi import trace


class RecordedToolError(Exception):
    """A recorded exception, raised again: its `type`, as `trace.ErrorInfo` spells it, and its
    `message`, its `str`. Picklable and copyable, as the agent's own exceptions are."""

    def __init__(self, type: str, message: str) -> None:
        super().__init__(message)
        self.type = type
        self.message = message

    def __str__(self) -> str:
        return self.message

    def __reduce__(self) -> tuple[object, tuple[trace.ErrorInfo]]:
        return recorded_error, (trace.ErrorInfo(self.type, self.message),)


# One class per builtin, so two recorded KeyErrors are of one type.
_AS_BUILTIN: dict[type[Exception], type[RecordedToolError]] = {}
_as_builtin_lock = threading.Lock()


def recorded_error(error: trace.ErrorInfo) -> RecordedToolError:
    """The RecordedToolError a recorded `error` is raised as (module doc). Never raises."""
    module, _, name = error.type.rpartition(".")
    builtin = getattr(builtins, name, None) if module == "builtins" else None
    if isinstance(builtin, type) and issubclass(builtin, Exception):
        try:
            return _as_builtin(builtin)(error.type, error.message)
        except Exception:  # a builtin whose layout or constructor this cannot share (UnicodeError)
            pass
    return RecordedToolError(error.type, error.message)


def _as_builtin(builtin: type[Exception]) -> type[RecordedToolError]:
    with _as_builtin_lock:
        made = _AS_BUILTIN.get(builtin)
        if made is None:
            # Named as the builtin is, module included, so an agent that logs `type(exc).__name__`,
            # or tells its model, and a traceback say what they said when it was recorded.
            # It pickles by `__reduce__`, never by that name.
            made = type(
                builtin.__name__,
                (RecordedToolError, builtin),
                {"__qualname__": builtin.__qualname__, "__module__": builtin.__module__},
            )
            _AS_BUILTIN[builtin] = made
        return made


def error_info(exc: BaseException) -> trace.ErrorInfo:
    """What a run or a tool call records it raised: a recorded error as it was recorded, so a
    replay's record and its recording's say one type, and anything else as `from_exception`."""
    if isinstance(exc, RecordedToolError):
        return trace.ErrorInfo(exc.type, exc.message)
    return trace.ErrorInfo.from_exception(exc)
