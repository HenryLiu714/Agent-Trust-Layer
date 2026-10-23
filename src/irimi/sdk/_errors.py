"""What a read tool raised when the recorded run called it, raised again in a replay.

The recording holds what a call raised as `trace.ErrorInfo`, a type's `module.qualname` and a
message, never the exception, and the SDK never imports a class a stored run names. So a replay
raises `RecordedToolError`, which carries both. `error_info` is what a run or a tool call records
an exception as, so a recorded error is recorded again as the type it was recorded as.
"""

import builtins
import threading

from irimi.protocol import trace


class RecordedToolError(Exception):
    """An exception a read tool raised in the recorded run, raised again in a replay.

    A replay answers a read tool from its recording; when the recorded call raised, the replay
    raises this in its place. For a builtin exception such as KeyError the raised error is also a
    subclass of that builtin, with its name, so the agent's existing `except KeyError` catches it
    as it caught the original; `isinstance(e, sdk.RecordedToolError)` tells it apart. Its `args`
    are `(message,)`: the original's are not stored. Picklable and copyable.

    Attributes:
        type_name: The original's type as the recording names it, such as `builtins.KeyError`.
        message: The original's `str`.

    Example:
        try:
            row = lookup_customer(customer_id)
        except sdk.RecordedToolError as e:
            log.info("the recorded run's lookup raised %s", e.type_name)
    """

    def __init__(self, type_name: str, message: str) -> None:
        super().__init__(message)
        self.type_name = type_name
        self.message = message

    def __str__(self) -> str:
        return self.message

    def __reduce__(self) -> tuple[object, tuple[trace.ErrorInfo]]:
        return recorded_error, (trace.ErrorInfo(self.type_name, self.message),)


# One class per builtin, so two recorded KeyErrors are of one type.
_AS_BUILTIN: dict[type[Exception], type[RecordedToolError]] = {}
_as_builtin_lock = threading.Lock()


def recorded_error(error: trace.ErrorInfo) -> RecordedToolError:
    """The RecordedToolError a recorded `error` is raised as: a subclass of the builtin it
    names, when it names one. Never raises."""
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
        return trace.ErrorInfo(exc.type_name, exc.message)
    return trace.ErrorInfo.from_exception(exc)
