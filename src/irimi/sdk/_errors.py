"""Re-raise in a replay what a read tool raised, without importing the class the run names."""

import builtins
import threading

from irimi.protocol import trace


class RecordedToolError(Exception):
    """An exception a read tool raised in the recorded run, raised again in a replay.

    For a builtin such as KeyError it also subclasses that builtin, so `except KeyError` still
    catches it. `args` is `(message,)`: the original args are not stored. Picklable.

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
    """Return the RecordedToolError for `error`, subclassing the builtin it names. Never raises."""
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
            # The builtin's name keeps tracebacks as recorded; pickling uses `__reduce__`.
            made = type(
                builtin.__name__,
                (RecordedToolError, builtin),
                {"__qualname__": builtin.__qualname__, "__module__": builtin.__module__},
            )
            _AS_BUILTIN[builtin] = made
        return made


def error_info(exc: BaseException) -> trace.ErrorInfo:
    """Return how to record `exc`; a RecordedToolError keeps its original type name."""
    if isinstance(exc, RecordedToolError):
        return trace.ErrorInfo(exc.type_name, exc.message)
    return trace.ErrorInfo.from_exception(exc)
