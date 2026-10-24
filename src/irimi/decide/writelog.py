"""The run's faked writes, and how they are applied to a read's document.

A write sent after its run ended, from a task that outlived it, is answered but not filed.
"""

import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from irimi import services
from irimi.core import bodies
from irimi.core.exchange import Exchange, Request
from irimi.core.limits import MAX_ENDED_RUNS


@dataclass(frozen=True)
class Translation:
    """A read the overlay rewrote, and the `key=value` query pair it removed from the wire."""

    request: Request
    removed: str


class EndedRuns:
    """The newest MAX_ENDED_RUNS ended run ids; not locked, as its owner's lock covers it."""

    def __init__(self) -> None:
        self._ids: dict[str, None] = {}  # oldest first

    def add(self, run_id: str) -> None:
        self._ids.pop(run_id, None)
        self._ids[run_id] = None
        if len(self._ids) > MAX_ENDED_RUNS:
            del self._ids[next(iter(self._ids))]

    def __contains__(self, run_id: str) -> bool:
        return run_id in self._ids


class WriteLog:
    """The faked writes each run has made, keyed by run id; a run sees only its own. Thread-safe."""

    def __init__(self) -> None:
        self._runs: dict[str, tuple[Exchange, ...]] = {}
        self._ended = EndedRuns()
        self._lock = threading.Lock()

    def for_run(self, run_id: str) -> tuple[Exchange, ...]:
        """Return the run's writes, oldest first, as an immutable tuple safe to share."""
        with self._lock:
            return self._runs.get(run_id, ())

    def add(self, exchange: Exchange) -> None:
        """File `exchange` under its own run, unless that run has ended (the module doc)."""
        with self._lock:
            if exchange.run_id in self._ended:
                return
            self._runs[exchange.run_id] = (*self._runs.get(exchange.run_id, ()), exchange)

    def end_run(self, run_id: str) -> None:
        """Drop the run's writes, and file none it sends later."""
        with self._lock:
            self._runs.pop(run_id, None)
            self._ended.add(run_id)


def decode(service: str, request: Request, write_log: Sequence[Exchange]) -> list[services.Write]:
    """Return the service's faked writes made in the read's scope (account, API version)."""
    wanted = services.scope_of(service, request)
    out: list[services.Write] = []
    for exchange in write_log:
        if exchange.service != service or exchange.response is None:
            continue
        if services.scope_of(service, exchange.request) != wanted:
            continue
        answer = bodies.json_object(exchange.response.body)
        if answer is None:
            continue
        out.append(
            services.Write(
                operation=exchange.operation,
                posted=bodies.reflect(exchange.request),
                answer=answer,
            )
        )
    return out


def scoped_writes[T](
    entry: T | None, service: str, request: Request, write_log: Sequence[Exchange]
) -> tuple[T, list[services.Write]] | None:
    """Return `(entry, writes in the read's scope)`, or None if either is empty."""
    if entry is None:
        return None
    writes = decode(service, request, write_log)
    if not writes:
        return None
    return entry, writes


def apply(
    service: str,
    operation: str,
    request: Request,
    document: Callable[[], Any],
    write_log: Sequence[Exchange],
    translated_from: str | None = None,
) -> services.Applied | None:
    """Apply the run's writes in the read's scope to the read's document.

    Returns None when the service models no effects or no write is in scope. `document` is
    called only when there is a write to apply; if it returns None (an unreadable body) the
    result is `partial` and unchanged.
    """
    found = scoped_writes(services.of(service).effects, service, request, write_log)
    if found is None:
        return None
    read = document()
    if read is None:
        return services.Applied(None, partial=True)
    effects, writes = found
    return effects(read_of(operation, request, translated_from), read, writes)


def read_of(operation: str, request: Request, translated_from: str | None = None) -> services.Read:
    """Return a live read as the effects receive it."""
    return services.Read(
        operation=operation,
        request=request,
        posted=bodies.reflect(request),
        translated_from=translated_from,
    )
