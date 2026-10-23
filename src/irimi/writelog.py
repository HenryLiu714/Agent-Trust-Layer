"""The run's faked writes, decoded out of the trace and matched to the reads they can affect.

Both `overlay` and `policy` need it: a precondition checks a write against real state *plus the
run's overlay*, and the two live in the same layer and may not import each other. It is its own
job in any case - turning `Exchange`es into
`services.Write`s is neither deciding nor overlaying - and it is pure, so replay decodes a
recording with the same function.

`WriteLog` keeps those writes per run: a run sees its own faked writes and no other run's, and a
run's writes are dropped when it ends.
"""

import threading
from collections.abc import Mapping, Sequence

from irimi import bodies, services
from irimi.exchange import Exchange, Request


class WriteLog:
    """The faked writes each run has made, keyed by run id.

    Requests that name no run share the id the engine files them under (`unattributed` in serve
    mode), so they form one log of their own. The engine adds from its event loop and the store
    ends runs from its writer thread, so one lock covers both.
    """

    def __init__(self) -> None:
        self._runs: dict[str, tuple[Exchange, ...]] = {}
        self._lock = threading.Lock()

    def for_run(self, run_id: str) -> tuple[Exchange, ...]:
        """This run's writes, oldest first. The tuple is the log's own and never changes, so the
        caller may read it from another thread without a copy."""
        with self._lock:
            return self._runs.get(run_id, ())

    def add(self, exchange: Exchange) -> None:
        """File `exchange` under its own run."""
        with self._lock:
            self._runs[exchange.run_id] = (*self._runs.get(exchange.run_id, ()), exchange)

    def end_run(self, run_id: str) -> None:
        """Drop the run's writes: nothing reads them once the run has ended."""
        with self._lock:
            self._runs.pop(run_id, None)


def decode(service: str, request: Request, write_log: Sequence[Exchange]) -> list[services.Write]:
    """This service's faked writes, in the scope the read is asking about.

    A write made against another connected account or another API version says nothing about
    this read, so it is left out. The scope is taken from each write's OWN request headers,
    because that is the world it was made in.
    """
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
    table: Mapping[str, T], service: str, request: Request, write_log: Sequence[Exchange]
) -> tuple[T, list[services.Write]] | None:
    """`table[service]` and this service's faked writes in the read's scope, or None when the
    service has no entry or the run has no such write.

    The overlay's effects and rewrites and the policy's L3 read all start here, so the three agree
    on which writes a read can see - and none of them parses a body before it knows it has a write
    to apply.
    """
    entry = table.get(service)
    if entry is None:
        return None
    writes = decode(service, request, write_log)
    if not writes:
        return None
    return entry, writes


def read_of(operation: str, request: Request, translated_from: str | None = None) -> services.Read:
    """A live read as the effects are handed it: its operation, its own body reflected once, and
    the query pair irimi dropped from it, if any."""
    return services.Read(
        operation=operation,
        request=request,
        posted=bodies.reflect(request),
        translated_from=translated_from,
    )
