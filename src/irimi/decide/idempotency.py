"""Each run's answers to writes with an idempotency key, so a retried write is applied once."""

import threading
from dataclasses import dataclass
from typing import Any

from irimi import services
from irimi.core import bodies
from irimi.core.exchange import AnsweredBy, Flag, Precondition, Request
from irimi.servicemaps import Route
from irimi.servicemaps.classify import Classification


@dataclass(frozen=True)
class FirstAnswer:
    """A key's whole first answer, flags and precondition included, which a retry reproduces."""

    status: int
    body: bytes
    content_type: str
    answered_by: AnsweredBy
    flags: tuple[Flag, ...]
    precondition: Precondition


def idempotency_key_of(service: str, request: Request) -> str:
    """Return the idempotency key this request carries, or "" if the service or caller has none."""
    spec = services.of(service).idempotency
    if spec is None:
        return ""
    return request.header(spec.header) or ""


def conflict(service: str, key: str) -> services.Rejection | None:
    """Return what `service` answers when `key` comes back with different parameters, or None."""
    spec = services.of(service).idempotency
    return None if spec is None else spec.conflict(key)


def replayed_header(service: str) -> tuple[str, str] | None:
    """Return the `(name, value)` header `service` marks a retry's answer with, or None."""
    spec = services.of(service).idempotency
    return None if spec is None else spec.replayed_header


# Sendings compare on more than the route: `payment_intents.cancel` is one route over every
# intent with an empty body, so a key reused on another intent must not match.
Canonical = tuple[str, str, str, dict[str, Any]]


def canonical(request: Request, route: Route) -> Canonical:
    """Return the write as compared across sendings, without the route's `compare_ignores:`."""
    posted = bodies.reflect(request)
    fields = {name: value for name, value in posted.items() if name not in route.compare_ignores}
    return request.method, request.path, request.query, fields


def slot_id(
    run_id: str, service: str, scope: tuple[str, ...], idempotency_key: str
) -> tuple[str, ...]:
    """Return the slot one write occupies: run, service, scope (`services.scope_of`), key."""
    return (run_id, service, *scope, idempotency_key)


@dataclass(frozen=True)
class Slot:
    """A write's place in the store, its compared parameters, and the key as the caller sent it."""

    id: tuple[str, ...]
    params: Canonical
    idempotency_key: str


def slot_for(request: Request, classification: Classification, run_id: str) -> Slot | None:
    """Return the `Slot` of a keyed `write` on a matched route, else None."""
    route = classification.route
    if route is None or classification.kind != "write":
        return None
    service = classification.service
    sent = idempotency_key_of(service, request)
    if not sent:
        return None
    scope = services.scope_of(service, request)
    return Slot(slot_id(run_id, service, scope, sent), canonical(request, route), sent)


class Store:
    """Each run's answered keys until the run ends. Thread-safe; the first answer wins."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._runs: dict[str, dict[tuple[str, ...], tuple[Canonical, FirstAnswer]]] = {}

    def get(self, slot: tuple[str, ...], params: Canonical) -> tuple[FirstAnswer | None, bool]:
        """Return `(first, conflict)`; a key reused on another write gives `(None, True)`."""
        with self._lock:
            entry = self._runs.get(slot[0], {}).get(slot)
        if entry is None:
            return None, False
        held, first = entry
        if held != params:
            return None, True
        return first, False

    def put(self, slot: tuple[str, ...], params: Canonical, first: FirstAnswer) -> None:
        """Remember the first answer to this slot, accepted or rejected; later puts are ignored."""
        with self._lock:
            self._runs.setdefault(slot[0], {}).setdefault(slot, (params, first))

    def end_run(self, run_id: str) -> None:
        """Drop the run's keys."""
        with self._lock:
            self._runs.pop(run_id, None)
