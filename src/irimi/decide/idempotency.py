"""The run's answers to writes that carried an idempotency key, so a retry is one write.

A service may let a caller name a write with an idempotency key, and promise that sending it twice
performs it once. irimi has to keep that promise itself: stripe-python puts a fresh UUID on every
POST, so an agent that retries a refund would otherwise get two different minted ids and the
overlay would apply the refund twice - a charge showing 200 refunded for a single 100 refund.
Which header names the key, what a reused key is refused with and how a retry's answer is marked
are the service's (`Service.idempotency`); this module is the same for every service.

Pure and in-memory. Nothing here decides anything: `policy` looks the answer up and puts the
first one in, and the store only remembers. That is also what lets `irimi replay` keep the same
promise over a recording with the same code.
"""

import threading
from dataclasses import dataclass
from typing import Any

from irimi import services
from irimi.core import bodies
from irimi.core.exchange import AnsweredBy, Flag, Precondition, Request
from irimi.decide.writelog import EndedRuns
from irimi.servicemaps import Route
from irimi.servicemaps.classify import Classification


@dataclass(frozen=True)
class FirstAnswer:
    """The whole of a key's first answer, so a retry reproduces it rather than re-deriving it.

    Re-deriving would let a fixture that has since become unreadable stamp `fake-L0` over a first
    call answered at `fake-L1`, and the trace would disagree with itself about one write. `flags`
    carries `fixture-failed` for the same reason, and `precondition` carries an L3 rejection, so
    a retried one still prints as a rejection and still stays out of the write log, and the
    currency the write's own check read found. There is no `issued`: a retry makes no
    precondition read, which is the point.
    """

    status: int
    body: bytes
    content_type: str
    answered_by: AnsweredBy
    flags: tuple[Flag, ...]
    precondition: Precondition


def idempotency_key_of(service: str, request: Request) -> str:
    """The idempotency key this request carries, or "" when there is none to key on.

    "" for a service whose spec has no `idempotency` (Slack has none) and for a caller
    that sent no key - a raw HTTP client may not, even though stripe-python always does.
    """
    spec = services.of(service).idempotency
    if spec is None:
        return ""
    return request.header(spec.header) or ""


def conflict(service: str, key: str) -> services.Rejection | None:
    """What `service` answers when `key` comes back with different parameters. None if unknown."""
    spec = services.of(service).idempotency
    return None if spec is None else spec.conflict(key)


def replayed_header(service: str) -> tuple[str, str] | None:
    """The `(name, value)` header `service` marks a retry's answer with, or None when it marks
    none or has no idempotency mechanism."""
    spec = services.of(service).idempotency
    return None if spec is None else spec.replayed_header


# What two sendings of one key are compared by: the whole of the write the caller named, as
# `(method, path, query, the posted fields the route does not ignore)`. The route is NOT enough
# to tell two writes apart - `payment_intents.cancel` is one route over every `pi_`, and it posts
# an empty body - so a key reused to cancel a different intent, or on another endpoint entirely,
# would compare equal on its fields alone and answer the second caller with the first write's
# object. A service compares the whole request and refuses a key that comes back on a different
# one, which is what carrying the identity in here makes irimi do too.
Canonical = tuple[str, str, str, dict[str, Any]]


def canonical(request: Request, route: Route) -> Canonical:
    """The write as the store compares two sendings of one key.

    The fields are the caller's own minus the route's `compare_ignores:` names, because a field
    that always differs between two sendings must not make every retry look like a different
    request. `bodies.reflect` is the one never-raising parser for both form and JSON bodies, and
    it is what `writelog` and the preconditions read a request with, so the store compares the
    same view of the write they do.
    The method, path and query are in front of it because a key names one write and not one
    route: without them a write whose parameters are all in its path - `payment_intents.cancel`
    posts nothing at all - is indistinguishable from every other write on the same route.
    """
    posted = bodies.reflect(request)
    fields = {name: value for name, value in posted.items() if name not in route.compare_ignores}
    return request.method, request.path, request.query, fields


def slot_id(
    run_id: str, service: str, scope: tuple[str, ...], idempotency_key: str
) -> tuple[str, ...]:
    """The slot one write occupies.

    Scoped like the overlay - the service, then the values of its scope headers
    (`services.scope_of`), which is what `scope` carries - because a key reused against another
    scope (for Stripe, another connected account or API version) is another write. `run_id`
    leads, because serve mode puts many runs in one process and a key is only promised unique
    within one caller's own sequence.
    """
    return (run_id, service, *scope, idempotency_key)


@dataclass(frozen=True)
class Slot:
    """Where one write sits in the store (`id`), what two sendings of it are compared by
    (`params`), and the idempotency key exactly as the caller sent it, for the service's own
    conflict body."""

    id: tuple[str, ...]
    params: Canonical
    idempotency_key: str


def slot_for(request: Request, classification: Classification, run_id: str) -> Slot | None:
    """The `Slot` a write the store covers occupies, else None.

    Three conditions. The route must be matched, because its `compare_ignores:` is what makes two
    sendings comparable and an unmapped POST has none. The kind must be `write`, so a route that
    THE SCOPE RULE downgraded to `unknown` is answered the way an unclassified request is and not
    out of a store. And the service must be one that HAS an idempotency mechanism, which is
    `idempotency_key_of` returning something.
    """
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
    """Each run's answered keys, until the run ends. In memory, and reached from the engine's
    worker thread.

    A plain dict behind a lock: the decision runs in `asyncio.to_thread`, so two writes can be in
    here at once, and `setdefault` is what makes the FIRST answer the one the run keeps however
    the two are interleaved. Nothing here touches a flow, a socket or the clock.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # Keyed by the run a slot id leads with (`slot_id`), so a run's entries go in one step.
        self._runs: dict[str, dict[tuple[str, ...], tuple[Canonical, FirstAnswer]]] = {}
        self._ended = EndedRuns()

    def get(self, slot: tuple[str, ...], params: Canonical) -> tuple[FirstAnswer | None, bool]:
        """`(the answer a retry gets, whether this key is held with DIFFERENT parameters)`.

        The two outcomes are distinct and neither is an error: `(None, False)` is a key never seen,
        `(first, False)` is a retry, and `(None, True)` is the caller reusing a key for a
        different write, which the service refuses.
        """
        with self._lock:
            entry = self._runs.get(slot[0], {}).get(slot)
        if entry is None:
            return None, False
        held, first = entry
        if held != params:
            return None, True
        return first, False

    def put(self, slot: tuple[str, ...], params: Canonical, first: FirstAnswer) -> None:
        """Remember the first answer to this slot. A second put for the same slot is ignored.

        A service stores the first response under a key whether it was accepted or rejected, so a
        retry of a rejected write returns the same refusal - which is why `policy` calls this on
        both of its answering paths and not only the happy one. A run that has ended keeps no
        key, as its write log keeps no write (`writelog`).
        """
        with self._lock:
            if slot[0] in self._ended:
                return
            self._runs.setdefault(slot[0], {}).setdefault(slot, (params, first))

    def end_run(self, run_id: str) -> None:
        """Drop the run's keys, and keep none it sends later."""
        with self._lock:
            self._runs.pop(run_id, None)
            self._ended.add(run_id)
