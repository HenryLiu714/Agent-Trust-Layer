"""The run's answers to writes that carried an idempotency key, so a retry is one write (#46).

A service lets a caller name a write (`Idempotency-Key`), and promises that sending it twice
performs it once. irimi has to keep that promise itself: stripe-python puts a fresh UUID on every
POST, so an agent that retries a refund would otherwise get two different minted ids and the
overlay would apply the refund twice - a charge showing 200 refunded for a single 100 refund.

Pure and in-memory. Nothing here decides anything: `policy` looks the answer up and puts the
first one in, and the store only remembers. That is also what lets Phase 5 replay keep the same
promise over a recording with the same code.
"""

import threading
from dataclasses import dataclass
from typing import Any

from irimi import echo, services
from irimi.exchange import AnsweredBy, PreconditionOutcome, Request
from irimi.servicemap import Route

# What irimi puts on a replayed answer. Stripe spells it `Idempotent-Replayed`; header names are
# case-insensitive on the wire and every header irimi sets is written lower-case, like
# `content-type` and `pipeline.ANSWERED_BY_HEADER` (#46).
REPLAYED_HEADER = "idempotent-replayed"
REPLAYED_VALUE = "true"


@dataclass(frozen=True)
class Stored:
    """The whole of a first answer, so a replay reproduces it rather than re-deriving it (#46).

    Re-deriving would let a fixture that has since become unreadable stamp `fake-L0` over a first
    call answered at `fake-L1`, and the trace would disagree with itself about one write. `flags`
    carries `fixture-failed` for the same reason, and `precondition` / `rejection_code` carry an
    L3 rejection so a replayed one still prints as a rejection and still stays out of the write
    log. `currency` rides along for the same reason: it is what the write's own L3 read found on
    the object it named, and a replay that re-derived it (or dropped it) would have the trace
    disagree with itself about what one write was denominated in (#60). There is no `issued`: a
    replay makes no precondition read, which is the point.
    """

    status: int
    body: bytes
    content_type: str
    answered_by: AnsweredBy
    flags: tuple[str, ...]
    precondition: PreconditionOutcome | None
    rejection_code: str
    currency: str


def key_of(service: str, request: Request) -> str:
    """The idempotency key this request carries, or "" when there is none to key on.

    "" for a service with no entry in `services.IDEMPOTENCY` (Slack has none) and for a caller
    that sent no key - a raw HTTP client may not, even though stripe-python always does.
    """
    spec = services.IDEMPOTENCY.get(service)
    if spec is None:
        return ""
    return next((v for k, v in request.headers if k == spec.header), "")


def conflict(service: str, key: str) -> services.Rejection | None:
    """What `service` answers when `key` comes back with different parameters. None if unknown."""
    spec = services.IDEMPOTENCY.get(service)
    return None if spec is None else spec.conflict(key)


# What two sendings of one key are compared by: the whole of the write the caller named, as
# `(method, path, query, the posted fields that are not volatile)` (#46). The route is NOT enough
# to tell two writes apart - `payment_intents.cancel` is one route over every `pi_`, and it posts
# an empty body - so a key reused to cancel a different intent, or on another endpoint entirely,
# would compare equal on its fields alone and replay the first write's object at the second
# caller. Stripe compares the whole request and refuses a key that comes back on a different one,
# which is what carrying the identity in here makes irimi do too.
Canonical = tuple[str, str, str, dict[str, Any]]


def canonical(request: Request, route: Route) -> Canonical:
    """The write as the store compares two sendings of one key.

    The fields are the caller's own minus the route's `volatile:` names - the map already marks
    `idempotency_key` on `refunds.create` - because a field that always differs between two
    sendings must not make every retry look like a different request. `echo.reflect` is the one
    never-raising parser for both form and JSON bodies, and it is what `writelog` and the
    preconditions read a request with, so the store compares the same view of the write they do.
    The method, path and query are in front of it because a key names one write and not one
    route: without them a write whose parameters are all in its path - `payment_intents.cancel`
    posts nothing at all - is indistinguishable from every other write on the same route.
    """
    posted = echo.reflect(request)
    fields = {name: value for name, value in posted.items() if name not in route.volatile}
    return request.method, request.path, request.query, fields


def key(run_id: str, service: str, scope: tuple[str, ...], idempotency_key: str) -> tuple[str, ...]:
    """The slot one write occupies.

    Scoped like the overlay - the service, then `writelog.scope`'s `(Stripe-Account,
    Stripe-Version)`, which is what `scope` carries - because a key reused against another
    connected account or another API version is another write. `run_id` leads, because Phase 3's
    `shadow --serve` puts many runs in one process and a key is only promised unique within one
    caller's own sequence.
    """
    return (run_id, service, *scope, idempotency_key)


class Store:
    """One run's answered keys. In memory, and reached from the engine's worker thread (#45).

    A plain dict behind a lock: the decision runs in `asyncio.to_thread`, so two writes can be in
    here at once, and `setdefault` is what makes the FIRST answer the one the run keeps however
    the two are interleaved. Nothing here touches a flow, a socket or the clock.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[tuple[str, ...], tuple[Canonical, Stored]] = {}

    def get(self, slot: tuple[str, ...], params: Canonical) -> tuple[Stored | None, bool]:
        """`(the answer to replay, whether this key is held with DIFFERENT parameters)`.

        The two outcomes are distinct and neither is an error: `(None, False)` is a key never seen,
        `(stored, False)` is a retry to replay, and `(None, True)` is the caller reusing a key for
        a different write, which the service refuses.
        """
        with self._lock:
            entry = self._entries.get(slot)
        if entry is None:
            return None, False
        held, stored = entry
        if held != params:
            return None, True
        return stored, False

    def put(self, slot: tuple[str, ...], params: Canonical, stored: Stored) -> None:
        """Remember the first answer to this slot. A second put for the same slot is ignored.

        Stripe stores the first response under a key whether it was accepted or rejected, so a
        replay of a rejected write returns the same refusal - which is why `policy` calls this on
        both of its answering paths and not only the happy one (#46).
        """
        with self._lock:
            self._entries.setdefault(slot, (params, stored))
