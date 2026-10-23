"""What a Stripe write is answered with: the generic echo and fixture body, plus the fields every
Stripe object carries - `created`, the `object` it is, and `livemode`. `effects` is the other
half: what such a write does to Stripe's later reads."""

import time
from typing import Any

from irimi.bodies import reflect
from irimi.exchange import Request
from irimi.servicemap import Route
from irimi.services.generic import mint_ids, reflect_over
from irimi.services.model import AnswerClock

# irimi performed nothing, so nothing it answers happened in live mode. Set on every fixture that
# has the field at all, and on no fixture that does not: a `livemode` on a refund would be a field
# the real API never sends there.
LIVEMODE = False


def object_name(operation: str) -> str:
    """The `object` an operation's route mints, derived from the operation name.

    The first dot-segment with a single trailing `s` stripped: `refunds.create` -> `refund`,
    `payment_intents.cancel` -> `payment_intent`. The map has no `object:` key, and a segment that
    does not end in `s` is returned unchanged.
    """
    head = operation.partition(".")[0]
    return head[:-1] if len(head) > 1 and head.endswith("s") else head


def l0_body(request: Request, route: Route | None) -> dict[str, Any]:
    """Stripe's L0 body: reflected fields, `created`, the route's ids, and the `object` it names.

    `object` is only added when the route mints an `id`, because that is the only case where we
    know the echo names a resource.
    """
    body = reflect(request)
    body["created"] = int(time.time())
    if route is not None:
        mint_ids(body, route, request.path)
        if "id" in route.ids:
            body["object"] = object_name(route.operation)
    return body


def l1_body(request: Request, route: Route, obj: dict[str, Any]) -> dict[str, Any]:
    """Stripe's L1 body: `generic.l1_body`'s rules, with `created` set to now and `livemode` to
    `LIVEMODE` on a fixture that has the field."""
    reflect_over(obj, reflect(request), route)
    obj["created"] = int(time.time())
    if "livemode" in obj:
        obj["livemode"] = LIVEMODE
    mint_ids(obj, route, request.path)
    return obj


def body(
    request: Request, route: Route | None, obj: dict[str, Any] | None, clock: AnswerClock
) -> dict[str, Any]:
    """A faked Stripe write's body: L1 over the route's fixture object when there is one, else L0.
    No Stripe answer reads `clock`."""
    if route is not None and obj is not None:
        return l1_body(request, route, obj)
    return l0_body(request, route)
