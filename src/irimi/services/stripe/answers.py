"""Stripe faked-write bodies: the generic echo and fixture, plus `created`, `object`, `livemode`."""

import time
from typing import Any

from irimi.core.bodies import reflect
from irimi.core.exchange import Request
from irimi.servicemaps import Route
from irimi.services.generic import mint_ids, reflect_over
from irimi.services.model import AnswerClock

# Nothing irimi fakes happened in live mode. Set only on fixtures that have the field: real Stripe
# sends no `livemode` on a refund.
LIVEMODE = False


def object_name(operation: str) -> str:
    """Return the `object` an operation mints: `refunds.create` -> `refund`."""
    head = operation.partition(".")[0]
    return head[:-1] if len(head) > 1 and head.endswith("s") else head


def l0_body(request: Request, route: Route | None) -> dict[str, Any]:
    """Return Stripe's echo: reflected fields, `created`, the route's ids, and `object`.

    `object` is added only when the route mints an `id`, the one sign the echo names a resource.
    """
    body = reflect(request)
    body["created"] = int(time.time())
    if route is not None:
        mint_ids(body, route, request.path)
        if "id" in route.ids:
            body["object"] = object_name(route.operation)
    return body


def l1_body(request: Request, route: Route, obj: dict[str, Any]) -> dict[str, Any]:
    """Return Stripe's fixture body: `generic.l1_body`'s rules, plus `created` and `livemode`."""
    reflect_over(obj, reflect(request), route)
    obj["created"] = int(time.time())
    if "livemode" in obj:
        obj["livemode"] = LIVEMODE
    mint_ids(obj, route, request.path)
    return obj


def body(
    request: Request, route: Route | None, obj: dict[str, Any] | None, clock: AnswerClock
) -> dict[str, Any]:
    """Return a faked Stripe write's body: the fixture when the route has one, else the echo."""
    if route is not None and obj is not None:
        return l1_body(request, route, obj)
    return l0_body(request, route)
