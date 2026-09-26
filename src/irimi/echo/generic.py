"""The L0 echo and the L1 fixture body, for any service (#11, #41). Minting, the id a
path already names, and writing a request's fields over a fixture object."""

import re
import secrets
import string
import time
from typing import Any

from irimi.bodies import SERVICE_OWNED, reflect
from irimi.exchange import FakeLevel, Request
from irimi.servicemap import Route, path_params

ID_ALPHABET = string.ascii_letters + string.digits
ID_LENGTH = 24

# What an id looks like after its prefix: one run of id characters, no second `_`. Stripe, OpenAI
# and Slack all mint `<prefix><token>`, which is what lets `named_id` tell `sub_2` from
# `sub_sched_1` when a route captures both.
_ID_TOKEN = re.compile(r"[A-Za-z0-9]+")
# Stripe spells a PaymentIntent's client secret `pi_<id>_secret_<token>`, so it starts with the
# same prefix as the id and is the one value in a path that must never be echoed back as one.
SECRET_MARKER = "_secret"

# What building a locally answered write produces: the body, the level it was built at, and any
# flag that level owes the trace (`fixture-failed`, #41).
Built = tuple[dict[str, Any], FakeLevel, tuple[str, ...]]


def mint_id(prefix: str) -> str:
    """`re_` -> `re_` + 24 characters from [A-Za-z0-9], drawn with secrets."""
    return prefix + "".join(secrets.choice(ID_ALPHABET) for _ in range(ID_LENGTH))


def object_name(operation: str) -> str:
    """The `object` an operation's route mints, derived from the operation name.

    The first dot-segment with a single trailing `s` stripped: `refunds.create` -> `refund`,
    `payment_intents.cancel` -> `payment_intent`. There is no `object:` field in the route schema,
    and a segment that does not end in `s` is returned unchanged.
    """
    head = operation.partition(".")[0]
    return head[:-1] if len(head) > 1 and head.endswith("s") else head


def named_id(route: Route, path: str, prefix: str) -> str | None:
    """The id this request already names in its own path, or None when it names none.

    A create posts to a collection (`POST /v1/refunds`) and the service mints the id. Every other
    write addresses a resource that exists (`POST /v1/customers/cus_REAL123`,
    `POST /v1/payment_intents/pi_REAL999/cancel`) and the live API answers with the id it was
    given, so minting a fresh one hands the agent an id for a resource that never existed (#26).

    The captured segment is matched by `prefix`, not by parameter name, because the two are
    spelled differently: the cancel route captures `{payment_intent}` and mints `id: pi_`, and the
    prefix is the only thing that ties them together. `path_params` percent-decodes the segment,
    so an over-encoded `cus%5FREAL123` is recognised as the id it is.

    A prefix can match more than one capture, and the first one is not always the right one:
    `sub_` matches `sub_sched_1` before `sub_2`, a real Stripe pair, and it matches a
    PaymentIntent **client secret** (`pi_ABC_secret_XYZ`) before anything else in that path. So a
    capture shaped like an id - the prefix and then one run of id characters, which is how Stripe,
    OpenAI and Slack all mint them - is preferred over one that merely starts with the prefix.
    That settles both: `sub_sched_1` and the client secret each carry a second `_` and neither is
    id-shaped. Nothing id-shaped leaves the old answer in place, minus a value carrying Stripe's
    own `_secret` marker, which is never an id and must not be echoed back as one (#33).
    """
    values = [v for v in path_params(route.path, path).values() if v.startswith(prefix)]
    shaped = [v for v in values if _ID_TOKEN.fullmatch(v[len(prefix) :])]
    if shaped:
        return shaped[0]
    return next((v for v in values if SECRET_MARKER not in v), None)


def l0_body(request: Request, route: Route | None) -> dict[str, Any]:
    """The generic L0 body: reflected fields, `created`, and the route's ids.

    An id is minted only when the request does not already name one - see `named_id`. A minted id
    still overwrites a reflected *body* field of the same name: what the service would have
    returned wins over what the caller happened to post. `object` is only added when the route
    carries an `id`, because that is the only case where we know the echo names a resource.
    """
    body = reflect(request)
    body["created"] = int(time.time())
    if route is not None and route.ids:
        for name, prefix in route.ids.items():
            body[name] = named_id(route, request.path, prefix) or mint_id(prefix)
        if "id" in route.ids:
            body["object"] = object_name(route.operation)
    return body


# irimi performed nothing, so nothing it answers happened in live mode. Set on every fixture that
# has the field at all, and on no fixture that does not: a `livemode` on a refund would be a field
# the real API never sends there (#41).
LIVEMODE = False


def _same_json_type(current: Any, value: Any) -> bool:
    """True when `value` may stand in for `current` in a fixture object.

    stripe-mock reflects a request field when the schema says it has the field's type; there is no
    schema here, so the fixture's own value is the type. Numbers are one type, because a form body
    coerces `amount=4900` to an int while a fixture may hold a float. `bool` is checked first: it
    is an `int` in Python, and `refunded=true` must not overwrite an amount.
    """
    if isinstance(current, bool) or isinstance(value, bool):
        return isinstance(current, bool) and isinstance(value, bool)
    if isinstance(current, int | float):
        return isinstance(value, int | float)
    return type(current) is type(value)


def reflect_over(obj: dict[str, Any], reflected: dict[str, Any], route: Route) -> None:
    """Write the request's own fields over a fixture object, in place. Shared by both L1 bodies.

    Extracted from `l1_body` when Slack got an L1 answer of its own (#42): the reflection rules
    are the same for both, but everything `l1_body` does after them - `created`, `livemode`, the
    minted ids - is Stripe-shaped, and a Slack message object has none of it.
    """
    for name, value in reflected.items():
        if name in SERVICE_OWNED or name in route.ids or name not in obj:
            continue
        if obj[name] is None or _same_json_type(obj[name], value):
            obj[name] = value


def l1_body(request: Request, route: Route, obj: dict[str, Any]) -> dict[str, Any]:
    """The L1 body: the fixture object with this request's own fields written over it.

    `obj` is the caller's to mutate - `fixture.get` hands out a private deep copy - and the rules
    are the ones stripe-mock's own generator applies, minus the OpenAPI schema it has and we do
    not:

    * a request field is reflected only when the fixture **names** it. A field the object does not
      have is one the real service would have refused, and inventing it would hand the agent a
      body no live response can produce. L0 keeps echoing everything, which is what makes it the
      honest floor for a route we have no fixture for.
    * it is reflected only when the types agree (`_same_json_type`). A fixture field holding
      `null` names no type, so any value is accepted there: `reason`, `description` and `customer`
      are all `null` in Stripe's own fixtures and all things a caller really does send.
    * `SERVICE_OWNED` fields and every field the route mints an id for are the service's answer,
      not the caller's argument, so they are set last and a posted value cannot reach them.

    The id rules are L0's, unchanged: `named_id` first, so an update or a cancel echoes the id its
    own path names (#26, #33), and a minted id only when the request named none.
    """
    reflect_over(obj, reflect(request), route)
    obj["created"] = int(time.time())
    if "livemode" in obj:
        obj["livemode"] = LIVEMODE
    for name, prefix in route.ids.items():
        obj[name] = named_id(route, request.path, prefix) or mint_id(prefix)
    return obj
