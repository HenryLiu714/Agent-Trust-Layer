"""Per-service L2 effects and the tables that name them.

One module per service holding what a faked write does to a later read, as plain functions over
plain data. The overlay decodes the write log and looks the service up here; nothing in this
package knows about mitmproxy, exchanges or flows, which is what lets a replay run the same
functions over a recording. `check` holds these tables to the loaded maps.
"""

from irimi import fixture
from irimi.exchange import Request
from irimi.servicemap import MapError, MapIndex
from irimi.services import slack, stripe
from irimi.services.model import (
    NOT_EVALUABLE,
    Applied,
    Check,
    Idempotency,
    NotEvaluable,
    Probe,
    Proposal,
    QueryRewrite,
    Read,
    ReadEffects,
    Rejection,
    Rewritten,
    Write,
)

__all__ = [
    "Applied",
    "Check",
    "EFFECTS",
    "IDEMPOTENCY",
    "Idempotency",
    "NOT_EVALUABLE",
    "NotEvaluable",
    "OPERATIONS",
    "PRECONDITIONS",
    "Probe",
    "Proposal",
    "QueryRewrite",
    "Read",
    "ReadEffects",
    "Rejection",
    "REWRITES",
    "Rewritten",
    "SCOPE_HEADERS",
    "Write",
    "check",
    "probe_headers_of",
    "scope_of",
]

# Every operation name irimi's Python keys on, by service name (a map's `service:`).
OPERATIONS: dict[str, frozenset[str]] = {
    slack.SERVICE: slack.OPERATIONS,
    stripe.SERVICE: stripe.OPERATIONS,
}
# What a service's faked writes do to its live reads, by service name.
EFFECTS: dict[str, ReadEffects] = {
    slack.SERVICE: slack.apply_read,
    stripe.SERVICE: stripe.apply_read,
}
# The L3 precondition a route's `precondition:` key names, by (service, name). The key is the
# opt-in and this table is what it opts into; `check` refuses a map naming one that is not here.
PRECONDITIONS: dict[tuple[str, str], Check] = {
    (stripe.SERVICE, "charge_refundable"): stripe.CHARGE_REFUNDABLE,
    (slack.SERVICE, "channel_postable"): slack.CHANNEL_POSTABLE,
}
# What a service translates in a read request before it is forwarded. Not every service has one.
REWRITES: dict[str, QueryRewrite] = {stripe.SERVICE: stripe.rewrite_query}
# The request headers that scope a service's state. A write made against one Stripe connected
# account, or one API version, says nothing about a read made against another, so the overlay
# only applies writes whose values match the read's. Absent on both sides is a match.
#
# Slack has no entry, so a run has exactly one Slack scope, which is the right answer today. If the
# token's team is wanted later, `SCOPE_HEADERS["slack"] = ("authorization",)` is the whole change:
# the header is already on each write's own request, and it is only ever compared, never stored.
SCOPE_HEADERS: dict[str, tuple[str, ...]] = {stripe.SERVICE: ("stripe-account", "stripe-version")}
# The request headers carrying a service's credential, which a precondition read copies from the
# write it checks. Stripe and Slack both send theirs in `Authorization`, the default.
PROBE_HEADERS: dict[str, tuple[str, ...]] = {}
DEFAULT_PROBE_HEADERS = ("authorization",)


def scope_of(service: str, request: Request) -> tuple[str, ...]:
    """The values of the headers that scope `service`'s state on this request (`SCOPE_HEADERS`),
    "" for each one it does not carry. Two requests in different scopes say nothing about each
    other: the write log, the idempotency store and the precondition read all compare this."""
    return tuple(request.header(name) or "" for name in SCOPE_HEADERS.get(service, ()))


def probe_headers_of(service: str) -> tuple[str, ...]:
    """The credential headers a precondition read for `service` carries (`PROBE_HEADERS`)."""
    return PROBE_HEADERS.get(service, DEFAULT_PROBE_HEADERS)


# How each service spells "this is the same write as before", by service name. Slack has no
# entry and no store: it ships no idempotency mechanism, and `chat.postMessage` sent twice really
# is two messages. A service absent here skips the store entirely rather than getting a default.
IDEMPOTENCY: dict[str, Idempotency] = {stripe.SERVICE: stripe.IDEMPOTENCY}


def check(index: MapIndex) -> None:
    """Refuse maps these tables do not agree with. Raises MapError naming the file.

    The tables key on the maps' service and operation names, and a route names its
    `precondition:` and `fixture:` by name, so a rename or a typo on either side would silently
    switch an effect, a check or a fixture off. Called once, when the maps load.
    """
    by_name = {sm.service: sm for sm in index.services}
    for service, operations in OPERATIONS.items():
        sm = by_name.get(service)
        if sm is None:
            raise MapError(
                f"the service maps define no service {service!r}, which irimi's built-in "
                "tables key on"
            )
        missing = sorted(operations - {route.operation for route in sm.routes})
        if missing:
            raise MapError(
                f"{sm.source}: no route names operation {missing[0]!r}, which irimi's "
                f"{service} tables key on"
            )
    for sm in index.services:
        for route in sm.routes:
            where = f"{sm.source}: route {route.method} {route.path}"
            if route.precondition and (sm.service, route.precondition) not in PRECONDITIONS:
                raise MapError(
                    f"{where}: `precondition: {route.precondition}` names no check irimi has "
                    f"for {sm.service}"
                )
            if route.fixture and route.fixture not in fixture.objects(sm.service):
                raise MapError(
                    f"{where}: `fixture: {route.fixture}` names no object in "
                    f"fixtures/{sm.service}.json"
                )
