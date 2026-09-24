"""Per-service L2 effects and the tables that name them (#43).

One module per service holding what a faked write does to a later read, as plain functions over
plain data. The overlay decodes the write log and looks the service up here; nothing in this
package knows about mitmproxy, exchanges, flows or maps, which is what lets Phase 5 replay run
the same functions over a recording (use case 1).
"""

from irimi.services import stripe
from irimi.services.model import Applied, QueryRewrite, ReadEffects, Rewritten, Write

__all__ = [
    "Applied",
    "EFFECTS",
    "QueryRewrite",
    "ReadEffects",
    "REWRITES",
    "Rewritten",
    "SCOPE_HEADERS",
    "Write",
]

# What a service's faked writes do to its live reads, by service name (a map's `service:`).
EFFECTS: dict[str, ReadEffects] = {stripe.SERVICE: stripe.apply_read}
# What a service translates in a read request before it is forwarded. Not every service has one.
REWRITES: dict[str, QueryRewrite] = {stripe.SERVICE: stripe.rewrite_query}
# The request headers that scope a service's state. A write made against one Stripe connected
# account, or one API version, says nothing about a read made against another, so the overlay
# only replays writes whose values match the read's. Absent on both sides is a match.
SCOPE_HEADERS: dict[str, tuple[str, ...]] = {stripe.SERVICE: ("stripe-account", "stripe-version")}
