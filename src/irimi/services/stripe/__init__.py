"""Stripe: everything irimi's Python knows about it, as one `Service` spec.

Its map is `irimi/maps/stripe.yaml` and its fixture objects are `irimi/fixtures/stripe.json`.
`answers` builds the bodies its faked writes get, `effects` applies those writes to later live
reads, and `preconditions` holds the refund check.
"""

from irimi.services.model import Answer, Idempotency, Rejection, Service
from irimi.services.stripe import answers, effects, preconditions

# Stripe's idempotency header, lower-case as `wire.parse` normalizes it. stripe-python sends
# one on every POST - a fresh UUID when the caller passes none - so every SDK write passes through
# the store.
IDEMPOTENCY_KEY_HEADER = "idempotency-key"
# Unlike `charge_already_refunded`, this one is the body's own `type` and not a `code`: Stripe
# sends no `code` for it, and stripe-python's error factory branches on `type` to raise
# `IdempotencyError`. It is still the machine name the summary prints.
IDEMPOTENCY_ERROR = "idempotency_error"


def _idempotency_conflict(key: str) -> Rejection:
    """What Stripe answers when a key is reused with different parameters.

    The message is Stripe's own wording, because the agent's logs are read by a human who will
    search for it, and the `type` is what makes stripe-python raise `IdempotencyError` rather than
    a generic `APIError`.
    """
    return Rejection(
        status=400,
        code=IDEMPOTENCY_ERROR,
        body={
            "error": {
                "message": (
                    "Keys for idempotent requests can only be used with the same parameters they "
                    f"were first used with. Try using a key other than '{key}' if you meant to "
                    "execute a different request."
                ),
                "type": IDEMPOTENCY_ERROR,
            }
        },
    )


# Stripe marks the answer to a retry `Idempotent-Replayed: true`. Header names are case-insensitive
# on the wire, and every header irimi sets is written lower-case.
IDEMPOTENCY = Idempotency(
    header=IDEMPOTENCY_KEY_HEADER,
    conflict=_idempotency_conflict,
    replayed_header=("idempotent-replayed", "true"),
)

SERVICE = Service(
    name="stripe",
    operations=frozenset(
        {
            "charges.list",
            "charges.retrieve",
            "customers.retrieve",
            "customers.update",
            "payment_intents.cancel",
            "payment_intents.retrieve",
            "refunds.create",
            "refunds.list",
            "refunds.retrieve",
        }
    ),
    answer=Answer(body=answers.body),
    effects=effects.apply_read,
    rewrite=effects.rewrite_query,
    preconditions={"charge_refundable": preconditions.CHARGE_REFUNDABLE},
    idempotency=IDEMPOTENCY,
    scope_headers=("stripe-account", "stripe-version"),
)
