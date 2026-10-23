"""The Stripe `Service` spec; its map is `maps/stripe.yaml` and fixtures `fixtures/stripe.json`."""

from irimi.services.model import Answer, Idempotency, Rejection, Service
from irimi.services.stripe import answers, effects, preconditions

# Lower-case, as `wire.parse` normalizes headers. stripe-python sends one on every POST.
IDEMPOTENCY_KEY_HEADER = "idempotency-key"
# Stripe sends this as the error's `type`, with no `code`; stripe-python raises `IdempotencyError`
# from it.
IDEMPOTENCY_ERROR = "idempotency_error"


def _idempotency_conflict(key: str) -> Rejection:
    """Return Stripe's answer, in its own wording, to a key reused with different parameters."""
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


# Stripe marks the answer to a retry `Idempotent-Replayed: true`.
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
