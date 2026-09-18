"""Create one refundable test-mode charge, so `agent.py` has something to refund.

    uv run --with stripe python examples/refund_agent/seed.py

Run it OUTSIDE `irimi shadow`: under shadow the payment would be faked and no charge would exist.
"""

import os
import sys

AMOUNT_MINOR = 4900
CURRENCY = "usd"


def main() -> int:
    import stripe

    if os.environ.get("IRIMI_ENGINE_ACTIVE") == "1":
        print(
            "error: seed.py must run outside `irimi shadow` - under shadow the charge would be "
            "faked and nothing would exist to refund.",
            file=sys.stderr,
        )
        return 2
    key = os.environ.get("STRIPE_API_KEY")
    if not key or not key.startswith("sk_test_"):
        print("error: set STRIPE_API_KEY to a Stripe test-mode key (sk_test_...).", file=sys.stderr)
        return 2
    stripe.api_key = key

    try:
        intent = stripe.PaymentIntent.create(
            amount=AMOUNT_MINOR,
            currency=CURRENCY,
            payment_method="pm_card_visa",
            confirm=True,
            automatic_payment_methods={"enabled": True, "allow_redirects": "never"},
        )
    except stripe.StripeError as exc:
        print(f"error: could not create the test charge: {exc}", file=sys.stderr)
        return 1

    print(f"seeded charge {intent.latest_charge} ({intent.id}, {AMOUNT_MINOR} {CURRENCY})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
