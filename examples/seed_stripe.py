"""Create one refundable test-mode charge for the examples to refund.

    python -m examples.seed_stripe

Run it outside irimi: under `irimi shadow` the payment would be faked and no charge would exist.
Needs `STRIPE_API_KEY`, a test-mode key (`sk_test_...`); it refuses any other. Prints
`seeded charge <ch_...> (<pi_...>, 4900 usd)`.
"""

import os
import sys

import stripe

AMOUNT_MINOR = 4900
CURRENCY = "usd"
TEST_KEY_PREFIX = "sk_test_"


def main() -> int:
    if os.environ.get("IRIMI_ENGINE_ACTIVE") == "1":
        print("error: run the seed outside irimi, or the charge is faked", file=sys.stderr)
        return 2
    key = os.environ.get("STRIPE_API_KEY", "")
    if not key.startswith(TEST_KEY_PREFIX):
        print(
            f"error: STRIPE_API_KEY must be a test-mode key ({TEST_KEY_PREFIX}...)",
            file=sys.stderr,
        )
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
