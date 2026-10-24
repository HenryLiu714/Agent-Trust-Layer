"""A refund agent on stripe-python: refund a charge, read it back, retry, and print the refusal.

Run bare, the refund is real. Run under `irimi shadow`, irimi fakes the refund, shows it to the two
reads after it, and refuses the retry as Stripe would; Stripe never hears of the refund.

    python -m examples.seed_stripe                       # a test-mode charge to refund
    irimi shadow -- python -m examples.quickstart.agent

Needs `STRIPE_API_KEY`, a test-mode key (`sk_test_...`); it refuses any other.
"""

import os
import sys
from collections.abc import Iterable, Mapping
from typing import Any

import stripe

from irimi import sdk

TEST_KEY_PREFIX = "sk_test_"


def connect(env: Mapping[str, str]) -> None:
    """Set stripe-python up from `env`, refusing a key that is not test mode."""
    key = env.get("STRIPE_API_KEY", "")
    if not key.startswith(TEST_KEY_PREFIX):
        raise ValueError(f"STRIPE_API_KEY must be a test-mode key ({TEST_KEY_PREFIX}...)")
    stripe.api_key = key
    stripe.api_base = env.get("STRIPE_API_BASE", "https://api.stripe.com")
    if env.get("IRIMI_ENGINE_ACTIVE") == "1":
        # stripe-python checks certificates against the bundle it ships, never SSL_CERT_FILE.
        # Base-URL mode is the other way in: stripe.api_base = f"{HTTPS_PROXY}/api.stripe.com".
        stripe.proxy = env["HTTPS_PROXY"]
        stripe.ca_bundle_path = env["SSL_CERT_FILE"]


def pick_charge(charges: Iterable[Any]) -> Any:
    """Return the first paid charge with any amount left to refund, or None."""
    for charge in charges:
        if charge.status == "succeeded" and charge.paid and charge.amount > charge.amount_refunded:
            return charge
    return None


def money(amount: int, currency: str) -> str:
    """Minor units as an amount: `money(4900, "usd")` is `49.00 USD`."""
    return f"{amount / 100:.2f} {currency.upper()}"


@sdk.trigger
def refund_one_charge() -> str | None:
    """Refund the first refundable charge in full, read it back twice, and retry the refund.

    Returns the code Stripe refused the retry with, or None when the retry went through.
    """
    charge = pick_charge(stripe.Charge.list(limit=10).data)
    if charge is None:
        raise LookupError("no refundable charge: run `python -m examples.seed_stripe` first")
    left = charge.amount - charge.amount_refunded
    print(f"charge {charge.id}: {money(left, charge.currency)} left to refund")

    # In full: only a charge refunded in full refuses the retry below.
    refund = stripe.Refund.create(charge=charge.id, amount=left)
    print(f"refund {refund.id}: {money(left, charge.currency)}")
    listed = [r.id for r in stripe.Refund.list(charge=charge.id, limit=10).data]
    print(f"refunds on {charge.id}: {', '.join(listed) or 'none'}")
    reread = stripe.Charge.retrieve(charge.id)
    print(f"charge {charge.id}: {money(reread.amount_refunded, charge.currency)} refunded")

    try:
        stripe.Refund.create(charge=charge.id, amount=left)
    except stripe.StripeError as exc:
        print(f"retry refused: {exc.code}")
        return exc.code
    print("retry accepted")
    return None


def main() -> int:
    try:
        connect(os.environ)
        refund_one_charge()
    except (ValueError, LookupError, stripe.StripeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
