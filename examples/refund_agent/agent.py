"""Minimal refund agent — the Phase 1 fixture for `irimi shadow`.

It does two things an agent does: a live read (list charges) and a consequential write (refund
one of them). Run it bare and the refund is real; run it under `irimi shadow` and the refund is
answered locally and never reaches Stripe.

    uv run --with stripe python examples/refund_agent/seed.py        # once per test account
    uv run --with stripe python examples/refund_agent/agent.py       # real refund
    uv run --with stripe irimi shadow -- python examples/refund_agent/agent.py   # no refund

Test mode only: it refuses any key that is not `sk_test_`. The README GIF is recorded elsewhere,
on a live account with a restricted key.
"""

import os
import sys
from collections.abc import Mapping, Sequence
from typing import Any

STRIPE_HOST = "api.stripe.com"

# $1.00. Small and fixed so the same seeded charge stays refundable across several real runs.
REFUND_AMOUNT_MINOR = 100


def door_base(env: Mapping[str, str], host: str) -> str | None:
    """The irimi reverse-door base URL for `host`, or None when irimi is not running this process.

    stripe-python ships its own CA bundle and ignores HTTPS_PROXY, so it cannot go through the
    forward proxy; it goes through the door instead. `irimi shadow` sets IRIMI_ENGINE_ACTIVE=1 and
    HTTPS_PROXY=http://127.0.0.1:<port>, and the door lives on that same listener, so the base URL
    is the proxy origin with the upstream host as the first path segment. Deriving it this way
    keeps `--port` working. `irimi doctor` will print these lines for you later (issue #23):

        stripe.api_base              = http://127.0.0.1:4000/api.stripe.com
        stripe.upload_api_base       = http://127.0.0.1:4000/files.stripe.com
        stripe.connect_api_base      = http://127.0.0.1:4000/connect.stripe.com
        stripe.meter_events_api_base = http://127.0.0.1:4000/meter-events.stripe.com
    """
    if env.get("IRIMI_ENGINE_ACTIVE") != "1":
        return None
    proxy = env.get("HTTPS_PROXY") or env.get("https_proxy")
    if not proxy:
        return None
    return f"{proxy.rstrip('/')}/{host}"


def field(obj: Any, name: str) -> Any:
    """One attribute of a StripeObject, or None when it is absent.

    A shadowed write is answered `{}` today (issue #11 mints a real `re_...` object), so every
    field of the refund is missing and plain attribute access would raise AttributeError. `.get()`
    is not an option: StripeObject rejects it.
    """
    try:
        return getattr(obj, name)
    except AttributeError:
        return None


def pick_charge(charges: Sequence[Any]) -> Any:
    """The first charge that can still take a REFUND_AMOUNT_MINOR refund, or None."""
    for charge in charges:
        if (
            field(charge, "status") == "succeeded"
            and field(charge, "paid")
            and not field(charge, "refunded")
            and (field(charge, "amount") or 0) - (field(charge, "amount_refunded") or 0)
            >= REFUND_AMOUNT_MINOR
        ):
            return charge
    return None


def money(amount_minor: int, currency: str) -> str:
    """Minor units as a human amount: `money(4900, "usd")` -> `49.00 USD`."""
    return f"{amount_minor / 100:.2f} {currency.upper()}"


def post_to_slack(text: str) -> None:
    """Optional second write, so the Slack map gets exercised too. Silent unless both
    SLACK_BOT_TOKEN and SLACK_CHANNEL are set. slack_sdk reads HTTPS_PROXY and SSL_CERT_FILE on its
    own, so this goes through the forward proxy with no extra arguments."""
    token = os.environ.get("SLACK_BOT_TOKEN")
    channel = os.environ.get("SLACK_CHANNEL")
    if not token or not channel:
        return
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    try:
        response = WebClient(token=token).chat_postMessage(channel=channel, text=text)
    except SlackApiError as exc:
        print(f"warning: Slack post failed: {exc}", file=sys.stderr)
        return
    print(f"slack: posted to {channel} (ts {response.get('ts')})")


def main() -> int:
    import stripe

    key = os.environ.get("STRIPE_API_KEY")
    if not key:
        print(
            "error: set STRIPE_API_KEY to a Stripe test-mode key (sk_test_...).",
            file=sys.stderr,
        )
        return 2
    if not key.startswith("sk_test_"):
        print(
            "error: STRIPE_API_KEY is not a test-mode key (sk_test_...). Refusing to run.",
            file=sys.stderr,
        )
        return 2
    stripe.api_key = key

    base = door_base(os.environ, STRIPE_HOST)
    if base is None:
        print("stripe: talking to api.stripe.com directly - a refund here is REAL.")
    else:
        stripe.api_base = base
        print(f"irimi: run {os.environ.get('IRIMI_RUN', '?')} - stripe.api_base = {base}")

    try:
        charges = stripe.Charge.list(limit=10)
    except stripe.StripeError as exc:
        print(f"error: could not list charges: {exc}", file=sys.stderr)
        return 1

    charge = pick_charge(charges.data)
    if charge is None:
        print(
            "error: no refundable charge in this account. Run "
            "`python examples/refund_agent/seed.py` first, outside irimi shadow.",
            file=sys.stderr,
        )
        return 3
    print(f"read:  charge {charge.id} - {money(charge.amount, charge.currency)}")

    try:
        refund = stripe.Refund.create(charge=charge.id, amount=REFUND_AMOUNT_MINOR)
    except stripe.StripeError as exc:
        print(f"error: could not create the refund: {exc}", file=sys.stderr)
        return 1

    refund_id = field(refund, "id")
    print(
        f"write: refund {money(REFUND_AMOUNT_MINOR, charge.currency)} on {charge.id} -> "
        f"{refund_id or '(no id - answered by irimi at L0)'}"
    )
    post_to_slack(f"refunded {money(REFUND_AMOUNT_MINOR, charge.currency)} on {charge.id}")
    print(f"AGENT-RESULT charge={charge.id} refund={refund_id or '-'} amount={REFUND_AMOUNT_MINOR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
