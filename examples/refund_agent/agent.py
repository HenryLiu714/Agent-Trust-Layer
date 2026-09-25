"""Minimal refund agent — the fixture for `irimi shadow`, and Phase 2's exit run.

It does what an agent does around one consequential write: a live read (list charges), the write
(refund one of them, in full), and then the three things an agent does after a write:

1. lists the charge's refunds and looks for its own;
2. re-reads the charge and prints `amount_refunded`;
3. retries the same refund, and notices when it is refused.

Run it bare and the refund is real, and real Stripe refuses the retry. Run it under `irimi shadow`
and the refund is answered locally and never reaches Stripe, the two reads after it are shown the
refund anyway, and irimi refuses the retry the way Stripe would have.

    uv sync --group examples                                          # installs the SDKs
    uv run python examples/refund_agent/seed.py                       # again after each bare run
    uv run python examples/refund_agent/agent.py                      # real refund
    uv run irimi shadow -- python examples/refund_agent/agent.py      # no refund

Test mode only: it refuses any key that is not `sk_test_`. The README GIF is recorded elsewhere,
on a live account with a restricted key.
"""

import os
import sys
from collections.abc import Mapping, Sequence
from typing import Any

STRIPE_HOST = "api.stripe.com"


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

    Since #41 a shadowed Stripe write is answered from a vendored response object, so the fields an
    agent branches on are there. This stays because a route with no `fixture:` is still answered
    with the L0 echo, which carries only the ids the route's map names, and plain attribute access
    on a field it lacks would raise AttributeError. `.get()` is not an option: StripeObject
    rejects it.
    """
    try:
        return getattr(obj, name)
    except AttributeError:
        return None


def pick_charge(charges: Sequence[Any]) -> Any:
    """The first succeeded, paid charge with any unrefunded amount left, or None."""
    for charge in charges:
        if (
            field(charge, "status") == "succeeded"
            and field(charge, "paid")
            and not field(charge, "refunded")
            and (field(charge, "amount") or 0) - (field(charge, "amount_refunded") or 0) > 0
        ):
            return charge
    return None


def money(amount_minor: int, currency: str) -> str:
    """Minor units as a human amount: `money(4900, "usd")` -> `49.00 USD`."""
    return f"{amount_minor / 100:.2f} {currency.upper()}"


def post_to_slack(text: str) -> None:
    """Optional second write, so the Slack map gets exercised too, and the read-back an agent makes
    after it. Silent unless both SLACK_BOT_TOKEN and SLACK_CHANNEL are set. slack_sdk reads
    HTTPS_PROXY and SSL_CERT_FILE on its own, so this goes through the forward proxy with no extra
    arguments.

    The read-back asks `conversations.history` for the channel the POST'S OWN RESPONSE names, as an
    agent would. Set SLACK_CHANNEL to a channel id (`C0123`) for that read to be `overlay: full`:
    `chat.postMessage` accepts `#general` but `conversations.history` requires the id, and irimi's
    faked post echoes back the spelling it was sent, so a post to `#general` is read back as a
    channel irimi cannot match to it (#44).
    """
    token = os.environ.get("SLACK_BOT_TOKEN")
    channel = os.environ.get("SLACK_CHANNEL")
    if not token or not channel:
        return
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    client = WebClient(token=token)
    try:
        response = client.chat_postMessage(channel=channel, text=text)
    except SlackApiError as exc:
        print(f"warning: Slack post failed: {exc}", file=sys.stderr)
        return
    print(f"slack: posted to {channel} (ts {response.get('ts')})")

    posted_in = response["channel"]
    try:
        history = client.conversations_history(channel=posted_in, limit=10)
    except SlackApiError as exc:
        print(f"warning: Slack read-back failed: {exc}", file=sys.stderr)
        return
    seen = any(message.get("text") == text for message in history.get("messages") or [])
    print(f"slack: history on {posted_in} {'shows' if seen else 'does not show'} {text!r}")


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

    # The FULL remaining amount, never a fixed slice of it. Stripe - and irimi's L3 check, which
    # models it - refuses the retry below with `charge_already_refunded` only when the charge reads
    # `refunded: true`, and that is true only once `amount_refunded == amount`. A partial refund
    # leaves the charge refundable, so the retry would be ACCEPTED and there would be no refusal to
    # notice. Refunding in full is also what makes a bare run and a shadowed one agree (#48).
    amount = charge.amount - (field(charge, "amount_refunded") or 0)

    try:
        refund = stripe.Refund.create(charge=charge.id, amount=amount)
    except stripe.StripeError as exc:
        print(f"error: could not create the refund: {exc}", file=sys.stderr)
        return 1

    refund_id = field(refund, "id")
    print(
        f"write: refund {money(amount, charge.currency)} on {charge.id} -> "
        f"{refund_id or '(no id - answered by irimi at L0)'}"
    )

    # `charge` and `limit` are both parameters irimi's overlay knows how to apply, so this read is
    # shown the refund in full rather than flagged partial (#43). A refund missing from the list is
    # printed, not failed on: a bare run against a busy account may legitimately page it off.
    try:
        refunds = stripe.Refund.list(charge=charge.id, limit=10)
    except stripe.StripeError as exc:
        print(f"error: could not list refunds: {exc}", file=sys.stderr)
        return 1
    listed = refund_id is not None and any(field(r, "id") == refund_id for r in refunds.data)
    print(f"read:  refunds on {charge.id} {'include' if listed else 'do not include'} {refund_id}")

    try:
        reread = stripe.Charge.retrieve(charge.id)
    except stripe.StripeError as exc:
        print(f"error: could not re-read the charge: {exc}", file=sys.stderr)
        return 1
    refunded = field(reread, "amount_refunded") or 0
    print(f"read:  charge {charge.id} - {money(refunded, charge.currency)} refunded")

    # The retry is a second `Refund.create`, so stripe-python sends it with a fresh
    # Idempotency-Key and it is L3 that must refuse it, not the idempotency store replaying the
    # first answer (#46, #48). An agent that never notices a refusal is one whose fakes are lying
    # to it, so a retry that goes through is this agent's failure, loudly.
    retry: str | None = None
    try:
        stripe.Refund.create(charge=charge.id, amount=amount)
    except stripe.StripeError as exc:
        retry = exc.code or "refused"
        print(f"write: retry refused - {retry}")

    post_to_slack(f"refunded {money(amount, charge.currency)} on {charge.id}")
    print(
        f"AGENT-RESULT charge={charge.id} refund={refund_id or '-'} amount={amount} "
        f"listed={'yes' if listed else 'no'} refunded={refunded} retry={retry or 'none'}"
    )
    if retry is None:
        print(
            f"error: the retry of refund {money(amount, charge.currency)} on {charge.id} was "
            "accepted; a charge refunded in full must refuse it.",
            file=sys.stderr,
        )
        return 4
    return 0


if __name__ == "__main__":
    sys.exit(main())
