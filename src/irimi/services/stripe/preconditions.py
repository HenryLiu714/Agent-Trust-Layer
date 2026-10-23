"""Stripe's precondition check: whether Stripe would accept a refund at all.

irimi's check read is the refunded charge, with this run's faked refunds already applied, so a
second refund of a charge the run fully refunded is refused.
"""

from typing import Any

from irimi.core.bodies import is_int
from irimi.services.model import (
    NOT_EVALUABLE,
    Check,
    NotEvaluable,
    Probe,
    Proposal,
    Rejection,
)
from irimi.services.stripe.effects import minor_units

# Stripe sends no `code` for the amount-exceeds error, so this label is irimi's, for the summary
# line only, and stays out of the body.
AMOUNT_TOO_LARGE = "amount_too_large"
CHARGE_ALREADY_REFUNDED = "charge_already_refunded"


def _refund_probe(proposal: Proposal) -> Probe | None:
    """Return the read of the refund's charge; None for a refund naming only a `payment_intent`."""
    charge = proposal.posted.get("charge")
    # A Stripe id is `[A-Za-z0-9_]`, so no quoting; a value that changes the path is not probed.
    if not isinstance(charge, str) or not charge or "/" in charge or "?" in charge:
        return None
    return Probe(operation="charges.retrieve", params={"charge": charge})


def _charge_currency(document: Any) -> str:
    """Return the charge's currency as read, or "" when the body names none; never a default."""
    if not isinstance(document, dict):
        return ""
    currency = document.get("currency")
    return currency.strip() if isinstance(currency, str) else ""


def _refund_verdict(proposal: Proposal, document: Any) -> Rejection | NotEvaluable | None:
    """`charge_already_refunded`, the amount-exceeds error, `NOT_EVALUABLE`, or None."""
    if (
        not isinstance(document, dict)
        or document.get("object") != "charge"
        or "amount_refunded" not in document
    ):
        # An unreadable charge neither rejects nor passes.
        return NOT_EVALUABLE
    charge_id = proposal.posted.get("charge")
    if document.get("refunded") is True:
        return Rejection(
            status=400,
            code=CHARGE_ALREADY_REFUNDED,
            body={
                "error": {
                    "code": CHARGE_ALREADY_REFUNDED,
                    "doc_url": "https://stripe.com/docs/error-codes/charge-already-refunded",
                    "message": f"Charge {charge_id} has already been refunded.",
                    "type": "invalid_request_error",
                }
            },
        )
    amount = proposal.posted.get("amount")
    charged, refunded = document.get("amount"), document.get("amount_refunded")
    # `minor_units` reads a malformed number as 0, which would reject every refund.
    if not (is_int(charged) and is_int(refunded)):
        return NOT_EVALUABLE
    if not is_int(amount):
        # No `amount` refunds whatever is left; a malformed one is Stripe's to refuse.
        return None
    remaining = minor_units(charged) - minor_units(refunded)
    if minor_units(amount) > remaining:
        # `invalid_request_error` with `param: amount` makes stripe-python raise
        # `InvalidRequestError`; the message wording is irimi's.
        currency = _charge_currency(document).upper()
        message = (
            f"Refund amount ({amount} {currency}) is greater than unrefunded amount on charge "
            f"({remaining} {currency})"
        )
        return Rejection(
            status=400,
            code=AMOUNT_TOO_LARGE,
            body={
                "error": {"message": message, "param": "amount", "type": "invalid_request_error"}
            },
        )
    return None


CHARGE_REFUNDABLE = Check(probe=_refund_probe, verdict=_refund_verdict, currency=_charge_currency)
