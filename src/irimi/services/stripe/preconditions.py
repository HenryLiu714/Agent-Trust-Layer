"""Stripe's L3 precondition: whether Stripe would have accepted a refund at all.

It is decided against the charge the refund names. The charge is the OVERLAID one: the policy
fetches it live and applies this run's own faked refunds to it with `effects.apply_read` before
`_refund_verdict` sees it. That is the point of checking it here rather than trusting the live body
alone - a second refund of a charge the run already refunded in full is one Stripe would refuse,
and the live charge cannot know about the first, because the first was never made.
"""

from typing import Any

from irimi.bodies import is_int
from irimi.services.model import (
    NOT_EVALUABLE,
    Check,
    NotEvaluable,
    Probe,
    Proposal,
    Rejection,
)
from irimi.services.stripe.effects import minor_units

# Stripe sends no `code` for the amount-exceeds error - the message and `param: amount` are the
# whole of it - so this is irimi's own label for the summary line, and it is deliberately NOT
# written into the modeled body.
AMOUNT_TOO_LARGE = "amount_too_large"
CHARGE_ALREADY_REFUNDED = "charge_already_refunded"


def _refund_probe(proposal: Proposal) -> Probe | None:
    """`GET /v1/charges/{charge}` for the charge this refund names.

    A refund posted with `payment_intent` and no `charge` is not checked: resolving the intent to
    its charge would take a second read and a second modeled object, neither of which irimi
    models, and guessing would be worse than saying so.
    """
    charge = proposal.posted.get("charge")
    # Not URL-quoted: a Stripe id is `[A-Za-z0-9_]` only, and a value that is not is one the real
    # service would refuse. One that would change the path or start a query is not probed at all.
    if not isinstance(charge, str) or not charge or "/" in charge or "?" in charge:
        return None
    return Probe(operation="charges.retrieve", params={"charge": charge})


def _charge_currency(document: Any) -> str:
    """The currency a charge is denominated in, or "" when this body does not name one.

    The document is the L3 precondition read's - `GET /v1/charges/{id}`, issued on the agent's own
    credentials - so this is a code irimi really read off real state and never a default. Anything
    that is not a non-empty string is "": the summary formats money only when it has a currency, and
    a wrong symbol is worse than an unformatted amount. No coercion and no `str()`, for the same
    reason `_refund_verdict` refuses to subtract numbers it cannot read.
    """
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
        # irimi never rejects on a body it did not understand: a false rejection is the untruth
        # this whole path exists to prevent. Nor does it pass on one - that would claim the write
        # was checked against a charge irimi never read. Saying so is `NOT_EVALUABLE`.
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
    # Both sides of the subtraction must be the charge's own numbers. `minor_units` reads a
    # missing or malformed one as 0, which would make every refund "too large" - a false rejection.
    if not (is_int(charged) and is_int(refunded)):
        return NOT_EVALUABLE
    if not is_int(amount):
        # A refund posting no `amount` is Stripe's "refund whatever is left", which is never over
        # the remaining amount; one posting a malformed amount is the real service's to refuse,
        # not irimi's to guess at. Neither is a failure to evaluate the charge.
        return None
    remaining = minor_units(charged) - minor_units(refunded)
    if minor_units(amount) > remaining:
        # The prose is irimi's: minor-unit formatting lives in `report`, and this package stays
        # pure and below it. The SHAPE is Stripe's - `invalid_request_error` with `param: amount` -
        # which is what makes stripe-python raise `InvalidRequestError`.
        # The same reading of the charge's currency the exchange carries, so the prose in this
        # body and the summary's `$49.00` can never disagree about one charge.
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
