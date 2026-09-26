"""The L3 checks as pure functions: the probe each write needs, and the verdict on its answer (#45).

Nothing here issues a read - that is the policy's. These pin what a check makes of a document it is
handed: it never rejects on one it did not understand, and it never passes on one either. A verdict
has three answers, and `NOT_EVALUABLE` is the one that says irimi asked and did not find out (#45).
"""

import pytest

from irimi import services
from irimi.services import NOT_EVALUABLE, Probe, Proposal, slack, stripe

CHARGE_ID = "ch_3QTESTONLY000000"


def _refund(**posted):
    return Proposal("refunds.create", {"charge": CHARGE_ID, **posted})


def _charge(**fields):
    return {
        "id": CHARGE_ID,
        "object": "charge",
        "amount": 4900,
        "amount_refunded": 0,
        "refunded": False,
        "currency": "usd",
        **fields,
    }


def _post(channel):
    return Proposal("chat.postMessage", {"channel": channel, "text": "hi"})


def _info(**channel):
    return {"ok": True, "channel": {"id": "C0123", **channel}}


# ------------------------------------------------------------------------------------ the table


def test_the_table_names_both_checks():
    assert services.PRECONDITIONS[("stripe", "charge_refundable")] is stripe.CHARGE_REFUNDABLE
    assert services.PRECONDITIONS[("slack", "channel_postable")] is slack.CHANNEL_POSTABLE


# ------------------------------------------------------------------------------------ stripe


def test_a_refund_probes_the_charge_it_names():
    assert stripe.CHARGE_REFUNDABLE.probe(_refund(amount=100)) == Probe(
        operation="charges.retrieve", method="GET", path=f"/v1/charges/{CHARGE_ID}"
    )


def test_a_refund_of_a_fully_refunded_charge_is_charge_already_refunded():
    document = _charge(amount_refunded=4900, refunded=True)
    rejection = stripe.CHARGE_REFUNDABLE.verdict(_refund(amount=4900), document)
    assert rejection is not None
    assert rejection.status == 400
    assert rejection.code == "charge_already_refunded"
    assert rejection.body == {
        "error": {
            "code": "charge_already_refunded",
            "doc_url": "https://stripe.com/docs/error-codes/charge-already-refunded",
            "message": f"Charge {CHARGE_ID} has already been refunded.",
            "type": "invalid_request_error",
        }
    }


def test_a_refund_over_the_remaining_amount_is_stripes_amount_error_with_irimis_label():
    document = _charge(amount_refunded=4000)
    rejection = stripe.CHARGE_REFUNDABLE.verdict(_refund(amount=1000), document)
    assert rejection is not None
    assert rejection.status == 400
    assert rejection.code == "amount_too_large"
    error = rejection.body["error"]
    assert error["type"] == "invalid_request_error"
    assert error["param"] == "amount"
    # Stripe sends no `code` for this one, so the modeled body carries none either.
    assert "code" not in error
    assert error["message"] == (
        "Refund amount (1000 USD) is greater than unrefunded amount on charge (900 USD)"
    )


@pytest.mark.parametrize("amount", [900, 1, None])
def test_a_refund_within_the_remaining_amount_passes(amount):
    posted = {} if amount is None else {"amount": amount}
    assert (
        stripe.CHARGE_REFUNDABLE.verdict(_refund(**posted), _charge(amount_refunded=4000)) is None
    )


def test_a_boolean_amount_is_not_an_amount():
    # `True` is an `int` in Python, and read as 1 it would be over the remaining 0. A bool is never
    # minor units.
    document = _charge(amount=0)
    assert stripe.CHARGE_REFUNDABLE.verdict(_refund(amount=True), document) is None


@pytest.mark.parametrize(
    "fields",
    [{"amount": None}, {"amount": "4900"}, {"amount_refunded": None}],
    ids=["no-amount", "string-amount", "null-amount-refunded"],
)
def test_a_charge_whose_amounts_are_not_numbers_is_not_evaluable(fields):
    # Read as 0, a missing charge amount would make every refund "too large": a false rejection.
    # Passing instead would claim a comparison irimi could not make, so it says neither (#45).
    document = _charge(**fields)
    assert stripe.CHARGE_REFUNDABLE.verdict(_refund(amount=100), document) is NOT_EVALUABLE


def test_a_refund_posting_no_amount_passes_because_stripe_refunds_what_is_left():
    """Absent `amount` is not an unreadable one: it is Stripe's "refund the remainder", which is
    never over the remaining amount (#45)."""
    assert stripe.CHARGE_REFUNDABLE.verdict(_refund(), _charge(amount_refunded=100)) is None


def test_a_refund_naming_only_a_payment_intent_has_no_probe():
    proposal = Proposal("refunds.create", {"payment_intent": "pi_123", "amount": 100})
    assert stripe.CHARGE_REFUNDABLE.probe(proposal) is None


@pytest.mark.parametrize("charge", ["ch_1/../customers", "ch_1?expand[]=x", "", 42, None])
def test_a_charge_that_would_leave_the_path_or_is_not_an_id_has_no_probe(charge):
    proposal = Proposal("refunds.create", {"charge": charge})
    assert stripe.CHARGE_REFUNDABLE.probe(proposal) is None


@pytest.mark.parametrize(
    "document",
    [
        {"id": "cus_1", "object": "customer", "refunded": True, "amount_refunded": 4900},
        {"id": CHARGE_ID, "object": "charge", "refunded": True, "amount": 4900},
        ["not", "a", "charge"],
        None,
    ],
    ids=["not-a-charge", "no-amount-refunded", "a-list", "none"],
)
def test_a_document_irimi_did_not_understand_is_neither_rejected_nor_passed(document):
    """A false rejection is the untruth this path exists to prevent; a false pass is the other
    half of it, because the summary prints `L3 preconditions passed` off one (#45)."""
    assert stripe.CHARGE_REFUNDABLE.verdict(_refund(amount=99999), document) is NOT_EVALUABLE


def test_a_charge_names_the_currency_its_refund_is_denominated_in():
    """#60. The one fact this read carries that is not a verdict: the summary formats `4900` as
    `$49.00` only from a currency irimi really read, and the charge is where it read it."""
    assert stripe.CHARGE_REFUNDABLE.currency is not None
    assert stripe.CHARGE_REFUNDABLE.currency(_charge()) == "usd"
    assert stripe.CHARGE_REFUNDABLE.currency(_charge(currency=" jpy ")) == "jpy"


@pytest.mark.parametrize(
    "document",
    [
        {"id": CHARGE_ID, "object": "charge", "amount": 4900},  # no currency at all
        {"id": CHARGE_ID, "object": "charge", "currency": 4900},  # not a string
        {"id": CHARGE_ID, "object": "charge", "currency": ""},  # empty
        ["not", "an", "object"],
        None,
    ],
)
def test_a_charge_that_names_no_currency_offers_none(document):
    """No coercion: a currency irimi is not sure of is no currency, and the amount then prints as
    the write sent it. A wrong symbol is worse than an unformatted number (`report.money`)."""
    assert stripe.CHARGE_REFUNDABLE.currency(document) == ""


def test_a_slack_post_has_no_currency_to_offer():
    """`conversations.info` has no such key, and a post has no amount. The field is None rather
    than a function returning "", so the policy asks nothing of a check that has nothing (#60)."""
    assert slack.CHANNEL_POSTABLE.currency is None


# ------------------------------------------------------------------------------------ slack


def test_a_post_to_an_id_shaped_channel_probes_conversations_info():
    assert slack.CHANNEL_POSTABLE.probe(_post("C0123")) == Probe(
        operation="conversations.info",
        method="POST",
        path="/api/conversations.info",
        posted={"channel": "C0123"},
    )


@pytest.mark.parametrize("channel", ["#general", "general", "c0123", "C0123\n", "", None, 7])
def test_a_channel_not_spelled_as_an_id_has_no_probe(channel):
    assert slack.CHANNEL_POSTABLE.probe(_post(channel)) is None


def test_channel_not_found_rejects_with_the_envelope():
    rejection = slack.CHANNEL_POSTABLE.verdict(
        _post("C0123"), {"ok": False, "error": "channel_not_found"}
    )
    assert rejection is not None
    assert (rejection.status, rejection.code) == (200, "channel_not_found")
    assert rejection.body == {"ok": False, "error": "channel_not_found"}


def test_an_archived_channel_rejects_with_the_envelope():
    rejection = slack.CHANNEL_POSTABLE.verdict(
        _post("C0123"), _info(is_channel=True, is_archived=True, is_member=True)
    )
    assert rejection is not None
    assert (rejection.status, rejection.code) == (200, "is_archived")
    assert rejection.body == {"ok": False, "error": "is_archived"}


@pytest.mark.parametrize("kind", ["is_channel", "is_group"])
def test_a_channel_the_bot_is_not_in_rejects_with_the_envelope(kind):
    rejection = slack.CHANNEL_POSTABLE.verdict(
        _post("C0123"), _info(is_member=False, **{kind: True})
    )
    assert rejection is not None
    assert (rejection.status, rejection.code) == (200, "not_in_channel")
    assert rejection.body == {"ok": False, "error": "not_in_channel"}


def test_a_dm_with_is_member_false_passes():
    document = _info(is_im=True, is_member=False)
    assert slack.CHANNEL_POSTABLE.verdict(_post("D0123"), document) is None


def test_is_member_absent_is_not_evidence_and_passes():
    assert slack.CHANNEL_POSTABLE.verdict(_post("C0123"), _info(is_channel=True)) is None


@pytest.mark.parametrize("error", ["ratelimited", "missing_scope", "invalid_auth"])
def test_an_error_irimi_does_not_model_is_not_evaluable(error):
    """#45 names a missing scope as the `not_evaluable` case, and Slack sends one at HTTP 200 -
    so the policy's own non-200 rule can never catch it and the verdict must say so."""
    document = {"ok": False, "error": error}
    assert slack.CHANNEL_POSTABLE.verdict(_post("C0123"), document) is NOT_EVALUABLE


def test_a_success_envelope_with_no_channel_object_is_not_evaluable():
    assert slack.CHANNEL_POSTABLE.verdict(_post("C0123"), {"ok": True}) is NOT_EVALUABLE


def test_a_healthy_channel_passes():
    document = _info(is_channel=True, is_member=True, is_archived=False)
    assert slack.CHANNEL_POSTABLE.verdict(_post("C0123"), document) is None
