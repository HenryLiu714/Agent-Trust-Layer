"""Stripe's L2 effects (#43): what a faked write does to a later live read, as pure functions."""

import copy

from irimi.exchange import Request
from irimi.services.model import Read, Rewritten, Write
from irimi.services.stripe import apply_read, rewrite_query


def _refund_write(refund_id="re_MINTED1", charge="ch_REAL1", amount=100, **answer):
    return Write(
        operation="refunds.create",
        posted={"charge": charge, "amount": amount},
        answer={
            "id": refund_id,
            "object": "refund",
            "charge": charge,
            "amount": amount,
            "status": "succeeded",
            "currency": "usd",
            "created": 1700000000,
            **answer,
        },
    )


def _read(path, query="", headers=()):
    return Request(
        method="GET",
        scheme="https",
        host="api.stripe.com",
        port=443,
        path=path,
        query=query,
        headers=tuple(headers),
        body=b"",
    )


def _asked(operation, path, query="", headers=()):
    """A `Read` for a Stripe GET: its parameters are in the query string and it posts nothing."""
    return Read(operation=operation, request=_read(path, query, headers), posted={})


def _charge_doc(**extra):
    return {"id": "ch_REAL1", "amount": 4900, "amount_refunded": 0, "refunded": False, **extra}


def _refunds_page(*ids, has_more=False):
    return {"object": "list", "has_more": has_more, "data": [{"id": i} for i in ids]}


def _customer_write(posted, customer_id="cus_1"):
    return Write(
        operation="customers.update",
        posted=posted,
        answer={"id": customer_id, "object": "customer", **posted},
    )


def _cancel_write(posted=None, intent_id="pi_1"):
    return Write(
        operation="payment_intents.cancel",
        posted=posted or {},
        answer={
            "id": intent_id,
            "object": "payment_intent",
            "status": "canceled",
            "created": 1700000000,
        },
    )


def _intent_doc():
    return {
        "id": "pi_1",
        "object": "payment_intent",
        "status": "requires_payment_method",
        "canceled_at": None,
        "cancellation_reason": None,
    }


# ------------------------------------------------------------------------------- the charge


def test_a_refund_updates_amount_refunded_and_refunded_on_the_charge():
    out = apply_read(
        _asked("charges.retrieve", "/v1/charges/ch_REAL1"), _charge_doc(), [_refund_write()]
    )
    assert out.document["amount_refunded"] == 100
    assert out.document["refunded"] is False
    assert out.changed is True
    assert out.partial is False


def test_a_full_refund_sets_refunded_true():
    out = apply_read(
        _asked("charges.retrieve", "/v1/charges/ch_REAL1"),
        _charge_doc(),
        [_refund_write(amount=4900)],
    )
    assert out.document["refunded"] is True
    assert out.changed is True
    assert out.partial is False


def test_two_refunds_on_one_charge_add_up():
    writes = [_refund_write("re_MINTED1"), _refund_write("re_MINTED2")]
    out = apply_read(_asked("charges.retrieve", "/v1/charges/ch_REAL1"), _charge_doc(), writes)
    assert out.document["amount_refunded"] == 200
    assert out.changed is True
    assert out.partial is False


def test_a_refund_for_another_charge_does_not_touch_this_one():
    charge = _charge_doc()
    before = copy.deepcopy(charge)
    out = apply_read(
        _asked("charges.retrieve", "/v1/charges/ch_REAL1"),
        charge,
        [_refund_write(charge="ch_OTHER")],
    )
    assert out.changed is False
    assert out.partial is False
    assert out.document == before


def test_an_expanded_refunds_list_gets_the_minted_refund_first_and_a_bigger_total():
    charge = _charge_doc(refunds={"data": [{"id": "re_REAL"}], "total_count": 1})
    out = apply_read(_asked("charges.retrieve", "/v1/charges/ch_REAL1"), charge, [_refund_write()])
    assert out.document["refunds"]["data"][0]["id"] == "re_MINTED1"
    assert out.document["refunds"]["data"][1]["id"] == "re_REAL"
    assert out.document["refunds"]["total_count"] == 2
    assert out.changed is True
    assert out.partial is False


def test_an_unexpanded_charge_grows_no_refunds_list():
    out = apply_read(
        _asked("charges.retrieve", "/v1/charges/ch_REAL1"), _charge_doc(), [_refund_write()]
    )
    assert "refunds" not in out.document
    assert out.changed is True


def test_every_matching_charge_in_a_list_is_updated():
    other = {"id": "ch_OTHER", "amount": 700, "amount_refunded": 0, "refunded": False}
    document = {"object": "list", "has_more": False, "data": [_charge_doc(), dict(other)]}
    out = apply_read(_asked("charges.list", "/v1/charges"), document, [_refund_write()])
    assert out.document["data"][0]["amount_refunded"] == 100
    assert out.document["data"][1] == other
    assert out.changed is True
    assert out.partial is False


# ------------------------------------------------------------------------- the refunds list


def test_refunds_page_one_puts_the_minted_refund_first():
    out = apply_read(
        _asked("refunds.list", "/v1/refunds"), _refunds_page("re_REAL1"), [_refund_write()]
    )
    assert out.document["data"][0]["id"] == "re_MINTED1"
    assert out.document["data"][1]["id"] == "re_REAL1"
    assert out.document["has_more"] is False
    assert out.changed is True
    assert out.partial is False


def test_a_full_refunds_page_is_truncated_to_its_limit_and_says_there_is_more():
    out = apply_read(
        _asked("refunds.list", "/v1/refunds", query="limit=2"),
        _refunds_page("re_REAL1", "re_REAL2"),
        [_refund_write()],
    )
    assert [r["id"] for r in out.document["data"]] == ["re_MINTED1", "re_REAL1"]
    assert out.document["has_more"] is True
    assert out.changed is True
    assert out.partial is False


def test_a_refunds_page_filtered_by_charge_only_shows_a_matching_minted_refund():
    other = apply_read(
        _asked("refunds.list", "/v1/refunds", query="charge=ch_OTHER"),
        _refunds_page(),
        [_refund_write()],
    )
    assert other.document["data"] == []
    assert other.changed is False
    assert other.partial is False

    mine = apply_read(
        _asked("refunds.list", "/v1/refunds", query="charge=ch_REAL1"),
        _refunds_page("re_REAL1"),
        [_refund_write()],
    )
    assert mine.document["data"][0]["id"] == "re_MINTED1"
    assert mine.changed is True
    assert mine.partial is False


def test_a_filter_the_minted_refund_cannot_answer_is_partial():
    """The refund fixture's `payment_intent` is null, so a refund posted with only `charge` cannot
    say whether it belongs on `?payment_intent=pi_X`. Dropping it and calling the page complete
    hid the agent's own refund; irimi says `partial` instead (#43)."""
    page = _refunds_page("re_REAL1")
    before = copy.deepcopy(page)
    out = apply_read(
        _asked("refunds.list", "/v1/refunds", query="payment_intent=pi_1"),
        page,
        [_refund_write(payment_intent=None)],
    )
    assert out.partial is True
    assert out.changed is False
    assert out.document == before


def test_a_filter_the_minted_refund_does_answer_is_not_partial():
    """`partial` is for what irimi cannot tell, not for what does not match: a minted refund
    carrying a different `payment_intent` really is absent from this page."""
    out = apply_read(
        _asked("refunds.list", "/v1/refunds", query="payment_intent=pi_OTHER"),
        _refunds_page("re_REAL1"),
        [_refund_write(payment_intent="pi_MINE")],
    )
    assert out.partial is False
    assert out.changed is False

    mine = apply_read(
        _asked("refunds.list", "/v1/refunds", query="payment_intent=pi_MINE"),
        _refunds_page("re_REAL1"),
        [_refund_write(payment_intent="pi_MINE")],
    )
    assert mine.document["data"][0]["id"] == "re_MINTED1"
    assert mine.changed is True
    assert mine.partial is False


def test_an_older_refund_that_might_be_behind_the_cursor_is_partial():
    """The page after a minted refund is only fully modelled when irimi knows every older minted
    refund is absent from it. One whose filter it cannot answer might belong here."""
    out = apply_read(
        _asked(
            "refunds.list",
            "/v1/refunds",
            query="payment_intent=pi_1",
            headers=[("irimi-rewrote", "starting_after=re_MINTED2")],
        ),
        _refunds_page("re_REAL1"),
        [_refund_write("re_MINTED1", payment_intent=None), _refund_write("re_MINTED2")],
    )
    assert out.partial is True
    assert out.changed is False


def test_ending_before_is_partial_and_unchanged():
    page = _refunds_page("re_REAL1")
    before = copy.deepcopy(page)
    out = apply_read(
        _asked("refunds.list", "/v1/refunds", query="ending_before=re_REAL2"),
        page,
        [_refund_write()],
    )
    assert out.partial is True
    assert out.changed is False
    assert out.document == before


def test_a_cursor_naming_a_real_refund_is_partial_and_unchanged():
    page = _refunds_page("re_REAL2")
    before = copy.deepcopy(page)
    out = apply_read(
        _asked("refunds.list", "/v1/refunds", query="starting_after=re_REAL1"),
        page,
        [_refund_write()],
    )
    assert out.partial is True
    assert out.changed is False
    assert out.document == before


def test_the_page_after_a_minted_refund_is_unchanged_and_fully_modelled():
    out = apply_read(
        _asked(
            "refunds.list", "/v1/refunds", headers=[("irimi-rewrote", "starting_after=re_MINTED1")]
        ),
        _refunds_page("re_REAL1"),
        [_refund_write()],
    )
    assert out.changed is False
    assert out.partial is False
    assert "re_MINTED1" not in [r["id"] for r in out.document["data"]]


def test_the_page_after_a_minted_refund_with_an_older_one_behind_it_is_partial():
    """re_MINTED2 is newer, so page 1 at `limit=1` held only it; the page after it should start
    with re_MINTED1, which the real list from its top does not carry."""
    page = _refunds_page("re_REAL1")
    before = copy.deepcopy(page)
    out = apply_read(
        _asked(
            "refunds.list",
            "/v1/refunds",
            query="limit=1",
            headers=[("irimi-rewrote", "starting_after=re_MINTED2")],
        ),
        page,
        [_refund_write("re_MINTED1"), _refund_write("re_MINTED2")],
    )
    assert out.partial is True
    assert out.changed is False
    assert out.document == before


def test_an_unknown_list_filter_is_partial():
    out = apply_read(
        _asked("refunds.list", "/v1/refunds", query="created[gte]=1"),
        _refunds_page("re_REAL1"),
        [_refund_write()],
    )
    assert out.partial is True
    assert out.changed is False


def test_expand_is_a_known_list_filter():
    out = apply_read(
        _asked("refunds.list", "/v1/refunds", query="expand[0]=data.charge"),
        _refunds_page("re_REAL1"),
        [_refund_write()],
    )
    assert out.document["data"][0]["id"] == "re_MINTED1"
    assert out.changed is True
    assert out.partial is False


# ------------------------------------------------------------------------- a single refund


def test_retrieving_a_minted_refund_is_partial():
    error = {
        "error": {
            "type": "invalid_request_error",
            "code": "resource_missing",
            "message": "No such refund: 're_MINTED1'",
            "param": "id",
        }
    }
    before = copy.deepcopy(error)
    out = apply_read(_asked("refunds.retrieve", "/v1/refunds/re_MINTED1"), error, [_refund_write()])
    assert out.partial is True
    assert out.changed is False
    assert out.document == before


def test_retrieving_a_real_refund_is_untouched():
    refund = {"id": "re_REAL1", "object": "refund", "amount": 500, "charge": "ch_REAL1"}
    before = copy.deepcopy(refund)
    out = apply_read(_asked("refunds.retrieve", "/v1/refunds/re_REAL1"), refund, [_refund_write()])
    assert out.partial is False
    assert out.changed is False
    assert out.document == before


# ------------------------------------------------------------------------------ the customer


def test_a_customer_update_patches_the_fields_the_customer_has():
    customer = {"id": "cus_1", "name": "Old", "email": None}
    out = apply_read(
        _asked("customers.retrieve", "/v1/customers/cus_1"),
        customer,
        [_customer_write({"name": "New", "nickname": "x"})],
    )
    assert out.document["name"] == "New"
    assert "nickname" not in out.document
    assert out.changed is True
    assert out.partial is False


def test_a_customer_update_merges_metadata():
    customer = {"id": "cus_1", "metadata": {"a": "1"}}
    out = apply_read(
        _asked("customers.retrieve", "/v1/customers/cus_1"),
        customer,
        [_customer_write({"metadata": {"b": "2"}})],
    )
    assert out.document["metadata"] == {"a": "1", "b": "2"}
    assert out.changed is True


def test_a_customer_update_cannot_move_a_service_owned_field():
    customer = {"id": "cus_1", "created": 1600000000, "name": "Old"}
    out = apply_read(
        _asked("customers.retrieve", "/v1/customers/cus_1"),
        customer,
        [
            Write(
                operation="customers.update",
                posted={"id": "cus_EVIL", "created": 1},
                answer={"id": "cus_1", "object": "customer"},
            )
        ],
    )
    assert out.document["id"] == "cus_1"
    assert out.document["created"] == 1600000000
    assert out.changed is False


# ------------------------------------------------------------------------ the payment intent


def test_a_cancel_sets_status_canceled_at_and_reason():
    out = apply_read(
        _asked("payment_intents.retrieve", "/v1/payment_intents/pi_1"),
        _intent_doc(),
        [_cancel_write({"cancellation_reason": "requested_by_customer"})],
    )
    assert out.document["status"] == "canceled"
    assert out.document["canceled_at"] == 1700000000
    assert out.document["cancellation_reason"] == "requested_by_customer"
    assert out.changed is True
    assert out.partial is False


def test_a_cancel_without_a_reason_leaves_cancellation_reason_null():
    out = apply_read(
        _asked("payment_intents.retrieve", "/v1/payment_intents/pi_1"),
        _intent_doc(),
        [_cancel_write()],
    )
    assert out.document["status"] == "canceled"
    assert out.document["cancellation_reason"] is None
    assert out.changed is True


# --------------------------------------------------------------------------------- the rest


def test_an_unmodelled_operation_is_unchanged():
    document = {"id": "txn_1", "object": "balance_transaction", "amount": 100}
    before = copy.deepcopy(document)
    out = apply_read(
        _asked("balance_transactions.retrieve", "/v1/balance_transactions/txn_1"),
        document,
        [_refund_write()],
    )
    assert out.changed is False
    assert out.partial is False
    assert out.document == before


def test_a_body_that_is_not_an_object_is_unchanged():
    out = apply_read(
        _asked("charges.retrieve", "/v1/charges/ch_REAL1"), [1, 2, 3], [_refund_write()]
    )
    assert out.document == [1, 2, 3]
    assert out.changed is False
    assert out.partial is False


# ------------------------------------------------------------------------------- the rewrite


def test_rewrite_drops_a_cursor_naming_a_minted_refund_and_names_what_it_dropped():
    out = rewrite_query(
        _asked("refunds.list", "/v1/refunds", query="limit=1&starting_after=re_MINTED1"),
        [_refund_write()],
    )
    assert out == Rewritten(query="limit=1", removed="starting_after=re_MINTED1")


def test_rewrite_leaves_a_cursor_naming_a_real_refund_alone():
    out = rewrite_query(
        _asked("refunds.list", "/v1/refunds", query="limit=1&starting_after=re_REAL1"),
        [_refund_write()],
    )
    assert out is None


def test_rewrite_ignores_every_operation_but_the_refunds_list():
    out = rewrite_query(
        _asked("charges.list", "/v1/charges", query="starting_after=re_MINTED1"),
        [_refund_write()],
    )
    assert out is None


def test_rewrite_returns_none_when_there_are_no_minted_refunds():
    out = rewrite_query(
        _asked("refunds.list", "/v1/refunds", query="limit=1&starting_after=re_MINTED1"), []
    )
    assert out is None
