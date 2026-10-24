"""`idempotency`: the run's answers to keyed writes, so a retry is one write and not two."""

import json
import threading
from dataclasses import replace

import pytest

from irimi import servicemaps
from irimi.core.exchange import Flag, Request
from irimi.decide import idempotency
from irimi.decide.idempotency import Store
from irimi.decide.policy import ShadowPolicy
from irimi.engine import wire
from irimi.servicemaps import Route
from tests.support import builders
from tests.support.builders import FORM_CONTENT_TYPE, FORM_HEADER
from tests.support.decisions import (
    AGENT_HEADERS,
    SHIPPED,
    ask,
    canned_reader,
    charge_document,
    json_response,
    make_request,
    refund_request,
    slack_post_request,
)

STRIPE = servicemaps.MapIndex(tuple(servicemaps.loader.load_shipped())).service_for(
    "api.stripe.com"
)
assert STRIPE is not None
REFUNDS_CREATE = servicemaps.model.match_route(STRIPE, "POST", "/v1/refunds")
assert REFUNDS_CREATE is not None

SLOT = ("7f3a", "stripe", "", "", "k-1")


def _params(**over: object) -> idempotency.Canonical:
    """`REFUND_PARAMS` again as a distinct but equal object, or with a posted field changed."""
    return (*builders.REFUND_PARAMS[:3], {**builders.REFUND_PARAMS[3], **over})


def _request(
    host: str = "api.stripe.com",
    path: str = "/v1/refunds",
    headers: tuple[tuple[str, str], ...] = (),
    body: bytes = b"",
    query: str = "",
) -> Request:
    """Through `wire.parse`, so the header names are lower-cased the way the addon does it."""
    request = wire.parse("POST", "https", host, 443, path, headers, body)
    return request if not query else replace(request, query=query)


# ------------------------------------------------------------------------------- the key itself


def test_the_key_is_the_header_the_service_names():
    request = _request(headers=(("Idempotency-Key", "abc"),))
    assert idempotency.idempotency_key_of("stripe", request) == "abc"


def test_a_service_with_no_idempotency_has_no_key():
    request = _request(
        host="slack.com", path="/api/chat.postMessage", headers=(("Idempotency-Key", "abc"),)
    )
    assert idempotency.idempotency_key_of("slack", request) == ""


def test_a_request_that_sent_no_key_has_none():
    assert idempotency.idempotency_key_of("stripe", _request()) == ""


# --------------------------------------------------------------------- what two sendings compare


def test_canonical_params_drop_the_routes_compare_ignores_names():
    assert REFUNDS_CREATE is not None
    route = replace(REFUNDS_CREATE, compare_ignores=("nonce",))
    request = _request(headers=(FORM_HEADER,), body=b"charge=ch_1&amount=100&nonce=n-1")
    assert idempotency.canonical(request, route) == (
        "POST",
        "/v1/refunds",
        "",
        {"charge": "ch_1", "amount": 100},
    )


def test_canonical_params_keep_everything_else():
    route = Route(method="POST", path="/v1/refunds", operation="refunds.create", kind="write")
    request = _request(headers=(FORM_HEADER,), body=b"charge=ch_1&amount=100&nonce=n-1")
    assert idempotency.canonical(request, route) == (
        "POST",
        "/v1/refunds",
        "",
        {"charge": "ch_1", "amount": 100, "nonce": "n-1"},
    )


def test_canonical_tells_two_objects_on_one_route_apart():
    """Cancels of two intents differ by path, since `payment_intents.cancel` posts nothing."""
    assert STRIPE is not None
    cancel = servicemaps.model.match_route(STRIPE, "POST", "/v1/payment_intents/pi_AAA/cancel")
    assert cancel is not None
    one = idempotency.canonical(_request(path="/v1/payment_intents/pi_AAA/cancel"), cancel)
    two = idempotency.canonical(_request(path="/v1/payment_intents/pi_BBB/cancel"), cancel)
    assert one[3] == two[3] == {}, "neither cancel posts a field to tell them apart"
    assert one != two


def test_canonical_tells_two_routes_with_the_same_empty_body_apart():
    """Two routes that both post nothing differ by path, so a reused key is another write."""
    assert STRIPE is not None
    cancel = servicemaps.model.match_route(STRIPE, "POST", "/v1/payment_intents/pi_AAA/cancel")
    update = servicemaps.model.match_route(STRIPE, "POST", "/v1/customers/cus_ZZZ")
    assert cancel is not None and update is not None
    assert idempotency.canonical(
        _request(path="/v1/payment_intents/pi_AAA/cancel"), cancel
    ) != idempotency.canonical(_request(path="/v1/customers/cus_ZZZ"), update)


def test_canonical_tells_two_sendings_apart_by_their_query():
    """A query parameter is part of a sending's identity, beside the path and the body."""
    route = Route(method="POST", path="/v1/refunds", operation="refunds.create", kind="write")
    plain = idempotency.canonical(_request(), route)
    with_query = idempotency.canonical(_request(query="amount=250"), route)
    assert plain != with_query


# ------------------------------------------------------------------------------------ the store


def test_an_unseen_slot_is_neither_a_replay_nor_a_conflict():
    assert Store().get(SLOT, builders.REFUND_PARAMS) == (None, False)


def test_the_same_slot_and_params_replay_the_stored_answer():
    store = Store()
    stored = builders.first_answer()
    store.put(SLOT, builders.REFUND_PARAMS, stored)
    assert store.get(SLOT, _params()) == (stored, False)


def test_the_same_slot_with_different_params_is_a_conflict():
    store = Store()
    store.put(SLOT, builders.REFUND_PARAMS, builders.first_answer())
    replayed, conflicted = store.get(SLOT, _params(amount=250))
    assert (replayed, conflicted) == (None, True)


def test_the_first_answer_is_the_one_the_run_keeps():
    store = Store()
    first, second = (
        builders.first_answer(b'{"id": "re_FIRST"}'),
        builders.first_answer(b'{"id": "re_SECOND"}'),
    )
    store.put(SLOT, builders.REFUND_PARAMS, first)
    store.put(SLOT, builders.REFUND_PARAMS, second)
    assert store.get(SLOT, builders.REFUND_PARAMS) == (first, False)


def test_two_stores_do_not_share_entries():
    one, two = Store(), Store()
    one.put(SLOT, builders.REFUND_PARAMS, builders.first_answer())
    assert two.get(SLOT, builders.REFUND_PARAMS) == (None, False)


def test_ending_a_run_drops_its_keys_and_no_other_run_s():
    """A long serve holds the keys of the runs still open alone."""
    store = Store()
    other = ("9b2c", *SLOT[1:])
    store.put(SLOT, builders.REFUND_PARAMS, builders.first_answer())
    store.put(other, builders.REFUND_PARAMS, builders.first_answer())
    store.end_run(SLOT[0])
    store.end_run("never-started")
    assert store.get(SLOT, builders.REFUND_PARAMS) == (None, False)
    assert store.get(other, builders.REFUND_PARAMS) == (builders.first_answer(), False)


def test_the_key_separates_runs_accounts_and_versions():
    base = idempotency.slot_id("7f3a", "stripe", ("acct_1", "2024-06-20"), "k-1")
    other_run = idempotency.slot_id("9b2c", "stripe", ("acct_1", "2024-06-20"), "k-1")
    other_account = idempotency.slot_id("7f3a", "stripe", ("acct_2", "2024-06-20"), "k-1")
    other_version = idempotency.slot_id("7f3a", "stripe", ("acct_1", "2025-01-01"), "k-1")
    assert len({base, other_run, other_account, other_version}) == 4


def test_concurrent_puts_of_one_slot_keep_one_answer():
    store = Store()
    barrier = threading.Barrier(20)

    def put(n: int) -> None:
        barrier.wait()
        store.put(
            SLOT, builders.REFUND_PARAMS, builders.first_answer(f'{{"id": "re_{n}"}}'.encode())
        )

    threads = [threading.Thread(target=put, args=(n,)) for n in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    kept = {store.get(SLOT, builders.REFUND_PARAMS) for _ in range(20)}
    assert len(kept) == 1
    stored, conflicted = kept.pop()
    assert stored is not None and conflicted is False


# ------------------------------------------------------------------------------- the refusal


def test_the_stripe_conflict_body_is_the_shape_stripe_python_raises_on():
    refusal = idempotency.conflict("stripe", "k")
    assert refusal is not None
    assert refusal.status == 400
    assert refusal.code == "idempotency_error"
    assert refusal.body["error"]["type"] == "idempotency_error"
    assert "'k'" in refusal.body["error"]["message"]
    assert idempotency.conflict("slack", "k") is None


# ------------------------------------------------------------------ the idempotency store


def _cancel_request(intent: str) -> Request:
    """`payment_intents.cancel`, which carries every parameter it has in its path."""
    return replace(
        make_request("POST", path=f"/v1/payment_intents/{intent}/cancel"), headers=AGENT_HEADERS
    )


def test_the_same_key_and_params_replay_the_first_answer():
    policy = ShadowPolicy(reader=canned_reader(json_response(200, charge_document())), maps=SHIPPED)
    first = ask(policy, refund_request())
    second = ask(policy, refund_request())
    assert second.response.body == first.response.body
    assert second.response.status == first.response.status == 200
    assert second.answered_by == first.answered_by == "fake-L1"
    assert ("idempotent-replayed", "true") in second.response.headers
    assert ("idempotent-replayed", "true") not in first.response.headers


def test_a_replay_carries_the_replay_flag_and_the_original_fidelity_flag():
    policy = ShadowPolicy(reader=canned_reader(json_response(200, charge_document())), maps=SHIPPED)
    first = ask(policy, refund_request())
    second = ask(policy, refund_request())
    assert first.flags == ()
    assert second.flags == (Flag.IDEMPOTENT_REPLAY,)


def test_a_replay_issues_no_precondition_read():
    """A retry makes no second precondition read: the write was checked once, when it was made."""
    reader = canned_reader(json_response(200, charge_document()))
    policy = ShadowPolicy(reader=reader, maps=SHIPPED)
    first = ask(policy, refund_request())
    second = ask(policy, refund_request())
    assert len(reader.asked) == 1
    assert len(first.issued) == 1
    assert second.issued == ()
    assert second.precondition == first.precondition == "passed"
    # The retry keeps the first answer's currency, so both exchanges agree on the one write.
    assert second.currency == first.currency == "usd"


def test_the_same_key_with_a_different_amount_is_an_idempotency_error():
    reader = canned_reader(json_response(200, charge_document()))
    policy = ShadowPolicy(reader=reader, maps=SHIPPED)
    ask(policy, refund_request(b"charge=ch_REAL&amount=100"))
    ans = ask(policy, refund_request(b"charge=ch_REAL&amount=250"))
    assert ans.response.status == 400
    error = json.loads(ans.response.body)["error"]
    assert error["type"] == "idempotency_error"
    assert "idem-1" in error["message"]
    assert Flag.IDEMPOTENCY_CONFLICT in ans.flags
    assert Flag.IDEMPOTENT_REPLAY not in ans.flags
    assert ans.rejection_code == "idempotency_error"
    # No precondition check ran, so a conflict is not `precondition: rejected`.
    assert ans.precondition is None
    assert ans.issued == ()
    assert len(reader.asked) == 1


def test_a_replayed_rejection_is_the_same_rejection():
    """A retry of a refused write is refused again with the same body, without a second check read.

    Stripe keeps the first response under a key whether it was accepted or refused."""
    reader = canned_reader(json_response(200, charge_document(refunded_so_far=4900)))
    policy = ShadowPolicy(reader=reader, maps=SHIPPED)
    first = ask(policy, refund_request())
    second = ask(policy, refund_request())
    assert first.precondition == "rejected"
    assert second.response.body == first.response.body
    assert second.response.status == first.response.status == 400
    assert second.precondition == "rejected"
    assert second.rejection_code == first.rejection_code == "charge_already_refunded"
    assert second.flags == (*first.flags, Flag.IDEMPOTENT_REPLAY)
    assert len(reader.asked) == 1


def test_a_write_that_sent_no_key_is_never_replayed():
    policy = ShadowPolicy()
    body = b"charge=ch_REAL&amount=4900"
    request = make_request("POST", path="/v1/refunds", body=body, content_type=FORM_CONTENT_TYPE)
    first = ask(policy, request)
    second = ask(policy, request)
    assert json.loads(first.response.body)["id"] != json.loads(second.response.body)["id"]
    assert Flag.IDEMPOTENT_REPLAY not in first.flags
    assert Flag.IDEMPOTENT_REPLAY not in second.flags


def test_a_slack_write_is_never_replayed():
    """A Slack write sent twice is two messages, whatever headers it carries: Slack has no keys."""
    policy = ShadowPolicy()
    post = slack_post_request("C0123")
    request = replace(post, headers=(*post.headers, ("idempotency-key", "idem-1")))
    first = ask(policy, request)
    second = ask(policy, request)
    assert json.loads(first.response.body)["ts"] != json.loads(second.response.body)["ts"]
    assert Flag.IDEMPOTENT_REPLAY not in second.flags
    assert Flag.IDEMPOTENCY_CONFLICT not in second.flags


def test_a_key_is_scoped_to_the_stripe_account():
    policy = ShadowPolicy()
    first = ask(policy, refund_request())
    other = tuple(
        (k, "acct_2" if k == "stripe-account" else v) for k, v in refund_request().headers
    )
    second = ask(policy, replace(refund_request(), headers=other))
    assert json.loads(first.response.body)["id"] != json.loads(second.response.body)["id"]
    assert second.response.status == 200
    assert Flag.IDEMPOTENT_REPLAY not in second.flags
    assert Flag.IDEMPOTENCY_CONFLICT not in second.flags


class _BrokenStore:
    """A store whose lookup or whose remembering raises, so each guard is reached on its own."""

    def __init__(self, broken: str):
        self.broken = broken

    def get(self, slot, params):
        if self.broken == "get":
            raise RuntimeError("the store is broken")
        return None, False

    def put(self, slot, params, stored):
        if self.broken == "put":
            raise RuntimeError("the store is broken")


@pytest.mark.parametrize("broken", ["get", "put"])
def test_a_store_that_raises_answers_the_write_afresh(broken):
    """A store that raises costs a retry its stored answer, nothing else (fail-closed hook rule)."""
    policy = ShadowPolicy()
    policy.idempotency = _BrokenStore(broken)  # type: ignore[assignment]
    first = ask(policy, refund_request())
    second = ask(policy, refund_request())
    assert first.response.status == second.response.status == 200
    assert first.answered_by == second.answered_by == "fake-L1"
    assert json.loads(first.response.body)["id"] != json.loads(second.response.body)["id"]
    assert Flag.IDEMPOTENT_REPLAY not in second.flags


def test_a_key_reused_on_another_object_of_one_route_is_refused():
    """A key reused to cancel a second intent is refused, not answered with the first intent."""
    policy = ShadowPolicy()
    first = ask(policy, _cancel_request("pi_AAA"))
    second = ask(policy, _cancel_request("pi_BBB"))
    assert json.loads(first.response.body)["id"] == "pi_AAA"
    assert second.response.status == 400
    assert json.loads(second.response.body)["error"]["type"] == "idempotency_error"
    assert Flag.IDEMPOTENCY_CONFLICT in second.flags
    assert Flag.IDEMPOTENT_REPLAY not in second.flags


def test_a_key_reused_on_another_route_is_refused():
    """A key reused across two routes with empty bodies is refused as a conflict."""
    policy = ShadowPolicy()
    ask(policy, _cancel_request("pi_AAA"))
    ans = ask(
        policy, replace(make_request("POST", path="/v1/customers/cus_ZZZ"), headers=AGENT_HEADERS)
    )
    assert ans.response.status == 400
    assert json.loads(ans.response.body)["error"]["type"] == "idempotency_error"
    assert Flag.IDEMPOTENCY_CONFLICT in ans.flags


def test_stripe_python_raises_idempotency_error_on_the_conflict_body():
    """stripe-python raises `IdempotencyError` on irimi's conflict body.

    The SDK branches on `type`, not `code`, so the class is asserted. Skipped without `stripe`."""
    pytest.importorskip("stripe")
    import stripe
    from stripe._api_requestor import _APIRequestor

    policy = ShadowPolicy()
    ask(policy, refund_request(b"charge=ch_REAL&amount=100"))
    ans = ask(policy, refund_request(b"charge=ch_REAL&amount=250"))
    raw = ans.response.body.decode()
    error = json.loads(raw)["error"]
    raised = _APIRequestor().specific_v1_api_error(raw, ans.response.status, raw, {}, error)
    assert isinstance(raised, stripe.IdempotencyError)
    assert raised.http_status == 400
