"""`idempotency`: the run's answers to keyed writes, so a retry is one write and not two (#46)"""

import threading
from dataclasses import replace

from irimi import idempotency, pipeline, servicemap
from irimi.exchange import Request
from irimi.idempotency import Store, Stored
from irimi.servicemap import Route

STRIPE = servicemap.MapIndex(tuple(servicemap.load_shipped())).service_for("api.stripe.com")
assert STRIPE is not None
REFUNDS_CREATE = servicemap.match_route(STRIPE, "POST", "/v1/refunds")
assert REFUNDS_CREATE is not None

FORM = ("content-type", "application/x-www-form-urlencoded")
SLOT = ("7f3a", "stripe", "", "", "k-1")
PARAMS: idempotency.Canonical = (
    "POST",
    "/v1/refunds",
    "",
    {"charge": "ch_REAL1", "amount": 100},
)


def _params(**over: object) -> idempotency.Canonical:
    """`PARAMS` again as a distinct but equal object, or with a posted field changed."""
    return (*PARAMS[:3], {**PARAMS[3], **over})


def _request(
    host: str = "api.stripe.com",
    path: str = "/v1/refunds",
    headers: tuple[tuple[str, str], ...] = (),
    body: bytes = b"",
    query: str = "",
) -> Request:
    """Through `pipeline.parse`, so the header names are lower-cased the way the addon does it."""
    request = pipeline.parse("POST", "https", host, 443, path, headers, body)
    return request if not query else replace(request, query=query)


def _stored(body: bytes = b'{"id": "re_MINTED1"}') -> Stored:
    return Stored(
        status=200,
        body=body,
        content_type="application/json",
        answered_by="fake-L1",
        flags=("fidelity:L1",),
        precondition="passed",
        rejection_code="",
        currency="usd",
    )


# ------------------------------------------------------------------------------- the key itself


def test_the_key_is_the_header_the_service_names():
    request = _request(headers=(("Idempotency-Key", "abc"),))
    assert idempotency.key_of("stripe", request) == "abc"


def test_a_service_with_no_idempotency_has_no_key():
    request = _request(
        host="slack.com", path="/api/chat.postMessage", headers=(("Idempotency-Key", "abc"),)
    )
    assert idempotency.key_of("slack", request) == ""


def test_a_request_that_sent_no_key_has_none():
    assert idempotency.key_of("stripe", _request()) == ""


# --------------------------------------------------------------------- what two sendings compare


def test_canonical_params_drop_the_routes_volatile_names():
    assert REFUNDS_CREATE is not None
    request = _request(headers=(FORM,), body=b"charge=ch_1&amount=100&idempotency_key=k")
    assert idempotency.canonical(request, REFUNDS_CREATE) == (
        "POST",
        "/v1/refunds",
        "",
        {"charge": "ch_1", "amount": 100},
    )


def test_canonical_params_keep_everything_else():
    route = Route(method="POST", path="/v1/refunds", operation="refunds.create", kind="write")
    request = _request(headers=(FORM,), body=b"charge=ch_1&amount=100&idempotency_key=k")
    assert idempotency.canonical(request, route) == (
        "POST",
        "/v1/refunds",
        "",
        {"charge": "ch_1", "amount": 100, "idempotency_key": "k"},
    )


def test_canonical_tells_two_objects_on_one_route_apart():
    """`payment_intents.cancel` is one route over every `pi_`, and it posts nothing at all, so
    the path is the whole of what says which object the caller named. Without it a key reused to
    cancel a second intent compared equal to the first and replayed its answer (#46)."""
    cancel = servicemap.match_route(STRIPE, "POST", "/v1/payment_intents/pi_AAA/cancel")
    assert cancel is not None
    one = idempotency.canonical(_request(path="/v1/payment_intents/pi_AAA/cancel"), cancel)
    two = idempotency.canonical(_request(path="/v1/payment_intents/pi_BBB/cancel"), cancel)
    assert one[3] == two[3] == {}, "neither cancel posts a field to tell them apart"
    assert one != two


def test_canonical_tells_two_routes_with_the_same_empty_body_apart():
    """A key reused on another endpoint is another write. Both of these post nothing, so only
    the path separates a cancelled intent from an updated customer (#46)."""
    cancel = servicemap.match_route(STRIPE, "POST", "/v1/payment_intents/pi_AAA/cancel")
    update = servicemap.match_route(STRIPE, "POST", "/v1/customers/cus_ZZZ")
    assert cancel is not None and update is not None
    assert idempotency.canonical(
        _request(path="/v1/payment_intents/pi_AAA/cancel"), cancel
    ) != idempotency.canonical(_request(path="/v1/customers/cus_ZZZ"), update)


def test_canonical_tells_two_sendings_apart_by_their_query():
    """A parameter a caller put in the query is still a parameter. `echo.reflect` reads the body
    only, so the query rides in the identity beside the path (#46)."""
    route = Route(method="POST", path="/v1/refunds", operation="refunds.create", kind="write")
    plain = idempotency.canonical(_request(), route)
    with_query = idempotency.canonical(_request(query="amount=250"), route)
    assert plain != with_query


# ------------------------------------------------------------------------------------ the store


def test_an_unseen_slot_is_neither_a_replay_nor_a_conflict():
    assert Store().get(SLOT, PARAMS) == (None, False)


def test_the_same_slot_and_params_replay_the_stored_answer():
    store = Store()
    stored = _stored()
    store.put(SLOT, PARAMS, stored)
    assert store.get(SLOT, _params()) == (stored, False)


def test_the_same_slot_with_different_params_is_a_conflict():
    store = Store()
    store.put(SLOT, PARAMS, _stored())
    replayed, conflicted = store.get(SLOT, _params(amount=250))
    assert (replayed, conflicted) == (None, True)


def test_the_first_answer_is_the_one_the_run_keeps():
    store = Store()
    first, second = _stored(b'{"id": "re_FIRST"}'), _stored(b'{"id": "re_SECOND"}')
    store.put(SLOT, PARAMS, first)
    store.put(SLOT, PARAMS, second)
    assert store.get(SLOT, PARAMS) == (first, False)


def test_two_stores_do_not_share_entries():
    one, two = Store(), Store()
    one.put(SLOT, PARAMS, _stored())
    assert two.get(SLOT, PARAMS) == (None, False)


def test_the_key_separates_runs_accounts_and_versions():
    base = idempotency.key("7f3a", "stripe", ("acct_1", "2024-06-20"), "k-1")
    other_run = idempotency.key("9b2c", "stripe", ("acct_1", "2024-06-20"), "k-1")
    other_account = idempotency.key("7f3a", "stripe", ("acct_2", "2024-06-20"), "k-1")
    other_version = idempotency.key("7f3a", "stripe", ("acct_1", "2025-01-01"), "k-1")
    assert len({base, other_run, other_account, other_version}) == 4


def test_concurrent_puts_of_one_slot_keep_one_answer():
    store = Store()
    barrier = threading.Barrier(20)

    def put(n: int) -> None:
        barrier.wait()
        store.put(SLOT, PARAMS, _stored(f'{{"id": "re_{n}"}}'.encode()))

    threads = [threading.Thread(target=put, args=(n,)) for n in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    kept = {store.get(SLOT, PARAMS) for _ in range(20)}
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
