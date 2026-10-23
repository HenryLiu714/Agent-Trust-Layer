"""Stripe's effects applied through the engine to reads of a loopback Stripe."""

import json
import threading

import pytest

from irimi.core.exchange import Flag
from irimi.decide import idempotency
from irimi.protocol.headers import ANSWERED_BY_HEADER, REWROTE_HEADER
from tests.support.engine import (
    engine_config,
    live_summary,
    read_via_proxy,
    start_engine,
    via_proxy,
)
from tests.support.maps import STRIPE_MAP, map_index
from tests.support.upstreams import Answer, FakeStripe, serve, stripe_list

# `/v1/refunds` answers a FULL page - this many real refunds, the same number a `limit` of that
# size asks for - so a test can prove the minted refund pushes the oldest real one off the page
# rather than making it longer than Stripe would.
REFUNDS_PAGE_SIZE = 2
RATE_LIMITED = Answer(429, {"error": {"type": "invalid_request_error", "code": "rate_limit"}})


def overlay_stripe() -> dict:
    """A Stripe-shaped upstream with real state for every read the effects table models. A test
    rate-limits every `GET /v1/refunds/{id}` with `stub.answers["/v1/refunds/"] = RATE_LIMITED`.
    Anything else is Stripe's own 404 for an id it has never heard of, whose code is what tells a
    minted object's read apart from a rate limit or a bad key on the same read."""
    real = [
        {"id": f"re_REAL{n}", "object": "refund", "charge": "ch_REAL1"}
        for n in range(1, REFUNDS_PAGE_SIZE + 1)
    ]
    return {
        "charges": {
            "ch_REAL1": {
                "id": "ch_REAL1",
                "object": "charge",
                "amount": 4900,
                "amount_refunded": 0,
                "refunded": False,
                "currency": "usd",
            }
        },
        "documents": {
            "/v1/refunds": stripe_list(*real),
            "/v1/customers/cus_REAL1": {
                "id": "cus_REAL1",
                "object": "customer",
                "email": "old@example.test",
                "metadata": {"tier": "free"},
            },
            "/v1/payment_intents/pi_REAL1": {
                "id": "pi_REAL1",
                "object": "payment_intent",
                "status": "requires_capture",
                "canceled_at": None,
                "cancellation_reason": None,
            },
        },
        "answers": {},
    }


@pytest.fixture
def stripe_stub(tmp_path, monkeypatch):
    """The stub on a free loopback port, and an engine over STRIPE_MAP with the real overlay,
    built the way the CLI builds it. Yields (proxy port, stub port, recorded exchanges)."""
    from irimi.decide.overlay import ServiceOverlay

    stub = serve(FakeStripe, threaded=True, **overlay_stripe())
    maps = map_index(tmp_path, monkeypatch, doc=STRIPE_MAP)
    eng, seen, stop = start_engine(
        engine_config(tmp_path, monkeypatch, maps=maps), overlay=ServiceOverlay()
    )
    yield eng.listen_port(), stub, seen
    stop()
    stub.stop()


def test_a_charge_reread_after_a_faked_refund_shows_the_refund(stripe_stub):
    proxy, stub, _ = stripe_stub
    status, _ = via_proxy(
        proxy,
        "POST",
        f"http://127.0.0.1:{stub}/v1/refunds",
        body=b"charge=ch_REAL1&amount=100",
        extra_headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert status == 200
    status, headers, data = read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/charges/ch_REAL1")
    assert status == 200
    charge = json.loads(data)
    assert charge["amount_refunded"] == 100
    assert charge["refunded"] is False
    assert headers[ANSWERED_BY_HEADER] == "overlay"
    assert [method for method, _ in stub.seen] == ["GET"], "a faked write reached Stripe"


def test_a_faked_refund_records_the_webhooks_it_would_have_fired(stripe_stub):
    """Would-fire webhooks through the proxy, where the `Plan` and the response hook's `Exchange`
    carry them: a faked refund lists `refund.created` and `charge.refunded`, and the read after it
    lists nothing."""
    proxy, stub, seen = stripe_stub
    status, _ = via_proxy(
        proxy,
        "POST",
        f"http://127.0.0.1:{stub}/v1/refunds",
        body=b"charge=ch_REAL1&amount=100",
        extra_headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert status == 200
    status, _, _ = read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/charges/ch_REAL1")
    assert status == 200
    (write,) = [ex for ex in seen if ex.request.method == "POST"]
    (read,) = [ex for ex in seen if ex.request.method == "GET"]
    assert write.would_fire == ("refund.created", "charge.refunded")
    assert read.would_fire == ()
    assert "POST" not in [method for method, _ in stub.seen], "a faked write reached Stripe"


def test_the_refunds_list_shows_the_minted_refund_first(stripe_stub):
    proxy, stub, _ = stripe_stub
    status, data = via_proxy(
        proxy,
        "POST",
        f"http://127.0.0.1:{stub}/v1/refunds",
        body=b"charge=ch_REAL1&amount=100",
        extra_headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert status == 200
    refund_id = json.loads(data)["id"]
    assert refund_id.startswith("re_")
    status, headers, data = read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/refunds")
    assert status == 200
    assert json.loads(data)["data"][0]["id"] == refund_id
    assert headers[ANSWERED_BY_HEADER] == "overlay"


def test_a_forged_rewrote_header_cannot_hide_a_refund_that_lands_mid_read(tmp_path, monkeypatch):
    """A forged header racing a refund. `request()` snapshots the write log, empty here, so the
    read is never rewritten; the refund then lands before the read's `response()`, which tests
    the live log and overlays the page. An overlay that read the agent's forged `Irimi-Rewrote:
    starting_after=...` off the recorded request would take the page for the one after a minted
    refund and leave the refund off it."""
    from irimi.decide.overlay import ServiceOverlay

    # The refunds list is held at the upstream until the test lets it go, so a write can land
    # while the read is in flight.
    stub = serve(FakeStripe, threaded=True, **overlay_stripe(), held=frozenset({"/v1/refunds"}))
    maps = map_index(tmp_path, monkeypatch, doc=STRIPE_MAP)
    eng, _, stop = start_engine(
        engine_config(tmp_path, monkeypatch, maps=maps), overlay=ServiceOverlay()
    )
    read: dict = {}

    def list_refunds():
        read["result"] = read_via_proxy(
            eng.listen_port(),
            f"http://127.0.0.1:{stub}/v1/refunds",
            extra_headers={REWROTE_HEADER: "starting_after=re_FORGED"},
        )

    reader = threading.Thread(target=list_refunds)
    try:
        reader.start()
        assert stub.arrived.wait(10), "the read never reached the upstream"
        status, data = via_proxy(
            eng.listen_port(),
            "POST",
            f"http://127.0.0.1:{stub}/v1/refunds",
            body=b"charge=ch_REAL1&amount=100",
            extra_headers={"content-type": "application/x-www-form-urlencoded"},
        )
        assert status == 200
        stub.release.set()
        reader.join(10)
    finally:
        stub.release.set()
        stop()
        stub.stop()
    status, headers, page = read["result"]
    assert status == 200
    assert json.loads(page)["data"][0]["id"] == json.loads(data)["id"]
    assert headers[ANSWERED_BY_HEADER] == "overlay"


def _keyed_refund(proxy, stub, key, amount=100):
    """One refund through the proxy carrying `Idempotency-Key`, as stripe-python sends every POST.
    Returns (status, response headers, body)."""
    return read_via_proxy(
        proxy,
        f"http://127.0.0.1:{stub}/v1/refunds",
        extra_headers={
            "content-type": "application/x-www-form-urlencoded",
            "idempotency-key": key,
        },
        method="POST",
        body=f"charge=ch_REAL1&amount={amount}".encode(),
    )


def test_a_retry_with_the_same_key_is_one_refund_not_two(stripe_stub):
    """A retry with the key it already sent gets the first answer's own bytes and never reaches
    the write log. A second minted id would be logged as a second write, and the overlay would
    apply one 100 refund as 200."""
    proxy, stub, seen = stripe_stub
    status, first_headers, first = _keyed_refund(proxy, stub, "k-1")
    assert status == 200
    status, second_headers, second = _keyed_refund(proxy, stub, "k-1")
    assert status == 200
    assert second == first
    stamp = ANSWERED_BY_HEADER
    assert second_headers[stamp] == first_headers[stamp]
    replayed, marker = idempotency.replayed_header("stripe") or ("", "")
    assert replayed not in first_headers
    assert second_headers[replayed] == marker
    refunds = [ex for ex in seen if ex.request.method == "POST"]
    assert [Flag.IDEMPOTENT_REPLAY in ex.flags for ex in refunds] == [False, True]

    _, _, data = read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/charges/ch_REAL1")
    assert json.loads(data)["amount_refunded"] == 100
    _, _, data = read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/refunds")
    minted = [r["id"] for r in json.loads(data)["data"] if not r["id"].startswith("re_REAL")]
    assert minted == [json.loads(first)["id"]]
    assert "POST" not in [method for method, _ in stub.seen], "a faked write reached Stripe"


def test_a_replayed_refund_records_no_webhooks(stripe_stub):
    """The retry's events are already on the first write's own exchange; listing them again
    would promise one refund's `refund.created` twice."""
    proxy, stub, seen = stripe_stub
    _keyed_refund(proxy, stub, "k-1")
    _keyed_refund(proxy, stub, "k-1")
    refunds = [ex for ex in seen if ex.request.method == "POST"]
    assert [ex.would_fire for ex in refunds] == [("refund.created", "charge.refunded"), ()]
    assert [Flag.IDEMPOTENT_REPLAY in ex.flags for ex in refunds] == [False, True]


def test_the_same_key_with_a_different_amount_is_refused(stripe_stub):
    """A key reused for a different write is Stripe's own `idempotency_error`, and it is not a
    write: the charge still shows the first refund only."""
    proxy, stub, seen = stripe_stub
    status, _, _ = _keyed_refund(proxy, stub, "k-2")
    assert status == 200
    status, _, data = _keyed_refund(proxy, stub, "k-2", amount=250)
    assert status == 400
    assert json.loads(data)["error"]["type"] == "idempotency_error"
    refused = [ex for ex in seen if ex.request.method == "POST"][-1]
    assert Flag.IDEMPOTENCY_CONFLICT in refused.flags

    _, _, data = read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/charges/ch_REAL1")
    assert json.loads(data)["amount_refunded"] == 100


def test_a_reused_key_on_a_different_write_records_no_webhooks(stripe_stub):
    """The real service would have refused the reused key, so nothing would have fired. A
    conflict is not `precondition: rejected`, so it is pinned on its own."""
    proxy, stub, seen = stripe_stub
    _keyed_refund(proxy, stub, "k-2")
    _keyed_refund(proxy, stub, "k-2", amount=250)
    first, refused = [ex for ex in seen if ex.request.method == "POST"]
    assert first.would_fire == ("refund.created", "charge.refunded")
    assert Flag.IDEMPOTENCY_CONFLICT in refused.flags
    assert refused.would_fire == ()


def test_a_read_before_the_refund_is_untouched_and_unstamped(stripe_stub):
    proxy, stub, seen = stripe_stub
    status, headers, data = read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/charges/ch_REAL1")
    assert status == 200
    assert json.loads(data)["amount_refunded"] == 0
    assert ANSWERED_BY_HEADER not in headers
    assert seen[-1].answered_by == "live"
    assert seen[-1].overlay is None
    # The same read after the refund IS overlaid, so the untouched one above was untouched
    # because it came first, not because this engine has no overlay.
    via_proxy(
        proxy,
        "POST",
        f"http://127.0.0.1:{stub}/v1/refunds",
        body=b"charge=ch_REAL1&amount=100",
        extra_headers={"content-type": "application/x-www-form-urlencoded"},
    )
    _, headers, data = read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/charges/ch_REAL1")
    assert json.loads(data)["amount_refunded"] == 100
    assert headers[ANSWERED_BY_HEADER] == "overlay"


def _fake_refund(proxy, stub, charge="ch_REAL1", amount=100):
    """One faked refund through the proxy. Returns the minted refund id."""
    status, data = via_proxy(
        proxy,
        "POST",
        f"http://127.0.0.1:{stub}/v1/refunds",
        body=f"charge={charge}&amount={amount}".encode(),
        extra_headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert status == 200
    return json.loads(data)["id"]


def test_a_full_refunds_page_drops_the_oldest_real_refund_and_says_there_is_more(stripe_stub):
    """A page Stripe filled to the limit cannot also hold the minted refund. The page stays the
    length the agent asked for and `has_more` becomes true, because a longer page is one no real
    Stripe read could return."""
    proxy, stub, seen = stripe_stub
    refund_id = _fake_refund(proxy, stub)
    limit = REFUNDS_PAGE_SIZE
    status, headers, data = read_via_proxy(
        proxy, f"http://127.0.0.1:{stub}/v1/refunds?limit={limit}"
    )
    assert status == 200
    page = json.loads(data)
    assert [item["id"] for item in page["data"]] == [refund_id, "re_REAL1"]
    assert page["has_more"] is True
    assert headers[ANSWERED_BY_HEADER] == "overlay"
    assert seen[-1].overlay == "full"


def test_a_cursor_naming_a_minted_refund_is_dropped_before_the_read_reaches_stripe(stripe_stub):
    """Stripe has never heard of the refund irimi minted and would answer the page with an error,
    so the cursor is dropped, and irimi keeps what it removed off the wire. The page that comes back
    is the real list from its top, and the minted refund is NOT prepended again: it belongs on the
    page before this one."""
    proxy, stub, seen = stripe_stub
    refund_id = _fake_refund(proxy, stub)
    stub.seen.clear()
    status, headers, data = read_via_proxy(
        proxy, f"http://127.0.0.1:{stub}/v1/refunds?starting_after={refund_id}"
    )
    assert status == 200
    assert stub.seen == [("GET", "/v1/refunds")], "the minted cursor reached Stripe"
    assert [item["id"] for item in json.loads(data)["data"]] == ["re_REAL1", "re_REAL2"]
    # The body really is the one Stripe sent, so it stays unstamped; the exchange is what records
    # that irimi translated the request, and shows the upstream what it asked.
    assert ANSWERED_BY_HEADER not in headers
    ex = seen[-1]
    assert ex.answered_by == "live"
    assert ex.overlay == "full"
    assert ex.request.query == ""
    assert REWROTE_HEADER not in {k for k, _ in ex.request.headers}


def test_a_customer_reread_after_a_faked_update_shows_the_posted_fields(stripe_stub):
    proxy, stub, seen = stripe_stub
    status, _ = via_proxy(
        proxy,
        "POST",
        f"http://127.0.0.1:{stub}/v1/customers/cus_REAL1",
        body=b"email=new%40example.test&metadata[tier]=pro",
        extra_headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert status == 200
    status, headers, data = read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/customers/cus_REAL1")
    assert status == 200
    customer = json.loads(data)
    assert customer["email"] == "new@example.test"
    assert customer["metadata"] == {"tier": "pro"}, "Stripe merges metadata, it does not replace it"
    assert headers[ANSWERED_BY_HEADER] == "overlay"
    assert seen[-1].overlay == "full"
    assert [method for method, _ in stub.seen] == ["GET"], "a faked write reached Stripe"


def test_a_payment_intent_reread_after_a_faked_cancel_shows_it_canceled(stripe_stub):
    proxy, stub, seen = stripe_stub
    status, _ = via_proxy(
        proxy,
        "POST",
        f"http://127.0.0.1:{stub}/v1/payment_intents/pi_REAL1/cancel",
        body=b"cancellation_reason=requested_by_customer",
        extra_headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert status == 200
    status, headers, data = read_via_proxy(
        proxy, f"http://127.0.0.1:{stub}/v1/payment_intents/pi_REAL1"
    )
    assert status == 200
    intent = json.loads(data)
    assert intent["status"] == "canceled"
    assert intent["cancellation_reason"] == "requested_by_customer"
    assert isinstance(intent["canceled_at"], int)
    assert headers[ANSWERED_BY_HEADER] == "overlay"
    assert seen[-1].overlay == "full"
    assert [method for method, _ in stub.seen] == ["GET"], "a faked write reached Stripe"


def test_a_reread_of_a_minted_refund_by_id_is_answered_not_404d(stripe_stub):
    """A minted refund read back by id, end to end through the real proxy. Stripe 404s an id it
    has never heard of; the agent stored that id because irimi told it the refund exists, and
    irimi answers with the object it minted, at 200, stamped `overlay`. The read still reached
    Stripe - that is what makes it a real read the summary counts."""
    proxy, stub, seen = stripe_stub
    # `_fake_refund` returns the minted id and asserts its own 200.
    minted = _fake_refund(proxy, stub)

    status, headers, data = read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/refunds/{minted}")
    assert status == 200
    assert headers[ANSWERED_BY_HEADER] == "overlay"
    assert json.loads(data)["id"] == minted
    ex = seen[-1]
    assert (ex.kind, ex.answered_by, ex.overlay, ex.issued_by) == (
        "read",
        "overlay",
        "full",
        "agent",
    )
    # The read reached Stripe and the refund did not: it is still a real read.
    assert stub.seen == [("GET", f"/v1/refunds/{minted}")], "a faked write reached Stripe"


def test_a_read_of_a_refund_stripe_really_does_not_have_keeps_its_404(stripe_stub):
    """The status moves for a minted object and for nothing else. An id neither Stripe nor irimi
    has is Stripe's own 404, unstamped and unflagged."""
    proxy, stub, seen = stripe_stub
    assert _fake_refund(proxy, stub).startswith("re_")
    status, headers, _ = read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/refunds/re_NOTHING")
    assert status == 404
    assert ANSWERED_BY_HEADER not in headers
    ex = seen[-1]
    assert ex.answered_by == "live"
    assert ex.overlay is None


def test_a_rate_limited_read_of_a_minted_refund_keeps_its_429(stripe_stub):
    """Only Stripe's `resource_missing` is answered. A 429 on the same read is Stripe's answer
    about the caller, which production would have sent for the real refund too, so the agent
    gets it as sent and the exchange says the world irimi showed is incomplete."""
    proxy, stub, seen = stripe_stub
    minted = _fake_refund(proxy, stub)
    stub.answers["/v1/refunds/"] = RATE_LIMITED
    status, headers, data = read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/refunds/{minted}")
    assert status == 429
    assert json.loads(data)["error"]["code"] == "rate_limit"
    assert ANSWERED_BY_HEADER not in headers
    ex = seen[-1]
    assert (ex.answered_by, ex.overlay) == ("live", "partial")


def test_a_minted_refund_read_by_id_is_listed_under_its_refund(stripe_stub, tmp_path, monkeypatch):
    """The summary line for a minted refund read by id, off a real run: the read hangs under the
    refund as one that saw it, not as `did not show it  real (partial)`."""
    proxy, stub, seen = stripe_stub
    minted = _fake_refund(proxy, stub)
    assert read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/refunds/{minted}")[0] == 200
    lines = live_summary(seen, map_index(tmp_path, monkeypatch, doc=STRIPE_MAP))
    write = next(i for i, line in enumerate(lines) if line.startswith("  ○ stripe  refund 100"))
    assert lines[write + 1] == f"    ↳ GET /v1/refunds/{minted} saw it  overlay"
