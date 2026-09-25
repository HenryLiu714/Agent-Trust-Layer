"""`ServiceOverlay`: the effect tables applied to a live read, and the cursor it translates (#43)"""

import json

from irimi.exchange import Exchange, Request, Response
from irimi.overlay import MAX_BODY_BYTES, ServiceOverlay
from irimi.servicemap import MapIndex, Route, ServiceMap

STRIPE = ServiceMap(
    service="stripe",
    hosts=frozenset({"api.stripe.com"}),
    routes=(
        Route("GET", "/v1/charges/{charge}", "charges.retrieve", "read"),
        Route("GET", "/v1/refunds", "refunds.list", "read"),
        Route("POST", "/v1/refunds", "refunds.create", "write", ids={"id": "re_"}),
    ),
)
MAPS = MapIndex(services=(STRIPE,))
MINTED = "re_MINTED1"
CHARGE = {
    "id": "ch_REAL1",
    "object": "charge",
    "amount": 4900,
    "amount_refunded": 0,
    "refunded": False,
}


def _read(path, query="", headers=(), host="api.stripe.com"):
    return Request(
        method="GET",
        scheme="https",
        host=host,
        port=443,
        path=path,
        query=query,
        headers=tuple(headers),
        body=b"",
    )


def _upstream(document=None, body=None):
    raw = body if body is not None else json.dumps(document).encode()
    return Response(status=200, headers=(("content-type", "application/json"),), body=raw)


def _write_exchange(headers=(), refund_id=MINTED):
    request = Request(
        method="POST",
        scheme="https",
        host="api.stripe.com",
        port=443,
        path="/v1/refunds",
        query="",
        headers=(("content-type", "application/x-www-form-urlencoded"), *headers),
        body=b"charge=ch_REAL1&amount=100",
    )
    answer = {
        "id": refund_id,
        "object": "refund",
        "charge": "ch_REAL1",
        "amount": 100,
        "status": "succeeded",
        "currency": "usd",
        "created": 1700000000,
    }
    return Exchange(
        request=request,
        response=Response(
            status=200,
            headers=(("content-type", "application/json"),),
            body=json.dumps(answer).encode(),
        ),
        service="stripe",
        operation="refunds.create",
        kind="write",
        answered_by="fake-L1",
        validation="unvalidated",
        run_id="7f3a",
    )


def _raising(*args, **kwargs):
    raise RuntimeError("boom")


def test_a_read_with_an_empty_write_log_is_untouched():
    upstream = _upstream(CHARGE)
    out = ServiceOverlay(MAPS)([], _read("/v1/charges/ch_REAL1"), upstream)
    assert out.response is upstream
    assert out.fidelity is None


def test_a_read_on_a_service_with_no_effects_is_untouched():
    upstream = _upstream(CHARGE)
    read = _read("/v1/charges/ch_REAL1", host="api.example.com")
    out = ServiceOverlay(MAPS)([_write_exchange()], read, upstream)
    assert out.response is upstream
    assert out.fidelity is None


def test_a_charge_read_after_a_faked_refund_is_rewritten():
    upstream = _upstream(CHARGE)
    out = ServiceOverlay(MAPS)([_write_exchange()], _read("/v1/charges/ch_REAL1"), upstream)
    assert out.response is not upstream
    assert json.loads(out.response.body)["amount_refunded"] == 100
    assert out.fidelity == "full"


def test_a_read_of_another_charge_is_left_byte_identical():
    upstream = _upstream({**CHARGE, "id": "ch_OTHER"})
    out = ServiceOverlay(MAPS)([_write_exchange()], _read("/v1/charges/ch_OTHER"), upstream)
    assert out.response is upstream


def test_a_write_from_another_connected_account_is_not_replayed():
    upstream = _upstream(CHARGE)
    log = [_write_exchange(headers=(("stripe-account", "acct_A"),))]
    out = ServiceOverlay(MAPS)(log, _read("/v1/charges/ch_REAL1"), upstream)
    assert out.response is upstream
    assert out.fidelity is None


def test_a_write_from_another_api_version_is_not_replayed():
    upstream = _upstream(CHARGE)
    log = [_write_exchange(headers=(("stripe-version", "2024-06-20"),))]
    out = ServiceOverlay(MAPS)(log, _read("/v1/charges/ch_REAL1"), upstream)
    assert out.response is upstream
    assert out.fidelity is None


def test_a_write_and_read_in_the_same_scope_are_replayed():
    upstream = _upstream(CHARGE)
    log = [_write_exchange(headers=(("stripe-account", "acct_A"),))]
    read = _read("/v1/charges/ch_REAL1", headers=(("stripe-account", "acct_A"),))
    out = ServiceOverlay(MAPS)(log, read, upstream)
    assert json.loads(out.response.body)["amount_refunded"] == 100
    assert out.fidelity == "full"


def test_a_body_that_is_not_json_is_flagged_partial():
    upstream = _upstream(body=b"<html>not json</html>")
    out = ServiceOverlay(MAPS)([_write_exchange()], _read("/v1/charges/ch_REAL1"), upstream)
    assert out.response is upstream
    assert out.fidelity == "partial"


def test_a_body_over_the_size_cap_is_flagged_partial():
    padded = {**CHARGE, "description": "x" * MAX_BODY_BYTES}
    upstream = _upstream(padded)
    out = ServiceOverlay(MAPS)([_write_exchange()], _read("/v1/charges/ch_REAL1"), upstream)
    assert out.response is upstream
    assert out.fidelity == "partial"


def test_an_effect_that_raises_leaves_the_read_alone_and_flags_it(monkeypatch):
    monkeypatch.setattr("irimi.services.EFFECTS", {"stripe": _raising})
    upstream = _upstream(CHARGE)
    out = ServiceOverlay(MAPS)([_write_exchange()], _read("/v1/charges/ch_REAL1"), upstream)
    assert out.response is upstream
    assert out.fidelity == "partial"


def test_rewrite_translates_a_cursor_and_stamps_what_it_removed():
    read = _read("/v1/refunds", query=f"limit=1&starting_after={MINTED}")
    out = ServiceOverlay(MAPS).rewrite([_write_exchange()], read)
    assert out is not read
    assert out.query == "limit=1"
    assert ("irimi-rewrote", f"starting_after={MINTED}") in out.headers


def test_rewrite_returns_the_same_object_when_there_is_nothing_to_translate():
    read = _read("/v1/refunds", query="limit=1&starting_after=re_REAL")
    assert ServiceOverlay(MAPS).rewrite([_write_exchange()], read) is read


def test_rewrite_never_raises(monkeypatch):
    monkeypatch.setattr("irimi.services.REWRITES", {"stripe": _raising})
    read = _read("/v1/refunds", query=f"starting_after={MINTED}")
    assert ServiceOverlay(MAPS).rewrite([_write_exchange()], read) is read
    # The failure path strips an agent-sent `irimi-rewrote` too: it is not ours to forward.
    spoofed = _read("/v1/refunds", headers=(("irimi-rewrote", "starting_after=re_ANYTHING"),))
    out = ServiceOverlay(MAPS).rewrite([_write_exchange()], spoofed)
    assert all(k != "irimi-rewrote" for k, _ in out.headers)


def test_a_rewrote_header_the_agent_sent_itself_is_not_trusted():
    overlay, log = ServiceOverlay(MAPS), [_write_exchange()]
    read = _read("/v1/refunds", headers=(("irimi-rewrote", "starting_after=re_ANYTHING"),))
    rewritten = overlay.rewrite(log, read)
    assert all(k != "irimi-rewrote" for k, _ in rewritten.headers)
    upstream = _upstream({"object": "list", "has_more": False, "data": [{"id": "re_REAL1"}]})
    out = overlay(log, rewritten, upstream)
    assert json.loads(out.response.body)["data"][0]["id"] == MINTED
