"""`ServiceOverlay`: the effect tables applied to a live read, and the cursor it translates (#43)"""

import json

from irimi.exchange import Exchange, Request, Response
from irimi.overlay import ServiceOverlay
from irimi.servicemap import MapIndex, Route, ServiceMap
from irimi.writelog import MAX_BODY_BYTES

STRIPE = ServiceMap(
    service="stripe",
    hosts=frozenset({"api.stripe.com"}),
    routes=(
        Route("GET", "/v1/charges/{charge}", "charges.retrieve", "read"),
        Route("GET", "/v1/refunds", "refunds.list", "read"),
        Route("GET", "/v1/refunds/{refund}", "refunds.retrieve", "read"),
        Route("POST", "/v1/refunds", "refunds.create", "write", ids={"id": "re_"}),
    ),
)
SLACK = ServiceMap(
    service="slack",
    hosts=frozenset({"slack.com"}),
    routes=(
        Route("POST", "/api/chat.postMessage", "chat.postMessage", "write"),
        Route("POST", "/api/conversations.history", "conversations.history", "read"),
        Route("POST", "/api/conversations.replies", "conversations.replies", "read"),
        Route("POST", "/api/conversations.info", "conversations.info", "read"),
    ),
    verbs="post-only",
)
MAPS = MapIndex(services=(STRIPE, SLACK))
MINTED = "re_MINTED1"
CHARGE = {
    "id": "ch_REAL1",
    "object": "charge",
    "amount": 4900,
    "amount_refunded": 0,
    "refunded": False,
}
SLACK_JSON = "application/json;charset=utf-8"  # what slack_sdk sends (#44)
HISTORY = {
    "ok": True,
    "messages": [{"type": "message", "ts": "1700000000.000100", "text": "real", "user": "U1"}],
    "has_more": False,
}


def _read(path, query="", headers=(), host="api.stripe.com", method="GET", body=b""):
    return Request(
        method=method,
        scheme="https",
        host=host,
        port=443,
        path=path,
        query=query,
        headers=tuple(headers),
        body=body,
    )


def _slack_read(params, headers=(), operation="conversations.history"):
    """A Slack read as slack_sdk sends it: a POST with every parameter in the JSON body, which is
    where the effects read them from (#44)."""
    return _read(
        f"/api/{operation}",
        headers=(("content-type", SLACK_JSON), *headers),
        host="slack.com",
        method="POST",
        body=json.dumps(params).encode(),
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


def _slack_post_exchange(ts, channel="C0123", headers=(), thread_ts=None):
    """A faked `chat.postMessage`, answered in `echo.SLACK_ENVELOPES`' shape (#42)."""
    posted = {"channel": channel, "text": "refund issued"}
    if thread_ts is not None:
        posted["thread_ts"] = thread_ts
    request = Request(
        method="POST",
        scheme="https",
        host="slack.com",
        port=443,
        path="/api/chat.postMessage",
        query="",
        headers=(("content-type", SLACK_JSON), *headers),
        body=json.dumps(posted).encode(),
    )
    answer = {
        "ok": True,
        "channel": channel,
        "ts": ts,
        "message": {"type": "message", "ts": ts, "text": "refund issued", "user": "U0BOT"},
    }
    return Exchange(
        request=request,
        response=Response(
            status=200,
            headers=(("content-type", "application/json"),),
            body=json.dumps(answer).encode(),
        ),
        service="slack",
        operation="chat.postMessage",
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


# ---------------------------------------------------------------------------- Slack (#44)


def test_a_slack_history_read_after_a_faked_post_is_rewritten():
    upstream = _upstream(HISTORY)
    log = [_slack_post_exchange("1800000000.000001")]
    out = ServiceOverlay(MAPS)(log, _slack_read({"channel": "C0123"}), upstream)
    assert out.response is not upstream
    messages = json.loads(out.response.body)["messages"]
    assert [m["ts"] for m in messages] == ["1800000000.000001", "1700000000.000100"]
    assert out.fidelity == "full"


def test_a_slack_read_with_an_empty_write_log_is_untouched():
    upstream = _upstream(HISTORY)
    out = ServiceOverlay(MAPS)([], _slack_read({"channel": "C0123"}), upstream)
    assert out.response is upstream
    assert out.fidelity is None


def test_slack_writes_under_different_tokens_are_all_replayed():
    """Slack has no `SCOPE_HEADERS` entry, so a run holds one Slack scope and the token a write
    was made with does not keep it off a read made with another (#44)."""
    upstream = _upstream(HISTORY)
    log = [
        _slack_post_exchange("1800000000.000001", headers=(("authorization", "Bearer xoxb-A"),)),
        _slack_post_exchange("1800000000.000002", headers=(("authorization", "Bearer xoxb-B"),)),
    ]
    read = _slack_read({"channel": "C0123"}, headers=(("authorization", "Bearer xoxb-C"),))
    out = ServiceOverlay(MAPS)(log, read, upstream)
    messages = json.loads(out.response.body)["messages"]
    assert [m["ts"] for m in messages] == [
        "1800000000.000002",
        "1800000000.000001",
        "1700000000.000100",
    ]
    assert out.fidelity == "full"


def test_rewrite_leaves_a_slack_read_as_the_same_object():
    """Slack has no `REWRITES` entry: its cursor is opaque and server-issued, so there is nothing
    of irimi's in it to translate (#44)."""
    read = _slack_read({"channel": "C0123", "cursor": "dXNlcjpVMEc5V0ZYTlo="})
    log = [_slack_post_exchange("1800000000.000001")]
    assert ServiceOverlay(MAPS).rewrite(log, read) is read


def test_a_slack_replies_read_after_a_faked_reply_is_rewritten():
    """#44's done-when for the other half: the reply at the tail of the last page, its parent's
    counts moved, and the whole read stamped through the seam."""
    parent_ts = "1700000000.000100"
    upstream = _upstream(
        {"ok": True, "messages": [dict(HISTORY["messages"][0])], "has_more": False}
    )
    log = [_slack_post_exchange("1800000000.000001", thread_ts=parent_ts)]
    read = _slack_read({"channel": "C0123", "ts": parent_ts}, operation="conversations.replies")
    out = ServiceOverlay(MAPS)(log, read, upstream)
    assert out.response is not upstream
    messages = json.loads(out.response.body)["messages"]
    assert [m["ts"] for m in messages] == [parent_ts, "1800000000.000001"]
    assert messages[0]["reply_count"] == 1
    assert messages[0]["latest_reply"] == "1800000000.000001"
    assert messages[-1]["thread_ts"] == parent_ts
    assert out.fidelity == "full"


def test_a_slack_read_the_effects_do_not_model_is_untouched():
    """`conversations.info` is not overlaid, so the read stays byte-identical and unflagged - the
    same object, which is how the engine knows not to stamp it (#44)."""
    upstream = _upstream({"ok": True, "channel": {"id": "C0123", "name": "general"}})
    read = _slack_read({"channel": "C0123"}, operation="conversations.info")
    out = ServiceOverlay(MAPS)([_slack_post_exchange("1800000000.000001")], read, upstream)
    assert out.response is upstream
    assert out.fidelity is None


def test_a_read_of_a_minted_refund_comes_back_as_that_refund_at_200():
    """#52 through the seam: the effects answer the read and the overlay carries the status onto
    the response, so the agent gets the object irimi told it exists rather than Stripe's 404."""
    overlay = ServiceOverlay(MAPS)
    upstream = Response(
        status=404,
        headers=(("content-type", "application/json"),),
        body=json.dumps({"error": {"code": "resource_missing"}}).encode(),
    )
    out = overlay([_write_exchange()], _read(f"/v1/refunds/{MINTED}"), upstream)
    assert out.response is not upstream
    assert out.response.status == 200
    assert out.fidelity == "full"
    assert json.loads(out.response.body)["id"] == MINTED


def test_a_read_of_a_refund_this_run_did_not_mint_keeps_stripes_own_404():
    """The status only moves for an object irimi minted. Stripe's own 404 about Stripe's own state
    reaches the agent byte-identical and unstamped (#52)."""
    overlay = ServiceOverlay(MAPS)
    upstream = Response(
        status=404,
        headers=(("content-type", "application/json"),),
        body=json.dumps({"error": {"code": "resource_missing"}}).encode(),
    )
    out = overlay([_write_exchange()], _read("/v1/refunds/re_SOMEONE_ELSE"), upstream)
    assert out.response is upstream
    assert out.response.status == 404
    assert out.fidelity is None


def test_a_slack_replies_read_of_a_minted_thread_comes_back_as_the_thread():
    """The Slack twin, which needs no status: Slack answers `thread_not_found` at 200 (#52)."""
    overlay = ServiceOverlay(MAPS)
    ts = "1800000000.000001"
    upstream = _upstream({"ok": False, "error": "thread_not_found"})
    log = [_slack_post_exchange(ts)]
    out = overlay(
        log,
        _slack_read({"channel": "C0123", "ts": ts}, operation="conversations.replies"),
        upstream,
    )
    assert out.response is not upstream
    assert out.response.status == 200
    assert out.fidelity == "full"
    document = json.loads(out.response.body)
    assert document["ok"] is True
    assert [m["ts"] for m in document["messages"]] == [ts]
