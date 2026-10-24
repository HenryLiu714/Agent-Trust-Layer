"""`ServiceOverlay`: the effect tables applied to a live read, and the cursor it translates."""

import json
from dataclasses import replace

from irimi import services
from irimi.core.exchange import Exchange, Request, Response
from irimi.core.limits import MAX_BODY_BYTES
from irimi.decide.overlay import NoOverlay, ServiceOverlay
from irimi.servicemaps import IdSpec, MapIndex, Route, ServiceMap
from irimi.servicemaps.classify import classify
from tests.support import builders
from tests.support.credentials import SLACK_BOT

STRIPE = ServiceMap(
    service="stripe",
    hosts=frozenset({"api.stripe.com"}),
    routes=(
        Route("GET", "/v1/charges/{charge}", "charges.retrieve", "read"),
        Route("GET", "/v1/refunds", "refunds.list", "read"),
        Route("GET", "/v1/refunds/{refund}", "refunds.retrieve", "read"),
        Route("POST", "/v1/refunds", "refunds.create", "write", ids={"id": IdSpec(prefix="re_")}),
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
    style="rpc",
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
SLACK_JSON = "application/json;charset=utf-8"  # what slack_sdk sends
HISTORY: dict = {
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
    """A Slack read as slack_sdk sends it: a POST with every parameter in the JSON body."""
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
        run_id="7f3a",
    )


def _slack_post_exchange(ts, channel="C0123", headers=(), thread_ts=None):
    """A faked `chat.postMessage`, answered in `slack.answers.SLACK_ENVELOPES`' shape."""
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
        run_id="7f3a",
    )


def _raising(*args, **kwargs):
    raise RuntimeError("boom")


class _Overlay:
    """`ServiceOverlay`, handed each read's classification over MAPS as the engine hands it."""

    def __init__(self):
        self.inner = ServiceOverlay()

    def __call__(self, write_log, read, upstream, translated_from=None):
        return self.inner(write_log, classify(read, MAPS), read, upstream, translated_from)

    def rewrite(self, write_log, read):
        return self.inner.rewrite(write_log, classify(read, MAPS), read)


def test_a_read_with_an_empty_write_log_is_untouched():
    upstream = _upstream(CHARGE)
    out = _Overlay()([], _read("/v1/charges/ch_REAL1"), upstream)
    assert out.response is upstream
    assert out.fidelity is None


def test_a_read_on_a_service_with_no_effects_is_untouched():
    upstream = _upstream(CHARGE)
    read = _read("/v1/charges/ch_REAL1", host="api.example.com")
    out = _Overlay()([_write_exchange()], read, upstream)
    assert out.response is upstream
    assert out.fidelity is None


def test_a_charge_read_after_a_faked_refund_is_rewritten():
    upstream = _upstream(CHARGE)
    out = _Overlay()([_write_exchange()], _read("/v1/charges/ch_REAL1"), upstream)
    assert out.response is not upstream
    assert json.loads(out.response.body)["amount_refunded"] == 100
    assert out.fidelity == "full"


def test_a_read_of_another_charge_is_left_byte_identical():
    upstream = _upstream({**CHARGE, "id": "ch_OTHER"})
    out = _Overlay()([_write_exchange()], _read("/v1/charges/ch_OTHER"), upstream)
    assert out.response is upstream


def test_a_write_from_another_connected_account_is_not_replayed():
    upstream = _upstream(CHARGE)
    log = [_write_exchange(headers=(("stripe-account", "acct_A"),))]
    out = _Overlay()(log, _read("/v1/charges/ch_REAL1"), upstream)
    assert out.response is upstream
    assert out.fidelity is None


def test_a_write_from_another_api_version_is_not_replayed():
    upstream = _upstream(CHARGE)
    log = [_write_exchange(headers=(("stripe-version", "2024-06-20"),))]
    out = _Overlay()(log, _read("/v1/charges/ch_REAL1"), upstream)
    assert out.response is upstream
    assert out.fidelity is None


def test_a_write_and_read_in_the_same_scope_are_replayed():
    upstream = _upstream(CHARGE)
    log = [_write_exchange(headers=(("stripe-account", "acct_A"),))]
    read = _read("/v1/charges/ch_REAL1", headers=(("stripe-account", "acct_A"),))
    out = _Overlay()(log, read, upstream)
    assert json.loads(out.response.body)["amount_refunded"] == 100
    assert out.fidelity == "full"


def test_a_body_that_is_not_json_is_flagged_partial():
    upstream = _upstream(body=b"<html>not json</html>")
    out = _Overlay()([_write_exchange()], _read("/v1/charges/ch_REAL1"), upstream)
    assert out.response is upstream
    assert out.fidelity == "partial"


def test_a_body_over_the_size_cap_is_flagged_partial():
    padded = {**CHARGE, "description": "x" * MAX_BODY_BYTES}
    upstream = _upstream(padded)
    out = _Overlay()([_write_exchange()], _read("/v1/charges/ch_REAL1"), upstream)
    assert out.response is upstream
    assert out.fidelity == "partial"


def test_an_effect_that_raises_leaves_the_read_alone_and_flags_it(monkeypatch):
    raising = replace(services.REGISTRY["stripe"], effects=_raising)
    monkeypatch.setitem(services.REGISTRY, "stripe", raising)
    upstream = _upstream(CHARGE)
    out = _Overlay()([_write_exchange()], _read("/v1/charges/ch_REAL1"), upstream)
    assert out.response is upstream
    assert out.fidelity == "partial"


def test_rewrite_translates_a_cursor_and_names_what_it_removed_off_the_wire():
    """The dropped cursor comes back beside the request, never as a header on it.

    Handed back with the response, it makes the page after the minted refund fully modelled."""
    overlay, log = _Overlay(), [_write_exchange()]
    read = _read("/v1/refunds", query=f"limit=1&starting_after={MINTED}")
    out = overlay.rewrite(log, read)
    assert out is not None
    assert out.request.query == "limit=1"
    assert out.request.headers == read.headers
    assert out.removed == f"starting_after={MINTED}"
    upstream = _upstream({"object": "list", "has_more": False, "data": [{"id": "re_REAL1"}]})
    applied = overlay(log, out.request, upstream, out.removed)
    assert (applied.response, applied.fidelity) == (upstream, "full")


def test_rewrite_returns_none_when_there_is_nothing_to_translate():
    read = _read("/v1/refunds", query="limit=1&starting_after=re_REAL")
    assert _Overlay().rewrite([_write_exchange()], read) is None


def test_a_rewrite_never_raises(monkeypatch):
    raising = replace(services.REGISTRY["stripe"], rewrite=_raising)
    monkeypatch.setitem(services.REGISTRY, "stripe", raising)
    read = _read("/v1/refunds", query=f"starting_after={MINTED}")
    assert _Overlay().rewrite([_write_exchange()], read) is None


def test_a_rewrote_header_the_agent_sent_itself_is_not_trusted():
    """A forged `Irimi-Rewrote` header cannot hide the minted refund from page 1."""
    overlay, log = _Overlay(), [_write_exchange()]
    read = _read("/v1/refunds", headers=(("irimi-rewrote", f"starting_after={MINTED}"),))
    assert overlay.rewrite(log, read) is None
    upstream = _upstream({"object": "list", "has_more": False, "data": [{"id": "re_REAL1"}]})
    out = overlay(log, read, upstream)
    assert json.loads(out.response.body)["data"][0]["id"] == MINTED


# ---------------------------------------------------------------------------- Slack


def test_a_slack_history_read_after_a_faked_post_is_rewritten():
    upstream = _upstream(HISTORY)
    log = [_slack_post_exchange("1800000000.000001")]
    out = _Overlay()(log, _slack_read({"channel": "C0123"}), upstream)
    assert out.response is not upstream
    messages = json.loads(out.response.body)["messages"]
    assert [m["ts"] for m in messages] == ["1800000000.000001", "1700000000.000100"]
    assert out.fidelity == "full"


def test_a_slack_read_with_an_empty_write_log_is_untouched():
    upstream = _upstream(HISTORY)
    out = _Overlay()([], _slack_read({"channel": "C0123"}), upstream)
    assert out.response is upstream
    assert out.fidelity is None


def test_slack_writes_under_different_tokens_are_all_replayed():
    """A Slack write applies to a read made with another token: Slack has no scope headers."""
    upstream = _upstream(HISTORY)
    bot = SLACK_BOT
    log = [
        _slack_post_exchange("1800000000.000001", headers=(("authorization", f"Bearer {bot}A"),)),
        _slack_post_exchange("1800000000.000002", headers=(("authorization", f"Bearer {bot}B"),)),
    ]
    read = _slack_read({"channel": "C0123"}, headers=(("authorization", f"Bearer {bot}C"),))
    out = _Overlay()(log, read, upstream)
    messages = json.loads(out.response.body)["messages"]
    assert [m["ts"] for m in messages] == [
        "1800000000.000002",
        "1800000000.000001",
        "1700000000.000100",
    ]
    assert out.fidelity == "full"


def test_rewrite_leaves_a_slack_read_alone():
    """A Slack cursor is never translated: it is opaque and server-issued."""
    read = _slack_read({"channel": "C0123", "cursor": "dXNlcjpVMEc5V0ZYTlo="})
    log = [_slack_post_exchange("1800000000.000001")]
    assert _Overlay().rewrite(log, read) is None


def test_a_slack_replies_read_after_a_faked_reply_is_rewritten():
    """A faked reply appears at the tail of a replies read, with its parent's counts moved."""
    parent_ts = "1700000000.000100"
    upstream = _upstream(
        {"ok": True, "messages": [dict(HISTORY["messages"][0])], "has_more": False}
    )
    log = [_slack_post_exchange("1800000000.000001", thread_ts=parent_ts)]
    read = _slack_read({"channel": "C0123", "ts": parent_ts}, operation="conversations.replies")
    out = _Overlay()(log, read, upstream)
    assert out.response is not upstream
    messages = json.loads(out.response.body)["messages"]
    assert [m["ts"] for m in messages] == [parent_ts, "1800000000.000001"]
    assert messages[0]["reply_count"] == 1
    assert messages[0]["latest_reply"] == "1800000000.000001"
    assert messages[-1]["thread_ts"] == parent_ts
    assert out.fidelity == "full"


def test_a_slack_read_the_effects_do_not_model_is_untouched():
    """An unmodelled Slack read (`conversations.info`) is returned as the same, unflagged object."""
    upstream = _upstream({"ok": True, "channel": {"id": "C0123", "name": "general"}})
    read = _slack_read({"channel": "C0123"}, operation="conversations.info")
    out = _Overlay()([_slack_post_exchange("1800000000.000001")], read, upstream)
    assert out.response is upstream
    assert out.fidelity is None


def test_a_read_of_a_minted_refund_comes_back_as_that_refund_at_200():
    """A read of a refund irimi minted comes back as that refund at 200, not Stripe's 404."""
    overlay = _Overlay()
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
    """A 404 for an object irimi did not mint reaches the agent byte-identical and unstamped."""
    overlay = _Overlay()
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
    """The Slack case needs no status change: Slack answers `thread_not_found` at 200."""
    overlay = _Overlay()
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


# A faked write in the log, and a read whose live answer is an empty JSON list.
_EMPTY_LIST = Response(status=200, headers=(("content-type", "application/json"),), body=b"[]")
_FAKED = builders.exchange(
    response=_EMPTY_LIST, operation="POST /v1/charges", kind="unknown", answered_by="fake-L0"
)


def test_no_overlay_returns_upstream_response_unchanged():
    req, resp = builders.request(), _EMPTY_LIST
    overlaid = NoOverlay()(
        write_log=[], classification=classify(req), read_request=req, upstream_response=resp
    )
    assert overlaid.response is resp
    assert overlaid.fidelity is None


def test_no_overlay_ignores_the_write_log():
    req, resp = builders.request(), _EMPTY_LIST
    assert (
        NoOverlay()(
            write_log=[_FAKED],
            classification=classify(req),
            read_request=req,
            upstream_response=resp,
        ).response
        is resp
    )


def test_no_overlay_rewrite_translates_nothing():
    req = builders.request()
    assert (
        NoOverlay().rewrite(write_log=[_FAKED], classification=classify(req), read_request=req)
        is None
    )
