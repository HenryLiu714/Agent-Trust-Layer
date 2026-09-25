from irimi.exchange import Exchange, Request, Response
from irimi.overlay import NoOverlay
from irimi.store import NullStore


def _req() -> Request:
    return Request(
        method="GET",
        scheme="https",
        host="api.stripe.com",
        port=443,
        path="/v1/charges",
        query="",
        headers=(),
        body=b"",
    )


def _resp() -> Response:
    return Response(status=200, headers=(("content-type", "application/json"),), body=b"[]")


def _exchange() -> Exchange:
    return Exchange(
        request=_req(),
        response=_resp(),
        service="api.stripe.com",
        operation="POST /v1/charges",
        kind="unknown",
        answered_by="fake-L0",
        validation="unvalidated",
        run_id="7f3a",
    )


def test_null_store_record_and_close_return_none():
    store = NullStore()
    assert store.record(_exchange()) is None
    assert store.close() is None


def test_no_overlay_returns_upstream_response_unchanged():
    req, resp = _req(), _resp()
    overlaid = NoOverlay()(write_log=[], read_request=req, upstream_response=resp)
    assert overlaid.response is resp
    assert overlaid.fidelity is None


def test_no_overlay_ignores_write_log():
    req, resp = _req(), _resp()
    assert (
        NoOverlay()(write_log=[_exchange()], read_request=req, upstream_response=resp).response
        is resp
    )


def test_no_overlay_rewrite_returns_the_request_unchanged():
    req = _req()
    assert NoOverlay().rewrite(write_log=[_exchange()], read_request=req) is req
