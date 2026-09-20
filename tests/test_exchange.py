from irimi.exchange import Exchange, Request, Response


def _req(scheme: str = "https", port: int = 443, query: str = "") -> Request:
    return Request(
        method="GET",
        scheme=scheme,
        host="api.stripe.com",
        port=port,
        path="/v1/charges",
        query=query,
        headers=(),
        body=b"",
    )


def test_url_https_default_port_omitted():
    assert _req().url == "https://api.stripe.com/v1/charges"


def test_url_https_non_default_port_included():
    assert _req(port=8443).url == "https://api.stripe.com:8443/v1/charges"


def test_url_query_appended_only_when_non_empty():
    assert _req(query="limit=1").url == "https://api.stripe.com/v1/charges?limit=1"
    assert "?" not in _req(query="").url


def test_url_http_default_port_omitted():
    assert _req(scheme="http", port=80).url == "http://api.stripe.com/v1/charges"


def _exchange() -> Exchange:
    return Exchange(
        request=_req(),
        response=Response(status=200, headers=(), body=b""),
        service="api.stripe.com",
        operation="GET /v1/charges",
        kind="read",
        answered_by="live",
        validation="unvalidated",
        run_id="7f3a",
    )


def test_exchange_default_flags_empty():
    assert _exchange().flags == ()


def test_exchanges_from_same_args_are_equal():
    assert _exchange() == _exchange()


def test_exchange_default_door_is_forward():
    assert _exchange().door == "forward"


def test_answered_by_names_the_three_ways_an_exchange_is_answered():
    """`delegated` is a frozen seam value, so it lands in Phase 1 even though the summary that
    counts it separately is #20 (design D20, issue #16)."""
    from typing import get_args

    from irimi.exchange import AnsweredBy

    assert set(get_args(AnsweredBy)) == {"live", "fake-L0", "delegated"}


def test_target_defaults_to_empty_and_records_a_delegated_address():
    ex = _exchange()
    assert ex.target == ""
    ex.target = "http://127.0.0.1:3000/refund"
    assert ex.target == "http://127.0.0.1:3000/refund"
