from dataclasses import replace

import pytest

from irimi.exchange import Request, Response
from irimi.pipeline import (
    Classification,
    ReverseDoorRefused,
    annotate,
    attribute_run,
    classify,
    detect_door,
    is_loopback,
    parse,
    respond,
    rewrite_reverse,
)


def _parse(**overrides) -> Request:
    args = {
        "method": "GET",
        "scheme": "https",
        "host": "api.stripe.com",
        "port": 443,
        "path_and_query": "/v1/charges",
        "headers": (),
        "body": b"",
    }
    args.update(overrides)
    return parse(**args)


def _req(method: str = "GET", headers: tuple[tuple[str, str], ...] = ()) -> Request:
    return Request(
        method=method,
        scheme="https",
        host="api.stripe.com",
        port=443,
        path="/v1/charges",
        query="",
        headers=headers,
        body=b"",
    )


ALLOWED = frozenset({"api.stripe.com", "127.0.0.1"})


def _door_req(
    path: str, host: str = "localhost", port: int = 4000, query: str = "", method: str = "GET"
) -> Request:
    """What the reverse door sees: plain http, addressed to the listener, host header set."""
    return Request(
        method=method,
        scheme="http",
        host=host,
        port=port,
        path=path,
        query=query,
        headers=(("host", f"{host}:{port}"), ("accept", "*/*")),
        body=b"",
    )


def test_parse_upper_cases_method():
    assert _parse(method="get").method == "GET"


def test_parse_lower_cases_host():
    assert _parse(host="API.Stripe.COM").host == "api.stripe.com"


def test_parse_splits_query():
    req = _parse(path_and_query="/v1/charges?limit=1")
    assert req.path == "/v1/charges"
    assert req.query == "limit=1"


def test_parse_without_query():
    req = _parse(path_and_query="/v1/charges")
    assert req.path == "/v1/charges"
    assert req.query == ""


def test_parse_empty_path_becomes_root():
    assert _parse(path_and_query="").path == "/"


def test_parse_lower_cases_header_names_only():
    req = _parse(headers=[("Content-Type", "Application/JSON"), ("Irimi-Run", "AbC")])
    assert req.headers == (("content-type", "Application/JSON"), ("irimi-run", "AbC"))


def test_parse_none_body_becomes_empty_bytes():
    assert _parse(body=None).body == b""


def test_parse_keeps_body_and_port():
    req = _parse(body=b'{"a":1}', port=8443)
    assert req.body == b'{"a":1}'
    assert req.port == 8443


@pytest.mark.parametrize("method", ["GET", "HEAD", "OPTIONS"])
def test_classify_safe_methods_are_reads(method):
    cls = classify(_req(method))
    assert cls.kind == "read"
    assert cls.flags == ()
    assert cls.operation == f"{method} /v1/charges"
    assert cls.service == "api.stripe.com"


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
def test_classify_other_methods_are_unknown(method):
    cls = classify(_req(method))
    assert cls.kind == "unknown"
    assert cls.flags == ("unclassified",)
    assert cls.operation == f"{method} /v1/charges"
    assert cls.service == "api.stripe.com"


def test_attribute_run_without_header_uses_default():
    assert attribute_run(_req(), "dflt") == "dflt"


def test_attribute_run_uses_header():
    assert attribute_run(_req(headers=(("irimi-run", "7f3a"),)), "dflt") == "7f3a"


def test_attribute_run_blank_header_uses_default():
    assert attribute_run(_req(headers=(("irimi-run", "   "),)), "dflt") == "dflt"


@pytest.mark.parametrize("answered_by", ["live", "fake-L0"])
def test_annotate_copies_fields(answered_by):
    req = _req()
    resp = Response(status=200, headers=(), body=b"{}")
    cls = Classification("api.stripe.com", "GET /v1/charges", "read", ())
    ex = annotate(req, resp, cls, answered_by, "7f3a")
    assert ex.request is req
    assert ex.response is resp
    assert ex.service == "api.stripe.com"
    assert ex.operation == "GET /v1/charges"
    assert ex.kind == "read"
    assert ex.answered_by == answered_by
    assert ex.validation == "unvalidated"
    assert ex.run_id == "7f3a"
    assert ex.flags == ()


def test_annotate_appends_extra_flags():
    cls = Classification("api.stripe.com", "POST /v1/charges", "unknown", ("unclassified",))
    ex = annotate(_req("POST"), None, cls, "live", "7f3a", extra_flags=("upstream-error",))
    assert ex.flags == ("unclassified", "upstream-error")
    assert ex.response is None


def test_respond_returns_exchange_response():
    resp = Response(status=200, headers=(), body=b"ok")
    cls = classify(_req())
    ex = annotate(_req(), resp, cls, "live", "7f3a")
    assert respond(ex) is resp


def test_respond_returns_none_without_response():
    ex = annotate(_req(), None, classify(_req()), "live", "7f3a")
    assert respond(ex) is None


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "::1"])
def test_detect_door_reverse_for_loopback_hosts(host):
    assert detect_door(_door_req("/api.stripe.com/v1", host=host), 4000) == "reverse"


def test_detect_door_forward_for_other_port():
    assert detect_door(_door_req("/api.stripe.com/v1", port=4001), 4000) == "forward"


def test_detect_door_forward_for_real_host():
    assert detect_door(_req(), 4000) == "forward"


def test_is_loopback():
    assert is_loopback("127.0.0.1")
    assert is_loopback("127.0.0.2")
    assert is_loopback("::1")
    assert not is_loopback("10.0.0.1")
    assert not is_loopback("")
    assert not is_loopback("nonsense")


def test_rewrite_reverse_basic():
    req = rewrite_reverse(_door_req("/api.stripe.com/v1/charges", query="limit=1"), ALLOWED)
    assert req.scheme == "https"
    assert req.host == "api.stripe.com"
    assert req.port == 443
    assert req.path == "/v1/charges"
    assert req.query == "limit=1"
    assert req.method == "GET"
    assert req.body == b""
    assert req.url == "https://api.stripe.com/v1/charges?limit=1"


def test_rewrite_reverse_rewrites_host_header():
    req = rewrite_reverse(_door_req("/api.stripe.com/v1/charges", query="limit=1"), ALLOWED)
    assert ("host", "api.stripe.com") in req.headers
    assert ("accept", "*/*") in req.headers
    assert not any("localhost" in value for _, value in req.headers)


def test_rewrite_reverse_lower_cases_host():
    assert rewrite_reverse(_door_req("/API.Stripe.COM/v1"), ALLOWED).host == "api.stripe.com"


def test_rewrite_reverse_explicit_port():
    req = rewrite_reverse(_door_req("/127.0.0.1:8443/hello"), ALLOWED)
    assert req.host == "127.0.0.1"
    assert req.port == 8443
    assert req.path == "/hello"
    assert ("host", "127.0.0.1:8443") in req.headers


@pytest.mark.parametrize("path", ["/api.stripe.com", "/api.stripe.com/"])
def test_rewrite_reverse_bare_host_becomes_root(path):
    assert rewrite_reverse(_door_req(path), ALLOWED).path == "/"


def test_rewrite_reverse_keeps_deeper_path():
    req = rewrite_reverse(_door_req("/api.stripe.com/v1/charges/ch_1/refunds"), ALLOWED)
    assert req.path == "/v1/charges/ch_1/refunds"


def test_rewrite_reverse_keeps_method_and_body():
    door = _door_req("/api.stripe.com/v1/refunds", method="POST")
    req = rewrite_reverse(replace(door, body=b"charge=ch_1"), ALLOWED)
    assert req.method == "POST"
    assert req.body == b"charge=ch_1"


def test_rewrite_reverse_refuses_unlisted_host():
    with pytest.raises(ReverseDoorRefused) as exc:
        rewrite_reverse(_door_req("/evil.example/x"), ALLOWED)
    assert "evil.example" in str(exc.value)
    assert "--allow-host" in str(exc.value)


def test_rewrite_reverse_refuses_missing_host():
    with pytest.raises(ReverseDoorRefused) as exc:
        rewrite_reverse(_door_req("/"), ALLOWED)
    assert "/<upstream-host>/<path>" in str(exc.value)


@pytest.mark.parametrize(
    "path", ["/api.stripe.com:abc/v1", "/api.stripe.com:0/v1", "/api.stripe.com:70000/v1"]
)
def test_rewrite_reverse_refuses_bad_port(path):
    with pytest.raises(ReverseDoorRefused) as exc:
        rewrite_reverse(_door_req(path), ALLOWED)
    assert "bad port" in str(exc.value)


def test_rewrite_reverse_empty_allowlist_refuses_everything():
    with pytest.raises(ReverseDoorRefused):
        rewrite_reverse(_door_req("/api.stripe.com/v1"), frozenset())


def test_annotate_door_defaults_to_forward():
    assert annotate(_req(), None, classify(_req()), "live", "7f3a").door == "forward"


def test_annotate_records_reverse_door():
    ex = annotate(_req(), None, classify(_req()), "live", "7f3a", door="reverse")
    assert ex.door == "reverse"
