import pytest

from irimi.exchange import Request, Response
from irimi.pipeline import (
    Classification,
    annotate,
    attribute_run,
    classify,
    parse,
    respond,
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
