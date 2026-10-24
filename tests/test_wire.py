"""`engine.wire`: parsing a request off the wire, attributing it to a run, irimi's own errors."""

import json
from typing import Any

import pytest

from irimi.core.exchange import Request
from irimi.engine import wire
from irimi.engine.wire import attribute_run, parse
from irimi.protocol.headers import ANSWERED_BY_HEADER
from tests.support import builders


def _parse(**overrides) -> Request:
    args: dict[str, Any] = {
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


def test_attribute_run_without_header_uses_default():
    assert attribute_run(builders.request(), "dflt") == "dflt"


def test_attribute_run_uses_header():
    assert attribute_run(builders.request(headers=(("irimi-run", "7f3a"),)), "dflt") == "7f3a"


def test_attribute_run_blank_header_uses_default():
    assert attribute_run(builders.request(headers=(("irimi-run", "   "),)), "dflt") == "dflt"


@pytest.mark.parametrize("value", ["../x", "", "a" * 65, "a b"])
def test_attribute_run_treats_an_invalid_run_id_as_absent(value):
    """A run id `trace.is_valid_run_id` refuses never names a run: it becomes a directory name."""
    assert attribute_run(builders.request(headers=(("irimi-run", value),)), "dflt") == "dflt"


def test_attribute_run_accepts_the_longest_valid_run_id():
    assert attribute_run(builders.request(headers=(("irimi-run", "a" * 64),)), "dflt") == "a" * 64


def test_attribute_run_compares_the_header_name_case_insensitively():
    """As `header_value` does: a `Request` built outside `parse` keeps its spelling."""
    assert attribute_run(builders.request(headers=(("Irimi-Run", "run_A-1"),)), "dflt") == "run_A-1"


def test_irimis_own_error_is_one_json_shape_and_stamped_by_the_caller():
    error = wire.irimi_error(403, "irimi_refused", "no", (("x-extra", "1"),))
    assert error.status == 403
    assert error.headers == (("content-type", "application/json"), ("x-extra", "1"))
    assert json.loads(error.body) == {"error": {"type": "irimi_refused", "message": "no"}}
    stamped = wire.stamp(error, "irimi")
    assert stamped.header(ANSWERED_BY_HEADER) == "irimi"
    assert wire.stamp(error, "live") is error
