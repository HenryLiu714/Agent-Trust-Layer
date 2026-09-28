"""Redaction before anything reaches disk (#69): one or more tests per rule in `irimi.redact`.

Every test builds its exchange by hand and asks `redact_exchange` or `redact_json` for the copy
the store (#70) will write. `tests/test_trace_e2e.py` runs the same function over what a real
`irimi shadow` run hands its store.
"""

import dataclasses
import hmac
import json
import logging
import os
import stat
import subprocess
import sys
import threading
import time

import pytest

from irimi import delegation, paths, redact
from irimi.exchange import REDACTION_FAILED_FLAG, Exchange, Request, Response
from irimi.redact import placeholder, redact_exchange, redact_json
from tests.test_trace import _filled

KEY = bytes(range(32))
OTHER_KEY = bytes(range(1, 33))
JSON = ("content-type", "application/json")
FORM = ("content-type", "application/x-www-form-urlencoded")
FAILED_BODY = redact.REDACTION_FAILED.encode()


def _ph(value: str, key: bytes = KEY) -> str:
    return placeholder(key, value)


def _request(**fields) -> Request:
    values = {
        "method": "POST",
        "scheme": "https",
        "host": "api.stripe.com",
        "port": 443,
        "path": "/v1/refunds",
        "query": "",
        "headers": (),
        "body": b"",
    }
    return Request(**{**values, **fields})


def _exchange(request: Request | None = None, response: Response | None = None, **fields):
    values = {
        "service": "stripe",
        "operation": "refunds.create",
        "kind": "write",
        "answered_by": "fake-L1",
        "validation": "unvalidated",
        "run_id": "r1",
    }
    return Exchange(request=request or _request(), response=response, **{**values, **fields})


def _body(body: bytes, *headers: tuple[str, str]) -> bytes:
    """The request body `body` stores as, sent with `headers`."""
    return redact_exchange(_exchange(_request(headers=headers, body=body)), KEY).request.body


def _response_body(body: bytes, *headers: tuple[str, str]) -> bytes:
    response = Response(status=200, headers=headers, body=body)
    stored = redact_exchange(_exchange(response=response), KEY).response
    assert stored is not None
    return stored.body


# ------------------------------------------------------------------------------ the placeholder


def test_a_placeholder_is_16_hex_digits_of_an_hmac_of_the_value_under_the_key():
    digest = hmac.new(KEY, b"sk_live_abc", "sha256").hexdigest()
    assert placeholder(KEY, "sk_live_abc") == f"<redacted:{digest[:16]}>"


def test_the_same_secret_gives_the_same_placeholder_and_another_key_a_different_one():
    """Design §5.3: what lets a replay match a request against its recording, and what keeps two
    installs' placeholders apart."""
    auth = "Bearer sk_test_one"
    ex = _exchange(_request(headers=(("authorization", auth),)))
    first, again = redact_exchange(ex, KEY), redact_exchange(ex, KEY)
    other = redact_exchange(ex, OTHER_KEY)
    assert first.request.headers == again.request.headers == (("authorization", _ph(auth)),)
    assert other.request.headers == (("authorization", _ph(auth, OTHER_KEY)),)
    assert _ph(auth, OTHER_KEY) != _ph(auth)


# ------------------------------------------------------------------------------ rule 1: headers


def test_a_credential_header_is_replaced_whole_on_the_request():
    basic = "Basic c2tfdGVzdF9hYmM6"  # base64 of `sk_test_abc:`, which no shape can see
    ex = _exchange(
        _request(headers=(("Authorization", basic), ("X-Api-Key", "k-123"), ("Accept", "*/*")))
    )
    assert redact_exchange(ex, KEY).request.headers == (
        ("Authorization", _ph(basic)),
        ("X-Api-Key", _ph("k-123")),
        ("Accept", "*/*"),
    )


def test_a_credential_header_is_replaced_on_the_response_and_set_cookie_there_only():
    response = Response(
        status=200,
        headers=(("Set-Cookie", "sid=abc; HttpOnly"), ("x-auth-token", "t"), ("Content-Type", "x")),
        body=b"",
    )
    ex = _exchange(_request(headers=(("set-cookie", "not-a-credential"),)), response)
    stored = redact_exchange(ex, KEY)
    assert stored.response is not None
    assert stored.response.headers == (
        ("Set-Cookie", _ph("sid=abc; HttpOnly")),
        ("x-auth-token", _ph("t")),
        ("Content-Type", "x"),
    )
    assert stored.request.headers == (("set-cookie", "not-a-credential"),)


def test_a_shape_in_a_header_name_is_replaced_on_the_request_and_the_response():
    secret = "sk_live_abc123"
    response = Response(status=200, headers=((f"X-{secret}", "v"),), body=b"")
    ex = _exchange(_request(headers=((f"x-{secret}", "v"), ("accept", "*/*"))), response)
    stored = redact_exchange(ex, KEY)
    assert stored.request.headers == ((f"x-{_ph(secret)}", "v"), ("accept", "*/*"))
    assert stored.response is not None
    assert stored.response.headers == ((f"X-{_ph(secret)}", "v"),)


def test_a_url_in_a_header_has_its_query_and_fragment_redacted_by_name():
    """An OAuth redirect hands the token back in `Location`'s query or fragment, and `Referer`
    repeats the page's; neither token has a shape. A URL's path to a credential-path host is the
    secret there too."""
    location = "https://app.example/cb?state=s1&access_token=at1#id_token=it1&x=1"
    referer = "https://hooks.slack.com/services/T1/B1/S3CR3T?a=1"
    response = Response(status=302, headers=(("Location", location),), body=b"")
    ex = _exchange(_request(headers=(("referer", referer), ("x-plain", "a?token=b"))), response)
    stored = redact_exchange(ex, KEY)
    assert stored.response is not None
    assert stored.response.headers == (
        (
            "Location",
            f"https://app.example/cb?state=s1&access_token={_ph('at1')}#id_token={_ph('it1')}&x=1",
        ),
    )
    assert stored.request.headers == (
        ("referer", f"https://hooks.slack.com/{_ph('/services/T1/B1/S3CR3T')}?a=1"),
        ("x-plain", "a?token=b"),
    )
    plain = "https://app.example/a%7Eb?c=d#e"
    same = redact_exchange(_exchange(_request(headers=(("referer", plain),))), KEY)
    assert same.request.headers == (("referer", plain),)


def test_the_credential_header_rule_is_the_one_delegation_strips_by():
    """One statement of "this header is a credential" (#69): a target may not see it, and disk may
    not see it. `delegation` re-exports it and states it nowhere else."""
    assert delegation.is_credential_header is redact.is_credential_header
    assert not hasattr(delegation, "CREDENTIAL_MARKERS")
    assert not hasattr(delegation, "CREDENTIAL_HEADERS")


# ------------------------------------------------------------------------------ rule 2: secret keys


def test_secret_keys_in_json_are_replaced_at_any_depth_and_token_counts_are_not():
    body = {
        "max_tokens": 1024,
        "usage": {"total_tokens": 7, "token_count": 3},
        "password": 123,
        "a": {"b": {"Client_Secret": "cs_1"}},
        "items": [{"TOKEN": "t-1"}, {"id": 1}],
    }
    stored = _body(json.dumps(body).encode(), JSON)
    expected = {
        "max_tokens": 1024,
        "usage": {"total_tokens": 7, "token_count": 3},
        "password": _ph("123"),
        "a": {"b": {"Client_Secret": _ph("cs_1")}},
        "items": [{"TOKEN": _ph("t-1")}, {"id": 1}],
    }
    assert stored == json.dumps(expected, separators=(",", ":"), ensure_ascii=False).encode()


def test_a_secret_keys_structured_value_is_hidden_by_its_json_with_sorted_keys():
    stored = json.loads(_body(b'{"secret": {"b": 1, "a": [2]}, "token": true}', JSON))
    assert stored == {"secret": _ph('{"a": [2], "b": 1}'), "token": _ph("true")}


def test_an_empty_or_null_secret_is_left_as_it_is():
    """A placeholder would turn "no token was sent" into "a token was sent"."""
    body = b'{"token": null, "password": ""}'
    assert _body(body, JSON) == body
    assert _body(b"token=&amount=1", FORM) == b"token=&amount=1"
    ex = _exchange(_request(headers=(("authorization", ""),)))
    assert redact_exchange(ex, KEY).request.headers == (("authorization", ""),)


def test_a_form_field_is_matched_by_its_last_bracket_segment():
    stored = _body(b"card[token]=tok_x&amount=100&expand[]=token", FORM)
    assert stored == f"card[token]={_ph('tok_x')}&amount=100&expand[]=token".encode()


def test_an_empty_bracket_segment_names_nothing_so_the_one_before_it_counts():
    """`token[]` is PHP's and Rails' spelling of a list of tokens: its name is `token`, not ``."""
    stored = _body(b"token[]=t1&a[password][]=p1&expand[]=x", FORM)
    assert stored == f"token[]={_ph('t1')}&a[password][]={_ph('p1')}&expand[]=x".encode()


def test_a_shape_in_a_form_fields_name_is_replaced():
    stored = _body(b"sk_live_abc123=1&x%20sk_test_abc=2", FORM)
    assert stored == f"{_ph('sk_live_abc123')}=1&x%20{_ph('sk_test_abc')}=2".encode()


def test_a_json_body_that_only_claims_to_be_a_form_is_walked_as_json():
    """Read as a form, `{"password": "hunter2"}` is one pair whose name is the whole document and
    whose value is empty, so no secret key is ever seen and `hunter2` has no shape."""
    stored = _body(b'{"password": "hunter2", "note": "sk_live_abc123"}', FORM)
    assert json.loads(stored) == {"password": _ph("hunter2"), "note": _ph("sk_live_abc123")}


def test_query_parameter_names_are_matched_whole_and_case_insensitively():
    ex = _exchange(_request(method="GET", query="API_KEY=abc&limit=3&tokens=9&access_token=x+y"))
    assert redact_exchange(ex, KEY).request.query == (
        f"API_KEY={_ph('abc')}&limit=3&tokens=9&access_token={_ph('x y')}"
    )


# ------------------------------------------------------------------------------ rule 3: shapes

SAMPLES = [
    "sk_live_abc123",
    "sk_test_abc123",
    "rk_live_abc123",
    "whsec_abc123",
    "xoxb-123-456-abc",
    "xapp-1-A0-abc",
    "ghp_" + "a" * 36,
    "github_pat_11AB_cd",
    "AKIAABCDEFGHIJKLMNOP",
    "sk-ant-api03-ab_c-d",
    "sk-proj-" + "a" * 20,
    "sk-" + "a" * 20,
]


def test_the_shapes_are_the_issues_list_exactly():
    assert redact.SECRET_PATTERNS == (
        r"sk_(live|test)_[A-Za-z0-9]+",
        r"rk_(live|test)_[A-Za-z0-9]+",
        r"whsec_[A-Za-z0-9]+",
        r"xox[abprs]-[A-Za-z0-9-]+",
        r"xapp-[A-Za-z0-9-]+",
        r"gh[pousr]_[A-Za-z0-9]{36,}",
        r"github_pat_[A-Za-z0-9_]+",
        r"AKIA[0-9A-Z]{16}",
        r"sk-ant-[A-Za-z0-9_-]+",
        r"sk-(proj-)?[A-Za-z0-9_-]{20,}",
    )


@pytest.mark.parametrize("secret", SAMPLES)
def test_every_shape_is_replaced_whole_inside_text(secret):
    assert redact_json(f"uses {secret}, then stops", KEY) == f"uses {_ph(secret)}, then stops"


@pytest.mark.parametrize(
    "text", ['"sk_live_a"', "=sk_live_a", "/sk_live_a", "_sk_live_a", "sk_live_a", "(sk_live_a)"]
)
def test_a_shape_after_punctuation_or_at_the_start_is_still_a_secret(text):
    assert "sk_live_a" not in str(redact_json(text, KEY))


# What raw text spells a separator as: a JSON string escape inside an SSE line or a text body, and
# a percent-escape in a path, a header or a form field's name. Each ends in a letter or a digit.
ESCAPES = ["\\n", "\\t", "\\r", "\\b", "\\f", "\\u0020", "\\u00A0", "%20", "%3D", "%2F", "%2c"]


@pytest.mark.parametrize("escape", ESCAPES)
def test_a_shape_after_an_escape_is_still_a_secret(escape):
    text = f"key:{escape}sk_live_abc123"
    assert redact_json(text, KEY) == f"key:{escape}{_ph('sk_live_abc123')}"


def test_a_shape_after_an_escape_is_replaced_where_text_is_stored_raw():
    secret = "sk_live_abc123"
    sse = f'data: {{"partial_json": "{{\\"note\\": \\"key:\\n{secret}'
    response = Response(status=200, headers=(("content-type", "text/event-stream"),), body=b"")
    ex = _exchange(
        _request(path=f"/v1/x/Bearer%20{secret}", headers=(("x-note", f"a%3D{secret}"),)),
        dataclasses.replace(response, body=f"{sse}\n\n".encode()),
    )
    stored = redact_exchange(ex, KEY)
    assert stored.request.path == f"/v1/x/Bearer%20{_ph(secret)}"
    assert stored.request.headers == (("x-note", f"a%3D{_ph(secret)}"),)
    assert stored.response is not None
    assert secret not in stored.response.body.decode()


def test_a_shape_glued_to_a_word_is_part_of_the_word():
    text = "run task_test_runner on risk-assessment-for-customer-accounts"
    assert redact_json(text, KEY) is text
    escaped = "\\ntask_test_runner %20risk-assessment-for-customer-accounts"
    assert redact_json(escaped, KEY) is escaped


def test_a_shape_is_replaced_in_every_place_text_is_stored():
    secret = "sk_live_abc123"
    response = Response(
        status=200,
        headers=(("Content-Type", "text/event-stream"),),
        body=f'event: e {secret}\ndata: {{"k": "{secret}"}}\n\n'.encode(),
    )
    ex = _exchange(
        _request(
            path=f"/v1/keys/{secret}",
            query=f"q=a+{secret}",
            headers=(("x-note", f"key {secret}"), JSON),
            body=json.dumps({"a": {"b": {"c": [f"deep {secret}"]}}}).encode(),
        ),
        response,
    )
    stored = redact_exchange(ex, KEY)
    assert stored.request.path == f"/v1/keys/{_ph(secret)}"
    assert stored.request.query == f"q=a+{_ph(secret)}"
    assert stored.request.headers[0] == ("x-note", f"key {_ph(secret)}")
    assert json.loads(stored.request.body) == {"a": {"b": {"c": [f"deep {_ph(secret)}"]}}}
    assert stored.response is not None
    assert (
        stored.response.body
        == f'event: e {_ph(secret)}\ndata: {{"k":"{_ph(secret)}"}}\n\n'.encode()
    )


def test_a_json_body_is_walked_whatever_its_content_type_claims():
    stored = _response_body(b'{"access_token": "at-1"}', ("Content-Type", "text/plain"))
    assert json.loads(stored) == {"access_token": _ph("at-1")}


def test_a_json_body_behind_a_byte_order_mark_is_walked_as_json():
    """`json.loads` refuses a leading U+FEFF as not JSON, so the body was scanned as text."""
    stored = _body(b'\xef\xbb\xbf{"token": "hunter2"}', JSON)
    assert stored == f'\ufeff{{"token":"{_ph("hunter2")}"}}'.encode()


def test_each_line_of_ndjson_and_each_sse_data_line_is_walked_as_json():
    """A body of one JSON document per line, or an SSE stream whose `data:` fields are each one,
    is not one JSON document, so it was scanned for shapes only and kept `hunter2`."""
    ndjson = b'{"token": "hunter2"}\r\n{"a": 1}\n\n'
    assert _response_body(ndjson, ("content-type", "application/x-ndjson")) == (
        f'{{"token":"{_ph("hunter2")}"}}\r\n{{"a": 1}}\n\n'.encode()
    )
    sse = b'event: e\ndata: {"password": "p1"}\ndata:[{"api_key": "k1"}]\ndata: [not json\n\n'
    assert (
        _response_body(sse, ("content-type", "text/event-stream"))
        == (
            f'event: e\ndata: {{"password":"{_ph("p1")}"}}\n'
            f'data:[{{"api_key":"{_ph("k1")}"}}]\ndata: [not json\n\n'
        ).encode()
    )


def test_an_sse_line_ended_by_a_lone_carriage_return_is_walked_as_its_own_line():
    """SSE ends a line at CRLF, LF or a lone CR. Split at LF alone, a CR-only stream was one line
    that was not JSON, scanned for shapes only, and kept `hunter2`."""
    sse = b'data: {"token": "hunter2"}\rdata: {"a": 1}\r\r'
    assert _response_body(sse, ("content-type", "text/event-stream")) == (
        f'data: {{"token":"{_ph("hunter2")}"}}\rdata: {{"a": 1}}\r\r'.encode()
    )


def test_a_json_line_the_walk_cannot_see_whole_fails_the_body_closed():
    body = b'data: {"token": "t1", "token": null}\n\n'
    stored = _response_body(body, ("content-type", "text/event-stream"))
    assert stored == FAILED_BODY


def test_a_shape_in_a_json_key_is_replaced_too():
    assert redact_json({"sk_live_k": 1}, KEY) == {_ph("sk_live_k"): 1}


# ------------------------------------------------------------------------------ rule 4: paths


def test_the_whole_path_of_a_credential_path_host_is_replaced():
    path = "/services/T000/B000/XXXXsecret"
    ex = _exchange(_request(host="hooks.slack.com", path=path), service="slack")
    assert redact_exchange(ex, KEY).request.path == "/" + _ph(path)


def test_a_credential_path_host_spelled_with_its_root_dot_is_the_same_host():
    path = "/services/T000/B000/XXXXsecret"
    ex = _exchange(_request(host="hooks.slack.com.", path=path))
    assert redact_exchange(ex, KEY).request.path == "/" + _ph(path)


def test_a_credential_path_is_replaced_wherever_else_the_exchange_carries_it():
    """irimi's own 502 for an unreachable answer target names the target's URL, and a bare-origin
    target's URL ends in the webhook's path; a stub may echo it back in a header. `/` alone is
    every URL's path and is left alone."""
    path = "/services/T000/B000/XXXXsecret"
    reason = f"answer target 'http://127.0.0.1:9/{path[1:]}' could not be reached"
    response = Response(
        status=502,
        headers=(JSON, ("x-echo", f"got {path}")),
        body=json.dumps({"error": {"type": "irimi_target_failed", "message": reason}}).encode(),
    )
    request = _request(host="hooks.slack.com", path=path, headers=(JSON,), body=b'{"text": "hi"}')
    stored = redact_exchange(_exchange(request, response), KEY)
    hidden = "/" + _ph(path)
    assert stored.response is not None
    assert json.loads(stored.response.body)["error"]["message"] == reason.replace(path, hidden)
    assert stored.response.headers[1] == ("x-echo", f"got {hidden}")
    assert stored.request.body == b'{"text": "hi"}'
    root = _exchange(_request(host="hooks.slack.com", path="/"), response)
    root_response = redact_exchange(root, KEY).response
    assert root_response is not None
    assert root_response.body == response.body


def test_a_credential_path_holding_a_shape_is_replaced_whole_wherever_else_it_is_carried():
    """The shape rule runs first, so a path holding `xoxb-…` was no longer there literally: the
    header and the 502 kept the team and bot ids beside a placeholder of the shape alone, not the
    one placeholder the request's path is stored as."""
    path = "/services/T0SHAPE/B0SHAPE/xoxb-12-abc"
    reason = f"answer target 'http://127.0.0.1:9{path}' could not be reached"
    response = Response(
        status=502,
        headers=(JSON, ("x-echo", f"got {path}")),
        body=json.dumps({"error": {"type": "irimi_target_failed", "message": reason}}).encode(),
    )
    request = _request(host="hooks.slack.com", path=path, headers=(("x-note", f"see {path}"),))
    stored = redact_exchange(_exchange(request, response), KEY)
    hidden = "/" + _ph(path)
    assert stored.request.path == hidden
    assert stored.request.headers == (("x-note", f"see {hidden}"),)
    assert stored.response is not None
    assert stored.response.headers[1] == ("x-echo", f"got {hidden}")
    assert json.loads(stored.response.body)["error"]["message"] == reason.replace(path, hidden)


def test_an_operation_named_after_the_path_is_redacted_with_the_path():
    """A request no route matched is named `METHOD /path` (`pipeline.classify`), and slack.yaml
    lists no route for `/workflows/...`: the operation would store the webhook's secret."""
    path = "/workflows/T1/A1/123/S3CR3T"
    hook = _exchange(
        _request(host="hooks.slack.com", path=path), service="slack", operation=f"POST {path}"
    )
    assert redact_exchange(hook, KEY).operation == f"POST /{_ph(path)}"
    keyed = _exchange(_request(method="GET", path="/v1/keys/sk_live_abc"))
    keyed = dataclasses.replace(keyed, operation="GET /v1/keys/sk_live_abc")
    assert redact_exchange(keyed, KEY).operation == f"GET /v1/keys/{_ph('sk_live_abc')}"
    assert redact_exchange(_exchange(), KEY).operation == "refunds.create"
    named = _exchange(operation="keys.sk_live_abc")
    assert redact_exchange(named, KEY).operation == f"keys.{_ph('sk_live_abc')}"


def test_the_answer_target_is_redacted_by_the_requests_rules():
    """A bare-origin target keeps the request's path (`delegation.target_url`), so a webhook's
    secret path reaches `Exchange.target` too."""
    path = "/services/T000/B000/XXXXsecret"
    hook = _exchange(
        _request(host="hooks.slack.com", path=path),
        service="slack",
        target=f"http://127.0.0.1:3000{path}?token=t1",
    )
    assert (
        redact_exchange(hook, KEY).target == f"http://127.0.0.1:3000/{_ph(path)}?token={_ph('t1')}"
    )
    stripe = _exchange(target="http://127.0.0.1:3000/v1/refunds?k=sk_test_abc")
    assert redact_exchange(stripe, KEY).target == (
        f"http://127.0.0.1:3000/v1/refunds?k={_ph('sk_test_abc')}"
    )
    assert redact_exchange(_exchange(), KEY).target == ""


# ------------------------------------------------------------------------------ rules 5 and 6


def test_a_body_with_nothing_to_redact_comes_back_byte_for_byte():
    body = b'{ "b" : 1,\n  "a": [ "x", "\\u00e9" ], "max_tokens": 5 }'
    assert _body(body, JSON) == body
    form = b"amount=100&metadata%5Bnote%5D=a+b%20c"
    assert _body(form, FORM) == form
    text = b"event: ping\ndata: {}\n\n"
    assert _response_body(text, ("Content-Type", "text/event-stream")) == text


def test_a_body_that_is_not_utf8_is_stored_unscanned():
    body = b"\xff\xfe sk_live_abc123"
    assert _body(body, ("content-type", "application/octet-stream")) == body


def test_redact_exchange_returns_a_new_exchange_and_copies_every_other_field():
    """`dataclasses.replace`, so every field survives - `started_at` and `ended_at` included, and
    any field added to Exchange later. Nothing in the original changes."""
    original = _filled(Exchange)
    before = dataclasses.replace(original)
    stored = redact_exchange(original, KEY)
    assert stored is not original
    assert original == before
    redacted = {"request", "response", "target", "flags"}
    for field in dataclasses.fields(Exchange):
        if field.name not in redacted:
            assert getattr(stored, field.name) == getattr(original, field.name), field.name
    assert (stored.started_at, stored.ended_at) == (original.started_at, original.ended_at)
    assert stored.flags == original.flags


# ------------------------------------------------------------------------------ rule 7: fail closed


def test_a_parser_that_raises_stores_neither_body_and_flags_the_exchange(monkeypatch):
    request = _request(headers=(JSON,), body=b'{"amount": 100}')
    response = Response(status=200, headers=(JSON,), body=b'{"id": "re_1"}')
    ex = _exchange(request, response, flags=("fidelity:L1",))

    def boom(*args, **kwargs):
        raise RuntimeError("parser broke")

    monkeypatch.setattr(redact.json, "loads", boom)
    stored = redact_exchange(ex, KEY)
    assert stored.response is not None
    assert (stored.request.body, stored.response.body) == (FAILED_BODY, FAILED_BODY)
    assert stored.flags == ("fidelity:L1", REDACTION_FAILED_FLAG)
    assert stored.request.headers == request.headers
    again = redact_exchange(dataclasses.replace(ex, flags=stored.flags), KEY)
    assert again.flags.count(REDACTION_FAILED_FLAG) == 1


@pytest.mark.parametrize(
    "body",
    [
        b'{"token": "tok_one", "token": null}',
        b'{"a": [{"password": "hunter2", "password": ""}]}',
        b'{"token": "tok_two", "n": ' + b"1" * 5000 + b"}",
    ],
    ids=["a-repeated-key", "a-repeated-key-deeper", "an-integer-past-the-digit-limit"],
)
def test_json_the_walk_cannot_see_whole_fails_closed_rather_than_scanned_as_text(body):
    """`json.loads` keeps a repeated key's last value, and refuses a long integer with a plain
    ValueError: a walk of the one, or a text scan of the other, would store the secret as sent."""
    stored = redact_exchange(_exchange(_request(headers=(JSON,), body=body)), KEY)
    assert stored.request.body == FAILED_BODY
    assert stored.flags == (REDACTION_FAILED_FLAG,)


def test_a_body_too_deep_to_redact_fails_closed_and_the_log_names_no_value(caplog):
    """Python 3.12's `json.loads` raises RecursionError on this body and 3.14's parses it, so the
    walk raises instead. Either way the body is not stored."""
    body = b"[" * 100_000 + b'"sk_live_abc"' + b"]" * 100_000
    with caplog.at_level(logging.WARNING, logger="irimi.redact"):
        stored = redact_exchange(_exchange(_request(headers=(JSON,), body=body)), KEY)
    assert stored.request.body == FAILED_BODY
    assert REDACTION_FAILED_FLAG in stored.flags
    assert "the request body" in caplog.text
    assert "sk_live" not in caplog.text


def test_a_header_that_cannot_be_hashed_fails_closed_alone(caplog):
    """A lone surrogate cannot be encoded for the HMAC, and the UnicodeEncodeError's own message
    would quote it: the log names the exception's type only."""
    headers = (("authorization", "Bearer \udcff"), ("accept", "*/*"))
    with caplog.at_level(logging.WARNING, logger="irimi.redact"):
        stored = redact_exchange(_exchange(_request(headers=headers)), KEY)
    assert stored.request.headers == (
        ("authorization", redact.REDACTION_FAILED),
        ("accept", "*/*"),
    )
    assert stored.flags == (REDACTION_FAILED_FLAG,)
    assert "UnicodeEncodeError" in caplog.text
    assert "\udcff" not in caplog.text
    assert "\\udcff" not in caplog.text  # the exception's message spells it this way


def test_a_path_that_cannot_be_hashed_fails_closed_and_still_starts_with_a_slash():
    ex = _exchange(_request(host="hooks.slack.com", path="/services/\udcff"))
    stored = redact_exchange(ex, KEY)
    assert stored.request.path == "/" + redact.REDACTION_FAILED
    assert stored.flags == (REDACTION_FAILED_FLAG,)


def test_a_lone_surrogate_in_a_body_that_changed_fails_that_body_alone():
    """`json.loads` accepts `"\\ud800"`, and the re-serialized body cannot be encoded as UTF-8."""
    request = _request(headers=(JSON,), body=b'{"token": "t1", "note": "\\ud800"}')
    response = Response(status=200, headers=(JSON,), body=b'{"id": "re_1"}')
    stored = redact_exchange(_exchange(request, response), KEY)
    assert stored.request.body == FAILED_BODY
    assert stored.response is not None
    assert stored.response.body == b'{"id": "re_1"}'
    assert stored.flags == (REDACTION_FAILED_FLAG,)


# ------------------------------------------------------------------------------ redact_json


def test_redact_json_hides_trigger_and_tool_arguments():
    args = {"argv": ["agent.py", "--key", "sk_test_abc"], "config": {"API_KEY": "v", "n": 2}}
    assert redact_json(args, KEY) == {
        "argv": ["agent.py", "--key", _ph("sk_test_abc")],
        "config": {"API_KEY": _ph("v"), "n": 2},
    }
    assert args["config"] == {"API_KEY": "v", "n": 2}
    untouched = {"a": [1, 2.5, None, True, "x"]}
    assert redact_json(untouched, KEY) is untouched


def test_redact_json_fails_closed_on_a_value_that_is_not_json():
    """A tuple is not JSON, and `json.dumps` would still write its secret as an array."""
    assert redact_json(("sk_live_abc",), KEY) == redact.REDACTION_FAILED
    assert redact_json({"a": {"b"}}, KEY) == redact.REDACTION_FAILED


def test_redact_json_fails_closed_on_a_value_too_deep_to_walk():
    deep: list = []
    for _ in range(100_000):
        deep = [deep]
    assert redact_json(deep, KEY) == redact.REDACTION_FAILED


# ------------------------------------------------------------------------------ the key


def test_load_key_creates_a_0600_key_of_32_bytes_once(tmp_path):
    home = tmp_path / "home"
    key = redact.load_key(home)
    path = home / paths.REDACT_KEY_NAME
    assert len(key) == 32
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert redact.load_key(home) == key
    assert path.read_bytes() == key
    assert sorted(p.name for p in home.iterdir()) == [paths.REDACT_KEY_NAME]


def test_concurrent_load_key_calls_return_one_key(tmp_path):
    home = tmp_path / "home"
    barrier = threading.Barrier(8)
    keys: list[bytes] = []

    def load() -> None:
        barrier.wait()
        keys.append(redact.load_key(home))

    threads = [threading.Thread(target=load) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(keys) == 8
    assert set(keys) == {(home / paths.REDACT_KEY_NAME).read_bytes()}
    assert sorted(p.name for p in home.iterdir()) == [paths.REDACT_KEY_NAME]


def test_a_process_that_loses_the_race_reads_the_winners_key(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    winner = b"w" * 32
    real_link = os.link

    def link_after_the_winner(src, dst):
        (home / paths.REDACT_KEY_NAME).write_bytes(winner)
        real_link(src, dst)

    monkeypatch.setattr(redact.os, "link", link_after_the_winner)
    assert redact.load_key(home) == winner
    assert sorted(p.name for p in home.iterdir()) == [paths.REDACT_KEY_NAME]


def test_a_key_file_irimi_did_not_write_is_refused(tmp_path):
    (tmp_path / paths.REDACT_KEY_NAME).write_bytes(b"short")
    with pytest.raises(redact.RedactKeyError, match="holds 5 bytes, not 32"):
        redact.load_key(tmp_path)


def test_a_key_path_that_is_not_a_regular_file_is_refused(tmp_path):
    """A directory and a dangling symlink: neither is a key, and each would otherwise be a bare
    OSError."""
    for name, make in (
        ("dir", lambda p: p.mkdir()),
        ("dangling", lambda p: p.symlink_to(tmp_path / "missing")),
    ):
        home = tmp_path / name
        home.mkdir()
        make(home / paths.REDACT_KEY_NAME)
        with pytest.raises(redact.RedactKeyError, match="not a regular file"):
            redact.load_key(home)


def test_a_symlinked_key_is_followed_so_two_installs_can_share_one(tmp_path):
    """A key mounted from elsewhere (a Kubernetes Secret volume is a tree of symlinks) is this
    install's key, and it is read, never replaced (#69)."""
    shared = tmp_path / "secret" / "redact.key"
    shared.parent.mkdir()
    shared.write_bytes(b"k" * 32)
    home = tmp_path / "home"
    home.mkdir()
    (home / paths.REDACT_KEY_NAME).symlink_to(shared)
    assert redact.load_key(home) == b"k" * 32
    assert shared.read_bytes() == b"k" * 32


def test_load_key_creates_a_missing_home_0700(tmp_path):
    """As `ca.generate_ca` makes the directory that holds its private key; an existing home keeps
    its mode, because it holds more than the key and the user may have chosen it."""
    old = os.umask(0o022)
    try:
        home = tmp_path / "a" / "home"
        redact.load_key(home)
        assert stat.S_IMODE(home.stat().st_mode) == 0o700
        mine = tmp_path / "mine"
        mine.mkdir(mode=0o755)
        redact.load_key(mine)
        assert stat.S_IMODE(mine.stat().st_mode) == 0o755
    finally:
        os.umask(old)


# Waits for the test's go file, then loads the key once and prints it. A separate interpreter per
# racer, because `O_CREAT | O_EXCL` and `os.link` are there for another PROCESS: threads share one.
KEY_RACER = """
import sys, time
from pathlib import Path
from irimi import redact

home, go, ready = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
ready.touch()
while not go.exists():
    time.sleep(0.001)
print(redact.load_key(home).hex())
"""


def test_load_key_raced_by_separate_processes_gives_each_the_one_0600_key(tmp_path):
    """#69's "if creation loses a race with another process, it reads the winner's file", between
    processes. Eight interpreters, released together against one home that does not exist yet,
    all read back the one key: 32 bytes, the file mode 0600, and no temporary file left behind."""
    home = tmp_path / "fresh-home"
    go = tmp_path / "go"
    ready = [tmp_path / f"ready-{i}" for i in range(8)]
    racers = [
        subprocess.Popen(
            [sys.executable, "-c", KEY_RACER, str(home), str(go), str(r)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for r in ready
    ]
    try:
        deadline = time.monotonic() + 20
        while not all(r.exists() for r in ready):
            assert time.monotonic() < deadline, "a racer never started"
            time.sleep(0.005)
        go.touch()
        outs = [racer.communicate(timeout=20) for racer in racers]
    finally:
        for racer in racers:
            racer.kill()
    assert [racer.returncode for racer in racers] == [0] * 8, [err for _, err in outs]
    (key,) = {bytes.fromhex(out.decode().strip()) for out, _ in outs}
    assert len(key) == redact.KEY_BYTES
    path = home / paths.REDACT_KEY_NAME
    assert path.read_bytes() == key
    assert stat.S_IMODE(path.stat().st_mode) == redact.KEY_MODE
    assert [p.name for p in home.iterdir()] == [paths.REDACT_KEY_NAME]
