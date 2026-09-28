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
import threading

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


def test_a_shape_in_a_body_that_only_claims_to_be_a_form_is_still_replaced():
    stored = _body(b'{"note": "sk_live_abc123"}', FORM)
    assert stored == f'{{"note": "{_ph("sk_live_abc123")}"}}'.encode()


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


def test_a_shape_glued_to_a_word_is_part_of_the_word():
    text = "run task_test_runner on risk-assessment-for-customer-accounts"
    assert redact_json(text, KEY) is text


def test_a_shape_is_replaced_in_every_place_text_is_stored():
    secret = "sk_live_abc123"
    response = Response(
        status=200,
        headers=(("Content-Type", "text/event-stream"),),
        body=f'data: {{"k": "{secret}"}}\n\n'.encode(),
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
    assert stored.response.body == f'data: {{"k": "{_ph(secret)}"}}\n\n'.encode()


def test_a_json_body_is_walked_whatever_its_content_type_claims():
    stored = _response_body(b'{"access_token": "at-1"}', ("Content-Type", "text/plain"))
    assert json.loads(stored) == {"access_token": _ph("at-1")}


def test_a_shape_in_a_json_key_is_replaced_too():
    assert redact_json({"sk_live_k": 1}, KEY) == {_ph("sk_live_k"): 1}


# ------------------------------------------------------------------------------ rule 4: paths


def test_the_whole_path_of_a_credential_path_host_is_replaced():
    path = "/services/T000/B000/XXXXsecret"
    ex = _exchange(_request(host="hooks.slack.com", path=path), service="slack")
    assert redact_exchange(ex, KEY).request.path == "/" + _ph(path)


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
