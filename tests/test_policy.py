import json
import re
from dataclasses import replace
from types import SimpleNamespace

import pytest

from irimi import delegation, echo, fixture, pipeline, servicemap
from irimi.exchange import (
    FIDELITY_L0_FLAG,
    FIDELITY_L1_FLAG,
    FIXTURE_FAILED_FLAG,
    Request,
    Response,
)
from irimi.pipeline import classify
from irimi.policy import ShadowPolicy

SHIPPED = servicemap.MapIndex(tuple(servicemap.load_shipped()))


def _req(
    method: str = "GET",
    host: str = "api.stripe.com",
    path: str = "/v1/charges",
    body: bytes = b"",
    content_type: str | None = None,
) -> Request:
    headers = (("content-type", content_type),) if content_type is not None else ()
    return Request(
        method=method,
        scheme="https",
        host=host,
        port=443,
        path=path,
        query="",
        headers=headers,
        body=body,
    )


def _answer(request: Request):
    """Classify against the shipped maps, then answer - the same order the addon uses."""
    return ShadowPolicy().answer(request, classify(request, SHIPPED))


@pytest.mark.parametrize("kind", ["read", "llm", "telemetry"])
def test_shadow_forwards_live(kind):
    cls = replace(classify(_req(), SHIPPED), kind=kind)
    ans = ShadowPolicy().answer(_req(), cls)
    assert ans.answered_by == "live"
    assert ans.response is None
    assert ans.flags == ()


@pytest.mark.parametrize("kind", ["write", "unknown"])
def test_shadow_fakes_l0(kind):
    cls = replace(classify(_req("POST"), SHIPPED), kind=kind)
    ans = ShadowPolicy().answer(_req("POST"), cls)
    assert ans.answered_by == "fake-L0"
    assert isinstance(ans.response, Response)
    assert ans.response.status == 200
    assert ("content-type", "application/json") in ans.response.headers
    assert ans.flags == (FIDELITY_L0_FLAG,)
    assert isinstance(json.loads(ans.response.body), dict)


def test_shadow_policy_shape():
    p = ShadowPolicy()
    assert p.name == "shadow"
    assert callable(p.answer)


def test_unmapped_write_reflects_and_stamps_created():
    """Nothing matched, so there is no id to mint and no object to name."""
    ans = _answer(
        _req(
            "POST",
            host="example.invalid",
            path="/things",
            body=b'{"a": 1, "b": "two"}',
            content_type="application/json",
        )
    )
    body = json.loads(ans.response.body)
    assert body["a"] == 1
    assert body["b"] == "two"
    assert isinstance(body["created"], int)
    assert "id" not in body
    assert "object" not in body


def test_stripe_refund_is_a_parseable_refund():
    """The shipped `refunds.create` route: minted ids, a derived object, the posted fields back."""
    ans = _answer(
        _req(
            "POST",
            path="/v1/refunds",
            body=b"charge=ch_test&amount=4900",
            content_type="application/x-www-form-urlencoded",
        )
    )
    body = json.loads(ans.response.body)
    assert re.fullmatch(r"re_[A-Za-z0-9]{24}", body["id"])
    assert re.fullmatch(r"txn_[A-Za-z0-9]{24}", body["balance_transaction"])
    assert body["object"] == "refund"
    assert body["amount"] == 4900  # an int, as stripe-python's own Refund.amount is
    assert body["charge"] == "ch_test"
    assert isinstance(body["created"], int)


def test_a_minted_id_overwrites_a_reflected_field_of_the_same_name():
    ans = _answer(
        _req(
            "POST",
            path="/v1/refunds",
            body=b"id=whatever-the-caller-sent",
            content_type="application/x-www-form-urlencoded",
        )
    )
    assert json.loads(ans.response.body)["id"].startswith("re_")


def test_livemode_is_overwritten_rather_than_taken_from_the_fixture():
    """Every shipped fixture already says `false`, so the rule is only visible against one that
    does not - and a fixture regenerated from a live-mode example would otherwise tell an agent
    its shadow write happened on the real ledger."""
    route = servicemap.Route(
        method="POST", path="/v1/things", operation="things.create", kind="write", fixture="thing"
    )
    body = echo.l1_body(_req("POST"), route, {"object": "thing", "livemode": True})
    assert body["livemode"] is False


def test_slack_write_gets_slacks_own_envelope():
    ans = _answer(
        _req(
            "POST",
            host="slack.com",
            path="/api/chat.postMessage",
            body=b"channel=C1&text=hi",
            content_type="application/x-www-form-urlencoded",
        )
    )
    body = json.loads(ans.response.body)
    assert body["ok"] is True
    assert re.fullmatch(r"\d+\.\d{6}", body["ts"])
    assert body["channel"] == "C1"
    assert "created" not in body
    assert "object" not in body
    assert "id" not in body
    assert "text" not in body  # the envelope replaces the echo, it does not extend it


def test_two_slack_writes_in_the_same_second_get_different_timestamps():
    """Slack uses `ts` as a message's identifier and as `thread_ts`, so a collision is two
    messages that are the same message. The random six digits collided 9% of the time in 200,000
    draws (#29); they are a counter now, so a run can post a million messages a second before one
    repeats."""
    seen = [echo.slack_ts() for _ in range(5_000)]
    assert len(set(seen)) == len(seen)
    # And increasing, because that is the other half of what a `ts` means: real ones sort by time,
    # so code that orders a transcript by `ts` reads the same answer here as it would from Slack.
    assert [float(ts) for ts in seen] == sorted(float(ts) for ts in seen)
    assert all(re.fullmatch(r"\d+\.\d{6}", ts) for ts in seen)


def test_a_slack_timestamp_still_names_the_current_second(monkeypatch):
    """The counter must not drift off the clock: a `ts` is a real epoch time, and an SDK that
    renders one as a date has to get today."""
    monkeypatch.setattr(echo, "_last_slack_ts", (0, 0))
    monkeypatch.setattr(echo.time, "time", lambda: 1_700_000_000.9)
    assert echo.slack_ts() == "1700000000.000000"
    assert echo.slack_ts() == "1700000000.000001"


def test_slack_write_reads_the_json_body_slack_sdk_actually_posts():
    """slack_sdk 3.x sends `application/json;charset=utf-8`, not a form - so parse both."""
    ans = _answer(
        _req(
            "POST",
            host="slack.com",
            path="/api/chat.postMessage",
            body=b'{"channel": "C1", "text": "hi"}',
            content_type="application/json;charset=utf-8",
        )
    )
    body = json.loads(ans.response.body)
    assert body["ok"] is True
    assert body["channel"] == "C1"


def test_an_unlisted_slack_route_does_not_get_the_slack_envelope():
    """`ok: true` is a claim of success, and we only know what success looks like for a route the
    maps claim. Answering an unmapped call with the envelope sends slack_sdk down its success
    branch into an uncatchable crash later (`files_upload_v2` reads `upload_url = None` and dies
    inside urllib); the generic echo leaves it raising the SlackApiError callers already catch."""
    ans = _answer(_req("POST", host="slack.com", path="/api/files.getUploadURLExternal"))
    body = json.loads(ans.response.body)
    assert "ok" not in body
    assert "created" in body


def test_reactions_add_answers_the_bare_ok_slack_really_sends():
    """Slack answers reactions.add with `{"ok": true}` and nothing else. The `ts` and `channel`
    the one-shape envelope used to add here are chat.postMessage's, and an agent that read the
    `ts` of its own reaction as a message id was reading a field the real API never sent (#42)."""
    ans = _answer(
        _req(
            "POST",
            host="slack.com",
            path="/api/reactions.add",
            body=b"channel=C1&name=tada&timestamp=1503435956.000247",
            content_type="application/x-www-form-urlencoded",
        )
    )
    assert json.loads(ans.response.body) == {"ok": True}
    assert ans.answered_by == "fake-L0"


@pytest.mark.parametrize(
    ("broken", "path"),
    [("fake_body", "/v1/charges/ch_1/refund"), ("l1_body", "/v1/refunds")],
)
def test_a_faker_that_fails_still_answers_locally(monkeypatch, broken, path):
    """A raising policy makes mitmproxy forward the flow, and a forwarded write is a real write.

    Both bodies, because L1 put a second one on the path: `/v1/refunds` is a fixture route and
    the unmapped Stripe path is an L0 one, so each parameter breaks the function its own request
    actually reaches. Breaking only `fake_body` would leave the L1 route answering normally and
    the test passing while proving nothing about it.
    """

    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(echo, broken, boom)
    ans = _answer(_req("POST", path=path))
    assert ans.answered_by == "fake-L0"
    assert ans.response.body == b"{}"
    assert ans.flags == (FIDELITY_L0_FLAG,)


def test_reflect_reads_a_json_object():
    assert echo.reflect(_req(body=b'{"a": 1}', content_type="application/json")) == {"a": 1}


def test_reflect_honours_content_type_parameters():
    req = _req(body=b'{"a": 1}', content_type="application/json; charset=utf-8")
    assert echo.reflect(req) == {"a": 1}


@pytest.mark.parametrize(
    "body",
    [b"[1, 2]", b'"a string"', b"null", b"7", b"{not json", b"", b"\xff\xfe\x00bad"],
)
def test_reflect_returns_nothing_for_a_body_that_is_not_a_json_object(body):
    assert echo.reflect(_req(body=body, content_type="application/json")) == {}


@pytest.mark.parametrize("body", [b'{"a": NaN}', b'{"a": Infinity}', b'{"a": 1e400}'])
def test_reflect_refuses_a_number_a_strict_json_parser_would_refuse(body):
    """json.dumps writes NaN/Infinity straight back out; the echo has to stay parseable."""
    assert echo.reflect(_req(body=body, content_type="application/json")) == {}


def test_reflect_reads_a_form_body():
    req = _req(body=b"a=1&b=two&c=", content_type="application/x-www-form-urlencoded")
    assert echo.reflect(req) == {"a": 1, "b": "two", "c": ""}


@pytest.mark.parametrize("value", [b"007", b"0012345", b"000123456789012345678", b"1e3", b"-1"])
def test_reflect_leaves_a_non_canonical_number_alone(value):
    req = _req(body=b"v=" + value, content_type="application/x-www-form-urlencoded")
    assert echo.reflect(req)["v"] == value.decode()


def test_reflect_leaves_a_slack_timestamp_alone():
    req = _req(body=b"ts=1700000000.000600", content_type="application/x-www-form-urlencoded")
    assert echo.reflect(req)["ts"] == "1700000000.000600"


def test_reflect_leaves_a_number_too_wide_for_a_double_alone():
    req = _req(body=b"n=1234567890123456", content_type="application/x-www-form-urlencoded")
    assert echo.reflect(req)["n"] == "1234567890123456"


def test_reflect_reads_a_form_body_that_is_not_utf8():
    req = _req(body=b"a=\xff\xfe", content_type="application/x-www-form-urlencoded")
    assert isinstance(echo.reflect(req)["a"], str)


@pytest.mark.parametrize("ct", [None, "text/plain", "multipart/form-data; boundary=x", ""])
def test_reflect_ignores_every_other_content_type(ct):
    assert echo.reflect(_req(body=b'{"a": 1}', content_type=ct)) == {}


def test_reflect_never_raises_on_a_big_body():
    body = json.dumps({"k": "x" * 2_000_000}).encode()
    assert echo.reflect(_req(body=body, content_type="application/json"))["k"].startswith("x")


def test_reflect_never_raises_on_a_deeply_nested_body():
    assert echo.reflect(_req(body=b"[" * 5000, content_type="application/json")) == {}


def test_an_answer_for_an_unparseable_body_is_still_json():
    ans = _answer(_req("POST", host="example.invalid", path="/x", body=b"\xff" * 64))
    assert json.loads(ans.response.body).keys() == {"created"}


@pytest.mark.parametrize(
    ("operation", "expected"),
    [
        ("refunds.create", "refund"),
        ("charges.create", "charge"),
        ("payment_intents.cancel", "payment_intent"),
        ("incoming_webhook", "incoming_webhook"),
        ("things.guess", "thing"),
        ("s.create", "s"),
        ("", ""),
    ],
)
def test_object_name(operation, expected):
    assert echo.object_name(operation) == expected


def test_mint_id_shape_and_uniqueness():
    first = echo.mint_id("re_")
    assert re.fullmatch(r"re_[A-Za-z0-9]{24}", first)
    assert first != echo.mint_id("re_")


def test_every_shipped_route_that_mints_an_id_names_a_real_object():
    """The derivation rule has to hold for the routes we actually ship, not just for Stripe."""
    for service in SHIPPED.services:
        for route in service.routes:
            if "id" in route.ids:
                assert echo.object_name(route.operation) not in ("", route.operation)


def test_stripe_python_parses_the_faked_refund():
    """Issue #11's done-criterion, against the real SDK.

    `stripe` is not a dependency of this project, so this test is skipped in the plain dev venv.
    `uv sync --group examples` installs it, after which `uv run pytest -q -k stripe` runs it.
    """
    stripe = pytest.importorskip("stripe")
    ans = _answer(
        _req(
            "POST",
            path="/v1/refunds",
            body=b"charge=ch_test&amount=4900",
            content_type="application/x-www-form-urlencoded",
        )
    )
    refund = stripe.Refund.construct_from(json.loads(ans.response.body), "sk_test_x")
    assert type(refund).__name__ == "Refund"  # the class comes from `object`, so it must be there
    assert refund.id.startswith("re_")
    assert refund.object == "refund"
    assert refund.amount == 4900
    assert refund.charge == "ch_test"


# ------------------------------------------------- form bodies with structure in them (#27)


def test_a_bracket_nested_form_field_becomes_a_nested_object():
    """stripe-python posts `metadata[order_id]=6735`; the live API always answers with
    `metadata`. Flat, `Refund.metadata` raised AttributeError - the bar policy.py sets itself."""
    body = echo.parse_form("charge=ch_test&amount=4900&metadata[order_id]=6735")
    assert body == {"charge": "ch_test", "amount": 4900, "metadata": {"order_id": "6735"}}


def test_a_metadata_value_stays_a_string_at_any_depth():
    """A Stripe metadata value is always a string on the live API, so coercing one hands back
    something other than what the caller sent (#27). The field name is what decides, wherever it
    sits on the path into the value."""
    assert echo.parse_form("metadata[n]=6735")["metadata"]["n"] == "6735"
    assert echo.parse_form("a[metadata][n]=6735")["a"]["metadata"]["n"] == "6735"


def test_a_nested_number_is_a_number_like_the_same_number_at_the_top_level():
    """`line_items[0][quantity]=2` echoed `"2"` while `amount=4900` echoed `4900`, so
    `quantity * 2` was `"22"` with no raise. "Stays a string" is right for `metadata` and wrong
    for every other numeric nested field (#33)."""
    assert echo.parse_form("n=6735")["n"] == 6735
    body = echo.parse_form("line_items[0][quantity]=2&line_items[0][price]=price_1")
    assert body == {"line_items": [{"quantity": 2, "price": "price_1"}]}
    assert echo.parse_form("expand[]=2")["expand"] == [2]
    # The same rules the top level has: not canonical, so not a number.
    assert echo.parse_form("a[b]=007")["a"]["b"] == "007"


def test_a_repeated_bare_key_collects_into_a_list():
    """`requests.post(data={"tags": ["a", "b"]})` and `urlencode(doseq=True)` both send these."""
    assert echo.parse_form("tags=a&tags=b&tags=c") == {"tags": ["a", "b", "c"]}
    assert echo.parse_form("tags=a") == {"tags": "a"}


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("expand[0]=a&expand[1]=b", {"expand": ["a", "b"]}),
        ("i[0][price]=p1&i[1][price]=p2", {"i": [{"price": "p1"}, {"price": "p2"}]}),
        ("a[b][c]=1", {"a": {"b": {"c": 1}}}),
        ("x[1]=only", {"x": {"1": "only"}}),  # a gap stays a dict; nothing is invented
        ("x[0]=a&x[2]=c", {"x": {"0": "a", "2": "c"}}),
        ("x[007]=a", {"x": {"007": "a"}}),  # not a canonical index, so not a list
    ],
)
def test_an_indexed_form_key_becomes_a_list_only_when_the_indices_are_complete(text, expected):
    assert echo.parse_form(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("a[b", {"a[b": ""}),
        ("a[b]c", {"a[b]c": ""}),
        ("a[[b]]", {"a[[b]]": ""}),
        ("a[][b]=1", {"a[][b]": 1}),  # an append in the middle of a path means nothing
        ("a[b][]=1", {"a[b][]": 1}),
        ("[b]=1", {"[b]": 1}),
    ],
)
def test_a_bracket_shape_we_will_not_guess_at_stays_flat(text, expected):
    """Echoing an odd key unchanged is wrong in a small, visible way; guessing is worse. The
    whole dict is asserted: a membership check here would pass on an empty result too."""
    assert echo.parse_form(text) == expected


@pytest.mark.parametrize(
    "text", ["a=2&a[0]=1", "a[0]=1&a=2", "a[0]=1&a=2&a[1]=3", "a=2&a[0]=1&a=9"]
)
def test_a_bracketed_key_beats_a_bare_one_in_either_order(text):
    """The same name spelled both ways is nonsense input, but it must not be order-dependent:
    structure surviving is what #27 is about, and a bare value cannot carry any."""
    result = echo.parse_form(text)
    assert isinstance(result["a"], list), result
    assert 1 in result["a"]


@pytest.mark.parametrize("text", ["a[0][b]=1&a[0]=2", "a[0]=2&a[0][b]=1"])
def test_a_bracket_path_beats_a_scalar_at_the_same_leaf_in_either_order(text):
    """The same class one level down, and the same answer: the deeper structure survives, so the
    echo does not depend on which spelling arrived first (#33)."""
    assert echo.parse_form(text) == {"a": [{"b": 1}]}


@pytest.mark.parametrize("text", ["a[]=1&a[0][b]=2", "a[0][b]=2&a[]=1"])
def test_a_bracket_path_beats_an_append_of_the_same_name_in_either_order(text):
    assert echo.parse_form(text) == {"a": [{"b": 2}]}


@pytest.mark.parametrize("text", ["a[]=1&a=2", "a=2&a[]=1"])
def test_an_append_beats_a_bare_key_of_the_same_name_in_either_order(text):
    assert echo.parse_form(text) == {"a": [1]}


def test_an_empty_bracket_pair_is_a_list(tmp_path):
    """`expand[]=a&expand[]=b` is Stripe's own documented curl spelling. It echoed the literal
    JSON key `"expand[]"`, which is a field no SDK looks for (#33). One repeat or none, the shape
    is the same: a caller writing `[]` means a list either way."""
    assert echo.parse_form("expand[]=a&expand[]=b") == {"expand": ["a", "b"]}
    assert echo.parse_form("expand[]=a") == {"expand": ["a"]}
    assert echo.parse_form("charge=ch_1&expand[]=a") == {"charge": "ch_1", "expand": ["a"]}
    # `metadata` is still the caller's to key and to spell, at this shape too.
    assert echo.parse_form("metadata[]=6735") == {"metadata": ["6735"]}


def _nest(depth: int):
    node: object = 1
    for _ in range(depth):
        node = {"b": node}
    return node


def test_a_bracket_path_deeper_than_the_cap_stays_flat():
    """Unwinding a thousand-level nest is a RecursionError inside a mitmproxy hook, and a hook
    that raises forwards the flow - which for a write means it escapes shadow mode. The cap is
    twice Stripe's deepest real key, `line_items[0][price_data][product_data][name]`."""
    # The value, not just the mechanism: written only against the constant, this test passes
    # with a cap of 2, which would flatten Stripe's real
    # `line_items[0][price_data][product_data][name]` (5 segments) and ship green.
    assert echo._MAX_FORM_DEPTH == 8
    assert echo.parse_form("line_items[0][price_data][product_data][name]=x") == {
        "line_items": [{"price_data": {"product_data": {"name": "x"}}}]
    }
    deep = "a" + "[b]" * echo._MAX_FORM_DEPTH
    assert echo.parse_form(deep + "=1") == {"a": _nest(echo._MAX_FORM_DEPTH)}
    too_deep = "a" + "[b]" * (echo._MAX_FORM_DEPTH + 1)
    assert echo.parse_form(too_deep + "=1") == {too_deep: 1}  # flat, so int-coerced


def test_parse_form_never_raises_on_hostile_input():
    for text in ["[" * 5000, "a" + "[b]" * 2000 + "=1", "=", "&&&", "a=%%%", "a[0]=1&a=2"]:
        assert isinstance(echo.parse_form(text), dict)


def test_a_hostile_form_body_still_reflects_and_never_raises():
    """The end-to-end guarantee: whatever the body, the policy answers locally."""
    request = _req(
        "POST",
        path="/v1/refunds",
        body=("a" + "[b]" * 2000 + "=1").encode(),
        content_type="application/x-www-form-urlencoded",
    )
    ans = _answer(request)
    assert ans.answered_by == "fake-L1"
    assert json.loads(ans.response.body)["id"].startswith("re_")


# ---------------------------------------------------- the id the request already names (#26)


@pytest.mark.parametrize(
    ("path", "named", "operation", "object_name"),
    [
        ("/v1/customers/cus_REAL123", "cus_REAL123", "customers.update", "customer"),
        (
            "/v1/payment_intents/pi_REAL999/cancel",
            "pi_REAL999",
            "payment_intents.cancel",
            "payment_intent",
        ),
    ],
)
def test_a_write_that_names_its_resource_echoes_that_id(path, named, operation, object_name):
    """The live API answers an update or a cancel with the id it was given. Minting a fresh one
    hands the agent an id for a resource that never existed, which it then logs or retrieves."""
    request = _req("POST", path=path)
    assert classify(request, SHIPPED).operation == operation
    body = json.loads(_answer(request).response.body)
    assert body["id"] == named
    assert body["object"] == object_name


def test_a_create_still_mints_every_id_it_names():
    """A create posts to a collection, so it captures no path segment and the service mints."""
    body = json.loads(_answer(_req("POST", path="/v1/refunds")).response.body)
    assert re.fullmatch(r"re_[A-Za-z0-9]{24}", body["id"])
    assert re.fullmatch(r"txn_[A-Za-z0-9]{24}", body["balance_transaction"])


def test_a_captured_segment_that_is_not_this_ids_prefix_is_not_used():
    """`named_id` matches on the id prefix, so an unrelated captured segment cannot become the
    id. There is no such shipped route, so the case is built here rather than left untested."""
    route = servicemap.Route(
        method="POST",
        path="/v1/widgets/{widget}/parts/{part}",
        operation="widgets.attach",
        kind="write",
        ids={"id": "wid_"},
    )
    assert echo.named_id(route, "/v1/widgets/wid_1/parts/prt_9", "wid_") == "wid_1"
    assert echo.named_id(route, "/v1/widgets/wid_1/parts/prt_9", "prt_") == "prt_9"
    assert echo.named_id(route, "/v1/widgets/w1/parts/p9", "wid_") is None
    assert echo.named_id(route, "/v1/widgets", "wid_") is None


def test_the_id_shaped_capture_wins_when_one_prefix_matches_two_segments():
    """`sub_` matches `sub_sched_1` before `sub_2` - a real Stripe pair, and the first capture is
    the wrong one. An id is its prefix plus one run of id characters, so the second `_` in
    `sub_sched_1` is what rules it out (#33)."""
    route = servicemap.Route(
        method="POST",
        path="/v1/subscription_schedules/{schedule}/subscriptions/{subscription}",
        operation="subscriptions.update",
        kind="write",
        ids={"id": "sub_"},
    )
    path = "/v1/subscription_schedules/sub_sched_1/subscriptions/sub_2"
    assert echo.named_id(route, path, "sub_") == "sub_2"
    # And the longer prefix still finds its own segment, which the shorter one must not steal.
    assert echo.named_id(route, path, "sub_sched_") == "sub_sched_1"


def test_a_client_secret_is_never_echoed_back_as_the_resource_id():
    """A PaymentIntent's client secret starts with the id's own prefix
    (`pi_ABC_secret_XYZ`), so prefix matching alone hands it back as `id` - a credential in the
    one field an agent is most likely to log or store (#33)."""
    route = servicemap.Route(
        method="POST",
        path="/v1/payment_intents/{payment_intent}/cancel",
        operation="payment_intents.cancel",
        kind="write",
        ids={"id": "pi_"},
    )
    assert echo.named_id(route, "/v1/payment_intents/pi_ABC_secret_XYZ/cancel", "pi_") is None
    body = echo.l0_body(_req("POST", path="/v1/payment_intents/pi_ABC_secret_XYZ/cancel"), route)
    assert body["id"] != "pi_ABC_secret_XYZ"
    assert re.fullmatch(r"pi_[A-Za-z0-9]{24}", body["id"])
    assert echo.named_id(route, "/v1/payment_intents/pi_REAL999/cancel", "pi_") == "pi_REAL999"


def test_a_percent_encoded_id_is_still_the_id_the_request_names():
    """`cus%5FREAL123` is the same customer as `cus_REAL123` to the service, so reading the raw
    segment minted a fresh id and reinstated #26 for any caller that over-encodes (#33)."""
    route = servicemap.Route(
        method="POST",
        path="/v1/customers/{customer}",
        operation="customers.update",
        kind="write",
        ids={"id": "cus_"},
    )
    assert echo.named_id(route, "/v1/customers/cus%5FREAL123", "cus_") == "cus_REAL123"
    body = json.loads(_answer(_req("POST", path="/v1/customers/cus%5FREAL123")).response.body)
    assert body["id"] == "cus_REAL123"


def test_every_shipped_write_route_that_names_ids_round_trips_or_mints():
    """Across every shipped write route: a route with a `{…}` segment carrying an id prefix must
    echo it, and one without must mint. This is what would have caught #26 when it shipped."""
    for sm in SHIPPED.services:
        for route in sm.routes:
            for name, prefix in route.ids.items():
                holes = [p for p in route.path.split("/") if p.startswith("{")]
                if not holes:
                    sample = route.path.replace("{", "").replace("}", "")
                    assert echo.named_id(route, sample, prefix) is None, route.operation
                    continue
                # Only the LAST hole carries the prefix; every other one gets a segment that
                # does not. Filling them all would make `named_id` match the first hole whatever
                # it held, and the test would pass on a route whose id is in the wrong segment.
                parts = route.path.split("/")
                last = max(i for i, part in enumerate(parts) if part.startswith("{"))
                filled = "/".join(
                    (prefix + "SAMPLE" if i == last else "other-segment")
                    if part.startswith("{")
                    else part
                    for i, part in enumerate(parts)
                )
                assert echo.named_id(route, filled, prefix) == prefix + "SAMPLE", (
                    f"{sm.service}.{route.operation} does not echo the {name} its path names"
                )


# ------------------------------------- content types, literal bodies and one encode (#29)


@pytest.mark.parametrize(
    "ct",
    [
        "application/json",
        "text/json",
        "application/vnd.api+json",
        "application/json-patch+json",
    ],
)
def test_every_json_content_type_is_parsed(ct):
    """Only the exact `application/json` was read before, so a `+json` body was indistinguishable
    from a malformed one and reflected nothing."""
    assert echo.reflect(_req(body=b'{"a": 1}', content_type=ct)) == {"a": 1}


def test_a_slack_incoming_webhook_answers_the_literal_ok():
    """A real incoming webhook answers the body `ok` as text/plain. The Web API envelope it got
    instead breaks `assert resp.text == "ok"`, the common raw-requests idiom."""
    ans = _answer(
        _req(
            "POST",
            host="hooks.slack.com",
            path="/services/T000/B000/xyz",
            body=b'{"text": "hi"}',
            content_type="application/json",
        )
    )
    assert ans.response.body == b"ok"
    assert dict(ans.response.headers)["content-type"] == "text/plain"
    assert ans.answered_by == "fake-L0"
    assert ans.flags == (FIDELITY_L0_FLAG,)


def test_the_web_api_still_gets_the_json_envelope():
    """The literal body is keyed on `(service, operation)`, not on the host, so the rest of
    `slack` is unaffected even though hooks.slack.com is part of the same service."""
    ans = _answer(
        _req(
            "POST",
            host="slack.com",
            path="/api/chat.postMessage",
            body=b"channel=C1",
            content_type="application/x-www-form-urlencoded",
        )
    )
    assert dict(ans.response.headers)["content-type"] == "application/json"
    assert json.loads(ans.response.body)["ok"] is True


def test_a_body_a_strict_json_parser_would_refuse_reflects_nothing():
    """`_has_non_finite` replaced a throwaway serialization of the whole body; it must still keep
    NaN and Infinity out, because it is what makes the single json.dumps unable to fail."""
    for raw in [b'{"v": NaN}', b'{"v": Infinity}', b'{"v": -Infinity}', b'{"v": [1e400]}']:
        assert echo.reflect(_req(body=raw, content_type="application/json")) == {}
    assert echo.reflect(_req(body=b'{"v": 1.5}', content_type="application/json")) == {"v": 1.5}


def test_the_body_is_serialized_exactly_once(monkeypatch):
    """It used to be encoded twice - once inside reflect purely to validate it and throw away,
    once in answer - and that second call was the only one outside the never-raise guard."""
    calls = []

    def counting(*args, **kwargs):
        calls.append(1)
        return json.dumps(*args, **kwargs)

    # Rebind the name `json` inside policy rather than mutating the shared stdlib module: the
    # latter counts every json.dumps in the process, including ones classify or servicemap make,
    # which turns this into a tripwire for whatever another batch adds to that path.
    monkeypatch.setattr(echo, "json", SimpleNamespace(dumps=counting, loads=json.loads))
    _answer(
        _req(
            "POST",
            path="/v1/refunds",
            body=b"charge=ch_test&amount=4900",
            content_type="application/x-www-form-urlencoded",
        )
    )
    assert calls == [1]


def test_a_digit_keyed_metadata_field_stays_an_object():
    """#27's failure class, one step along. `metadata[0]=zero` is the key "0", and the live API
    answers `{"metadata": {"0": "zero"}}`. Promoted to a list, `refund.metadata["0"]` raises
    TypeError instead of returning the value the caller itself just sent."""
    assert echo.parse_form("metadata[0]=zero") == {"metadata": {"0": "zero"}}
    assert echo.parse_form("metadata[0]=a&metadata[1]=b") == {"metadata": {"0": "a", "1": "b"}}
    # Nested under another field, and mixed with a string key, it is the same field name.
    assert echo.parse_form("a[metadata][0]=z") == {"a": {"metadata": {"0": "z"}}}
    assert echo.parse_form("metadata[0]=z&metadata[k]=v") == {"metadata": {"0": "z", "k": "v"}}


def test_a_real_array_field_is_still_promoted_to_a_list():
    """The exemption is by field name, so the rule `metadata` opts out of still applies to
    everything else: `expand[0]=a&expand[1]=b` is an array on the wire and a list in the echo."""
    assert echo.parse_form("expand[0]=charge&expand[1]=customer") == {
        "expand": ["charge", "customer"]
    }
    assert echo.parse_form("line_items[0][price]=p") == {"line_items": [{"price": "p"}]}


# ------------------------------------------------------- the L1 fixture answer (#41)


@pytest.fixture
def no_fixtures(tmp_path, monkeypatch):
    """Point `fixture` at an empty directory, so every `fixture:` route degrades to L0."""
    empty = tmp_path / "fixtures"
    empty.mkdir()
    monkeypatch.setattr(fixture, "fixtures_dir", lambda: empty)
    fixture.clear_cache()
    yield
    fixture.clear_cache()


def _refund(body: bytes = b"charge=ch_test&amount=4900"):
    request = _req("POST", path="/v1/refunds", body=body, content_type=FORM)
    return _answer(request)


FORM = "application/x-www-form-urlencoded"


def test_a_mapped_write_is_answered_at_l1_and_says_so():
    ans = _refund()
    assert ans.answered_by == "fake-L1"
    assert ans.flags == (FIDELITY_L1_FLAG,)
    assert ans.response.status == 200
    assert ("content-type", "application/json") in ans.response.headers


def test_the_faked_refund_is_a_whole_refund_object_not_just_what_was_posted():
    """The point of L1. An agent branches on `status`, `currency` and `destination_details`,
    and at L0 none of them are there at all - it reads `None` and takes the wrong branch."""
    body = json.loads(_refund().response.body)
    assert body.keys() == fixture.get("stripe", "refund").keys()
    assert body["status"] == "succeeded"
    assert body["currency"] == "usd"
    assert body["destination_details"] == {"card": {"type": "reversal"}, "type": "card"}


def test_the_posted_fields_still_win_over_the_fixtures_own_values():
    body = json.loads(_refund(b"charge=ch_REAL&amount=1234&reason=fraudulent").response.body)
    assert body["charge"] == "ch_REAL"
    assert body["amount"] == 1234  # an int, as stripe-python's own Refund.amount is
    # `reason` is null in the fixture, so it names no type and whatever was sent is taken.
    assert body["reason"] == "fraudulent"


def test_a_field_the_fixture_does_not_name_is_not_invented():
    """The live API refuses an unknown parameter outright. Echoing it back instead would hand
    the agent a refund object no real response can produce, which is the drift a fixture exists
    to stop; the L0 echo keeps reflecting everything, and that is what makes it the honest floor.
    """
    body = json.loads(_refund(b"charge=ch_1&not_a_refund_field=surprise").response.body)
    assert "not_a_refund_field" not in body
    assert body["charge"] == "ch_1"


def test_a_value_of_the_wrong_type_leaves_the_fixtures_own_in_place():
    """`amount` is a number in every real refund. A form body can say anything, and a string
    there is what makes `refund.amount / 100` raise inside the agent instead of here."""
    body = json.loads(_refund(b"amount=not-a-number").response.body)
    assert body["amount"] == fixture.get("stripe", "refund")["amount"]


def test_the_service_owns_the_fields_that_say_what_this_object_is():
    """`object` decides which class stripe-python builds, and `id` is what the agent logs. A
    body posting either must not choose them, or a refund route answers a `charge`."""
    body = json.loads(_refund(b"object=charge&id=re_MINE&created=1&livemode=true").response.body)
    assert body["object"] == "refund"
    assert re.fullmatch(r"re_[A-Za-z0-9]{24}", body["id"])
    assert body["created"] != 1
    assert "livemode" not in body  # the real refund object has none, so neither has ours


def test_livemode_is_set_where_the_object_has_it():
    """irimi performed nothing, so nothing it answers happened in live mode. Only where the
    real object carries the field: inventing one on a refund would be a field Stripe never sends.
    """
    body = json.loads(_answer(_req("POST", path="/v1/customers/cus_REAL123")).response.body)
    assert body["livemode"] is False
    assert "livemode" not in json.loads(_refund().response.body)


def test_l1_still_echoes_the_id_the_path_names_and_mints_the_rest():
    """L0's #26 and #33 fixes are properties of the answer, not of the echo, so they hold here
    too: an update answers with the id it was given, a create mints one."""
    update = json.loads(_answer(_req("POST", path="/v1/customers/cus_REAL123")).response.body)
    assert update["id"] == "cus_REAL123"
    assert update["object"] == "customer"

    cancel = json.loads(
        _answer(_req("POST", path="/v1/payment_intents/pi_REAL999/cancel")).response.body
    )
    assert cancel["id"] == "pi_REAL999"
    assert cancel["object"] == "payment_intent"

    created = json.loads(_refund().response.body)
    assert re.fullmatch(r"re_[A-Za-z0-9]{24}", created["id"])
    assert re.fullmatch(r"txn_[A-Za-z0-9]{24}", created["balance_transaction"])


def test_a_client_secret_is_still_never_echoed_back_as_the_id_at_l1():
    """#33's rule, on the route that has a client secret in its own path. The fixture carries a
    `client_secret` of its own, and the one thing that must not happen is the path's copy of it
    becoming `id`."""
    body = json.loads(
        _answer(_req("POST", path="/v1/payment_intents/pi_ABC_secret_XYZ/cancel")).response.body
    )
    assert body["id"] != "pi_ABC_secret_XYZ"
    assert re.fullmatch(r"pi_[A-Za-z0-9]{24}", body["id"])


def test_metadata_keeps_its_string_keys_and_string_values_at_l1():
    """#27 and its follow-on, pinned against L1: `metadata` is a dict in the fixture, so the
    parsed form value replaces it wholesale and every rule `parse_form` applies survives."""
    body = json.loads(_refund(b"metadata[order_id]=6735&metadata[0]=zero").response.body)
    assert body["metadata"] == {"order_id": "6735", "0": "zero"}


def test_a_hostile_body_cannot_reach_the_fixtures_own_fields():
    body = json.loads(_refund(("a" + "[b]" * 2000 + "=1").encode()).response.body)
    assert body["object"] == "refund"
    assert body["status"] == "succeeded"


def test_two_writes_do_not_share_one_fixture():
    """The faker writes over the object it is given, so a cached object handed out twice would
    make every later refund carry the first caller's arguments."""
    first = json.loads(_refund(b"charge=ch_FIRST&amount=111").response.body)
    second = json.loads(_refund(b"charge=ch_SECOND").response.body)
    assert first["charge"] == "ch_FIRST"
    assert second["charge"] == "ch_SECOND"
    assert second["amount"] == fixture.get("stripe", "refund")["amount"]


def test_a_route_whose_fixture_cannot_be_read_falls_back_to_l0_and_says_so(no_fixtures):
    """A damaged or trimmed install must still answer the write - L0 is the floor - but the
    trace has to say the fidelity is not the one the map promised, or the run reads as a
    working L1 (#41)."""
    ans = _refund()
    assert ans.answered_by == "fake-L0"
    assert ans.flags == (FIDELITY_L0_FLAG, FIXTURE_FAILED_FLAG)
    body = json.loads(ans.response.body)
    assert body["charge"] == "ch_test"  # the L0 echo, which still parses
    assert body["id"].startswith("re_")
    assert "status" not in body


def test_an_unmapped_write_is_still_l0_with_no_flag():
    """L1 is per route. A host no map claims has no fixture to start from and must not pretend."""
    ans = _answer(_req("POST", host="example.invalid", path="/things", body=b"{}"))
    assert ans.answered_by == "fake-L0"
    assert ans.flags == (FIDELITY_L0_FLAG,)


def test_every_shipped_fixture_route_names_an_object_that_ships():
    """A `fixture:` naming an object the package does not have degrades to L0 silently but for
    the flag, which is the `--allow-host` class of bug (#4): configured, never consulted."""
    for sm in SHIPPED.services:
        for route in sm.routes:
            if route.fixture:
                assert fixture.get(sm.service, route.fixture) is not None, (
                    f"{sm.service} {route.method} {route.path}: no such fixture {route.fixture!r}"
                )


@pytest.mark.parametrize(
    ("path", "body", "cls_name", "fixture_field"),
    [
        ("/v1/refunds", b"charge=ch_test&amount=4900", "Refund", "status"),
        ("/v1/customers/cus_REAL123", b"name=Ada", "Customer", "tax_exempt"),
        ("/v1/payment_intents/pi_REAL999/cancel", b"", "PaymentIntent", "capture_method"),
    ],
)
def test_stripe_python_parses_every_faked_object_with_every_field(
    path, body, cls_name, fixture_field
):
    """Issue #41's done-criterion, against the real SDK. `stripe` is not a dependency of this
    project, so this is skipped in the plain dev venv; `uv sync --group examples` installs it."""
    stripe = pytest.importorskip("stripe")
    ans = _answer(_req("POST", path=path, body=body, content_type=FORM))
    assert ans.answered_by == "fake-L1"
    parsed = json.loads(ans.response.body)
    obj = getattr(stripe, cls_name).construct_from(parsed, "sk_test_x")
    assert obj.object == parsed["object"]
    # Every field survives the SDK's own round trip, which is what "with every fixture field
    # present" means. Through `str(obj)` - the SDK's JSON serialization - because it turns each
    # nested object into a StripeObject of its own, so a plain `==` against the dict fails.
    assert json.loads(str(obj)) == parsed
    # And the one field the caller never sent is readable as an attribute, not an AttributeError,
    # which is the whole difference from L0.
    assert getattr(obj, fixture_field) == parsed[fixture_field]


# ------------------------------------------------------------------ answer targets (#16, D20)


def _targeted_index(targets=(), target_reads=()):
    maps = servicemap.apply_cli_targets(list(servicemap.load_shipped()), targets, target_reads)
    return servicemap.MapIndex(tuple(maps))


def _answer_with(index, request):
    return ShadowPolicy().answer(request, classify(request, index))


def test_a_route_target_makes_the_answer_delegated_not_faked():
    index = _targeted_index([("api.stripe.com", "/v1/refunds", "http://127.0.0.1:3000/refund")])
    ans = _answer_with(index, _req("POST", path="/v1/refunds"))
    assert ans.answered_by == "delegated"
    assert ans.response is None  # the engine forwards; the policy does no I/O
    assert ans.forward_to == delegation.ForwardTo(
        url="http://127.0.0.1:3000/refund", forward_auth=False
    )


def test_self_is_the_default_target_and_is_exactly_the_old_behaviour():
    """`target: self` is today's local fake, now named rather than assumed (D20)."""
    ans = _answer(_req("POST", path="/v1/refunds"))
    assert ans.answered_by == "fake-L1"
    assert ans.forward_to is None


def test_a_service_target_covers_writes_but_not_reads_without_target_reads():
    index = _targeted_index([("api.stripe.com", "", "http://127.0.0.1:3000")])
    write = _answer_with(index, _req("POST", path="/v1/refunds"))
    assert write.answered_by == "delegated"
    assert write.forward_to.url == "http://127.0.0.1:3000/v1/refunds"
    read = _answer_with(index, _req("GET", path="/v1/charges"))
    assert read.answered_by == "live" and read.forward_to is None


def test_target_reads_delegates_the_services_reads_too():
    index = _targeted_index(
        [("api.stripe.com", "", "http://127.0.0.1:3000")], target_reads=["api.stripe.com"]
    )
    read = _answer_with(index, _req("GET", path="/v1/charges"))
    assert read.answered_by == "delegated"
    assert read.forward_to.url == "http://127.0.0.1:3000/v1/charges"


def test_an_unlisted_route_on_a_targeted_service_is_delegated_too():
    """Half the stub's world and half ours would be worse than either."""
    index = _targeted_index([("api.stripe.com", "", "http://127.0.0.1:3000")])
    ans = _answer_with(index, _req("POST", path="/v1/tax/calculations"))
    assert ans.answered_by == "delegated"
    assert ans.forward_to.url == "http://127.0.0.1:3000/v1/tax/calculations"


def test_llm_and_telemetry_are_never_delegated():
    """`target_for` refuses them on a matched route; `delegate` has to say the same thing for an
    unlisted one, or a service target would quietly start forwarding inference."""
    index = _targeted_index(
        [("api.openai.com", "", "http://127.0.0.1:3000")], target_reads=["api.openai.com"]
    )
    llm = _answer_with(index, _req("POST", host="api.openai.com", path="/v1/chat/completions"))
    assert llm.answered_by == "live" and llm.forward_to is None


def test_delegate_returns_none_for_a_host_no_map_claims():
    ans = _answer(_req("POST", host="nowhere.example", path="/x"))
    assert ans.answered_by == "fake-L0" and ans.forward_to is None


def test_forward_auth_reaches_the_engine_through_the_answer():
    from dataclasses import replace as _replace

    index = _targeted_index([("api.stripe.com", "/v1/refunds", "http://127.0.0.1:3000/r")])
    stripe = index.service_for("api.stripe.com")
    routes = tuple(
        _replace(r, forward_auth=True) if r.target != servicemap.SELF_TARGET else r
        for r in stripe.routes
    )
    index = servicemap.MapIndex(
        tuple(_replace(sm, routes=routes) if sm is stripe else sm for sm in index.services)
    )
    ans = _answer_with(index, _req("POST", path="/v1/refunds"))
    assert ans.forward_to.forward_auth is True


def test_a_delegated_answer_carries_its_own_fidelity_flag():
    """Symmetry with `fidelity:L0`: every locally decided answer says how it was produced, so a
    delegated exchange is not the one row in the log with no fidelity at all (#16 §5)."""
    index = _targeted_index([("api.stripe.com", "/v1/refunds", "http://127.0.0.1:3000/r")])
    ans = _answer_with(index, _req("POST", path="/v1/refunds"))
    assert ans.flags == ("fidelity:delegated",)
    assert _answer(_req("POST", path="/v1/refunds")).flags == (FIDELITY_L1_FLAG,)


def test_an_unlisted_read_on_a_targeted_service_is_not_delegated_without_target_reads():
    """The unmatched branch of `delegate`, which `target_for` never sees. Replacing its whole
    condition with `elif True:` passed all 421 tests: the two tests whose docstrings claim to
    cover it both use *listed* routes, so they exercise `target_for` and stop above the `elif`."""
    index = _targeted_index([("api.stripe.com", "", "http://127.0.0.1:3000")])
    ans = _answer_with(index, _req("GET", path="/v1/tax/calculations"))  # GET, and unlisted
    assert ans.answered_by == "live"
    assert ans.forward_to is None


def test_an_unmatched_llm_or_telemetry_request_is_never_delegated():
    """The other half of the same branch. A service target must not quietly start forwarding
    inference or someone else's events just because the map does not list the route.

    The classification is built here rather than loaded, because the only way a map could give an
    *unmatched* route a live kind is a live `default_kind`, which the loader now refuses (#30).
    """
    sm = servicemap.load_shipped()[0]
    sm = replace(sm, target="http://127.0.0.1:3000", target_reads=True)
    for kind in ("llm", "telemetry", "read"):
        cls = pipeline.Classification(
            service=sm.service,
            operation="",
            kind=kind,
            flags=(),
            matched=None,  # unlisted: this is the `elif` branch, not `target_for`
            service_map=sm,
        )
        forward = delegation.delegate(_req("POST", path="/anything"), cls)
        if kind == "read":
            assert forward is not None, "target_reads is what delegates a read"
        else:
            assert forward is None, f"an unmatched {kind} request was delegated"


def test_a_credential_path_host_is_never_delegated_off_the_machine():
    """THE SCOPE RULE, decision half. The loader already refuses this configuration whichever
    layer it arrived through; this asks the same question of the request and the answer actually
    in front of `delegate`, so a target reaching it some other way is refused rather than
    escaping. The map is built here because the loader will not produce one (#16 review D-2)."""
    slack = next(sm for sm in servicemap.load_shipped() if sm.service == "slack")
    assert "hooks.slack.com" in slack.hosts
    off_machine = replace(slack, target="http://stub.internal:9000")
    index = servicemap.MapIndex((off_machine,))

    for path in ("/services/T0/B0/SECRET", "/workflows/T0/A0/SECRET/xyz", "/triggers/T0/1/abc"):
        request = _req("POST", host="hooks.slack.com", path=path)
        forward = delegation.delegate(request, classify(request, index))
        assert forward is None, f"{path} was delegated to {forward and forward.url}"

    # The same service's other hosts are not credential-path hosts, so they still delegate.
    request = _req("POST", host="slack.com", path="/api/chat.postMessage")
    forward = delegation.delegate(request, classify(request, index))
    assert forward is not None and forward.url.startswith("http://stub.internal:9000")

    # And a loopback target on the credential host is fine - that is the supported way to stub it.
    local = servicemap.MapIndex((replace(slack, target="http://127.0.0.1:3000"),))
    request = _req("POST", host="hooks.slack.com", path="/workflows/T0/A0/SECRET/xyz")
    forward = delegation.delegate(request, classify(request, local))
    assert forward is not None and forward.url.startswith("http://127.0.0.1:3000")


# ------------------------------------------------------- the L1 Slack envelope (#42)


def _post_message(body: bytes = b'{"channel": "C1", "text": "hi"}'):
    return _answer(
        _req(
            "POST",
            host="slack.com",
            path="/api/chat.postMessage",
            body=body,
            content_type="application/json",
        )
    )


def test_a_faked_slack_post_nests_the_fixture_message_inside_the_envelope():
    """slack_sdk reads `ok` to decide the call succeeded and `ts` to address the message it just
    posted; both are the envelope's. The fixture is what sits under `message:` - the fields a real
    response carries and the request never sent (#42)."""
    ans = _post_message()
    body = json.loads(ans.response.body)
    assert ans.answered_by == "fake-L1"
    assert (body["ok"], body["channel"]) == (True, "C1")
    assert body["message"]["text"] == "hi"
    assert body["message"]["type"] == "message"
    assert body["message"]["user"] == fixture.get("slack", "message")["user"]
    assert "text" not in body  # the payload nests; it does not leak into the envelope


def test_the_faked_messages_ts_is_the_envelopes_and_not_the_fixtures():
    """`ts` is the message's identity for the rest of the run, so it is minted per answer. Taking
    the fixture's frozen one would make every faked message the same message."""
    body = json.loads(_post_message().response.body)
    assert body["message"]["ts"] == body["ts"]
    assert body["ts"] != fixture.get("slack", "message")["ts"]


def test_a_slack_post_still_answers_the_envelope_when_the_fixture_cannot_be_read(no_fixtures):
    """A fixture this install cannot read must not cost the agent the `ok` it reads to decide the
    call worked: it degrades to the envelope alone, flagged, the way Stripe degrades to L0."""
    ans = _post_message()
    body = json.loads(ans.response.body)
    assert ans.answered_by == "fake-L0"
    assert FIXTURE_FAILED_FLAG in ans.flags
    assert body["ok"] is True
    assert body["channel"] == "C1"
    assert "message" not in body


def test_slack_sdk_parses_the_faked_post():
    """#42's done-when: slack_sdk has to read this body as a success rather than raise on it."""
    slack_sdk = pytest.importorskip("slack_sdk")
    body = json.loads(_post_message().response.body)
    response = slack_sdk.web.slack_response.SlackResponse(
        client=None,
        http_verb="POST",
        api_url="https://slack.com/api/chat.postMessage",
        req_args={},
        data=body,
        headers={},
        status_code=200,
    )
    response.validate()
    assert response["ok"] is True
    assert response["message"]["text"] == "hi"
    assert response["ts"] == response["message"]["ts"]


# ------------------------------------------------------- the `ts` watermark (#42)
#
# Every test here monkeypatches `echo._last_slack_ts`, which restores it at teardown: the
# watermark is module state for the life of the process, and a test that raised it and walked away
# would push every later test's minted `ts` into the future.


def test_a_minted_ts_sorts_after_a_real_one_the_run_has_seen(monkeypatch):
    """#42's done-when. A history read going past first is what puts the faked message after the
    real ones instead of somewhere in the middle of them."""
    monkeypatch.setattr(echo, "_last_slack_ts", (0, 0))
    newest = "1999999999.000500"
    echo.observe_slack_history(
        json.dumps({"ok": True, "messages": [{"ts": "1999999998.000000"}, {"ts": newest}]}).encode()
    )
    assert float(echo.slack_ts()) > float(newest)


def test_an_older_real_ts_does_not_move_the_watermark_backwards(monkeypatch):
    """Reading an old channel must not rewind the sequence: two messages with one `ts` are two
    messages that are the same message, which is the whole reason `ts` is minted the way it is."""
    monkeypatch.setattr(echo, "_last_slack_ts", (1_999_999_999, 500))
    echo.observe_slack_history(json.dumps({"messages": [{"ts": "1000000000.000000"}]}).encode())
    assert echo.slack_ts() == "1999999999.000501"


def test_a_nested_ts_is_observed_wherever_it_sits(monkeypatch):
    """`conversations.info` carries the channel's newest message under `latest`, not in a list of
    messages. The walk does not care which shape the body is."""
    monkeypatch.setattr(echo, "_last_slack_ts", (0, 0))
    echo.observe_slack_history(
        json.dumps({"channel": {"latest": {"ts": "1999999999.000001"}}}).encode()
    )
    assert float(echo.slack_ts()) > 1999999999.000001


@pytest.mark.parametrize(
    "body",
    [
        b"",
        b"not json",
        b"[]",
        b'{"messages": "nope"}',
        b'{"messages": [{"ts": "not-a-ts"}]}',
        b'{"messages": [{"ts": 1700000000}]}',
        b'{"ts": null}',
    ],
)
def test_observing_a_body_that_carries_no_timestamp_changes_nothing(monkeypatch, body):
    """This runs inside a mitmproxy hook, where a raise forwards the flow and a forwarded write
    escapes shadow mode. Every shape a real service - or a hostile one - can send is a no-op."""
    monkeypatch.setattr(echo, "_last_slack_ts", (1_700_000_000, 7))
    echo.observe_slack_history(body)
    assert echo._last_slack_ts == (1_700_000_000, 7)
