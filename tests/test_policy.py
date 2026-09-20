import json
import re
from dataclasses import replace

import pytest

from irimi import policy, servicemap
from irimi.exchange import Request, Response
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
    assert ans.flags == (policy.FIDELITY_L0_FLAG,)
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


def test_slack_envelope_without_a_channel_omits_it():
    ans = _answer(_req("POST", host="slack.com", path="/api/reactions.add"))
    assert "channel" not in json.loads(ans.response.body)


def test_a_faker_that_fails_still_answers_locally(monkeypatch):
    """A raising policy makes mitmproxy forward the flow, and a forwarded write is a real write."""

    def boom(request, classification):
        raise RuntimeError("boom")

    monkeypatch.setattr(policy, "fake_body", boom)
    ans = _answer(_req("POST", path="/v1/refunds"))
    assert ans.answered_by == "fake-L0"
    assert ans.response.body == b"{}"
    assert ans.flags == (policy.FIDELITY_L0_FLAG,)


def test_reflect_reads_a_json_object():
    assert policy.reflect(_req(body=b'{"a": 1}', content_type="application/json")) == {"a": 1}


def test_reflect_honours_content_type_parameters():
    req = _req(body=b'{"a": 1}', content_type="application/json; charset=utf-8")
    assert policy.reflect(req) == {"a": 1}


@pytest.mark.parametrize(
    "body",
    [b"[1, 2]", b'"a string"', b"null", b"7", b"{not json", b"", b"\xff\xfe\x00bad"],
)
def test_reflect_returns_nothing_for_a_body_that_is_not_a_json_object(body):
    assert policy.reflect(_req(body=body, content_type="application/json")) == {}


@pytest.mark.parametrize("body", [b'{"a": NaN}', b'{"a": Infinity}', b'{"a": 1e400}'])
def test_reflect_refuses_a_number_a_strict_json_parser_would_refuse(body):
    """json.dumps writes NaN/Infinity straight back out; the echo has to stay parseable."""
    assert policy.reflect(_req(body=body, content_type="application/json")) == {}


def test_reflect_reads_a_form_body():
    req = _req(body=b"a=1&b=two&c=", content_type="application/x-www-form-urlencoded")
    assert policy.reflect(req) == {"a": 1, "b": "two", "c": ""}


@pytest.mark.parametrize("value", [b"007", b"0012345", b"000123456789012345678", b"1e3", b"-1"])
def test_reflect_leaves_a_non_canonical_number_alone(value):
    req = _req(body=b"v=" + value, content_type="application/x-www-form-urlencoded")
    assert policy.reflect(req)["v"] == value.decode()


def test_reflect_leaves_a_slack_timestamp_alone():
    req = _req(body=b"ts=1700000000.000600", content_type="application/x-www-form-urlencoded")
    assert policy.reflect(req)["ts"] == "1700000000.000600"


def test_reflect_leaves_a_number_too_wide_for_a_double_alone():
    req = _req(body=b"n=1234567890123456", content_type="application/x-www-form-urlencoded")
    assert policy.reflect(req)["n"] == "1234567890123456"


def test_reflect_reads_a_form_body_that_is_not_utf8():
    req = _req(body=b"a=\xff\xfe", content_type="application/x-www-form-urlencoded")
    assert isinstance(policy.reflect(req)["a"], str)


@pytest.mark.parametrize("ct", [None, "text/plain", "multipart/form-data; boundary=x", ""])
def test_reflect_ignores_every_other_content_type(ct):
    assert policy.reflect(_req(body=b'{"a": 1}', content_type=ct)) == {}


def test_reflect_never_raises_on_a_big_body():
    body = json.dumps({"k": "x" * 2_000_000}).encode()
    assert policy.reflect(_req(body=body, content_type="application/json"))["k"].startswith("x")


def test_reflect_never_raises_on_a_deeply_nested_body():
    assert policy.reflect(_req(body=b"[" * 5000, content_type="application/json")) == {}


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
    assert policy.object_name(operation) == expected


def test_mint_id_shape_and_uniqueness():
    first = policy.mint_id("re_")
    assert re.fullmatch(r"re_[A-Za-z0-9]{24}", first)
    assert first != policy.mint_id("re_")


def test_every_shipped_route_that_mints_an_id_names_a_real_object():
    """The derivation rule has to hold for the routes we actually ship, not just for Stripe."""
    for service in SHIPPED.services:
        for route in service.routes:
            if "id" in route.ids:
                assert policy.object_name(route.operation) not in ("", route.operation)


def test_stripe_python_parses_the_faked_refund():
    """Issue #11's done-criterion, against the real SDK.

    `stripe` is not a dependency of this project, so this test is skipped in the dev venv. The
    orchestrator runs it for real with `uv run --with stripe pytest -q -k stripe`.
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
    body = policy.parse_form("charge=ch_test&amount=4900&metadata[order_id]=6735")
    assert body == {"charge": "ch_test", "amount": 4900, "metadata": {"order_id": "6735"}}


def test_a_value_inside_a_bracket_path_stays_a_string():
    """A Stripe metadata value is always a string on the live API."""
    assert policy.parse_form("metadata[n]=6735")["metadata"]["n"] == "6735"
    assert policy.parse_form("n=6735")["n"] == 6735


def test_a_repeated_bare_key_collects_into_a_list():
    """`requests.post(data={"tags": ["a", "b"]})` and `urlencode(doseq=True)` both send these."""
    assert policy.parse_form("tags=a&tags=b&tags=c") == {"tags": ["a", "b", "c"]}
    assert policy.parse_form("tags=a") == {"tags": "a"}


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("expand[0]=a&expand[1]=b", {"expand": ["a", "b"]}),
        ("i[0][price]=p1&i[1][price]=p2", {"i": [{"price": "p1"}, {"price": "p2"}]}),
        ("a[b][c]=1", {"a": {"b": {"c": "1"}}}),
        ("x[1]=only", {"x": {"1": "only"}}),  # a gap stays a dict; nothing is invented
        ("x[0]=a&x[2]=c", {"x": {"0": "a", "2": "c"}}),
        ("x[007]=a", {"x": {"007": "a"}}),  # not a canonical index, so not a list
    ],
)
def test_an_indexed_form_key_becomes_a_list_only_when_the_indices_are_complete(text, expected):
    assert policy.parse_form(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("a[b", {"a[b": ""}),
        ("a[b]c", {"a[b]c": ""}),
        ("a[[b]]", {"a[[b]]": ""}),
        ("a[]=1", {"a[]": 1}),
        ("[b]=1", {"[b]": 1}),
    ],
)
def test_a_bracket_shape_we_will_not_guess_at_stays_flat(text, expected):
    """Echoing an odd key unchanged is wrong in a small, visible way; guessing is worse. The
    whole dict is asserted: a membership check here would pass on an empty result too."""
    assert policy.parse_form(text) == expected


@pytest.mark.parametrize(
    "text", ["a=2&a[0]=1", "a[0]=1&a=2", "a[0]=1&a=2&a[1]=3", "a=2&a[0]=1&a=9"]
)
def test_a_bracketed_key_beats_a_bare_one_in_either_order(text):
    """The same name spelled both ways is nonsense input, but it must not be order-dependent:
    structure surviving is what #27 is about, and a bare value cannot carry any."""
    result = policy.parse_form(text)
    assert isinstance(result["a"], list), result
    assert "1" in result["a"]


def _nest(depth: int):
    node: object = "1"
    for _ in range(depth):
        node = {"b": node}
    return node


def test_a_bracket_path_deeper_than_the_cap_stays_flat():
    """Unwinding a thousand-level nest is a RecursionError inside a mitmproxy hook, and a hook
    that raises forwards the flow - which for a write means it escapes shadow mode. The cap is
    twice Stripe's deepest real key, `line_items[0][price_data][product_data][name]`."""
    deep = "a" + "[b]" * policy._MAX_FORM_DEPTH
    assert policy.parse_form(deep + "=1") == {"a": _nest(policy._MAX_FORM_DEPTH)}
    too_deep = "a" + "[b]" * (policy._MAX_FORM_DEPTH + 1)
    assert policy.parse_form(too_deep + "=1") == {too_deep: 1}  # flat, so int-coerced


def test_parse_form_never_raises_on_hostile_input():
    for text in ["[" * 5000, "a" + "[b]" * 2000 + "=1", "=", "&&&", "a=%%%", "a[0]=1&a=2"]:
        assert isinstance(policy.parse_form(text), dict)


def test_a_hostile_form_body_still_reflects_and_never_raises():
    """The end-to-end guarantee: whatever the body, the policy answers locally."""
    request = _req(
        "POST",
        path="/v1/refunds",
        body=("a" + "[b]" * 2000 + "=1").encode(),
        content_type="application/x-www-form-urlencoded",
    )
    ans = _answer(request)
    assert ans.answered_by == "fake-L0"
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
    assert policy.named_id(route, "/v1/widgets/wid_1/parts/prt_9", "wid_") == "wid_1"
    assert policy.named_id(route, "/v1/widgets/wid_1/parts/prt_9", "prt_") == "prt_9"
    assert policy.named_id(route, "/v1/widgets/w1/parts/p9", "wid_") is None
    assert policy.named_id(route, "/v1/widgets", "wid_") is None


def test_every_shipped_write_route_that_names_ids_round_trips_or_mints():
    """Across every shipped write route: a route with a `{…}` segment carrying an id prefix must
    echo it, and one without must mint. This is what would have caught #26 when it shipped."""
    for sm in SHIPPED.services:
        for route in sm.routes:
            for name, prefix in route.ids.items():
                holes = [p for p in route.path.split("/") if p.startswith("{")]
                if not holes:
                    sample = route.path.replace("{", "").replace("}", "")
                    assert policy.named_id(route, sample, prefix) is None, route.operation
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
                assert policy.named_id(route, filled, prefix) == prefix + "SAMPLE", (
                    f"{sm.service}.{route.operation} does not echo the {name} its path names"
                )
