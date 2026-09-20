import json
import re
from dataclasses import replace

import pytest

from irimi import pipeline, policy, servicemap
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
    assert ans.forward_to == policy.ForwardTo(
        url="http://127.0.0.1:3000/refund", forward_auth=False
    )


def test_self_is_the_default_target_and_is_exactly_the_old_behaviour():
    """`target: self` is today's local fake, now named rather than assumed (D20)."""
    ans = _answer(_req("POST", path="/v1/refunds"))
    assert ans.answered_by == "fake-L0"
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
    assert _answer(_req("POST", path="/v1/refunds")).flags == (policy.FIDELITY_L0_FLAG,)


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
        forward = policy.delegate(_req("POST", path="/anything"), cls)
        if kind == "read":
            assert forward is not None, "target_reads is what delegates a read"
        else:
            assert forward is None, f"an unmatched {kind} request was delegated"
