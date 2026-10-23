import json
import re
import socket
import threading
from dataclasses import dataclass, replace
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace

import pytest

from irimi import servicemaps, services
from irimi.core import bodies
from irimi.core.exchange import Exchange, Flag, Request, Response
from irimi.core.netaddr import Target
from irimi.decide import reader, targets
from irimi.decide.policy import Decided, Delegate, Respond, ShadowPolicy
from irimi.decide.reader import UpstreamReader
from irimi.servicemaps import IdSpec
from irimi.servicemaps.classify import Classification, classify
from irimi.services import generic
from irimi.services.slack import answers as slack_answers
from irimi.services.stripe import answers as stripe_answers
from tests.support.credentials import STRIPE_TEST_X
from tests.support.servers import stop_server

SHIPPED = servicemaps.MapIndex(tuple(servicemaps.loader.load_shipped()))


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


@dataclass(frozen=True)
class Flat:
    """A policy's `Decided`, flattened into the fields these tests assert on: who answers and
    with what, and what irimi decided about the write."""

    answered_by: str
    response: Response | None
    flags: tuple
    issued: tuple
    forward_to: targets.ForwardTo | None = None
    precondition: str | None = None
    rejection_code: str = ""
    currency: str = ""
    would_fire: tuple = ()
    overlay: str | None = None


def flat(decided: Decided) -> Flat:
    d, flags, issued = decided.decision, decided.flags, decided.issued
    if isinstance(d, Respond):
        p = d.outcome.precondition
        return Flat(
            d.answered_by,
            d.response,
            flags,
            issued,
            None,
            p.outcome,
            p.rejection_code,
            p.currency,
            d.outcome.would_fire,
            d.overlay,
        )
    if isinstance(d, Delegate):
        p = d.precondition
        return Flat(
            d.answered_by, None, flags, issued, d.target, p.outcome, p.rejection_code, p.currency
        )
    return Flat(d.answered_by, None, flags, issued)


def answer(policy, request, classification, write_log=(), run_id=""):
    """`policy`'s decision, flattened, with the answer target resolved first, as the session
    resolves it."""
    target = targets.delegate(request, classification)
    return flat(policy.decide(request, classification, write_log, run_id, target))


def _answer(request: Request):
    """Classify against the shipped maps, then answer - the same order the addon uses."""
    return answer(ShadowPolicy(), request, classify(request, SHIPPED))


@pytest.mark.parametrize("kind", ["read", "llm", "telemetry"])
def test_shadow_forwards_live(kind):
    cls = replace(classify(_req(), SHIPPED), kind=kind)
    ans = answer(ShadowPolicy(), _req(), cls)
    assert ans.answered_by == "live"
    assert ans.response is None
    assert ans.flags == ()


@pytest.mark.parametrize("kind", ["write", "unknown"])
def test_shadow_fakes_l0(kind):
    cls = replace(classify(_req("POST"), SHIPPED), kind=kind)
    ans = answer(ShadowPolicy(), _req("POST"), cls)
    assert ans.answered_by == "fake-L0"
    assert isinstance(ans.response, Response)
    assert ans.response.status == 200
    assert ("content-type", "application/json") in ans.response.headers
    assert ans.flags == ()
    assert isinstance(json.loads(ans.response.body), dict)


def test_shadow_policy_shape():
    p = ShadowPolicy()
    assert p.name == "shadow"
    assert callable(p.decide)


def test_unmapped_write_reflects_its_own_fields_and_nothing_else():
    """Nothing matched, so there is no id to mint, and the echo of a service irimi has no shape
    for carries none of Stripe's fields."""
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
    assert "created" not in body
    assert "id" not in body
    assert "object" not in body


def test_an_unmapped_stripe_write_still_gets_stripes_created():
    """Stripe's answer path adds `created` to every Stripe echo, a route or not."""
    ans = _answer(_req("POST", path="/v1/charges/ch_1/refund"))
    body = json.loads(ans.response.body)
    assert isinstance(body["created"], int)
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
    route = servicemaps.Route(
        method="POST", path="/v1/things", operation="things.create", kind="write", fixture="thing"
    )
    body = stripe_answers.l1_body(_req("POST"), route, {"object": "thing", "livemode": True})
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
    messages that are the same message. Random six digits collide 9% of the time in 200,000
    draws; a counter lets a run post a million messages a second before one repeats."""
    clock = services.TsClock()
    seen = [clock.mint() for _ in range(5_000)]
    assert len(set(seen)) == len(seen)
    # And increasing, because that is the other half of what a `ts` means: real ones sort by time,
    # so code that orders a transcript by `ts` reads the same answer here as it would from Slack.
    assert [float(ts) for ts in seen] == sorted(float(ts) for ts in seen)
    assert all(re.fullmatch(r"\d+\.\d{6}", ts) for ts in seen)


def test_a_slack_timestamp_still_names_the_current_second(monkeypatch):
    """The counter must not drift off the clock: a `ts` is a real epoch time, and an SDK that
    renders one as a date has to get today."""
    monkeypatch.setattr(slack_answers.time, "time", lambda: 1_700_000_000.9)  # type: ignore[attr-defined]
    clock = services.TsClock()
    assert clock.mint() == "1700000000.000000"
    assert clock.mint() == "1700000000.000001"


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


def test_a_threaded_post_answers_with_the_thread_it_was_posted_in():
    """A reply's faked `message` carries the posted `thread_ts`. Real Slack puts it there, and
    irimi knows the value exactly - the caller posted it. The fixture names the field so that
    `generic.reflect_over` has something to write over."""
    ans = _answer(
        _req(
            "POST",
            host="slack.com",
            path="/api/chat.postMessage",
            body=b'{"channel": "C1", "thread_ts": "1700000000.000100", "text": "on it"}',
            content_type="application/json;charset=utf-8",
        )
    )
    body = json.loads(ans.response.body)
    assert ans.answered_by == "fake-L1"
    assert body["message"]["thread_ts"] == "1700000000.000100"
    # The envelope's own fields are unchanged: `ts` is minted and is the reply's identity, not the
    # thread's, so the two must differ.
    assert body["message"]["ts"] == body["ts"] != body["message"]["thread_ts"]


def test_a_top_level_post_answers_with_no_thread_ts_key_at_all():
    """A top-level post's faked `message` has no `thread_ts` key. The fixture holds `thread_ts` as
    `null` so a posted value can land on it, and `reflect_over` reflects over a `null`. Shipping
    the placeholder would send `thread_ts: null`, which real Slack never sends."""
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
    assert ans.answered_by == "fake-L1"
    assert "thread_ts" not in body["message"]


def test_slack_sdk_parses_a_threaded_and_a_top_level_faked_post():
    """slack_sdk reads both faked posts' `thread_ts` as Slack sends it. The SDK is what an agent
    actually reads the field through, and a `None` where it expects a string would break it."""
    slack_sdk = pytest.importorskip("slack_sdk")

    def answer(body: bytes) -> dict:
        ans = _answer(
            _req(
                "POST",
                host="slack.com",
                path="/api/chat.postMessage",
                body=body,
                content_type="application/json;charset=utf-8",
            )
        )
        response = slack_sdk.web.slack_response.SlackResponse(
            client=None,
            http_verb="POST",
            api_url="https://slack.com/api/chat.postMessage",
            req_args={},
            data=json.loads(ans.response.body),
            headers={},
            status_code=200,
        )
        response.validate()
        return response["message"]

    reply = answer(b'{"channel": "C1", "thread_ts": "1700000000.000100", "text": "on it"}')
    top = answer(b'{"channel": "C1", "text": "hi"}')
    assert reply["thread_ts"] == "1700000000.000100"
    assert "thread_ts" not in top


def test_an_unlisted_slack_route_does_not_get_the_slack_envelope():
    """`ok: true` is a claim of success, and we only know what success looks like for a route the
    maps claim. Answering an unmapped call with the envelope sends slack_sdk down its success
    branch into an uncatchable crash later (`files_upload_v2` reads `upload_url = None` and dies
    inside urllib); the generic echo leaves it raising the SlackApiError callers already catch."""
    ans = _answer(_req("POST", host="slack.com", path="/api/files.getUploadURLExternal"))
    assert json.loads(ans.response.body) == {}


def test_reactions_add_answers_the_bare_ok_slack_really_sends():
    """Slack answers reactions.add with `{"ok": true}` and nothing else. The `ts` and `channel`
    a one-shape envelope would add here are chat.postMessage's, and an agent that read the `ts`
    of its own reaction as a message id would be reading a field the real API never sends."""
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


def test_files_upload_answers_the_bare_ok_too():
    """Same rule as reactions.add: the generic shape's top-level `ts` is a field files.upload
    never returned. Having no `fixture:` is not having no known shape - the `file` object is
    left out because Slack retired the method, not because irimi does not know the envelope."""
    ans = _answer(
        _req(
            "POST",
            host="slack.com",
            path="/api/files.upload",
            body=b"filename=a.txt&channels=C1",
            content_type="application/x-www-form-urlencoded",
        )
    )
    assert json.loads(ans.response.body) == {"ok": True}
    assert ans.answered_by == "fake-L0"


@pytest.mark.parametrize(
    ("broken", "path"),
    [(0, "/v1/charges/ch_1/refund"), (1, "/v1/refunds")],
)
def test_a_faker_that_fails_still_answers_locally(monkeypatch, broken, path):
    """A raising policy makes mitmproxy forward the flow, and a forwarded write is a real write.

    Both bodies, because L1 put a second one on the path: `/v1/refunds` is a fixture route and
    the unmapped Stripe path is an L0 one, so each parameter breaks the function its own request
    actually reaches. Breaking only the L0 body would leave the L1 route answering normally and
    the test passing while proving nothing about it.
    """

    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(stripe_answers, ("l0_body", "l1_body")[broken], boom)
    ans = _answer(_req("POST", path=path))
    assert ans.answered_by == "fake-L0"
    assert ans.response.body == b"{}"
    assert ans.flags == ()


def test_reflect_reads_a_json_object():
    assert bodies.reflect(_req(body=b'{"a": 1}', content_type="application/json")) == {"a": 1}


def test_reflect_honours_content_type_parameters():
    req = _req(body=b'{"a": 1}', content_type="application/json; charset=utf-8")
    assert bodies.reflect(req) == {"a": 1}


@pytest.mark.parametrize(
    "body",
    [b"[1, 2]", b'"a string"', b"null", b"7", b"{not json", b"", b"\xff\xfe\x00bad"],
)
def test_reflect_returns_nothing_for_a_body_that_is_not_a_json_object(body):
    assert bodies.reflect(_req(body=body, content_type="application/json")) == {}


@pytest.mark.parametrize("body", [b'{"a": NaN}', b'{"a": Infinity}', b'{"a": 1e400}'])
def test_reflect_refuses_a_number_a_strict_json_parser_would_refuse(body):
    """json.dumps writes NaN/Infinity straight back out; the echo has to stay parseable."""
    assert bodies.reflect(_req(body=body, content_type="application/json")) == {}


def test_reflect_reads_a_form_body():
    req = _req(body=b"a=1&b=two&c=", content_type="application/x-www-form-urlencoded")
    assert bodies.reflect(req) == {"a": 1, "b": "two", "c": ""}


@pytest.mark.parametrize("value", [b"007", b"0012345", b"000123456789012345678", b"1e3", b"-1"])
def test_reflect_leaves_a_non_canonical_number_alone(value):
    req = _req(body=b"v=" + value, content_type="application/x-www-form-urlencoded")
    assert bodies.reflect(req)["v"] == value.decode()


def test_reflect_leaves_a_slack_timestamp_alone():
    req = _req(body=b"ts=1700000000.000600", content_type="application/x-www-form-urlencoded")
    assert bodies.reflect(req)["ts"] == "1700000000.000600"


def test_reflect_leaves_a_number_too_wide_for_a_double_alone():
    req = _req(body=b"n=1234567890123456", content_type="application/x-www-form-urlencoded")
    assert bodies.reflect(req)["n"] == "1234567890123456"


def test_reflect_reads_a_form_body_that_is_not_utf8():
    req = _req(body=b"a=\xff\xfe", content_type="application/x-www-form-urlencoded")
    assert isinstance(bodies.reflect(req)["a"], str)


@pytest.mark.parametrize("ct", [None, "text/plain", "multipart/form-data; boundary=x", ""])
def test_reflect_ignores_every_other_content_type(ct):
    assert bodies.reflect(_req(body=b'{"a": 1}', content_type=ct)) == {}


def test_reflect_never_raises_on_a_big_body():
    body = json.dumps({"k": "x" * 2_000_000}).encode()
    assert bodies.reflect(_req(body=body, content_type="application/json"))["k"].startswith("x")


def test_reflect_never_raises_on_a_deeply_nested_body():
    assert bodies.reflect(_req(body=b"[" * 5000, content_type="application/json")) == {}


def test_an_answer_for_an_unparseable_body_is_still_json():
    ans = _answer(_req("POST", host="example.invalid", path="/x", body=b"\xff" * 64))
    assert json.loads(ans.response.body) == {}


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
    assert stripe_answers.object_name(operation) == expected


def test_mint_id_shape_and_uniqueness():
    first = generic.mint_id("re_")
    assert re.fullmatch(r"re_[A-Za-z0-9]{24}", first)
    assert first != generic.mint_id("re_")


def test_every_shipped_route_that_mints_an_id_names_a_real_object():
    """The derivation rule has to hold for the routes we actually ship, not just for Stripe."""
    for service in SHIPPED.services:
        for route in service.routes:
            if "id" in route.ids:
                assert stripe_answers.object_name(route.operation) not in ("", route.operation)


def test_stripe_python_parses_the_faked_refund():
    """stripe-python parses irimi's faked refund into a `Refund` with the fields it sent.

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
    refund = stripe.Refund.construct_from(json.loads(ans.response.body), STRIPE_TEST_X)
    assert type(refund).__name__ == "Refund"  # the class comes from `object`, so it must be there
    assert refund.id.startswith("re_")
    assert refund.object == "refund"
    assert refund.amount == 4900
    assert refund.charge == "ch_test"


# ------------------------------------------------- form bodies with structure in them


def test_a_bracket_nested_form_field_becomes_a_nested_object():
    """stripe-python posts `metadata[order_id]=6735`; the live API always answers with
    `metadata`. Flat, `Refund.metadata` raised AttributeError - the bar policy.py sets itself."""
    body = bodies.parse_form("charge=ch_test&amount=4900&metadata[order_id]=6735")
    assert body == {"charge": "ch_test", "amount": 4900, "metadata": {"order_id": "6735"}}


def test_a_metadata_value_stays_a_string_at_any_depth():
    """A Stripe metadata value is always a string on the live API, so coercing one hands back
    something other than what the caller sent. The field name is what decides, wherever it
    sits on the path into the value."""
    assert bodies.parse_form("metadata[n]=6735")["metadata"]["n"] == "6735"
    assert bodies.parse_form("a[metadata][n]=6735")["a"]["metadata"]["n"] == "6735"


def test_a_nested_number_is_a_number_like_the_same_number_at_the_top_level():
    """`line_items[0][quantity]=2` echoed `"2"` while `amount=4900` echoed `4900`, so
    `quantity * 2` was `"22"` with no raise. "Stays a string" is right for `metadata` and wrong
    for every other numeric nested field."""
    assert bodies.parse_form("n=6735")["n"] == 6735
    body = bodies.parse_form("line_items[0][quantity]=2&line_items[0][price]=price_1")
    assert body == {"line_items": [{"quantity": 2, "price": "price_1"}]}
    assert bodies.parse_form("expand[]=2")["expand"] == [2]
    # The same rules the top level has: not canonical, so not a number.
    assert bodies.parse_form("a[b]=007")["a"]["b"] == "007"


def test_a_repeated_bare_key_collects_into_a_list():
    """`requests.post(data={"tags": ["a", "b"]})` and `urlencode(doseq=True)` both send these."""
    assert bodies.parse_form("tags=a&tags=b&tags=c") == {"tags": ["a", "b", "c"]}
    assert bodies.parse_form("tags=a") == {"tags": "a"}


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
    assert bodies.parse_form(text) == expected


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
    assert bodies.parse_form(text) == expected


@pytest.mark.parametrize(
    "text", ["a=2&a[0]=1", "a[0]=1&a=2", "a[0]=1&a=2&a[1]=3", "a=2&a[0]=1&a=9"]
)
def test_a_bracketed_key_beats_a_bare_one_in_either_order(text):
    """The same name spelled both ways is nonsense input, but it must not be order-dependent:
    form-body structure must survive parsing, and a bare value cannot carry any."""
    result = bodies.parse_form(text)
    assert isinstance(result["a"], list), result
    assert 1 in result["a"]


@pytest.mark.parametrize("text", ["a[0][b]=1&a[0]=2", "a[0]=2&a[0][b]=1"])
def test_a_bracket_path_beats_a_scalar_at_the_same_leaf_in_either_order(text):
    """The same class one level down, and the same answer: the deeper structure survives, so the
    echo does not depend on which spelling arrived first."""
    assert bodies.parse_form(text) == {"a": [{"b": 1}]}


@pytest.mark.parametrize("text", ["a[]=1&a[0][b]=2", "a[0][b]=2&a[]=1"])
def test_a_bracket_path_beats_an_append_of_the_same_name_in_either_order(text):
    assert bodies.parse_form(text) == {"a": [{"b": 2}]}


@pytest.mark.parametrize("text", ["a[]=1&a=2", "a=2&a[]=1"])
def test_an_append_beats_a_bare_key_of_the_same_name_in_either_order(text):
    assert bodies.parse_form(text) == {"a": [1]}


def test_an_empty_bracket_pair_is_a_list(tmp_path):
    """`expand[]=a&expand[]=b` is Stripe's own documented curl spelling. It echoed the literal
    JSON key `"expand[]"`, which is a field no SDK looks for. One repeat or none, the shape
    is the same: a caller writing `[]` means a list either way."""
    assert bodies.parse_form("expand[]=a&expand[]=b") == {"expand": ["a", "b"]}
    assert bodies.parse_form("expand[]=a") == {"expand": ["a"]}
    assert bodies.parse_form("charge=ch_1&expand[]=a") == {"charge": "ch_1", "expand": ["a"]}
    # `metadata` is still the caller's to key and to spell, at this shape too.
    assert bodies.parse_form("metadata[]=6735") == {"metadata": ["6735"]}


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
    assert bodies._MAX_FORM_DEPTH == 8
    assert bodies.parse_form("line_items[0][price_data][product_data][name]=x") == {
        "line_items": [{"price_data": {"product_data": {"name": "x"}}}]
    }
    deep = "a" + "[b]" * bodies._MAX_FORM_DEPTH
    assert bodies.parse_form(deep + "=1") == {"a": _nest(bodies._MAX_FORM_DEPTH)}
    too_deep = "a" + "[b]" * (bodies._MAX_FORM_DEPTH + 1)
    assert bodies.parse_form(too_deep + "=1") == {too_deep: 1}  # flat, so int-coerced


def test_parse_form_never_raises_on_hostile_input():
    for text in ["[" * 5000, "a" + "[b]" * 2000 + "=1", "=", "&&&", "a=%%%", "a[0]=1&a=2"]:
        assert isinstance(bodies.parse_form(text), dict)


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


# ---------------------------------------------------- the id the request already names


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
    route = servicemaps.Route(
        method="POST",
        path="/v1/widgets/{widget}/parts/{part}",
        operation="widgets.attach",
        kind="write",
        ids={"id": IdSpec(prefix="wid_")},
    )
    assert generic.named_id(route, "/v1/widgets/wid_1/parts/prt_9", "wid_") == "wid_1"
    assert generic.named_id(route, "/v1/widgets/wid_1/parts/prt_9", "prt_") == "prt_9"
    assert generic.named_id(route, "/v1/widgets/w1/parts/p9", "wid_") is None
    assert generic.named_id(route, "/v1/widgets", "wid_") is None


def test_the_id_shaped_capture_wins_when_one_prefix_matches_two_segments():
    """`sub_` matches `sub_sched_1` before `sub_2` - a real Stripe pair, and the first capture is
    the wrong one. An id is its prefix plus one run of id characters, so the second `_` in
    `sub_sched_1` is what rules it out."""
    route = servicemaps.Route(
        method="POST",
        path="/v1/subscription_schedules/{schedule}/subscriptions/{subscription}",
        operation="subscriptions.update",
        kind="write",
        ids={"id": IdSpec(prefix="sub_")},
    )
    path = "/v1/subscription_schedules/sub_sched_1/subscriptions/sub_2"
    assert generic.named_id(route, path, "sub_") == "sub_2"
    # And the longer prefix still finds its own segment, which the shorter one must not steal.
    assert generic.named_id(route, path, "sub_sched_") == "sub_sched_1"


def test_a_client_secret_is_never_echoed_back_as_the_resource_id():
    """A PaymentIntent's client secret starts with the id's own prefix
    (`pi_ABC_secret_XYZ`), so prefix matching alone hands it back as `id` - a credential in the
    one field an agent is most likely to log or store."""
    route = servicemaps.Route(
        method="POST",
        path="/v1/payment_intents/{payment_intent}/cancel",
        operation="payment_intents.cancel",
        kind="write",
        ids={"id": IdSpec(prefix="pi_")},
    )
    assert generic.named_id(route, "/v1/payment_intents/pi_ABC_secret_XYZ/cancel", "pi_") is None
    body = stripe_answers.l0_body(
        _req("POST", path="/v1/payment_intents/pi_ABC_secret_XYZ/cancel"), route
    )
    assert body["id"] != "pi_ABC_secret_XYZ"
    assert re.fullmatch(r"pi_[A-Za-z0-9]{24}", body["id"])
    assert generic.named_id(route, "/v1/payment_intents/pi_REAL999/cancel", "pi_") == "pi_REAL999"


def test_a_percent_encoded_id_is_still_the_id_the_request_names():
    """`cus%5FREAL123` is the same customer as `cus_REAL123` to the service, so reading the raw
    segment would mint a fresh id where the request names one, for any caller that over-encodes."""
    route = servicemaps.Route(
        method="POST",
        path="/v1/customers/{customer}",
        operation="customers.update",
        kind="write",
        ids={"id": IdSpec(prefix="cus_")},
    )
    assert generic.named_id(route, "/v1/customers/cus%5FREAL123", "cus_") == "cus_REAL123"
    body = json.loads(_answer(_req("POST", path="/v1/customers/cus%5FREAL123")).response.body)
    assert body["id"] == "cus_REAL123"


def test_every_shipped_write_route_that_names_ids_round_trips_or_mints():
    """Across every shipped write route: a route with a `{…}` segment carrying an id prefix must
    echo it, and one without must mint, so no route answers with a fresh id for a named one."""
    for sm in SHIPPED.services:
        for route in sm.routes:
            for name, spec in route.ids.items():
                prefix = spec.prefix
                holes = [p for p in route.path.split("/") if p.startswith("{")]
                if not holes:
                    sample = route.path.replace("{", "").replace("}", "")
                    assert generic.named_id(route, sample, prefix) is None, route.operation
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
                assert generic.named_id(route, filled, prefix) == prefix + "SAMPLE", (
                    f"{sm.service}.{route.operation} does not echo the {name} its path names"
                )


# ------------------------------------- content types, literal bodies and one encode


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
    assert bodies.reflect(_req(body=b'{"a": 1}', content_type=ct)) == {"a": 1}


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
    assert ans.flags == ()


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
        assert bodies.reflect(_req(body=raw, content_type="application/json")) == {}
    assert bodies.reflect(_req(body=b'{"v": 1.5}', content_type="application/json")) == {"v": 1.5}


def test_the_body_is_serialized_exactly_once(monkeypatch):
    """Encoding it a second time, say inside reflect purely to validate it, would put that call
    outside the never-raise guard."""
    calls = []

    def counting(*args, **kwargs):
        calls.append(1)
        return json.dumps(*args, **kwargs)

    # Rebind the name `json` inside policy rather than mutating the shared stdlib module: the
    # latter counts every json.dumps in the process, including ones classify or servicemaps make,
    # which turns this into a tripwire for whatever another batch adds to that path.
    monkeypatch.setattr(services, "json", SimpleNamespace(dumps=counting, loads=json.loads))
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
    """Form-body structure survives for digit keys too. `metadata[0]=zero` is the key "0", and
    the live API answers `{"metadata": {"0": "zero"}}`. Promoted to a list,
    `refund.metadata["0"]` raises TypeError instead of returning the value the caller sent."""
    assert bodies.parse_form("metadata[0]=zero") == {"metadata": {"0": "zero"}}
    assert bodies.parse_form("metadata[0]=a&metadata[1]=b") == {"metadata": {"0": "a", "1": "b"}}
    # Nested under another field, and mixed with a string key, it is the same field name.
    assert bodies.parse_form("a[metadata][0]=z") == {"a": {"metadata": {"0": "z"}}}
    assert bodies.parse_form("metadata[0]=z&metadata[k]=v") == {"metadata": {"0": "z", "k": "v"}}


def test_a_real_array_field_is_still_promoted_to_a_list():
    """The exemption is by field name, so the rule `metadata` opts out of still applies to
    everything else: `expand[0]=a&expand[1]=b` is an array on the wire and a list in the echo."""
    assert bodies.parse_form("expand[0]=charge&expand[1]=customer") == {
        "expand": ["charge", "customer"]
    }
    assert bodies.parse_form("line_items[0][price]=p") == {"line_items": [{"price": "p"}]}


# ------------------------------------------------------- the L1 fixture answer


@pytest.fixture
def no_fixtures(tmp_path, monkeypatch):
    """Point the fixtures at an empty directory, so every `fixture:` route degrades to L0."""
    empty = tmp_path / "fixtures"
    empty.mkdir()
    monkeypatch.setattr(generic, "fixtures_dir", lambda: empty)
    generic.clear_fixture_cache()
    yield
    generic.clear_fixture_cache()


def _refund(body: bytes = b"charge=ch_test&amount=4900"):
    request = _req("POST", path="/v1/refunds", body=body, content_type=FORM)
    return _answer(request)


FORM = "application/x-www-form-urlencoded"


def test_a_mapped_write_is_answered_at_l1_and_says_so():
    ans = _refund()
    assert ans.answered_by == "fake-L1"
    assert ans.flags == ()
    assert ans.response.status == 200
    assert ("content-type", "application/json") in ans.response.headers


def test_the_faked_refund_is_a_whole_refund_object_not_just_what_was_posted():
    """The point of L1. An agent branches on `status`, `currency` and `destination_details`,
    and at L0 none of them are there at all - it reads `None` and takes the wrong branch."""
    body = json.loads(_refund().response.body)
    fixture = generic.fixture_object("stripe", "refund")
    assert fixture is not None
    assert body.keys() == fixture.keys()
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
    fixture = generic.fixture_object("stripe", "refund")
    assert fixture is not None
    assert body["amount"] == fixture["amount"]


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
    """Echoing a named id and never echoing a client secret are properties of the answer, not
    of the echo, so they hold here too: an update answers with the id it was given, a create
    mints one."""
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
    """A client secret in the path never becomes the answer's `id`, on the route that has one.
    The fixture carries a `client_secret` of its own, and the one thing that must not happen is
    the path's copy of it becoming `id`."""
    body = json.loads(
        _answer(_req("POST", path="/v1/payment_intents/pi_ABC_secret_XYZ/cancel")).response.body
    )
    assert body["id"] != "pi_ABC_secret_XYZ"
    assert re.fullmatch(r"pi_[A-Za-z0-9]{24}", body["id"])


def test_metadata_keeps_its_string_keys_and_string_values_at_l1():
    """Form-body structure, pinned against L1: `metadata` is a dict in the fixture, so the
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
    fixture = generic.fixture_object("stripe", "refund")
    assert fixture is not None
    assert second["amount"] == fixture["amount"]


def test_a_route_whose_fixture_cannot_be_read_falls_back_to_l0_and_says_so(no_fixtures):
    """A damaged or trimmed install must still answer the write - L0 is the floor - but the
    trace has to say the fidelity is not the one the map promised, or the run reads as a
    working L1."""
    ans = _refund()
    assert ans.answered_by == "fake-L0"
    assert ans.flags == (Flag.FIXTURE_FAILED,)
    body = json.loads(ans.response.body)
    assert body["charge"] == "ch_test"  # the L0 echo, which still parses
    assert body["id"].startswith("re_")
    assert "status" not in body


def test_an_unmapped_write_is_still_l0_with_no_flag():
    """L1 is per route. A host no map claims has no fixture to start from and must not pretend."""
    ans = _answer(_req("POST", host="example.invalid", path="/things", body=b"{}"))
    assert ans.answered_by == "fake-L0"
    assert ans.flags == ()


def test_every_shipped_fixture_route_names_an_object_that_ships():
    """A `fixture:` naming an object the package does not have degrades to L0 silently but for
    the flag, which is the `--base-url-host` class of bug: configured, never consulted."""
    for sm in SHIPPED.services:
        for route in sm.routes:
            if route.fixture:
                assert generic.fixture_object(sm.service, route.fixture) is not None, (
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
    """stripe-python parses every fixture-faked object, every field included. `stripe` is not a
    dependency of this project, so this is skipped in the plain dev venv; `uv sync --group
    examples` installs it."""
    stripe = pytest.importorskip("stripe")
    ans = _answer(_req("POST", path=path, body=body, content_type=FORM))
    assert ans.answered_by == "fake-L1"
    parsed = json.loads(ans.response.body)
    obj = getattr(stripe, cls_name).construct_from(parsed, STRIPE_TEST_X)
    assert obj.object == parsed["object"]
    # Every field survives the SDK's own round trip, which is what "with every fixture field
    # present" means. Through `str(obj)` - the SDK's JSON serialization - because it turns each
    # nested object into a StripeObject of its own, so a plain `==` against the dict fails.
    assert json.loads(str(obj)) == parsed
    # And the one field the caller never sent is readable as an attribute, not an AttributeError,
    # which is the whole difference from L0.
    assert getattr(obj, fixture_field) == parsed[fixture_field]


# ------------------------------------------------------------------ answer targets


def _targeted_index(targets=(), target_reads=()):
    maps = servicemaps.loader.apply_cli_targets(
        list(servicemaps.loader.load_shipped()), targets, target_reads
    )
    return servicemaps.MapIndex(tuple(maps))


def _answer_with(index, request):
    return answer(ShadowPolicy(), request, classify(request, index))


def test_a_route_target_makes_the_answer_delegated_not_faked():
    index = _targeted_index([("api.stripe.com", "/v1/refunds", "http://127.0.0.1:3000/refund")])
    ans = _answer_with(index, _req("POST", path="/v1/refunds"))
    assert ans.answered_by == "delegated"
    assert ans.response is None  # the engine forwards; the policy does no I/O
    assert ans.forward_to == targets.ForwardTo(
        Target.parse("http://127.0.0.1:3000/refund"), forward_auth=False
    )


def test_self_is_the_default_target_and_is_exactly_the_old_behaviour():
    """`target: self` is the local fake, named rather than assumed."""
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
        _replace(r, forward_auth=True) if r.target != servicemaps.SELF_TARGET else r
        for r in stripe.routes
    )
    index = servicemaps.MapIndex(
        tuple(_replace(sm, routes=routes) if sm is stripe else sm for sm in index.services)
    )
    ans = _answer_with(index, _req("POST", path="/v1/refunds"))
    assert ans.forward_to.forward_auth is True


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
    *unmatched* route a live kind is a live `default_kind`, which the loader refuses.
    """
    sm = servicemaps.loader.load_shipped()[0]
    sm = replace(sm, target=Target.parse("http://127.0.0.1:3000"), target_reads=True)
    for kind in ("llm", "telemetry", "read"):
        cls = Classification(
            service=sm.service,
            operation="",
            kind=kind,
            flags=(),
            matched=None,  # unlisted: this is the `elif` branch, not `target_for`
            service_map=sm,
        )
        forward = targets.delegate(_req("POST", path="/anything"), cls)
        if kind == "read":
            assert forward is not None, "target_reads is what delegates a read"
        else:
            assert forward is None, f"an unmatched {kind} request was delegated"


def test_a_credential_path_host_is_never_delegated_off_the_machine():
    """THE SCOPE RULE, decision half. The loader already refuses this configuration whichever
    layer it arrived through; this asks the same question of the request and the answer actually
    in front of `delegate`, so a target reaching it some other way is refused rather than
    escaping. The map is built here because the loader will not produce one."""
    slack = next(sm for sm in servicemaps.loader.load_shipped() if sm.service == "slack")
    assert "hooks.slack.com" in slack.hosts
    off_machine = replace(slack, target=Target.parse("http://stub.internal:9000"))
    index = servicemaps.MapIndex((off_machine,))

    for path in ("/services/T0/B0/SECRET", "/workflows/T0/A0/SECRET/xyz", "/triggers/T0/1/abc"):
        request = _req("POST", host="hooks.slack.com", path=path)
        forward = targets.delegate(request, classify(request, index))
        assert forward is None, f"{path} was delegated to {forward and forward.url}"

    # The same service's other hosts are not credential-path hosts, so they still delegate.
    request = _req("POST", host="slack.com", path="/api/chat.postMessage")
    forward = targets.delegate(request, classify(request, index))
    assert forward is not None and forward.url.startswith("http://stub.internal:9000")

    # And a loopback target on the credential host is fine - that is the supported way to stub it.
    local = servicemaps.MapIndex((replace(slack, target=Target.parse("http://127.0.0.1:3000")),))
    request = _req("POST", host="hooks.slack.com", path="/workflows/T0/A0/SECRET/xyz")
    forward = targets.delegate(request, classify(request, local))
    assert forward is not None and forward.url.startswith("http://127.0.0.1:3000")


# ------------------------------------------------------- the L1 Slack envelope


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
    response carries and the request never sent."""
    ans = _post_message()
    body = json.loads(ans.response.body)
    assert ans.answered_by == "fake-L1"
    assert (body["ok"], body["channel"]) == (True, "C1")
    assert body["message"]["text"] == "hi"
    assert body["message"]["type"] == "message"
    fixture = generic.fixture_object("slack", "message")
    assert fixture is not None
    assert body["message"]["user"] == fixture["user"]
    assert "text" not in body  # the payload nests; it does not leak into the envelope


def test_the_faked_messages_ts_is_the_envelopes_and_not_the_fixtures():
    """`ts` is the message's identity for the rest of the run, so it is minted per answer. Taking
    the fixture's frozen one would make every faked message the same message."""
    body = json.loads(_post_message().response.body)
    assert body["message"]["ts"] == body["ts"]
    fixture = generic.fixture_object("slack", "message")
    assert fixture is not None
    assert body["ts"] != fixture["ts"]


def test_a_slack_post_still_answers_the_envelope_when_the_fixture_cannot_be_read(no_fixtures):
    """A fixture this install cannot read must not cost the agent the `ok` it reads to decide the
    call worked: it degrades to the envelope alone, flagged, the way Stripe degrades to L0."""
    ans = _post_message()
    body = json.loads(ans.response.body)
    assert ans.answered_by == "fake-L0"
    assert Flag.FIXTURE_FAILED in ans.flags
    assert body["ok"] is True
    assert body["channel"] == "C1"
    assert "message" not in body


def test_slack_sdk_parses_the_faked_post():
    """slack_sdk reads the faked post's body as a success rather than raising on it."""
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


# ------------------------------------------------------- the `ts` watermark


def _ts(value: str) -> tuple[int, int]:
    """A `ts` as the pair it sorts by. Not a float: consecutive `ts` values are two ULPs apart at
    Slack's magnitude, so comparing floats tests something narrower than the promise, which is
    over the two integers."""
    whole, _, fraction = value.partition(".")
    return int(whole), int(fraction)


def test_a_minted_ts_sorts_after_a_real_one_the_run_has_seen():
    """A minted `ts` sorts after every real one the run has read. A history read going past first
    is what puts the faked message after the real ones instead of somewhere in the middle."""
    clock = services.TsClock()
    newest = "1999999999.000500"
    clock.observe_history(
        json.dumps({"ok": True, "messages": [{"ts": "1999999998.000000"}, {"ts": newest}]}).encode()
    )
    assert _ts(clock.mint()) > _ts(newest)


def test_an_older_real_ts_does_not_move_the_watermark_backwards():
    """Reading an old channel must not rewind the sequence: two messages with one `ts` are two
    messages that are the same message, which is the whole reason `ts` is minted the way it is."""
    clock = services.TsClock()
    clock.last = (1_999_999_999, 500)
    clock.observe_history(json.dumps({"messages": [{"ts": "1000000000.000000"}]}).encode())
    assert clock.mint() == "1999999999.000501"


def test_a_nested_ts_is_observed_wherever_it_sits():
    """`conversations.info` carries the channel's newest message under `latest`, not in a list of
    messages. The walk does not care which shape the body is."""
    clock = services.TsClock()
    clock.observe_history(json.dumps({"channel": {"latest": {"ts": "1999999999.000001"}}}).encode())
    assert _ts(clock.mint()) > (1_999_999_999, 1)


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
def test_observing_a_body_that_carries_no_timestamp_changes_nothing(body):
    """The clock runs inside a mitmproxy hook, where a raise forwards the flow and a forwarded write
    escapes shadow mode. Every shape a real service - or a hostile one - can send is a no-op."""
    clock = services.TsClock()
    clock.last = (1_700_000_000, 7)
    clock.observe_history(body)
    assert clock.last == (1_700_000_000, 7)


def test_only_a_service_with_an_observer_learns_from_a_read():
    """The engine hands `observe_read` every read it forwards and names no service of its own.
    Which services learn anything from a body is this module's table to hold."""
    clock = services.TsClock()
    body = json.dumps({"messages": [{"ts": "1999999999.000500"}]}).encode()
    services.observe_read("stripe", body, clock)
    assert clock.last == (0, 0)
    services.observe_read("slack", body, clock)
    assert clock.last == (1_999_999_999, 500)


def test_two_policies_never_share_a_ts_sequence():
    """The clock is the policy's, one per engine: what one engine saw or minted does not move
    another's sequence."""
    seen, fresh = ShadowPolicy(), ShadowPolicy()
    seen.observe_read("slack", json.dumps({"messages": [{"ts": "1999999999.000500"}]}).encode())
    assert seen.slack_ts.last == (1_999_999_999, 500)
    assert fresh.slack_ts.last == (0, 0)


# ------------------------------------------------------------------- L3 preconditions


class _Reader(reader.Reader):
    """A reader serving canned answers in order, and writing down every request it was asked.

    An answer that is an exception is raised instead of returned, so "the reader raised" is one
    more canned answer rather than a second helper.
    """

    def __init__(self, answers):
        self.answers = list(answers)
        self.asked: list[Request] = []

    def __call__(self, request):
        self.asked.append(request)
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def _reader(*answers) -> _Reader:
    return _Reader(answers)


def _json(status: int, document) -> Response:
    return Response(status, (("content-type", "application/json"),), json.dumps(document).encode())


def _charge(ident="ch_REAL", amount=4900, refunded_so_far=0):
    return {
        "id": ident,
        "object": "charge",
        "amount": amount,
        "amount_refunded": refunded_so_far,
        "refunded": refunded_so_far == amount,
        "currency": "usd",
    }


AGENT_HEADERS = (
    ("authorization", f"Bearer {STRIPE_TEST_X}"),
    ("stripe-account", "acct_1"),
    ("stripe-version", "2024-06-20"),
    ("idempotency-key", "idem-1"),
    ("user-agent", "stripe-python"),
    ("content-type", FORM),
)


def _refund_request(body: bytes = b"charge=ch_REAL&amount=4900") -> Request:
    return replace(_req("POST", path="/v1/refunds", body=body), headers=AGENT_HEADERS)


def _checked(reader, request, index=SHIPPED, write_log=()):
    """Answer `request` the way the shadow run will: a real reader, the run's maps, its log."""
    policy = ShadowPolicy(reader=reader, maps=index)
    return answer(policy, request, classify(request, index), write_log, "t3st")


def test_a_refund_of_a_fully_refunded_charge_is_answered_with_stripes_own_refusal():
    reader = _reader(_json(200, _charge(refunded_so_far=4900)))
    ans = _checked(reader, _refund_request())
    assert ans.precondition == "rejected"
    assert ans.rejection_code == "charge_already_refunded"
    assert ans.answered_by == "fake-L1"
    assert ans.flags == ()
    assert ans.response.status == 400
    error = json.loads(ans.response.body)["error"]
    assert error["code"] == "charge_already_refunded"
    assert error["type"] == "invalid_request_error"
    assert [(r.method, r.path) for r in reader.asked] == [("GET", "/v1/charges/ch_REAL")]


def test_stripe_python_raises_invalid_request_error_off_the_modeled_refusal():
    """Against the real SDK, the modeled refusal is not merely Stripe-shaped, it is
    what makes stripe-python raise the error the agent's `except` clause is already written for.

    `stripe` is not a dependency of this project, so this is skipped in the plain dev venv;
    `uv sync --group examples` installs it, the same note as
    `test_stripe_python_parses_the_faked_refund`. The SDK's own error factory is used rather than
    a hand-rolled mapping, so a change in how stripe-python classifies a 400 fails here.
    """
    pytest.importorskip("stripe")
    import stripe
    from stripe._api_requestor import _APIRequestor

    ans = _checked(_reader(_json(200, _charge(refunded_so_far=4900))), _refund_request())
    raw = ans.response.body.decode()
    error = json.loads(raw)["error"]
    raised = _APIRequestor().specific_v1_api_error(raw, ans.response.status, raw, {}, error)
    assert isinstance(raised, stripe.InvalidRequestError)
    assert raised.code == "charge_already_refunded"
    assert raised.http_status == 400


def test_a_policy_with_no_reader_does_not_do_l3_at_all():
    """A bare `ShadowPolicy()` asks nothing, so it claims nothing: the refund is the ordinary L1
    fixture and the outcome is None, not `not_evaluable`. Every other bare construction in the
    suite relies on this."""
    request = _refund_request()
    ans = answer(ShadowPolicy(), request, classify(request, SHIPPED))
    assert ans.precondition is None
    assert ans.issued == ()
    assert ans.answered_by == "fake-L1"
    assert ans.response.status == 200
    assert json.loads(ans.response.body)["object"] == "refund"


def test_asked_and_could_not_tell_is_not_the_same_as_never_asked():
    """The two states the whole tri-state rests on, side by side: a reader that answered with a
    body irimi cannot use is `not_evaluable`, and a policy that had no reader is None."""
    request = _refund_request()
    asked = _checked(_reader(_json(200, ["not", "an", "object"])), request)
    assert asked.precondition == "not_evaluable"
    assert asked.answered_by == "fake-L1"  # still faked, at its ordinary level
    never = answer(ShadowPolicy(), request, classify(request, SHIPPED))
    assert never.precondition is None


def test_a_precondition_read_that_429s_is_not_evaluable_and_is_still_recorded():
    ans = _checked(_reader(_json(429, {"error": {"type": "rate_limit_error"}})), _refund_request())
    assert ans.precondition == "not_evaluable"
    assert ans.answered_by == "fake-L1"
    (read,) = ans.issued
    assert read.response.status == 429
    assert read.issued_by == "engine"
    assert (read.kind, read.answered_by, read.operation) == ("read", "live", "charges.retrieve")
    assert read.run_id == "t3st"


def test_a_passed_check_carries_the_charges_currency_onto_the_answer():
    """`Refund.create(charge=, amount=)` names no currency; the charge irimi read
    to decide about it does, and that is the only honest source there is."""
    ans = _checked(_reader(_json(200, _charge())), _refund_request())
    assert ans.precondition == "passed"
    assert ans.currency == "usd"


def test_a_rejected_check_carries_it_too():
    """`report.summary.write_sentence` renders the map's `human:` sentence before it branches on the
    rejection, so the `✗` line prints an amount as well and needs the same currency."""
    ans = _checked(_reader(_json(200, _charge(refunded_so_far=4900))), _refund_request())
    assert ans.precondition == "rejected"
    assert ans.currency == "usd"


def test_a_policy_with_no_reader_offers_no_currency():
    """Never asked, so nothing claimed - the same rule `precondition: None` follows."""
    request = _refund_request()
    ans = answer(ShadowPolicy(), request, classify(request, SHIPPED))
    assert ans.currency == ""


@pytest.mark.parametrize(
    "answer",
    [
        _json(429, {"error": {"type": "rate_limit_error"}}),
        _json(404, {"error": {"type": "invalid_request_error"}}),
        Response(200, (), b"<html>not json</html>"),
        None,
    ],
)
def test_a_read_that_never_produced_a_document_offers_no_currency(answer):
    """The currency comes only from a document irimi actually read. None of these is
    one, so the amount still prints as the write sent it."""
    ans = _checked(_reader(answer), _refund_request())
    assert ans.precondition == "not_evaluable"
    assert ans.currency == ""


def test_a_verdict_that_could_not_tell_still_knows_what_the_charge_is_in():
    """A check that could not decide still carries the currency: the charge WAS read and
    parsed, so the currency is known. Whether irimi could decide the service
    would have taken the write is a different question from what the write is denominated in."""
    charge = _charge()
    charge["amount_refunded"] = "not a number"  # `_refund_verdict` -> NOT_EVALUABLE
    ans = _checked(_reader(_json(200, charge)), _refund_request())
    assert ans.precondition == "not_evaluable"
    assert ans.currency == "usd"


@pytest.mark.parametrize(
    "answer",
    [
        RuntimeError("the reader raised"),
        None,
        Response(200, (), b"<html>not json</html>"),
        _json(200, ["not", "an", "object"]),
        _json(404, {"error": {"type": "invalid_request_error"}}),
        _json(500, {"error": {"type": "api_error"}}),
        _json(503, {}),
    ],
    ids=["raises", "none", "not-json", "not-an-object", "404", "500", "503"],
)
def test_a_reader_that_cannot_tell_leaves_the_write_faked_and_not_evaluable(answer):
    """Only a 200 carrying a JSON object is evaluable. Everything else is irimi
    saying it could not find out, and the write is faked at its ordinary level regardless."""
    ans = _checked(_reader(answer), _refund_request())
    assert ans.precondition == "not_evaluable"
    assert ans.answered_by == "fake-L1"
    assert ans.response.status == 200
    assert json.loads(ans.response.body)["object"] == "refund"
    # The read was attempted, so it is on the record whatever came of it - with no response when
    # the reader got none, the way `IrimiAddon.error` records a flow that never completed.
    (read,) = ans.issued
    assert (read.issued_by, read.kind, read.answered_by) == ("engine", "read", "live")
    expected = answer if isinstance(answer, Response) else None
    assert read.response == expected


def test_the_runs_own_earlier_refund_is_what_makes_the_second_one_rejected():
    """The precondition check sees the run's own writes. The real charge says `refunded: false`
    both times - the first refund was never made - and the overlay applying it is the only thing
    that can turn the second into the refusal Stripe would have given."""
    request = _refund_request()
    cls = classify(request, SHIPPED)
    first = _checked(_reader(_json(200, _charge())), request)
    assert first.precondition == "passed"
    log = [
        Exchange(
            request=request,
            response=first.response,
            service=cls.service,
            operation=cls.operation,
            kind=cls.kind,
            answered_by=first.answered_by,
            run_id="t3st",
            flags=cls.flags,
        )
    ]

    reader = _reader(_json(200, _charge()))
    second = _checked(reader, request, write_log=log)
    assert second.precondition == "rejected"
    assert second.rejection_code == "charge_already_refunded"
    assert second.response.status == 400
    # And without the log the very same live charge passes: the log is the cause, nothing else.
    assert _checked(_reader(_json(200, _charge())), request).precondition == "passed"


def test_an_overlay_that_knows_it_is_incomplete_makes_the_check_not_evaluable(monkeypatch):
    """A write checked against a world irimi knows is incomplete was not checked."""
    stripe = services.REGISTRY["stripe"]
    partial = replace(stripe, effects=lambda read, doc, writes: services.Applied(doc, partial=True))
    monkeypatch.setitem(services.REGISTRY, "stripe", partial)
    request = _refund_request()
    cls = classify(request, SHIPPED)
    earlier = _checked(_reader(_json(200, _charge())), request)
    log = [
        Exchange(
            request=request,
            response=earlier.response,
            service=cls.service,
            operation=cls.operation,
            kind=cls.kind,
            answered_by=earlier.answered_by,
            run_id="t3st",
            flags=cls.flags,
        )
    ]
    ans = _checked(_reader(_json(200, _charge(refunded_so_far=4900))), request, write_log=log)
    assert ans.precondition == "not_evaluable"
    assert ans.response.status == 200


def test_the_probe_carries_the_writes_credentials_and_scope_and_nothing_else():
    """Credentials and account/version scope are what make the read see the world the write
    would have been made in. The rest of the agent's headers are not copied: an
    `Idempotency-Key` on a GET is a request the agent never made."""
    reader = _reader(_json(200, _charge()))
    _checked(reader, _refund_request())
    (probe,) = reader.asked
    headers = dict(probe.headers)
    assert headers["authorization"] == f"Bearer {STRIPE_TEST_X}"
    assert headers["stripe-account"] == "acct_1"
    assert headers["stripe-version"] == "2024-06-20"
    assert headers["accept"] == "application/json"
    assert "idempotency-key" not in headers
    assert "user-agent" not in headers
    assert "content-type" not in headers
    assert (probe.method, probe.scheme, probe.host, probe.port) == (
        "GET",
        "https",
        "api.stripe.com",
        443,
    )
    assert probe.body == b""


def test_a_route_with_no_precondition_issues_no_read():
    reader = _reader()
    request = replace(_req("POST", path="/v1/customers/cus_REAL123"), headers=AGENT_HEADERS)
    ans = _checked(reader, request)
    assert ans.precondition is None
    assert ans.issued == ()
    assert reader.asked == []


def test_a_delegated_write_on_a_checked_route_is_not_evaluable_and_issues_no_read():
    index = _targeted_index([("api.stripe.com", "/v1/refunds", "http://127.0.0.1:3000/refund")])
    reader = _reader()
    ans = _checked(reader, _refund_request(), index=index)
    assert ans.answered_by == "delegated"
    assert ans.precondition == "not_evaluable"
    assert reader.asked == []


def test_a_probe_the_maps_do_not_call_a_read_is_refused(monkeypatch):
    """A probe may make read operations only, which is enforced by asking the maps, not by the
    verb."""
    check = services.Check(
        probe=lambda proposal: services.Probe("refunds.create", "POST", "/v1/refunds"),  # type: ignore[arg-type]
        verdict=lambda proposal, document: None,
    )
    stripe = replace(services.REGISTRY["stripe"], preconditions={"charge_refundable": check})
    monkeypatch.setitem(services.REGISTRY, "stripe", stripe)
    reader = _reader()
    ans = _checked(reader, _refund_request())
    assert ans.precondition == "not_evaluable"
    assert reader.asked == []


def _slack_post(channel: str) -> Request:
    return _req(
        "POST",
        host="slack.com",
        path="/api/chat.postMessage",
        body=json.dumps({"channel": channel, "text": "hi"}).encode(),
        content_type="application/json",
    )


@pytest.mark.parametrize("channel", ["#general", "general"])
def test_a_slack_post_to_a_channel_name_is_not_probed(channel):
    """`conversations.info` only knows ids, so probing a name would report a
    rejection Slack would never have made."""
    reader = _reader()
    ans = _checked(reader, _slack_post(channel))
    assert ans.precondition == "not_evaluable"
    assert ans.answered_by == "fake-L1"
    assert reader.asked == []


def test_a_slack_post_to_a_channel_id_is_probed_with_conversations_info():
    healthy = {"ok": True, "channel": {"id": "C0123", "is_channel": True, "is_member": True}}
    reader = _reader(_json(200, healthy))
    ans = _checked(reader, _slack_post("C0123"))
    assert ans.precondition == "passed"
    (probe,) = reader.asked
    assert (probe.method, probe.path) == ("POST", "/api/conversations.info")
    assert json.loads(probe.body) == {"channel": "C0123"}
    assert dict(probe.headers)["content-type"] == "application/json; charset=utf-8"
    assert ans.issued[0].operation == "conversations.info"


@pytest.mark.parametrize("error", ["missing_scope", "invalid_auth", "ratelimited"])
def test_a_slack_error_irimi_cannot_read_is_not_evaluable_and_the_post_is_still_faked(error):
    """Slack answers these at HTTP 200, so the policy's own non-200 rule never sees them. Before
    the verdict had a third state they were recorded `passed`, and the summary then printed
    `L3 preconditions passed` for a write irimi had not managed to check at all."""
    reader = _reader(_json(200, {"ok": False, "error": error}))
    ans = _checked(reader, _slack_post("C0123"))
    assert ans.precondition == "not_evaluable"
    assert ans.answered_by == "fake-L1", "the write is still faked at its own level"
    assert ans.rejection_code == ""
    assert len(reader.asked) == 1, "asked once, and not retried"
    assert ans.issued[0].issued_by == "engine"


def test_a_charge_the_check_could_not_read_is_not_evaluable_rather_than_passed():
    """The Stripe half of the same rule: a 200 that is not a charge irimi can read."""
    reader = _reader(_json(200, {"object": "list", "data": []}))
    ans = _checked(reader, _refund_request())
    assert ans.precondition == "not_evaluable"
    assert ans.answered_by == "fake-L1"


def test_a_slack_post_to_an_archived_channel_is_refused_the_way_slack_refuses_it():
    reader = _reader(_json(200, {"ok": True, "channel": {"id": "C0123", "is_archived": True}}))
    ans = _checked(reader, _slack_post("C0123"))
    assert ans.precondition == "rejected"
    assert ans.rejection_code == "is_archived"
    assert ans.response.status == 200
    assert json.loads(ans.response.body) == {"ok": False, "error": "is_archived"}
    assert ans.answered_by == "fake-L1"


def test_a_rejection_carries_the_level_the_refund_would_have_been_answered_at():
    fake = services.fake_rejection(classify(_refund_request(), SHIPPED), {"error": {}})
    assert (fake.answered_by, fake.flags) == ("fake-L1", ())
    assert json.loads(fake.body) == {"error": {}}


def test_a_rejection_of_a_write_with_no_fixture_is_honestly_l0():
    """There is no `fake-L3`, and `reactions.add` has no fixture to be L1 from."""
    request = _req("POST", host="slack.com", path="/api/reactions.add", body=b"{}")
    fake = services.fake_rejection(classify(request, SHIPPED), {"ok": False, "error": "x"})
    assert (fake.answered_by, fake.flags) == ("fake-L0", ())


def test_a_rejection_whose_fixture_will_not_load_says_so(monkeypatch):
    monkeypatch.setattr(generic, "fixture_object", lambda service, name: None)
    fake = services.fake_rejection(classify(_refund_request(), SHIPPED), {"error": {}})
    assert (fake.answered_by, fake.flags) == ("fake-L0", (Flag.FIXTURE_FAILED,))


# ------------------------------------------------------------------ the upstream reader


class _Upstream(BaseHTTPRequestHandler):
    status = 200
    body = b'{"object": "charge"}'
    location: str | None = None
    seen: list[str] = []

    def do_GET(self):
        _Upstream.seen.append(self.path)
        self.send_response(_Upstream.status)
        if _Upstream.location is not None:
            self.send_header("location", _Upstream.location)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(_Upstream.body)))
        self.end_headers()
        self.wfile.write(_Upstream.body)

    def log_message(self, *args):
        pass


@pytest.fixture
def upstream(monkeypatch):
    monkeypatch.setattr(_Upstream, "status", 200)
    monkeypatch.setattr(_Upstream, "location", None)
    monkeypatch.setattr(_Upstream, "seen", [])
    srv = HTTPServer(("127.0.0.1", 0), _Upstream)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    yield srv.server_address[1]
    stop_server(srv)


def _local(port: int) -> Request:
    return Request("GET", "http", "127.0.0.1", port, "/v1/charges/ch_1", "", (), b"")


@pytest.mark.parametrize("status", [200, 429])
def test_the_upstream_reader_returns_what_the_service_answered(upstream, status, monkeypatch):
    """A 429 is information the exchange records, so it comes back as a response, not as None."""
    monkeypatch.setattr(_Upstream, "status", status)
    response = UpstreamReader()(_local(upstream))
    assert response is not None
    assert response.status == status
    assert response.body == _Upstream.body


def test_the_upstream_reader_refuses_a_body_past_the_cap(upstream, monkeypatch):
    monkeypatch.setattr(reader, "MAX_BODY_BYTES", len(_Upstream.body) - 1)
    assert UpstreamReader()(_local(upstream)) is None


def _dead_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_an_unreachable_upstream_is_none_and_not_a_raise():
    assert UpstreamReader()(_local(_dead_port())) is None


def test_the_upstream_reader_dials_the_upstream_whatever_proxy_the_shell_exported(
    upstream, monkeypatch
):
    """No proxy: `urlopen`'s default opener would send the engine's own read to the
    `HTTP_PROXY`/`HTTPS_PROXY` a shell exported, which mitmproxy's forwarding never reads, so a
    dead proxy would make every L3 check `not_evaluable`. The read reaches the upstream it names."""
    dead = f"http://127.0.0.1:{_dead_port()}"
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY"):
        monkeypatch.setenv(name, dead)
    for name in ("NO_PROXY", "no_proxy"):
        monkeypatch.delenv(name, raising=False)
    response = UpstreamReader()(_local(upstream))
    assert response is not None
    assert (response.status, response.body) == (200, _Upstream.body)
    assert _Upstream.seen == ["/v1/charges/ch_1"]


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_the_upstream_reader_returns_a_redirect_and_never_follows_it(upstream, status, monkeypatch):
    """No redirect: the read dials the host the classifier saw and no other,
    so every 3xx urllib would follow for a GET comes back as the answer it is, body and all, and
    wherever it pointed is never asked."""
    monkeypatch.setattr(_Upstream, "status", status)
    monkeypatch.setattr(_Upstream, "location", f"http://127.0.0.1:{upstream}/elsewhere")
    response = UpstreamReader()(_local(upstream))
    assert response is not None
    assert (response.status, response.body) == (status, _Upstream.body)
    assert _Upstream.seen == ["/v1/charges/ch_1"]


# ------------------------------------------------------------------ the idempotency store


def _cancel_request(intent: str) -> Request:
    """`payment_intents.cancel`, which carries every parameter it has in its path."""
    return replace(_req("POST", path=f"/v1/payment_intents/{intent}/cancel"), headers=AGENT_HEADERS)


def _ask(policy: ShadowPolicy, request: Request):
    """`_checked`, but against a policy the test holds, so two calls share one run's store."""
    return answer(policy, request, classify(request, SHIPPED), (), "t3st")


def test_the_same_key_and_params_replay_the_first_answer():
    policy = ShadowPolicy(reader=_reader(_json(200, _charge())), maps=SHIPPED)
    first = _ask(policy, _refund_request())
    second = _ask(policy, _refund_request())
    assert second.response.body == first.response.body
    assert second.response.status == first.response.status == 200
    assert second.answered_by == first.answered_by == "fake-L1"
    assert ("idempotent-replayed", "true") in second.response.headers
    assert ("idempotent-replayed", "true") not in first.response.headers


def test_a_replay_carries_the_replay_flag_and_the_original_fidelity_flag():
    policy = ShadowPolicy(reader=_reader(_json(200, _charge())), maps=SHIPPED)
    first = _ask(policy, _refund_request())
    second = _ask(policy, _refund_request())
    assert first.flags == ()
    assert second.flags == (Flag.IDEMPOTENT_REPLAY,)


def test_a_replay_issues_no_precondition_read():
    """The write was checked once, when it was made. A second read would be a real request made
    on the agent's behalf for a write the agent only made once."""
    reader = _reader(_json(200, _charge()))
    policy = ShadowPolicy(reader=reader, maps=SHIPPED)
    first = _ask(policy, _refund_request())
    second = _ask(policy, _refund_request())
    assert len(reader.asked) == 1
    assert len(first.issued) == 1
    assert second.issued == ()
    assert second.precondition == first.precondition == "passed"
    # The stored answer's currency comes back too, so the replayed exchange and the first one agree
    # about what that single write was denominated in.
    assert second.currency == first.currency == "usd"


def test_the_same_key_with_a_different_amount_is_an_idempotency_error():
    reader = _reader(_json(200, _charge()))
    policy = ShadowPolicy(reader=reader, maps=SHIPPED)
    _ask(policy, _refund_request(b"charge=ch_REAL&amount=100"))
    ans = _ask(policy, _refund_request(b"charge=ch_REAL&amount=250"))
    assert ans.response.status == 400
    error = json.loads(ans.response.body)["error"]
    assert error["type"] == "idempotency_error"
    assert "idem-1" in error["message"]
    assert Flag.IDEMPOTENCY_CONFLICT in ans.flags
    assert Flag.IDEMPOTENT_REPLAY not in ans.flags
    assert ans.rejection_code == "idempotency_error"
    # L3 was never asked, so it decided nothing: a conflict is not `precondition: rejected`.
    assert ans.precondition is None
    assert ans.issued == ()
    assert len(reader.asked) == 1


def test_a_replayed_rejection_is_the_same_rejection():
    """Stripe keeps the first response under a key whether it was accepted or refused, so a
    retry of a refused write is refused again - with the same body, and without asking again."""
    reader = _reader(_json(200, _charge(refunded_so_far=4900)))
    policy = ShadowPolicy(reader=reader, maps=SHIPPED)
    first = _ask(policy, _refund_request())
    second = _ask(policy, _refund_request())
    assert first.precondition == "rejected"
    assert second.response.body == first.response.body
    assert second.response.status == first.response.status == 400
    assert second.precondition == "rejected"
    assert second.rejection_code == first.rejection_code == "charge_already_refunded"
    assert second.flags == (*first.flags, Flag.IDEMPOTENT_REPLAY)
    assert len(reader.asked) == 1


def test_a_write_that_sent_no_key_is_never_replayed():
    policy = ShadowPolicy()
    body = b"charge=ch_REAL&amount=4900"
    request = _req("POST", path="/v1/refunds", body=body, content_type=FORM)
    first = _ask(policy, request)
    second = _ask(policy, request)
    assert json.loads(first.response.body)["id"] != json.loads(second.response.body)["id"]
    assert Flag.IDEMPOTENT_REPLAY not in first.flags
    assert Flag.IDEMPOTENT_REPLAY not in second.flags


def test_a_slack_write_is_never_replayed():
    """Slack has no idempotency mechanism, so its spec has no `idempotency`: a
    `chat.postMessage` sent twice really is two messages, whatever headers it carries."""
    policy = ShadowPolicy()
    post = _slack_post("C0123")
    request = replace(post, headers=(*post.headers, ("idempotency-key", "idem-1")))
    first = _ask(policy, request)
    second = _ask(policy, request)
    assert json.loads(first.response.body)["ts"] != json.loads(second.response.body)["ts"]
    assert Flag.IDEMPOTENT_REPLAY not in second.flags
    assert Flag.IDEMPOTENCY_CONFLICT not in second.flags


def test_a_key_is_scoped_to_the_stripe_account():
    policy = ShadowPolicy()
    first = _ask(policy, _refund_request())
    other = tuple(
        (k, "acct_2" if k == "stripe-account" else v) for k, v in _refund_request().headers
    )
    second = _ask(policy, replace(_refund_request(), headers=other))
    assert json.loads(first.response.body)["id"] != json.loads(second.response.body)["id"]
    assert second.response.status == 200
    assert Flag.IDEMPOTENT_REPLAY not in second.flags
    assert Flag.IDEMPOTENCY_CONFLICT not in second.flags


class _BrokenStore:
    """A store whose lookup or whose remembering raises, so each guard is reached on its own."""

    def __init__(self, broken: str):
        self.broken = broken

    def get(self, slot, params):
        if self.broken == "get":
            raise RuntimeError("the store is broken")
        return None, False

    def put(self, slot, params, stored):
        if self.broken == "put":
            raise RuntimeError("the store is broken")


@pytest.mark.parametrize("broken", ["get", "put"])
def test_a_store_that_raises_answers_the_write_afresh(broken):
    """Nothing on the answer path may raise: a raised hook forwards the flow, and a forwarded
    write escapes shadow mode. A broken store costs the retry its replay and nothing else."""
    policy = ShadowPolicy()
    policy.idempotency = _BrokenStore(broken)  # type: ignore[assignment]
    first = _ask(policy, _refund_request())
    second = _ask(policy, _refund_request())
    assert first.response.status == second.response.status == 200
    assert first.answered_by == second.answered_by == "fake-L1"
    assert json.loads(first.response.body)["id"] != json.loads(second.response.body)["id"]
    assert Flag.IDEMPOTENT_REPLAY not in second.flags


def test_a_key_reused_on_another_object_of_one_route_is_refused():
    """A key names one write, not one route. `payment_intents.cancel` posts nothing at all, so
    without comparing the path a key reused to cancel a second intent would replay the first
    intent's answer and tell the agent it had cancelled an object it never named."""
    policy = ShadowPolicy()
    first = _ask(policy, _cancel_request("pi_AAA"))
    second = _ask(policy, _cancel_request("pi_BBB"))
    assert json.loads(first.response.body)["id"] == "pi_AAA"
    assert second.response.status == 400
    assert json.loads(second.response.body)["error"]["type"] == "idempotency_error"
    assert Flag.IDEMPOTENCY_CONFLICT in second.flags
    assert Flag.IDEMPOTENT_REPLAY not in second.flags


def test_a_key_reused_on_another_route_is_refused():
    """The same hole across two routes: a cancel and a customer update both post an empty body,
    so without the path the agent asking to update a customer would get a payment intent back."""
    policy = ShadowPolicy()
    _ask(policy, _cancel_request("pi_AAA"))
    ans = _ask(policy, replace(_req("POST", path="/v1/customers/cus_ZZZ"), headers=AGENT_HEADERS))
    assert ans.response.status == 400
    assert json.loads(ans.response.body)["error"]["type"] == "idempotency_error"
    assert Flag.IDEMPOTENCY_CONFLICT in ans.flags


def test_stripe_python_raises_idempotency_error_on_the_conflict_body():
    """The conflict body is what makes stripe-python raise `IdempotencyError`, the class an
    agent's `except` clause names. Stripe's body carries no `code` for it - the SDK branches on
    `type` - so the class is what is asserted and not `code`. Skipped without `stripe`, like
    `test_stripe_python_raises_invalid_request_error_off_the_modeled_refusal`."""
    pytest.importorskip("stripe")
    import stripe
    from stripe._api_requestor import _APIRequestor

    policy = ShadowPolicy()
    _ask(policy, _refund_request(b"charge=ch_REAL&amount=100"))
    ans = _ask(policy, _refund_request(b"charge=ch_REAL&amount=250"))
    raw = ans.response.body.decode()
    error = json.loads(raw)["error"]
    raised = _APIRequestor().specific_v1_api_error(raw, ans.response.status, raw, {}, error)
    assert isinstance(raised, stripe.IdempotencyError)
    assert raised.http_status == 400


# ---------------------------------------------------- would-have-fired


def test_a_faked_refund_lists_the_webhooks_it_would_have_fired():
    """At the decision, a faked refund lists `refund.created` and `charge.refunded`, off its
    route's `fires:`."""
    ans = _answer(_req("POST", path="/v1/refunds"))
    assert ans.answered_by == "fake-L1"
    assert ans.would_fire == ("refund.created", "charge.refunded")


def test_a_faked_customer_update_lists_its_one_event():
    """Each route lists its own events, not the refund's."""
    ans = _answer(_req("POST", path="/v1/customers/cus_REAL123"))
    assert ans.would_fire == ("customer.updated",)


def test_a_write_on_an_unmapped_host_lists_nothing():
    """No route, no `fires:`, so no claim about what the service would have sent. It is still an
    accepted fake, so this is the `route is None` arm of the one line that sets it."""
    ans = _answer(_req("POST", host="example.invalid", path="/things", body=b"{}"))
    assert ans.answered_by == "fake-L0"
    assert ans.would_fire == ()


def test_a_live_read_lists_nothing():
    """A read is forwarded and performs nothing, so there is nothing that did not fire."""
    ans = _answer(_req())
    assert ans.answered_by == "live"
    assert ans.would_fire == ()


@pytest.mark.parametrize(
    ("reader", "outcome"),
    [
        (_reader(_json(200, _charge())), "passed"),
        (_reader(_json(429, {"error": {"type": "rate_limit_error"}})), "not_evaluable"),
        (None, None),
    ],
    ids=["passed", "not-evaluable", "never-asked"],
)
def test_every_precondition_outcome_but_rejected_lists_the_webhooks(reader, outcome):
    """`precondition` has four states, and three of them mean irimi faked the write: it passed,
    irimi could not find out, or nothing was asked. All three are accepted writes and list their
    events; only `rejected` lists nothing."""
    ans = answer(
        ShadowPolicy(reader=reader, maps=SHIPPED),
        _refund_request(),
        classify(_refund_request(), SHIPPED),
    )
    assert ans.precondition == outcome
    assert ans.would_fire == ("refund.created", "charge.refunded")


def test_a_rejected_write_lists_no_webhooks():
    """L3 said the service would have refused this refund. Nothing was performed, so nothing
    would have fired."""
    policy = ShadowPolicy(reader=_reader(_json(200, _charge(refunded_so_far=4900))), maps=SHIPPED)
    ans = _ask(policy, _refund_request())
    assert ans.precondition == "rejected"
    assert ans.rejection_code == "charge_already_refunded"
    assert ans.would_fire == ()


def test_a_replayed_write_lists_no_webhooks():
    """A replay is the first write's own answer. Its events are already on the first write's
    exchange; listing them again would promise `refund.created` twice for one refund."""
    policy = ShadowPolicy(reader=_reader(_json(200, _charge())), maps=SHIPPED)
    first = _ask(policy, _refund_request())
    second = _ask(policy, _refund_request())
    assert first.would_fire == ("refund.created", "charge.refunded")
    assert Flag.IDEMPOTENT_REPLAY in second.flags
    assert second.would_fire == ()


def test_an_idempotency_conflict_lists_no_webhooks():
    """The service would have refused the reused key, so nothing would have fired. A conflict is
    not `precondition: rejected`, so a check written against that alone would let it through.
    """
    policy = ShadowPolicy(reader=_reader(_json(200, _charge())), maps=SHIPPED)
    _ask(policy, _refund_request(b"charge=ch_REAL&amount=100"))
    ans = _ask(policy, _refund_request(b"charge=ch_REAL&amount=250"))
    assert Flag.IDEMPOTENCY_CONFLICT in ans.flags
    assert ans.rejection_code == "idempotency_error"
    assert ans.precondition is None
    assert ans.would_fire == ()


def test_a_delegated_write_lists_no_webhooks():
    """The target performed the write, or did not; irimi built nothing and claims nothing - the
    same reason `report._fidelity` refuses to print `L2` for one."""
    index = _targeted_index([("api.stripe.com", "/v1/refunds", "http://127.0.0.1:3000/refund")])
    ans = _answer_with(index, _req("POST", path="/v1/refunds"))
    assert ans.answered_by == "delegated"
    assert ans.would_fire == ()


# ------------------------------------------------------------------ id formats and a rejection


def test_an_integer_id_is_minted_as_an_integer_and_echoed_from_its_own_segment():
    route = servicemaps.Route(
        method="PATCH",
        path="/repos/{owner}/{repo}/issues/{number}",
        operation="issues.update",
        kind="write",
        ids={"number": IdSpec(format="int"), "id": IdSpec(format="int")},
    )
    body = generic.l0_body(_req("PATCH", path="/repos/o/r/issues/42"), route)
    assert body["number"] == 42
    assert isinstance(body["id"], int) and not isinstance(body["id"], bool)
    assert 10**9 <= body["id"] < 10**10


def test_a_uuid_id_is_minted_as_a_uuid_and_echoed_from_its_own_segment():
    route = servicemaps.Route(
        method="POST",
        path="/issues/{id}",
        operation="issues.update",
        kind="write",
        ids={"id": IdSpec(format="uuid")},
    )
    named = "0b6b1f1e-2c1a-4c55-9a39-6f1b3c9d2e10"
    assert generic.l0_body(_req("POST", path=f"/issues/{named}"), route)["id"] == named
    minted = generic.l0_body(_req("POST", path="/issues/not-a-uuid"), route)["id"]
    assert re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", minted
    )


def test_a_rejection_mints_no_slack_ts():
    """A rejected post never happened, so it must not move the `ts` sequence later posts sort by."""
    clock = services.TsClock()
    request = _req(
        "POST",
        host="slack.com",
        path="/api/chat.postMessage",
        body=b"channel=C1&text=hi",
        content_type="application/x-www-form-urlencoded",
    )
    services.fake_rejection(classify(request, SHIPPED), {"ok": False, "error": "x"})
    assert clock.last == (0, 0)
    assert services.fake_level(classify(request, SHIPPED)) == ("fake-L1", ())


def test_a_slack_write_no_envelope_names_gets_the_default_envelope():
    """`ok: true` with the posted channel and a minted `ts`, never the generic echo: slack_sdk
    raises on a body without `ok`."""
    route = servicemaps.Route("POST", "/api/chat.update", "chat.update", "write")
    slack = servicemaps.ServiceMap(service="slack", hosts=frozenset({"slack.com"}), routes=(route,))
    request = _req(
        "POST",
        host="slack.com",
        path="/api/chat.update",
        body=b"channel=C1&text=hi",
        content_type="application/x-www-form-urlencoded",
    )
    fake = services.fake_response(
        request, classify(request, servicemaps.MapIndex((slack,))), services.TsClock()
    )
    body = json.loads(fake.body)
    assert sorted(body) == ["channel", "ok", "ts"]
    assert (body["ok"], body["channel"], fake.answered_by) == (True, "C1", "fake-L0")
