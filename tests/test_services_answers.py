"""What a faked write is answered with: the echo, the fixture, and each service's envelope."""

import json
import re
from types import SimpleNamespace

import pytest

from irimi import servicemaps, services
from irimi.core import bodies
from irimi.core.exchange import Flag
from irimi.decide.policy import ShadowPolicy
from irimi.servicemaps import IdSpec
from irimi.servicemaps.classify import classify
from irimi.services import generic
from irimi.services.slack import answers as slack_answers
from irimi.services.stripe import answers as stripe_answers
from tests.support.builders import FORM_CONTENT_TYPE
from tests.support.credentials import STRIPE_TEST_X
from tests.support.decisions import SHIPPED, answer_shipped, make_request


def test_unmapped_write_reflects_its_own_fields_and_nothing_else():
    """Nothing matched, so there is no id to mint, and the echo of a service irimi has no shape
    for carries none of Stripe's fields."""
    ans = answer_shipped(
        make_request(
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
    ans = answer_shipped(make_request("POST", path="/v1/charges/ch_1/refund"))
    body = json.loads(ans.response.body)
    assert isinstance(body["created"], int)
    assert "object" not in body


def test_stripe_refund_is_a_parseable_refund():
    """The shipped `refunds.create` route: minted ids, a derived object, the posted fields back."""
    ans = answer_shipped(
        make_request(
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
    ans = answer_shipped(
        make_request(
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
    body = stripe_answers.l1_body(
        make_request("POST"), route, {"object": "thing", "livemode": True}
    )
    assert body["livemode"] is False


def test_slack_write_gets_slacks_own_envelope():
    ans = answer_shipped(
        make_request(
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
    ans = answer_shipped(
        make_request(
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
    ans = answer_shipped(
        make_request(
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
    ans = answer_shipped(
        make_request(
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
        ans = answer_shipped(
            make_request(
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
    ans = answer_shipped(
        make_request("POST", host="slack.com", path="/api/files.getUploadURLExternal")
    )
    assert json.loads(ans.response.body) == {}


def test_reactions_add_answers_the_bare_ok_slack_really_sends():
    """Slack answers reactions.add with `{"ok": true}` and nothing else. The `ts` and `channel`
    a one-shape envelope would add here are chat.postMessage's, and an agent that read the `ts`
    of its own reaction as a message id would be reading a field the real API never sends."""
    ans = answer_shipped(
        make_request(
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
    ans = answer_shipped(
        make_request(
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
    ans = answer_shipped(make_request("POST", path=path))
    assert ans.answered_by == "fake-L0"
    assert ans.response.body == b"{}"
    assert ans.flags == ()


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
def test_object_name_is_the_object_the_operation_makes(operation, expected):
    assert stripe_answers.object_name(operation) == expected


def test_a_minted_id_has_its_prefix_and_is_unique():
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
    ans = answer_shipped(
        make_request(
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
    request = make_request("POST", path=path)
    assert classify(request, SHIPPED).operation == operation
    body = json.loads(answer_shipped(request).response.body)
    assert body["id"] == named
    assert body["object"] == object_name


def test_a_create_still_mints_every_id_it_names():
    """A create posts to a collection, so it captures no path segment and the service mints."""
    body = json.loads(answer_shipped(make_request("POST", path="/v1/refunds")).response.body)
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
        make_request("POST", path="/v1/payment_intents/pi_ABC_secret_XYZ/cancel"), route
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
    body = json.loads(
        answer_shipped(make_request("POST", path="/v1/customers/cus%5FREAL123")).response.body
    )
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
    assert bodies.reflect(make_request(body=b'{"a": 1}', content_type=ct)) == {"a": 1}


def test_a_slack_incoming_webhook_answers_the_literal_ok():
    """A real incoming webhook answers the body `ok` as text/plain. The Web API envelope it got
    instead breaks `assert resp.text == "ok"`, the common raw-requests idiom."""
    ans = answer_shipped(
        make_request(
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
    ans = answer_shipped(
        make_request(
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
        assert bodies.reflect(make_request(body=raw, content_type="application/json")) == {}
    assert bodies.reflect(make_request(body=b'{"v": 1.5}', content_type="application/json")) == {
        "v": 1.5
    }


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
    answer_shipped(
        make_request(
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


def _faked_refund(body: bytes = b"charge=ch_test&amount=4900"):
    request = make_request("POST", path="/v1/refunds", body=body, content_type=FORM_CONTENT_TYPE)
    return answer_shipped(request)


def test_a_mapped_write_is_answered_at_l1_and_says_so():
    ans = _faked_refund()
    assert ans.answered_by == "fake-L1"
    assert ans.flags == ()
    assert ans.response.status == 200
    assert ("content-type", "application/json") in ans.response.headers


def test_the_faked_refund_is_a_whole_refund_object_not_just_what_was_posted():
    """The point of L1. An agent branches on `status`, `currency` and `destination_details`,
    and at L0 none of them are there at all - it reads `None` and takes the wrong branch."""
    body = json.loads(_faked_refund().response.body)
    fixture = generic.fixture_object("stripe", "refund")
    assert fixture is not None
    assert body.keys() == fixture.keys()
    assert body["status"] == "succeeded"
    assert body["currency"] == "usd"
    assert body["destination_details"] == {"card": {"type": "reversal"}, "type": "card"}


def test_the_posted_fields_still_win_over_the_fixtures_own_values():
    body = json.loads(_faked_refund(b"charge=ch_REAL&amount=1234&reason=fraudulent").response.body)
    assert body["charge"] == "ch_REAL"
    assert body["amount"] == 1234  # an int, as stripe-python's own Refund.amount is
    # `reason` is null in the fixture, so it names no type and whatever was sent is taken.
    assert body["reason"] == "fraudulent"


def test_a_field_the_fixture_does_not_name_is_not_invented():
    """The live API refuses an unknown parameter outright. Echoing it back instead would hand
    the agent a refund object no real response can produce, which is the drift a fixture exists
    to stop; the L0 echo keeps reflecting everything, and that is what makes it the honest floor.
    """
    body = json.loads(_faked_refund(b"charge=ch_1&not_a_refund_field=surprise").response.body)
    assert "not_a_refund_field" not in body
    assert body["charge"] == "ch_1"


def test_a_value_of_the_wrong_type_leaves_the_fixtures_own_in_place():
    """`amount` is a number in every real refund. A form body can say anything, and a string
    there is what makes `refund.amount / 100` raise inside the agent instead of here."""
    body = json.loads(_faked_refund(b"amount=not-a-number").response.body)
    fixture = generic.fixture_object("stripe", "refund")
    assert fixture is not None
    assert body["amount"] == fixture["amount"]


def test_the_service_owns_the_fields_that_say_what_this_object_is():
    """`object` decides which class stripe-python builds, and `id` is what the agent logs. A
    body posting either must not choose them, or a refund route answers a `charge`."""
    body = json.loads(
        _faked_refund(b"object=charge&id=re_MINE&created=1&livemode=true").response.body
    )
    assert body["object"] == "refund"
    assert re.fullmatch(r"re_[A-Za-z0-9]{24}", body["id"])
    assert body["created"] != 1
    assert "livemode" not in body  # the real refund object has none, so neither has ours


def test_livemode_is_set_where_the_object_has_it():
    """irimi performed nothing, so nothing it answers happened in live mode. Only where the
    real object carries the field: inventing one on a refund would be a field Stripe never sends.
    """
    body = json.loads(
        answer_shipped(make_request("POST", path="/v1/customers/cus_REAL123")).response.body
    )
    assert body["livemode"] is False
    assert "livemode" not in json.loads(_faked_refund().response.body)


def test_l1_still_echoes_the_id_the_path_names_and_mints_the_rest():
    """Echoing a named id and never echoing a client secret are properties of the answer, not
    of the echo, so they hold here too: an update answers with the id it was given, a create
    mints one."""
    update = json.loads(
        answer_shipped(make_request("POST", path="/v1/customers/cus_REAL123")).response.body
    )
    assert update["id"] == "cus_REAL123"
    assert update["object"] == "customer"

    cancel = json.loads(
        answer_shipped(
            make_request("POST", path="/v1/payment_intents/pi_REAL999/cancel")
        ).response.body
    )
    assert cancel["id"] == "pi_REAL999"
    assert cancel["object"] == "payment_intent"

    created = json.loads(_faked_refund().response.body)
    assert re.fullmatch(r"re_[A-Za-z0-9]{24}", created["id"])
    assert re.fullmatch(r"txn_[A-Za-z0-9]{24}", created["balance_transaction"])


def test_a_client_secret_is_still_never_echoed_back_as_the_id_at_l1():
    """A client secret in the path never becomes the answer's `id`, on the route that has one.
    The fixture carries a `client_secret` of its own, and the one thing that must not happen is
    the path's copy of it becoming `id`."""
    body = json.loads(
        answer_shipped(
            make_request("POST", path="/v1/payment_intents/pi_ABC_secret_XYZ/cancel")
        ).response.body
    )
    assert body["id"] != "pi_ABC_secret_XYZ"
    assert re.fullmatch(r"pi_[A-Za-z0-9]{24}", body["id"])


def test_metadata_keeps_its_string_keys_and_string_values_at_l1():
    """Form-body structure, pinned against L1: `metadata` is a dict in the fixture, so the
    parsed form value replaces it wholesale and every rule `parse_form` applies survives."""
    body = json.loads(_faked_refund(b"metadata[order_id]=6735&metadata[0]=zero").response.body)
    assert body["metadata"] == {"order_id": "6735", "0": "zero"}


def test_a_hostile_body_cannot_reach_the_fixtures_own_fields():
    body = json.loads(_faked_refund(("a" + "[b]" * 2000 + "=1").encode()).response.body)
    assert body["object"] == "refund"
    assert body["status"] == "succeeded"


def test_two_writes_do_not_share_one_fixture():
    """The faker writes over the object it is given, so a cached object handed out twice would
    make every later refund carry the first caller's arguments."""
    first = json.loads(_faked_refund(b"charge=ch_FIRST&amount=111").response.body)
    second = json.loads(_faked_refund(b"charge=ch_SECOND").response.body)
    assert first["charge"] == "ch_FIRST"
    assert second["charge"] == "ch_SECOND"
    fixture = generic.fixture_object("stripe", "refund")
    assert fixture is not None
    assert second["amount"] == fixture["amount"]


def test_a_route_whose_fixture_cannot_be_read_falls_back_to_l0_and_says_so(no_fixtures):
    """A damaged or trimmed install must still answer the write - L0 is the floor - but the
    trace has to say the fidelity is not the one the map promised, or the run reads as a
    working L1."""
    ans = _faked_refund()
    assert ans.answered_by == "fake-L0"
    assert ans.flags == (Flag.FIXTURE_FAILED,)
    body = json.loads(ans.response.body)
    assert body["charge"] == "ch_test"  # the L0 echo, which still parses
    assert body["id"].startswith("re_")
    assert "status" not in body


def test_an_unmapped_write_is_still_l0_with_no_flag():
    """L1 is per route. A host no map claims has no fixture to start from and must not pretend."""
    ans = answer_shipped(make_request("POST", host="example.invalid", path="/things", body=b"{}"))
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
    ans = answer_shipped(make_request("POST", path=path, body=body, content_type=FORM_CONTENT_TYPE))
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


# ------------------------------------------------------- the L1 Slack envelope


def _post_message(body: bytes = b'{"channel": "C1", "text": "hi"}'):
    return answer_shipped(
        make_request(
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


# ------------------------------------------------------------------ id formats and a rejection


def test_an_integer_id_is_minted_as_an_integer_and_echoed_from_its_own_segment():
    route = servicemaps.Route(
        method="PATCH",
        path="/repos/{owner}/{repo}/issues/{number}",
        operation="issues.update",
        kind="write",
        ids={"number": IdSpec(format="int"), "id": IdSpec(format="int")},
    )
    body = generic.l0_body(make_request("PATCH", path="/repos/o/r/issues/42"), route)
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
    assert generic.l0_body(make_request("POST", path=f"/issues/{named}"), route)["id"] == named
    minted = generic.l0_body(make_request("POST", path="/issues/not-a-uuid"), route)["id"]
    assert re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", minted
    )


def test_a_rejection_mints_no_slack_ts():
    """A rejected post never happened, so it must not move the `ts` sequence later posts sort by."""
    clock = services.TsClock()
    request = make_request(
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
    request = make_request(
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
