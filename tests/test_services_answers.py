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
    """An unmapped write's echo carries its own fields only: no minted id, no Stripe fields."""
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
    """A fixture answer always says `livemode: false`, whatever the fixture holds.

    A fixture regenerated from a live-mode example must not tell an agent its write was real."""
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
    """Every minted Slack `ts` is unique, since `ts` is a message's identifier."""
    clock = services.TsClock()
    seen = [clock.mint() for _ in range(5_000)]
    assert len(set(seen)) == len(seen)
    # Increasing too: real `ts` values sort by time, and agents order transcripts by them.
    assert [float(ts) for ts in seen] == sorted(float(ts) for ts in seen)
    assert all(re.fullmatch(r"\d+\.\d{6}", ts) for ts in seen)


def test_a_slack_timestamp_still_names_the_current_second(monkeypatch):
    """A minted `ts` stays on the current second, since an SDK may render it as a date."""
    monkeypatch.setattr(slack_answers.time, "time", lambda: 1_700_000_000.9)  # type: ignore[attr-defined]
    clock = services.TsClock()
    assert clock.mint() == "1700000000.000000"
    assert clock.mint() == "1700000000.000001"


def test_slack_write_reads_the_json_body_slack_sdk_actually_posts():
    """A Slack write sent as JSON, as slack_sdk 3.x sends it, is read as well as a form."""
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
    """A faked reply's `message` carries the posted `thread_ts`, as real Slack's does."""
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
    # `ts` is minted and names the reply, not the thread, so the two differ.
    assert body["message"]["ts"] == body["ts"] != body["message"]["thread_ts"]


def test_a_top_level_post_answers_with_no_thread_ts_key_at_all():
    """A faked top-level post's `message` has no `thread_ts` key, never `thread_ts: null`."""
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
    """slack_sdk reads `thread_ts` from a faked reply and a faked top-level post as Slack's own."""
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
    """An unlisted Slack route gets the generic echo, not an `ok: true` envelope.

    A false success sends slack_sdk into a later crash (`files_upload_v2` reads `upload_url =
    None`); the echo makes it raise the `SlackApiError` callers already catch."""
    ans = answer_shipped(
        make_request("POST", host="slack.com", path="/api/files.getUploadURLExternal")
    )
    assert json.loads(ans.response.body) == {}


def test_reactions_add_answers_the_bare_ok_slack_really_sends():
    """`reactions.add` is answered `{"ok": true}` alone, with no `chat.postMessage` fields."""
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
    """`files.upload` is answered with the bare `ok` too; the method is retired."""
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
    """A body builder that raises still leaves the write answered by irimi (fail-closed hook rule).

    Each case breaks the builder its own request reaches: the echo for the unmapped path, the
    fixture for `/v1/refunds`."""

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
    """Every shipped route that mints an `id` derives a real object name from its operation."""
    for service in SHIPPED.services:
        for route in service.routes:
            if "id" in route.ids:
                assert stripe_answers.object_name(route.operation) not in ("", route.operation)


def test_stripe_python_parses_the_faked_refund():
    """stripe-python parses irimi's faked refund into a `Refund` with the fields it sent."""
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
    """An update or a cancel echoes the id its path names, as the live API does, not a fresh one."""
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
    """A captured segment without the id's prefix never becomes the id."""
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
    """When one prefix matches two segments, the id-shaped one wins (`sub_2`, not `sub_sched_1`).

    An id is its prefix plus one run of id characters, so the second `_` rules out `sub_sched_1`."""
    route = servicemaps.Route(
        method="POST",
        path="/v1/subscription_schedules/{schedule}/subscriptions/{subscription}",
        operation="subscriptions.update",
        kind="write",
        ids={"id": IdSpec(prefix="sub_")},
    )
    path = "/v1/subscription_schedules/sub_sched_1/subscriptions/sub_2"
    assert generic.named_id(route, path, "sub_") == "sub_2"
    # The longer prefix still finds its own segment.
    assert generic.named_id(route, path, "sub_sched_") == "sub_sched_1"


def test_a_client_secret_is_never_echoed_back_as_the_resource_id():
    """A client secret (`pi_ABC_secret_XYZ`) is never echoed back as the resource id.

    It starts with the id's prefix, and an agent is most likely to log or store the `id` field."""
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
    """A percent-encoded id (`cus%5FREAL123`) is decoded and echoed, not replaced by a fresh one."""
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
    """Every shipped route echoes an id its path names and mints one when its path names none."""
    for sm in SHIPPED.services:
        for route in sm.routes:
            for name, spec in route.ids.items():
                prefix = spec.prefix
                holes = [p for p in route.path.split("/") if p.startswith("{")]
                if not holes:
                    sample = route.path.replace("{", "").replace("}", "")
                    assert generic.named_id(route, sample, prefix) is None, route.operation
                    continue
                # Only the last hole carries the prefix: filling them all would pass a route
                # whose id is in the wrong segment.
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
    """Every JSON media type, `+json` suffixes included, is parsed as JSON."""
    assert bodies.reflect(make_request(body=b'{"a": 1}', content_type=ct)) == {"a": 1}


def test_a_slack_incoming_webhook_answers_the_literal_ok():
    """A Slack incoming webhook is answered with the text/plain body `ok`, as the real one is."""
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
    """The webhook's literal body is keyed by operation, so the Web API keeps its JSON envelope."""
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
    """A body with NaN or Infinity reflects nothing, so the one `json.dumps` cannot fail."""
    for raw in [b'{"v": NaN}', b'{"v": Infinity}', b'{"v": -Infinity}', b'{"v": [1e400]}']:
        assert bodies.reflect(make_request(body=raw, content_type="application/json")) == {}
    assert bodies.reflect(make_request(body=b'{"v": 1.5}', content_type="application/json")) == {
        "v": 1.5
    }


def test_the_body_is_serialized_exactly_once(monkeypatch):
    """A faked body is serialised once, inside the guard that keeps the answer path from raising."""
    calls = []

    def counting(*args, **kwargs):
        calls.append(1)
        return json.dumps(*args, **kwargs)

    # Rebind `json` in `services` only: patching the stdlib module would count every caller.
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
    """`metadata[0]=zero` stays `{"0": "zero"}`, as the live API answers, not a list."""
    assert bodies.parse_form("metadata[0]=zero") == {"metadata": {"0": "zero"}}
    assert bodies.parse_form("metadata[0]=a&metadata[1]=b") == {"metadata": {"0": "a", "1": "b"}}
    # The same holds nested under another field, and mixed with a string key.
    assert bodies.parse_form("a[metadata][0]=z") == {"a": {"metadata": {"0": "z"}}}
    assert bodies.parse_form("metadata[0]=z&metadata[k]=v") == {"metadata": {"0": "z", "k": "v"}}


def test_a_real_array_field_is_still_promoted_to_a_list():
    """An array field other than `metadata` (`expand[0]=a&expand[1]=b`) still becomes a list."""
    assert bodies.parse_form("expand[0]=charge&expand[1]=customer") == {
        "expand": ["charge", "customer"]
    }
    assert bodies.parse_form("line_items[0][price]=p") == {"line_items": [{"price": "p"}]}


# ------------------------------------------------------- the fixture answer


@pytest.fixture
def no_fixtures(tmp_path, monkeypatch):
    """Point the fixtures at an empty directory, so every `fixture:` route answers with the echo."""
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
    """A fixture answer is a whole refund object, with the fields an agent branches on."""
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
    # `reason` is null in the fixture, so it names no type and the posted value is taken.
    assert body["reason"] == "fraudulent"


def test_a_field_the_fixture_does_not_name_is_not_invented():
    """A fixture answer drops a posted field the object does not have, as no real one carries it."""
    body = json.loads(_faked_refund(b"charge=ch_1&not_a_refund_field=surprise").response.body)
    assert "not_a_refund_field" not in body
    assert body["charge"] == "ch_1"


def test_a_value_of_the_wrong_type_leaves_the_fixtures_own_in_place():
    """A posted value of the wrong type leaves the fixture's own value in place."""
    body = json.loads(_faked_refund(b"amount=not-a-number").response.body)
    fixture = generic.fixture_object("stripe", "refund")
    assert fixture is not None
    assert body["amount"] == fixture["amount"]


def test_the_service_owns_the_fields_that_say_what_this_object_is():
    """A posted `object`, `id`, `created` or `livemode` never overrides the service's own."""
    body = json.loads(
        _faked_refund(b"object=charge&id=re_MINE&created=1&livemode=true").response.body
    )
    assert body["object"] == "refund"
    assert re.fullmatch(r"re_[A-Za-z0-9]{24}", body["id"])
    assert body["created"] != 1
    assert "livemode" not in body  # the real refund object has none, so neither has ours


def test_livemode_is_set_where_the_object_has_it():
    """`livemode: false` is set only on objects whose real shape carries the field."""
    body = json.loads(
        answer_shipped(make_request("POST", path="/v1/customers/cus_REAL123")).response.body
    )
    assert body["livemode"] is False
    assert "livemode" not in json.loads(_faked_refund().response.body)


def test_l1_still_echoes_the_id_the_path_names_and_mints_the_rest():
    """A fixture answer echoes the id its path names and mints the rest, as the echo does."""
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
    """A client secret in the path never becomes a fixture answer's `id`."""
    body = json.loads(
        answer_shipped(
            make_request("POST", path="/v1/payment_intents/pi_ABC_secret_XYZ/cancel")
        ).response.body
    )
    assert body["id"] != "pi_ABC_secret_XYZ"
    assert re.fullmatch(r"pi_[A-Za-z0-9]{24}", body["id"])


def test_metadata_keeps_its_string_keys_and_string_values_at_l1():
    """A fixture answer's `metadata` is the parsed form value, string keys and values intact."""
    body = json.loads(_faked_refund(b"metadata[order_id]=6735&metadata[0]=zero").response.body)
    assert body["metadata"] == {"order_id": "6735", "0": "zero"}


def test_a_hostile_body_cannot_reach_the_fixtures_own_fields():
    body = json.loads(_faked_refund(("a" + "[b]" * 2000 + "=1").encode()).response.body)
    assert body["object"] == "refund"
    assert body["status"] == "succeeded"


def test_two_writes_do_not_share_one_fixture():
    """Two faked writes never share a fixture object, so one caller's fields never leak."""
    first = json.loads(_faked_refund(b"charge=ch_FIRST&amount=111").response.body)
    second = json.loads(_faked_refund(b"charge=ch_SECOND").response.body)
    assert first["charge"] == "ch_FIRST"
    assert second["charge"] == "ch_SECOND"
    fixture = generic.fixture_object("stripe", "refund")
    assert fixture is not None
    assert second["amount"] == fixture["amount"]


def test_a_route_whose_fixture_cannot_be_read_falls_back_to_l0_and_says_so(no_fixtures):
    """An unreadable fixture falls back to the echo, flagged `fixture-failed`."""
    ans = _faked_refund()
    assert ans.answered_by == "fake-L0"
    assert ans.flags == (Flag.FIXTURE_FAILED,)
    body = json.loads(ans.response.body)
    assert body["charge"] == "ch_test"  # the echo, which still parses
    assert body["id"].startswith("re_")
    assert "status" not in body


def test_an_unmapped_write_is_still_l0_with_no_flag():
    """A write to an unmapped host is an echo with no flag: it has no fixture to fail."""
    ans = answer_shipped(make_request("POST", host="example.invalid", path="/things", body=b"{}"))
    assert ans.answered_by == "fake-L0"
    assert ans.flags == ()


def test_every_shipped_fixture_route_names_an_object_that_ships():
    """Every shipped `fixture:` names an object that ships."""
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
    """stripe-python parses every fixture-faked object, every field included."""
    stripe = pytest.importorskip("stripe")
    ans = answer_shipped(make_request("POST", path=path, body=body, content_type=FORM_CONTENT_TYPE))
    assert ans.answered_by == "fake-L1"
    parsed = json.loads(ans.response.body)
    obj = getattr(stripe, cls_name).construct_from(parsed, STRIPE_TEST_X)
    assert obj.object == parsed["object"]
    # Compared through `str(obj)`, the SDK's JSON: its nested StripeObjects defeat a plain `==`.
    assert json.loads(str(obj)) == parsed
    # A field the caller never sent reads as an attribute, which an echo could not give.
    assert getattr(obj, fixture_field) == parsed[fixture_field]


# ------------------------------------------------------- the Slack envelope


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
    """A faked Slack post nests the fixture message under `message:` inside the envelope."""
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
    """A faked message's `ts` is the envelope's minted one, never the fixture's."""
    body = json.loads(_post_message().response.body)
    assert body["message"]["ts"] == body["ts"]
    fixture = generic.fixture_object("slack", "message")
    assert fixture is not None
    assert body["ts"] != fixture["ts"]


def test_a_slack_post_still_answers_the_envelope_when_the_fixture_cannot_be_read(no_fixtures):
    """An unreadable Slack fixture leaves the envelope alone, still `ok`, and flagged."""
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
    """A `ts` as the integer pair it sorts by; floats are too coarse at Slack's magnitude."""
    whole, _, fraction = value.partition(".")
    return int(whole), int(fraction)


def test_a_minted_ts_sorts_after_a_real_one_the_run_has_seen():
    """A minted `ts` sorts after every real one the run has read, so faked messages come last."""
    clock = services.TsClock()
    newest = "1999999999.000500"
    clock.observe_history(
        json.dumps({"ok": True, "messages": [{"ts": "1999999998.000000"}, {"ts": newest}]}).encode()
    )
    assert _ts(clock.mint()) > _ts(newest)


def test_an_older_real_ts_does_not_move_the_watermark_backwards():
    """Reading an old channel never rewinds the `ts` sequence, so no `ts` repeats."""
    clock = services.TsClock()
    clock.last = (1_999_999_999, 500)
    clock.observe_history(json.dumps({"messages": [{"ts": "1000000000.000000"}]}).encode())
    assert clock.mint() == "1999999999.000501"


def test_a_nested_ts_is_observed_wherever_it_sits():
    """A `ts` is observed wherever it sits, such as `conversations.info`'s `latest`."""
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
    """A body without a usable `ts` changes nothing and never raises (fail-closed hook rule)."""
    clock = services.TsClock()
    clock.last = (1_700_000_000, 7)
    clock.observe_history(body)
    assert clock.last == (1_700_000_000, 7)


def test_only_a_service_with_an_observer_learns_from_a_read():
    """Only a service whose spec has a read observer learns from a forwarded read."""
    clock = services.TsClock()
    body = json.dumps({"messages": [{"ts": "1999999999.000500"}]}).encode()
    services.observe_read("stripe", body, clock)
    assert clock.last == (0, 0)
    services.observe_read("slack", body, clock)
    assert clock.last == (1_999_999_999, 500)


def test_two_policies_never_share_a_ts_sequence():
    """Each policy owns its `ts` clock, so one engine's reads never move another's sequence."""
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
    """A Slack write with no envelope of its own gets `ok`, its channel and a minted `ts`."""
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
