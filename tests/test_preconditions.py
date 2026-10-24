"""`ShadowPolicy`'s precondition checks: irimi's check read, and the verdict on its answer."""

import json
from dataclasses import replace

import pytest

from irimi import services
from irimi.core.exchange import Exchange, Flag, Response
from irimi.decide.policy import ShadowPolicy
from irimi.servicemaps.classify import classify
from irimi.services import generic
from tests.support.credentials import STRIPE_TEST_X
from tests.support.decisions import (
    AGENT_HEADERS,
    SHIPPED,
    answer,
    canned_reader,
    charge_document,
    checked,
    json_response,
    make_request,
    refund_request,
    shipped_with_targets,
    slack_post_request,
)


def test_a_refund_of_a_fully_refunded_charge_is_answered_with_stripes_own_refusal():
    reader = canned_reader(json_response(200, charge_document(refunded_so_far=4900)))
    ans = checked(reader, refund_request())
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
    """stripe-python raises `InvalidRequestError` on irimi's modelled refusal.

    The SDK's own error factory is used, so a change in how stripe-python classifies a 400 fails
    here. Skipped without `stripe`."""
    pytest.importorskip("stripe")
    import stripe
    from stripe._api_requestor import _APIRequestor

    ans = checked(
        canned_reader(json_response(200, charge_document(refunded_so_far=4900))), refund_request()
    )
    raw = ans.response.body.decode()
    error = json.loads(raw)["error"]
    raised = _APIRequestor().specific_v1_api_error(raw, ans.response.status, raw, {}, error)
    assert isinstance(raised, stripe.InvalidRequestError)
    assert raised.code == "charge_already_refunded"
    assert raised.http_status == 400


def test_a_policy_with_no_reader_does_not_do_l3_at_all():
    """A policy with no reader makes no check: the refund is its fixture and the outcome None."""
    request = refund_request()
    ans = answer(ShadowPolicy(), request, classify(request, SHIPPED))
    assert ans.precondition is None
    assert ans.issued == ()
    assert ans.answered_by == "fake-L1"
    assert ans.response.status == 200
    assert json.loads(ans.response.body)["object"] == "refund"


def test_asked_and_could_not_tell_is_not_the_same_as_never_asked():
    """An unusable check answer is `not_evaluable`; a policy that never checked is None."""
    request = refund_request()
    asked = checked(canned_reader(json_response(200, ["not", "an", "object"])), request)
    assert asked.precondition == "not_evaluable"
    assert asked.answered_by == "fake-L1"  # still faked, at its ordinary level
    never = answer(ShadowPolicy(), request, classify(request, SHIPPED))
    assert never.precondition is None


def test_a_precondition_read_that_429s_is_not_evaluable_and_is_still_recorded():
    ans = checked(
        canned_reader(json_response(429, {"error": {"type": "rate_limit_error"}})), refund_request()
    )
    assert ans.precondition == "not_evaluable"
    assert ans.answered_by == "fake-L1"
    (read,) = ans.issued
    assert read.response.status == 429
    assert read.issued_by == "engine"
    assert (read.kind, read.answered_by, read.operation) == ("read", "live", "charges.retrieve")
    assert read.run_id == "t3st"


def test_a_passed_check_carries_the_charges_currency_onto_the_answer():
    """A passed check takes the currency from the charge irimi read; the refund names none."""
    ans = checked(canned_reader(json_response(200, charge_document())), refund_request())
    assert ans.precondition == "passed"
    assert ans.currency == "usd"


def test_a_rejected_check_carries_it_too():
    """A rejected check carries the currency too, since the `✗` line prints an amount."""
    ans = checked(
        canned_reader(json_response(200, charge_document(refunded_so_far=4900))), refund_request()
    )
    assert ans.precondition == "rejected"
    assert ans.currency == "usd"


def test_a_policy_with_no_reader_offers_no_currency():
    """A policy that never checked claims no currency."""
    request = refund_request()
    ans = answer(ShadowPolicy(), request, classify(request, SHIPPED))
    assert ans.currency == ""


@pytest.mark.parametrize(
    "answer",
    [
        json_response(429, {"error": {"type": "rate_limit_error"}}),
        json_response(404, {"error": {"type": "invalid_request_error"}}),
        Response(200, (), b"<html>not json</html>"),
        None,
    ],
)
def test_a_read_that_never_produced_a_document_offers_no_currency(answer):
    """A check read that yields no document offers no currency."""
    ans = checked(canned_reader(answer), refund_request())
    assert ans.precondition == "not_evaluable"
    assert ans.currency == ""


def test_a_verdict_that_could_not_tell_still_knows_what_the_charge_is_in():
    """A check that could not decide still carries the currency of the charge it read."""
    charge = charge_document()
    charge["amount_refunded"] = "not a number"  # `_refund_verdict` -> NOT_EVALUABLE
    ans = checked(canned_reader(json_response(200, charge)), refund_request())
    assert ans.precondition == "not_evaluable"
    assert ans.currency == "usd"


@pytest.mark.parametrize(
    "answer",
    [
        RuntimeError("the reader raised"),
        None,
        Response(200, (), b"<html>not json</html>"),
        json_response(200, ["not", "an", "object"]),
        json_response(404, {"error": {"type": "invalid_request_error"}}),
        json_response(500, {"error": {"type": "api_error"}}),
        json_response(503, {}),
    ],
    ids=["raises", "none", "not-json", "not-an-object", "404", "500", "503"],
)
def test_a_reader_that_cannot_tell_leaves_the_write_faked_and_not_evaluable(answer):
    """Only a 200 with a JSON object is evaluable; otherwise the write is faked as usual."""
    ans = checked(canned_reader(answer), refund_request())
    assert ans.precondition == "not_evaluable"
    assert ans.answered_by == "fake-L1"
    assert ans.response.status == 200
    assert json.loads(ans.response.body)["object"] == "refund"
    # An attempted read is recorded whatever came of it, with no response when it got none.
    (read,) = ans.issued
    assert (read.issued_by, read.kind, read.answered_by) == ("engine", "read", "live")
    expected = answer if isinstance(answer, Response) else None
    assert read.response == expected


def test_the_runs_own_earlier_refund_is_what_makes_the_second_one_rejected():
    """The run's own earlier faked refund makes the precondition check reject the second.

    The live charge says `refunded: false` both times; applying the first refund turns the second
    into Stripe's refusal."""
    request = refund_request()
    cls = classify(request, SHIPPED)
    first = checked(canned_reader(json_response(200, charge_document())), request)
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

    reader = canned_reader(json_response(200, charge_document()))
    second = checked(reader, request, write_log=log)
    assert second.precondition == "rejected"
    assert second.rejection_code == "charge_already_refunded"
    assert second.response.status == 400
    # Without the log the same live charge passes, so the log is the cause.
    assert (
        checked(canned_reader(json_response(200, charge_document())), request).precondition
        == "passed"
    )


def test_an_overlay_that_knows_it_is_incomplete_makes_the_check_not_evaluable(monkeypatch):
    """A check against state irimi knows is incomplete is `not_evaluable`."""
    stripe = services.REGISTRY["stripe"]
    partial = replace(stripe, effects=lambda read, doc, writes: services.Applied(doc, partial=True))
    monkeypatch.setitem(services.REGISTRY, "stripe", partial)
    request = refund_request()
    cls = classify(request, SHIPPED)
    earlier = checked(canned_reader(json_response(200, charge_document())), request)
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
    ans = checked(
        canned_reader(json_response(200, charge_document(refunded_so_far=4900))),
        request,
        write_log=log,
    )
    assert ans.precondition == "not_evaluable"
    assert ans.response.status == 200


def test_the_probe_carries_the_writes_credentials_and_scope_and_nothing_else():
    """irimi's check read carries the write's credentials and scope headers and nothing else.

    Copying the rest would make a request the agent never made, such as a keyed GET."""
    reader = canned_reader(json_response(200, charge_document()))
    checked(reader, refund_request())
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
    reader = canned_reader()
    request = replace(make_request("POST", path="/v1/customers/cus_REAL123"), headers=AGENT_HEADERS)
    ans = checked(reader, request)
    assert ans.precondition is None
    assert ans.issued == ()
    assert reader.asked == []


def test_a_delegated_write_on_a_checked_route_is_not_evaluable_and_issues_no_read():
    index = shipped_with_targets(
        [("api.stripe.com", "/v1/refunds", "http://127.0.0.1:3000/refund")]
    )
    reader = canned_reader()
    ans = checked(reader, refund_request(), index=index)
    assert ans.answered_by == "delegated"
    assert ans.precondition == "not_evaluable"
    assert reader.asked == []


def test_a_probe_the_maps_do_not_call_a_read_is_refused(monkeypatch):
    """A check read the maps do not class as a read is refused, whatever its verb."""
    check = services.Check(
        probe=lambda proposal: services.Probe("refunds.create", "POST", "/v1/refunds"),  # type: ignore[arg-type]
        verdict=lambda proposal, document: None,
    )
    stripe = replace(services.REGISTRY["stripe"], preconditions={"charge_refundable": check})
    monkeypatch.setitem(services.REGISTRY, "stripe", stripe)
    reader = canned_reader()
    ans = checked(reader, refund_request())
    assert ans.precondition == "not_evaluable"
    assert reader.asked == []


@pytest.mark.parametrize("channel", ["#general", "general"])
def test_a_slack_post_to_a_channel_name_is_not_probed(channel):
    """A Slack post to a channel name is not checked: `conversations.info` only knows ids."""
    reader = canned_reader()
    ans = checked(reader, slack_post_request(channel))
    assert ans.precondition == "not_evaluable"
    assert ans.answered_by == "fake-L1"
    assert reader.asked == []


def test_a_slack_post_to_a_channel_id_is_probed_with_conversations_info():
    healthy = {"ok": True, "channel": {"id": "C0123", "is_channel": True, "is_member": True}}
    reader = canned_reader(json_response(200, healthy))
    ans = checked(reader, slack_post_request("C0123"))
    assert ans.precondition == "passed"
    (probe,) = reader.asked
    assert (probe.method, probe.path) == ("POST", "/api/conversations.info")
    assert json.loads(probe.body) == {"channel": "C0123"}
    assert dict(probe.headers)["content-type"] == "application/json; charset=utf-8"
    assert ans.issued[0].operation == "conversations.info"


@pytest.mark.parametrize("error", ["missing_scope", "invalid_auth", "ratelimited"])
def test_a_slack_error_irimi_cannot_read_is_not_evaluable_and_the_post_is_still_faked(error):
    """A Slack error answered at HTTP 200 is `not_evaluable`, not `passed`."""
    reader = canned_reader(json_response(200, {"ok": False, "error": error}))
    ans = checked(reader, slack_post_request("C0123"))
    assert ans.precondition == "not_evaluable"
    assert ans.answered_by == "fake-L1", "the write is still faked at its own level"
    assert ans.rejection_code == ""
    assert len(reader.asked) == 1, "asked once, and not retried"
    assert ans.issued[0].issued_by == "engine"


def test_a_charge_the_check_could_not_read_is_not_evaluable_rather_than_passed():
    """A 200 that is not a charge irimi can read is `not_evaluable`, not `passed`."""
    reader = canned_reader(json_response(200, {"object": "list", "data": []}))
    ans = checked(reader, refund_request())
    assert ans.precondition == "not_evaluable"
    assert ans.answered_by == "fake-L1"


def test_a_slack_post_to_an_archived_channel_is_refused_the_way_slack_refuses_it():
    reader = canned_reader(
        json_response(200, {"ok": True, "channel": {"id": "C0123", "is_archived": True}})
    )
    ans = checked(reader, slack_post_request("C0123"))
    assert ans.precondition == "rejected"
    assert ans.rejection_code == "is_archived"
    assert ans.response.status == 200
    assert json.loads(ans.response.body) == {"ok": False, "error": "is_archived"}
    assert ans.answered_by == "fake-L1"


def test_a_rejection_carries_the_level_the_refund_would_have_been_answered_at():
    fake = services.fake_rejection(classify(refund_request(), SHIPPED), {"error": {}})
    assert (fake.answered_by, fake.flags) == ("fake-L1", ())
    assert json.loads(fake.body) == {"error": {}}


def test_a_rejection_of_a_write_with_no_fixture_is_honestly_l0():
    """A rejection of a write with no fixture is an echo; there is no `fake-L3`."""
    request = make_request("POST", host="slack.com", path="/api/reactions.add", body=b"{}")
    fake = services.fake_rejection(classify(request, SHIPPED), {"ok": False, "error": "x"})
    assert (fake.answered_by, fake.flags) == ("fake-L0", ())


def test_a_rejection_whose_fixture_will_not_load_says_so(monkeypatch):
    monkeypatch.setattr(generic, "fixture_object", lambda service, name: None)
    fake = services.fake_rejection(classify(refund_request(), SHIPPED), {"error": {}})
    assert (fake.answered_by, fake.flags) == ("fake-L0", (Flag.FIXTURE_FAILED,))
