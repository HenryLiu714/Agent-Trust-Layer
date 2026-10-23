"""`ShadowPolicy`'s decision: forward a read, fake a write, and what a faked write would fire."""

import json
from dataclasses import replace

import pytest

from irimi.core.exchange import Flag, Response
from irimi.decide.policy import ShadowPolicy
from irimi.servicemaps.classify import classify
from tests.support.decisions import (
    SHIPPED,
    answer,
    answer_shipped,
    answer_with,
    ask,
    canned_reader,
    charge_document,
    json_response,
    make_request,
    refund_request,
    shipped_with_targets,
)


@pytest.mark.parametrize("kind", ["read", "llm", "telemetry"])
def test_shadow_policy_forwards_live_kinds(kind):
    cls = replace(classify(make_request(), SHIPPED), kind=kind)
    ans = answer(ShadowPolicy(), make_request(), cls)
    assert ans.answered_by == "live"
    assert ans.response is None
    assert ans.flags == ()


@pytest.mark.parametrize("kind", ["write", "unknown"])
def test_shadow_policy_fakes_a_write_with_an_echo(kind):
    cls = replace(classify(make_request("POST"), SHIPPED), kind=kind)
    ans = answer(ShadowPolicy(), make_request("POST"), cls)
    assert ans.answered_by == "fake-L0"
    assert isinstance(ans.response, Response)
    assert ans.response.status == 200
    assert ("content-type", "application/json") in ans.response.headers
    assert ans.flags == ()
    assert isinstance(json.loads(ans.response.body), dict)


def test_shadow_policy_is_named_shadow_and_decides():
    p = ShadowPolicy()
    assert p.name == "shadow"
    assert callable(p.decide)


# ---------------------------------------------------- would-have-fired


def test_a_faked_refund_lists_the_webhooks_it_would_have_fired():
    """At the decision, a faked refund lists `refund.created` and `charge.refunded`, off its
    route's `fires:`."""
    ans = answer_shipped(make_request("POST", path="/v1/refunds"))
    assert ans.answered_by == "fake-L1"
    assert ans.would_fire == ("refund.created", "charge.refunded")


def test_a_faked_customer_update_lists_its_one_event():
    """Each route lists its own events, not the refund's."""
    ans = answer_shipped(make_request("POST", path="/v1/customers/cus_REAL123"))
    assert ans.would_fire == ("customer.updated",)


def test_a_write_on_an_unmapped_host_lists_nothing():
    """No route, no `fires:`, so no claim about what the service would have sent. It is still an
    accepted fake, so this is the `route is None` arm of the one line that sets it."""
    ans = answer_shipped(make_request("POST", host="example.invalid", path="/things", body=b"{}"))
    assert ans.answered_by == "fake-L0"
    assert ans.would_fire == ()


def test_a_live_read_lists_nothing():
    """A read is forwarded and performs nothing, so there is nothing that did not fire."""
    ans = answer_shipped(make_request())
    assert ans.answered_by == "live"
    assert ans.would_fire == ()


@pytest.mark.parametrize(
    ("reader", "outcome"),
    [
        (canned_reader(json_response(200, charge_document())), "passed"),
        (
            canned_reader(json_response(429, {"error": {"type": "rate_limit_error"}})),
            "not_evaluable",
        ),
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
        refund_request(),
        classify(refund_request(), SHIPPED),
    )
    assert ans.precondition == outcome
    assert ans.would_fire == ("refund.created", "charge.refunded")


def test_a_rejected_write_lists_no_webhooks():
    """L3 said the service would have refused this refund. Nothing was performed, so nothing
    would have fired."""
    policy = ShadowPolicy(
        reader=canned_reader(json_response(200, charge_document(refunded_so_far=4900))),
        maps=SHIPPED,
    )
    ans = ask(policy, refund_request())
    assert ans.precondition == "rejected"
    assert ans.rejection_code == "charge_already_refunded"
    assert ans.would_fire == ()


def test_a_replayed_write_lists_no_webhooks():
    """A replay is the first write's own answer. Its events are already on the first write's
    exchange; listing them again would promise `refund.created` twice for one refund."""
    policy = ShadowPolicy(reader=canned_reader(json_response(200, charge_document())), maps=SHIPPED)
    first = ask(policy, refund_request())
    second = ask(policy, refund_request())
    assert first.would_fire == ("refund.created", "charge.refunded")
    assert Flag.IDEMPOTENT_REPLAY in second.flags
    assert second.would_fire == ()


def test_an_idempotency_conflict_lists_no_webhooks():
    """The service would have refused the reused key, so nothing would have fired. A conflict is
    not `precondition: rejected`, so a check written against that alone would let it through.
    """
    policy = ShadowPolicy(reader=canned_reader(json_response(200, charge_document())), maps=SHIPPED)
    ask(policy, refund_request(b"charge=ch_REAL&amount=100"))
    ans = ask(policy, refund_request(b"charge=ch_REAL&amount=250"))
    assert Flag.IDEMPOTENCY_CONFLICT in ans.flags
    assert ans.rejection_code == "idempotency_error"
    assert ans.precondition is None
    assert ans.would_fire == ()


def test_a_delegated_write_lists_no_webhooks():
    """The target performed the write, or did not; irimi built nothing and claims nothing - the
    same reason `report._fidelity` refuses to print `L2` for one."""
    index = shipped_with_targets(
        [("api.stripe.com", "/v1/refunds", "http://127.0.0.1:3000/refund")]
    )
    ans = answer_with(index, make_request("POST", path="/v1/refunds"))
    assert ans.answered_by == "delegated"
    assert ans.would_fire == ()
