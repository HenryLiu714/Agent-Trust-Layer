"""W5 `dispute_responder`: a Stripe webhook consumer, pinned per scenario.

The labels and events are in `examples/workflows/w05_dispute_responder/agent.py`.
"""

import json

from examples.workflows.harness.services import client_secret
from irimi import redact
from irimi.exchange import Exchange

W = "w05_dispute_responder"


def actions(result):
    return [(e["type"], e["action"]) for e in result.events("event")]


READS = [
    ("dispute", 200, None),
    ("charge", 200, None),
    ("payment_intent", 200, None),
    ("customer", 200, None),
    ("llm", 200, None),
]


def test_a_large_dispute_is_tagged_alerted_and_fought_without_a_refund(run_workflow):
    shadow = run_workflow(W, "over_threshold", "shadow")
    assert shadow.answered() == [
        *READS,
        ("tag_customer", 200, "fake-L1"),
        ("slack.com/api/chat.postMessage", 200, "fake-L1"),
    ]
    assert actions(shadow) == [("charge.dispute.created", "fight")]
    # `/v1/disputes/{id}` is a route the Stripe map does not name: a GET on a mapped host with no
    # route falls back to the verb rule, so it is forwarded live as a read.
    assert "live      read      GET api.stripe.com/v1/disputes/dp_BIG -> 200" in (
        shadow.exchange_lines()
    )
    summary = shadow.summary()
    assert "  ○ update customer cus_DISPUTER  unvalidated (L1)" in summary
    assert "  These writes did not happen. Would have fired: customer.updated." in summary


def test_the_intents_client_secret_reaches_the_agent_and_only_its_placeholder_reaches_disk(
    run_workflow,
):
    """Reads are real, so the agent is handed the payment intent's real `client_secret`. The store
    keeps that live read's response with the secret swapped for its placeholder and nothing else
    changed (#69, #70), and invariant 4 finds the canary in no file under irimi's home. The one
    corpus response that carries a credential: every other canary is one the agent sends."""
    shadow = run_workflow(W, "over_threshold", "shadow")
    path = "/v1/payment_intents/pi_BIG"
    [live] = [ex for ex in shadow.reported if ex.request.path == path]
    assert live.answered_by == "live" and live.response is not None
    sent = json.loads(live.response.body)
    assert sent["client_secret"] == client_secret("pi_BIG")
    [stored] = [
        e for e in shadow.stored_events() if isinstance(e, Exchange) and e.request.path == path
    ]
    assert stored.response is not None
    hidden = redact.placeholder(redact.load_key(shadow.home), client_secret("pi_BIG"))
    assert json.loads(stored.response.body) == {**sent, "client_secret": hidden}


def test_a_small_dispute_is_refunded_and_the_refund_would_have_fired_two_events(run_workflow):
    shadow = run_workflow(W, "under_threshold", "shadow")
    bare = run_workflow(W, "under_threshold", "bare")
    assert shadow.answered()[-1] == ("refund", 200, "fake-L1")
    assert actions(shadow) == actions(bare) == [("charge.dispute.created", "refund")]
    assert [r.path for r in bare.internet.writes()] == [
        "/v1/customers/cus_DISPUTER",
        "/api/chat.postMessage",
        "/v1/refunds",
    ]
    summary = shadow.summary()
    assert (
        "  These writes did not happen. Would have fired: customer.updated, refund.created, "
        "charge.refunded." in summary
    )
    # LOOKS WRONG: a refund with no `amount` refunds the whole charge. Stripe (and the bare run)
    # answer 1500, but irimi's L1 refund carries its fixture's amount, 100, although its own L3
    # read found the charge and its 1500 unrefunded. The summary cannot say how much either.
    assert [e["amount"] for e in bare.events("refund")] == [1500]
    assert [e["amount"] for e in shadow.events("refund")] == [100]
    assert "  ○ refund ? on ch_SMALL  unvalidated (L3 preconditions passed)" in summary


def test_the_refunds_own_charge_refunded_event_is_a_second_run_with_no_calls(run_workflow):
    for mode in ("bare", "shadow"):
        result = run_workflow(W, "cascade", mode)
        assert actions(result) == [
            ("charge.dispute.created", "refund"),
            ("charge.refunded", "noted"),
        ]
        assert [a["outcome"] for a in result.result()["answers"]] == ["refund", "noted"]
        # Everything the agent called, it called while handling the dispute.
        assert len(result.calls()) == 8
    shadow = run_workflow(W, "cascade", "shadow")
    runs = shadow.events("run.start")
    assert [r["name"] for r in runs] == ["on_event", "on_event"]
    assert {c["run"] for c in shadow.calls()} == {runs[0]["run"]}
    # The second run is built from irimi's minted refund: in production this event would never
    # arrive for a shadowed refund. Phase 4's would-have-fired list is where it shows instead.
    refund_id = shadow.events("refund")[0]["id"]
    assert shadow.events("event")[1]["id"] == f"evt_refunded_{refund_id}"
    # The event fed back is one irimi says the refund would have fired (#47).
    assert shadow.summary()[-1] == (
        "  These writes did not happen. Would have fired: customer.updated, refund.created, "
        "charge.refunded."
    )


def test_a_forged_signature_is_refused_before_any_call(run_workflow):
    for mode in ("bare", "shadow"):
        result = run_workflow(W, "bad_signature", mode)
        assert result.result()["answers"] == [{"status": 400}]
        assert [e["reason"] for e in result.events("rejected")] == ["bad signature"]
        assert result.calls() == []
        assert result.internet.requests() == []
        assert result.events("run.start") == []


def test_a_replayed_event_id_is_handled_once(run_workflow):
    for mode in ("bare", "shadow"):
        result = run_workflow(W, "replayed_event", mode)
        assert actions(result) == [
            ("charge.dispute.created", "refund"),
            ("charge.dispute.created", "already handled"),
        ]
        assert len([c for c in result.calls() if c["label"] == "refund"]) == 1
    bare = run_workflow(W, "replayed_event", "bare")
    assert len(bare.internet.writes()) == 3
    shadow = run_workflow(W, "replayed_event", "shadow")
    assert [line.split()[0] for line in shadow.exchange_lines() if " write " in line] == [
        "fake-L1"
    ] * 3
