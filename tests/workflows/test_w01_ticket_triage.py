"""W1 `ticket_triage`: a multi-turn LLM tool loop, pinned per prompt variant.

The labels are the calls in `examples/workflows/w01_ticket_triage/agent.py`; `#N` is the turn.
"""

from examples.workflows.w01_ticket_triage.agent import DB_NAME

W = "w01_ticket_triage"
AGENT = "examples.workflows.w01_ticket_triage.agent"
REFUND = "issue_refund#2"


def replies(result):
    return result.query(DB_NAME, "SELECT ticket_id, body FROM replies")


def test_a_full_refund_walks_the_same_loop_as_bare_and_neither_write_happens(run_workflow):
    bare = run_workflow(W, "full_refund", "bare")
    shadow = run_workflow(W, "full_refund", "shadow")
    # The agent cannot tell the two runs apart: same turns, same statuses, same final answer.
    assert shadow.result(with_time=False) == bare.result(with_time=False)
    assert shadow.result()["refund_statuses"] == [200]
    assert shadow.by_label()[REFUND] == (200, "fake-L1")
    # The HTTP write: real bare, faked under shadow.
    assert len(bare.world.stripe.refunds) == 1
    assert shadow.world.stripe.refunds == []
    assert shadow.world.stripe.charges["ch_TICKET1"]["amount_refunded"] == 0
    # The tool write: its real body ran bare and only its stand-in under shadow.
    assert bare.tools() == [(f"{AGENT}.lookup_customer", "real"), (f"{AGENT}.send_reply", "real")]
    assert shadow.tools() == [
        (f"{AGENT}.lookup_customer", "real"),
        (f"{AGENT}.send_reply", "shadow"),
    ]
    assert replies(bare) == [("T-1001", "Refund of 4900 requested (statuses: 200).")]
    assert replies(shadow) == []
    assert "  ○ refund $49.00 on ch_TICKET1  unvalidated (L3 preconditions passed)" in (
        shadow.summary()
    )
    # One run, attributed by the SDK stand-in, ended ok.
    assert [e["outcome"] for e in shadow.events("run.end")] == ["ok"]


def test_the_half_prompt_changes_only_the_refund(run_workflow):
    bare = run_workflow(W, "half_refund", "bare")
    shadow = run_workflow(W, "half_refund", "shadow")
    assert bare.world.stripe.refunds[0]["amount"] == 2450
    assert "  ○ refund $24.50 on ch_TICKET1  unvalidated (L3 preconditions passed)" in (
        shadow.summary()
    )
    # The loop has the same shape as the full refund's: the prompt moved one argument.
    full = run_workflow(W, "full_refund", "shadow")
    assert [c.get("label") for c in shadow.calls()] == [c.get("label") for c in full.calls()]
    assert len(shadow.world.llm.calls) == len(full.world.llm.calls) == 4


def test_a_refund_larger_than_the_charge_is_refused_the_way_stripe_refuses_it(run_workflow):
    bare = run_workflow(W, "too_large", "bare")
    shadow = run_workflow(W, "too_large", "shadow")
    assert bare.result()["refund_statuses"] == shadow.result()["refund_statuses"] == [400]
    assert shadow.by_label()[REFUND] == (400, "fake-L1")
    summary = shadow.summary()
    assert "  ✗ refund $98.00 on ch_TICKET1  would fail: amount_too_large" in summary
    # A write L3 rejected fires nothing.
    assert "  These writes did not happen." in summary


def test_a_second_refund_is_refused_because_the_overlay_shows_the_first(run_workflow):
    bare = run_workflow(W, "double_refund", "bare")
    shadow = run_workflow(W, "double_refund", "shadow")
    assert bare.result()["refund_statuses"] == shadow.result()["refund_statuses"] == [200, 400]
    assert shadow.by_label()["issue_refund#3"] == (400, "fake-L1")
    summary = shadow.summary()
    assert "  ○ refund $49.00 on ch_TICKET1  unvalidated (L3 preconditions passed)" in summary
    assert "  ✗ refund $49.00 on ch_TICKET1  would fail: charge_already_refunded" in summary
    # Each refund's L3 read went to Stripe; the second saw the first only through the overlay.
    reads = shadow.internet.requests("api.stripe.com")
    assert [(r.method, r.path) for r in reads] == [("GET", "/v1/charges/ch_TICKET1")] * 3


def test_a_refund_the_model_repeats_with_the_same_key_is_one_write(run_workflow):
    bare = run_workflow(W, "retried_refund", "bare")
    shadow = run_workflow(W, "retried_refund", "shadow")
    assert bare.result()["refund_statuses"] == shadow.result()["refund_statuses"] == [200, 200]
    # Both sends carry `T-1001-turn2`. Stripe refunds once and answers the repeat with the same
    # refund, marked `Idempotent-Replayed`; irimi's idempotency store does the same without a
    # second L3 read (#46).
    assert len(bare.world.stripe.refunds) == 1
    for result in (bare, shadow):
        first, again = result.events("refund")
        assert first["id"] == again["id"]
        assert (first["replayed"], again["replayed"]) == (None, "true")
    refund = "fake-L1   write     POST api.stripe.com/v1/refunds -> 200"
    assert [line for line in shadow.exchange_lines() if "/v1/refunds" in line] == [
        f"{refund}  [fidelity:L1]",
        f"{refund}  [fidelity:L1, idempotent-replay]",
    ]
    summary = shadow.summary()
    assert "  api.stripe.com     1 read  1 engine read  1 write intercepted" in summary
    assert [line for line in summary if "refund $" in line] == [
        "  ○ refund $49.00 on ch_TICKET1  unvalidated (L3 preconditions passed)"
    ]


def test_a_model_that_never_stops_is_cut_off_at_max_turns_in_both_modes(run_workflow):
    bare = run_workflow(W, "runaway_loop", "bare")
    shadow = run_workflow(W, "runaway_loop", "shadow")
    assert bare.exit_code == shadow.exit_code == 1
    assert len(bare.world.llm.calls) == len(shadow.world.llm.calls) == 6
    assert shadow.result()["error"] == "RuntimeError: ticket T-1001: no answer after 6 turns"
    assert [(e["outcome"], e["error"]) for e in shadow.events("run.end")] == [
        ("error", "RuntimeError")
    ]
    assert replies(shadow) == replies(bare) == []
