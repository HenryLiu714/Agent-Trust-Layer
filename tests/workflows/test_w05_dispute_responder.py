"""W5 `dispute_responder`: a Stripe webhook consumer, pinned per scenario.

The labels and events are in `examples/workflows/w05_dispute_responder/agent.py`.
"""

import base64
import json

import pytest

from examples.workflows.harness.run import check_invariants, compare, rows_not_shown
from examples.workflows.harness.services import client_secret
from irimi import redact
from irimi.exchange import Exchange
from irimi.report.model import run_elapsed

W = "w05_dispute_responder"
AGENT = "examples.workflows.w05_dispute_responder.agent"
# Each test runs under `irimi shadow -- <cmd>` and under serve mode, its agent beside irimi.
MODES = ("shadow", "serve")


def actions(result):
    return [(e["type"], e["action"]) for e in result.events("event")]


READS = [
    ("dispute", 200, None),
    ("charge", 200, None),
    ("payment_intent", 200, None),
    ("customer", 200, None),
    ("llm", 200, None),
]


@pytest.mark.parametrize("mode", MODES)
def test_a_large_dispute_is_tagged_alerted_and_fought_without_a_refund(run_workflow, mode):
    shadow = run_workflow(W, "over_threshold", mode)
    assert shadow.answered() == [
        *READS,
        ("tag_customer", 200, "fake-L1"),
        ("slack.com/api/chat.postMessage", 200, "fake-L1"),
    ]
    assert actions(shadow) == [("charge.dispute.created", "fight")]
    # `/v1/disputes/{id}` is a route the Stripe map does not name: a GET on a mapped host with no
    # route falls back to the verb rule, so it is forwarded live as a read.
    assert "real          read      GET api.stripe.com/v1/disputes/dp_BIG -> 200" in (
        shadow.exchange_lines()
    )
    summary = shadow.summaries()
    assert "  ○ stripe  update customer cus_DISPUTER  faked; not checked" in summary
    assert "  would have fired: customer.updated" in summary


@pytest.mark.parametrize("mode", MODES)
def test_the_intents_client_secret_reaches_the_agent_and_only_its_placeholder_reaches_disk(
    run_workflow,
    mode,
):
    """Reads are real, so the agent is handed the payment intent's real `client_secret`. The store
    keeps that live read's response with the secret swapped for its placeholder and nothing else
    changed, and invariant 4 finds the canary in no file under irimi's home. The one
    corpus response that carries a credential: every other canary is one the agent sends."""
    shadow = run_workflow(W, "over_threshold", mode)
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


@pytest.mark.parametrize("mode", MODES)
def test_a_small_dispute_is_refunded_and_the_refund_would_have_fired_two_events(run_workflow, mode):
    shadow = run_workflow(W, "under_threshold", mode)
    bare = run_workflow(W, "under_threshold", "bare")
    assert shadow.answered()[-1] == ("refund", 200, "fake-L1")
    assert actions(shadow) == actions(bare) == [("charge.dispute.created", "refund")]
    assert [r.path for r in bare.internet.writes()] == [
        "/v1/customers/cus_DISPUTER",
        "/api/chat.postMessage",
        "/v1/refunds",
    ]
    summary = shadow.summaries()
    assert "  would have fired: customer.updated, refund.created, charge.refunded" in summary
    # LOOKS WRONG: a refund with no `amount` refunds the whole charge. Stripe (and the bare run)
    # answer 1500, but irimi's L1 refund carries its fixture's amount, 100, although its own L3
    # read found the charge and its 1500 unrefunded. The summary cannot say how much either.
    assert [e["amount"] for e in bare.events("refund")] == [1500]
    assert [e["amount"] for e in shadow.events("refund")] == [100]
    assert (
        "  ○ stripe  refund ? on ch_SMALL  faked; checked against live state: would succeed"
        in summary
    )


def test_the_refunds_own_charge_refunded_event_is_a_second_run_with_no_calls(run_workflow):
    for mode in ("bare", "shadow", "serve"):
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
    assert [r["name"] for r in runs] == [f"{AGENT}:on_event"] * 2
    assert {c["run"] for c in shadow.calls()} == {runs[0]["run"]}
    # The second run is built from irimi's minted refund: in production this event would never
    # arrive for a shadowed refund. The summary's would-have-fired list is where it shows instead.
    refund_id = shadow.events("refund")[0]["id"]
    assert shadow.events("event")[1]["id"] == f"evt_refunded_{refund_id}"
    # The event fed back is one irimi says the refund would have fired.
    assert "  would have fired: customer.updated, refund.created, charge.refunded" in (
        shadow.summary()
    )


def test_under_serve_the_cascade_is_two_runs_and_two_summaries_the_second_empty(run_workflow):
    """Under `irimi serve` each delivery is its own run, printed when it ends: the
    dispute's run with its three faked writes and what they would have fired, and the fed-back
    `charge.refunded`'s run with no calls at all."""
    serve = run_workflow(W, "cascade", "serve")
    first, second = (r["run"] for r in serve.events("run.start"))
    assert {c["run"] for c in serve.calls()} == {first}
    blocks = serve.run_blocks()
    assert set(blocks) == {first, second}
    assert (
        "  would have fired: customer.updated, refund.created, charge.refunded" in (blocks[first])
    )
    assert len([line for line in blocks[first] if line.startswith("  ○ ")]) == 3
    assert blocks[second] == [
        f"irimi shadow · run {second} · 0 exchanges · {_elapsed(serve, second)}",
        "",
        "  0 exchanges · 0 real · 0 answered by irimi (0 faked writes) · 0 sent to targets",
    ]


def _elapsed(result, run_id):
    [record] = [r for r in result.stored().list_runs() if r.run_id == run_id]
    return f"{run_elapsed(record) or 0.0:.1f}s"


def test_a_forged_signature_is_refused_before_any_call(run_workflow):
    for mode in ("bare", "shadow", "serve"):
        result = run_workflow(W, "bad_signature", mode)
        assert result.result()["answers"] == [{"status": 400}]
        assert [e["reason"] for e in result.events("rejected")] == ["bad signature"]
        assert result.calls() == []
        assert result.internet.requests() == []
        assert result.events("run.start") == []


def test_a_replayed_event_id_is_handled_once(run_workflow):
    for mode in ("bare", "shadow", "serve"):
        result = run_workflow(W, "replayed_event", mode)
        assert actions(result) == [
            ("charge.dispute.created", "refund"),
            ("charge.dispute.created", "already handled"),
        ]
        assert len([c for c in result.calls() if c["label"] == "refund"]) == 1
    bare = run_workflow(W, "replayed_event", "bare")
    assert len(bare.internet.writes()) == 3
    for mode in MODES:
        shadow = run_workflow(W, "replayed_event", mode)
        writes = [line.split()[0] for line in shadow.exchange_lines() if " write " in line]
        assert writes == ["faked"] * 3


@pytest.mark.parametrize("mode", MODES)
def test_the_raw_webhook_body_is_stored_as_the_triggers_bytes_and_is_replayable(run_workflow, mode):
    """A `bytes` argument is captured whole: `on_event(raw, signature)` stores the body it verified
    as `__irimi_bytes__`, which replay can hand back byte for byte."""
    shadow = run_workflow(W, "over_threshold", mode)
    [run] = shadow.sdk_runs()
    assert run.trigger is not None and run.trigger.replayable
    assert run.trigger.entrypoint == "examples.workflows.w05_dispute_responder.agent:on_event"
    args = run.trigger.args
    assert isinstance(args, dict) and set(args) == {"raw", "signature"}
    raw = args["raw"]
    assert isinstance(raw, dict) and set(raw) == {"__irimi_bytes__"}
    event = json.loads(base64.b64decode(str(raw["__irimi_bytes__"])))
    assert (event["id"], event["type"]) == ("evt_dp_BIG", "charge.dispute.created")


# ---------------------------------------------------------------------- irimi compare


@pytest.mark.parametrize("mode", MODES)
def test_irimi_compare_cannot_tell_two_refunds_of_the_rest_of_a_charge_apart(
    shared_store_run, run_workflow, mode
):
    """`under_threshold` and `partly_refunded` deliver the same dispute event, and the agent
    refunds what is left of `ch_SMALL`: all 1500 of it, or the 1000 left after an earlier refund
    of 500, as the bare runs' refunds show. Recorded into one store, B under shadow or serve.

    LOOKS WRONG (the finding pinned above, #79's "Since #89 landed"): a refund with no `amount`
    prints `refund ? on ch_SMALL`, so compare calls the two refunds the same and exits 0, though
    irimi's own L3 read of each charge knew how much was left."""
    full = shared_store_run(W, "under_threshold")
    rest = shared_store_run(W, "partly_refunded", mode)
    assert check_invariants(rest) == []
    assert [
        [e["amount"] for e in run_workflow(W, scenario, "bare").events("refund")]
        for scenario in ("under_threshold", "partly_refunded")
    ] == [[1500], [1000]]
    code, out, err = compare(full, rest)
    a, b = (result.one("run.start")["run"] for result in (full, rest))
    on_event = "examples.workflows.w05_dispute_responder.agent:on_event"
    # The same event's bytes, and a signature that names the second it was signed in: the args
    # differ only when the two deliveries were signed in different seconds.
    triggers = [rest.stored().load_run(run_id).record.trigger for run_id in (a, b)]
    args = [trigger.args for trigger in triggers if trigger is not None]
    assert [arg["raw"] for arg in args if isinstance(arg, dict)] == [args[0]["raw"]] * 2
    differ = ["  (the triggers' args differ)"] if args[0] != args[1] else []
    assert (code, err) == (0, [])
    assert out == [
        f"irimi compare · {a} vs {b}",
        f"  A  {on_event}",
        f"  B  {on_event}",
        *differ,
        "",
        "  = ○ stripe  update customer cus_DISPUTER  faked; not checked",
        '  = ○ slack  post to #C0DISPUTES: "dispute dp_SMALL on ch_SMALL (1500): refund"'
        "  faked; checked against live state: would succeed",
        "  = ○ stripe  refund ? on ch_SMALL  faked; checked against live state: would succeed",
        "",
        "  3 same · 0 changed · 0 only in A · 0 only in B",
    ]
    assert rows_not_shown(rest, a, b, out) == []


# ------------------------------------------------------------------------------- replay


def test_the_raw_webhook_body_is_revived_as_bytes_and_the_trigger_replays(replayed):
    """`on_event(raw, signature)` is given the webhook's raw body, which the SDK captures as
    `__irimi_bytes__`, and `irimi replay` revives as the same bytes (`capture.revive`): the
    trigger, called alone, verifies the same signature over them, handles the same dispute, and is
    answered from its recording, so nothing reaches the fake internet and the writes are the
    same."""
    recorded, replay = replayed(W, "under_threshold")
    [run] = recorded.sdk_runs()
    assert run.trigger is not None and run.trigger.replayable
    assert set(run.trigger.args["raw"]) == {"__irimi_bytes__"}
    assert base64.b64decode(run.trigger.args["raw"]["__irimi_bytes__"]).startswith(b"{")
    assert (replay.exit_code, replay.agent_exit_code()) == (0, 0)
    # The trigger was called with `bytes` again, as when it was recorded, and did what it did.
    called_with = {"raw": "bytes", "signature": "str"}
    assert replay.one("run.start")["called_with"] == called_with
    assert recorded.one("run.start")["called_with"] == called_with
    assert replay.returned() == recorded.returned() == "refund"
    assert replay.replay_result()[1:3] == [
        "  agent exited 0",
        "  no divergence: every call was answered from the recording",
    ]
    assert replay.internet.requests() == []
