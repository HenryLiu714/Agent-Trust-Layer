"""W5 `dispute_responder`: a Stripe webhook consumer, pinned per scenario."""

import base64
import json

import pytest

from irimi.core.exchange import Exchange
from irimi.record import redact
from irimi.report.model import run_elapsed
from tests.e2e.workflows.harness.compare import compare, rows_not_shown
from tests.e2e.workflows.harness.invariants import check_invariants
from tests.e2e.workflows.harness.services import client_secret

W = "w05_dispute_responder"
AGENT = "tests.e2e.workflows.agents.w05_dispute_responder.agent"
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
    # The Stripe map names no `/v1/disputes/{id}` route, so the verb rule forwards the GET live.
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
    """The agent gets the live `client_secret`; the store keeps it only as its placeholder.

    This is the corpus's one response that carries a credential."""
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
    # KNOWN BUG: a refund with no `amount` is faked with its fixture's 100, where Stripe refunds
    # the whole 1500 irimi's check read found unrefunded, and the summary says neither.
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
    # The second run is built from irimi's minted refund. In production this event never arrives
    # for a faked refund; the summary's would-have-fired list names it instead.
    refund_id = shadow.events("refund")[0]["id"]
    assert shadow.events("event")[1]["id"] == f"evt_refunded_{refund_id}"
    assert "  would have fired: customer.updated, refund.created, charge.refunded" in (
        shadow.summary()
    )


def test_under_serve_the_cascade_is_two_runs_and_two_summaries_the_second_empty(run_workflow):
    """Under serve each delivery is a run: the dispute's with three faked writes, the next empty."""
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
    """A `bytes` trigger argument is stored whole as `__irimi_bytes__`, so the run is replayable."""
    shadow = run_workflow(W, "over_threshold", mode)
    [run] = shadow.sdk_runs()
    assert run.trigger is not None and run.trigger.replayable
    assert (
        run.trigger.entrypoint == "tests.e2e.workflows.agents.w05_dispute_responder.agent:on_event"
    )
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
    """Compare calls refunds of 1500 and of the 1000 left on `ch_SMALL` the same, and exits 0.

    `under_threshold` and `partly_refunded` deliver one dispute; the agent refunds what is left.
    KNOWN BUG: a refund with no `amount` prints `refund ? on ch_SMALL`, though irimi's check read
    of each charge knew how much was left."""
    full = shared_store_run(W, "under_threshold")
    rest = shared_store_run(W, "partly_refunded", mode)
    assert check_invariants(rest) == []
    assert [
        [e["amount"] for e in run_workflow(W, scenario, "bare").events("refund")]
        for scenario in ("under_threshold", "partly_refunded")
    ] == [[1500], [1000]]
    code, out, err = compare(full, rest)
    a, b = (result.one("run.start")["run"] for result in (full, rest))
    on_event = "tests.e2e.workflows.agents.w05_dispute_responder.agent:on_event"
    # The signature names the second it was signed in, so the args differ only across seconds.
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
    """Replay revives the raw body as the same bytes, so the trigger verifies and replays whole.

    The trigger handles the same dispute from its recording, and nothing reaches the fake
    internet."""
    recorded, replay = replayed(W, "under_threshold")
    [run] = recorded.sdk_runs()
    assert run.trigger is not None and run.trigger.replayable
    assert set(run.trigger.args["raw"]) == {"__irimi_bytes__"}
    assert base64.b64decode(run.trigger.args["raw"]["__irimi_bytes__"]).startswith(b"{")
    assert (replay.exit_code, replay.agent_exit_code()) == (0, 0)
    # The trigger is called with `bytes` again, and returns what it returned when recorded.
    called_with = {"raw": "bytes", "signature": "str"}
    assert replay.one("run.start")["called_with"] == called_with
    assert recorded.one("run.start")["called_with"] == called_with
    assert replay.returned() == recorded.returned() == "refund"
    assert replay.replay_result()[1:3] == [
        "  agent exited 0",
        "  no divergence: every call was answered from the recording",
    ]
    assert replay.internet.requests() == []
