"""W3 `queue_worker`: one run per message, over threads, asyncio and four HTTP clients.

Runs exist only under shadow (the SDK is inactive in a bare run), so every per-run check reads the
shadow run. Order between runs is never asserted: the pool and the event loop interleave freely.
"""

import importlib.util
from collections import Counter, defaultdict

import pytest

W = "w03_queue_worker"
REFUND_LINE = "fake-L1   write     POST api.stripe.com/v1/refunds -> 200  [fidelity:L1]"


def by_run(result):
    """{run id: [label, ...]} over the agent's http calls."""
    runs = defaultdict(list)
    for call in result.calls():
        runs[call["run"]].append(call["label"])
    return dict(runs)


def own_charge_only(labels):
    return len({label.split(":", 1)[1] for label in labels}) == 1


def ends(result):
    return {e["run"]: e["outcome"] for e in result.events("run.end")}


@pytest.mark.parametrize("scenario", ["threads_8", "asyncio_8"])
def test_eight_concurrent_runs_never_mix(run_workflow, scenario):
    shadow = run_workflow(W, scenario, "shadow")
    runs = by_run(shadow)
    assert None not in runs, "a call with no run: the context did not follow it"
    assert len(runs) == 8
    for labels in runs.values():
        assert own_charge_only(labels), labels
        assert Counter(label.split(":")[0] for label in labels) == {"read": 1, "refund": 1}
    starts = {e["run"] for e in shadow.events("run.start")}
    assert starts == set(runs) == set(ends(shadow))
    assert set(ends(shadow).values()) == {"ok"}
    # irimi faked one refund per message, whichever client sent it, and none reached Stripe.
    assert shadow.exchange_lines().count(REFUND_LINE) == 8
    assert all(c["answered_by"] == "fake-L1" for c in shadow.calls() if c["method"] == "POST")
    assert shadow.result()["ok"] == 8
    # Bare, the same eight refunds really land.
    assert len(run_workflow(W, scenario, "bare").internet.writes()) == 8


def test_every_client_went_through_the_proxy(run_workflow):
    shadow = run_workflow(W, "threads_8", "shadow")
    clients = {c.get("client", "urllib") for c in shadow.calls()}
    # `requests` is only installed with the `examples` group; without it message 3 falls back to
    # urllib. The agent runs in this interpreter's environment, so the two agree.
    with_requests = importlib.util.find_spec("requests") is not None
    assert clients == {"urllib", "http.client", "asyncio"} | (
        {"requests"} if with_requests else set()
    )
    # Every one of them was answered by irimi on the write: none bypassed it.
    assert {c.get("client", "urllib") for c in shadow.calls() if c["method"] == "POST"} == clients


def test_a_thread_started_without_propagate_loses_its_run(run_workflow):
    shadow = run_workflow(W, "unpropagated_thread", "shadow")
    for call in shadow.calls():
        if call["label"].startswith("refund:"):
            # The refund was still faked; it just belongs to no run. With #75 it lands in
            # `unattributed/`, which is what this scenario is for.
            assert call["run"] is None
            assert call["answered_by"] == "fake-L1"
        else:
            assert call["run"] is not None
    assert len(shadow.events("run.start")) == 2


def test_a_nested_trigger_joins_the_run_it_is_called_in(run_workflow):
    shadow = run_workflow(W, "nested_trigger", "shadow")
    runs = by_run(shadow)
    assert len(runs) == 2 and None not in runs
    for labels in runs.values():
        assert own_charge_only(labels)
        assert sorted(label.split(":")[0] for label in labels) == ["audit", "read", "refund"]
    # The nested `audit` trigger started no run of its own.
    assert [e["name"] for e in shadow.events("run.start")] == ["handle", "handle"]
    # Its refund list is overlaid with the run's own faked refund.
    audits = [c for c in shadow.calls() if c["label"].startswith("audit:")]
    assert {c["answered_by"] for c in audits} == {"overlay"}


def test_one_failing_message_fails_only_its_own_run(run_workflow):
    shadow = run_workflow(W, "one_message_fails", "shadow")
    outcomes = ends(shadow)
    assert sorted(outcomes.values()) == ["error", "ok", "ok", "ok"]
    failed = next(run for run, outcome in outcomes.items() if outcome == "error")
    assert by_run(shadow)[failed] == ["read:ch_Q2"]
    assert shadow.exit_code == 0
    assert (shadow.result()["ok"], shadow.result()["failed"]) == (3, 1)
    assert shadow.exchange_lines().count(REFUND_LINE) == 3


def test_a_second_run_sees_the_first_runs_faked_refund(run_workflow):
    """The write log is the engine's, not the run's: run 2's read of the charge is overlaid with
    run 1's faked refund, and L3 then refuses run 2's refund against it. That is what production
    would have done (the bare run agrees), but it means two runs in one engine are not isolated.
    Serve mode (#77) and the Phase 3 exit ("two concurrent requests don't mix") have to decide
    whether that is the intended world; this pins today's answer."""
    shadow = run_workflow(W, "shared_charge", "shadow")
    bare = run_workflow(W, "shared_charge", "bare")
    saw = [e["amount_refunded"] for e in shadow.events("saw")]
    assert saw == [0, 3000] == [e["amount_refunded"] for e in bare.events("saw")]
    reads = [c["answered_by"] for c in shadow.calls() if c["method"] == "GET"]
    assert reads == [None, "overlay"]
    assert [c["status"] for c in shadow.calls() if c["method"] == "POST"] == [200, 400]
    assert len({c["run"] for c in shadow.calls()}) == 2
    assert "  ✗ refund $30.00 on ch_QSHARED  would fail: amount_too_large" in shadow.summary()
