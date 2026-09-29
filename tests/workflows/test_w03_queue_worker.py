"""W3 `queue_worker`: one run per message, over threads, asyncio and four HTTP clients.

Runs exist only under shadow (the SDK is inactive in a bare run), so every per-run check reads the
shadow run. Order between runs is never asserted: the pool and the event loop interleave freely.
"""

import importlib.util
from collections import Counter
from urllib.parse import parse_qs

import pytest

from irimi.exchange import Exchange

W = "w03_queue_worker"
REFUND_LINE = "fake-L1   write     POST api.stripe.com/v1/refunds -> 200  [fidelity:L1]"


def own_charge_only(labels):
    return len({label.split(":", 1)[1] for label in labels}) == 1


def ends(result):
    return {e["run"]: e["outcome"] for e in result.events("run.end")}


@pytest.mark.parametrize("scenario", ["threads_8", "asyncio_8"])
def test_eight_concurrent_runs_never_mix(run_workflow, scenario):
    shadow = run_workflow(W, scenario, "shadow")
    runs = shadow.by_run()
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


def charge_named(ex: Exchange) -> str:
    """The charge a stored exchange is about: a charge read names it in its path, a refund in its
    form body."""
    if ex.request.path.startswith("/v1/charges/"):
        return ex.request.path.rsplit("/", 1)[1]
    return parse_qs(ex.request.body.decode())["charge"][0]


def test_eight_messages_are_eight_stored_runs_each_holding_only_its_own_charge(run_workflow):
    """Until #74 lands, the SDK stand-in's own `Irimi-Run` label on each message's calls is all
    irimi knows of a run, so each message is a `header` run: created by its first event, with no
    trigger and no outcome (#70). The process run holds none of them."""
    shadow = run_workflow(W, "threads_8", "shadow")
    reader = shadow.stored()
    records = reader.list_runs()
    assert sorted(r.attribution for r in records) == ["header"] * 8 + ["process"]
    headers = [r for r in records if r.attribution == "header"]
    assert {r.run_id for r in headers} == set(shadow.by_run())
    for record in headers:
        assert (record.trigger, record.outcome) == (None, None)
        events = reader.load_run(record.run_id).events
        assert all(isinstance(e, Exchange) for e in events)
        assert len({charge_named(e) for e in events if isinstance(e, Exchange)}) == 1, events
    (process,) = [r for r in records if r.attribution == "process"]
    assert reader.load_run(process.run_id).events == []


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
    posts = {
        (c.get("client", "urllib"), c["answered_by"])
        for c in shadow.calls()
        if c["method"] == "POST"
    }
    assert posts == {(client, "fake-L1") for client in clients}


def test_a_thread_started_without_propagate_loses_its_run(run_workflow):
    shadow = run_workflow(W, "unpropagated_thread", "shadow")
    runs = [e["run"] for e in shadow.events("run.start")]
    assert len(runs) == 2
    # Each read belongs to its message's run; each refund, made in a thread started without
    # `sdk.propagate`, belongs to none. It was still faked. With #75 it falls back to the process
    # run (`unattributed` only in serve mode, #77), which is what this scenario is for.
    assert sorted((c["label"], c["run"], c["answered_by"]) for c in shadow.calls()) == sorted(
        [("read:ch_Q0", runs[0], None), ("read:ch_Q1", runs[1], None)]
        + [(f"refund:ch_Q{i}", None, "fake-L1") for i in (0, 1)]
    )


def test_a_nested_trigger_joins_the_run_it_is_called_in(run_workflow):
    shadow = run_workflow(W, "nested_trigger", "shadow")
    runs = shadow.by_run()
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
    assert shadow.by_run()[failed] == ["read:ch_Q2"]
    assert shadow.exit_code == 0
    assert (shadow.result()["ok"], shadow.result()["failed"]) == (3, 1)
    assert shadow.exchange_lines().count(REFUND_LINE) == 3


def test_a_second_run_sees_the_first_runs_faked_refund(run_workflow):
    """LOOKS WRONG: the write log is the engine's, not the run's: run 2's read of the charge is
    overlaid with run 1's faked refund, and L3 then refuses run 2's refund against it. That is what
    production would have done (the bare run agrees), but it means two runs in one engine are not
    isolated. Serve mode (#77) and the Phase 3 exit ("two concurrent requests don't mix") have to
    decide whether that is the intended world; this pins today's answer."""
    shadow = run_workflow(W, "shared_charge", "shadow")
    bare = run_workflow(W, "shared_charge", "bare")
    saw = [e["amount_refunded"] for e in shadow.events("saw")]
    assert saw == [0, 3000] == [e["amount_refunded"] for e in bare.events("saw")]
    reads = [c["answered_by"] for c in shadow.calls() if c["method"] == "GET"]
    assert reads == [None, "overlay"]
    assert [c["status"] for c in shadow.calls() if c["method"] == "POST"] == [200, 400]
    assert len({c["run"] for c in shadow.calls()}) == 2
    assert "  ✗ refund $30.00 on ch_QSHARED  would fail: amount_too_large" in shadow.summary()
