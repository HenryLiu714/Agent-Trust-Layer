"""W3 `queue_worker`: one run per message, over threads, asyncio and six HTTP clients.

Runs exist only under shadow (the SDK is inactive in a bare run), so every per-run check reads the
shadow run. Order between runs is never asserted: the pool and the event loop interleave freely.
"""

import re
import sys
from collections import Counter
from urllib.parse import parse_qs

import pytest

import irimi
from examples.workflows.w03_queue_worker.agent import CLIENTS, POOLED
from examples.workflows.w03_queue_worker.scenarios import AGENT_VERSION
from irimi.exchange import Exchange
from irimi.trace import ErrorInfo

W = "w03_queue_worker"
AGENT = "examples.workflows.w03_queue_worker.agent"
# The trigger each scenario's runs are started by: `handle` on a thread pool, `handle_async` under
# `asyncio.gather` (#74).
TRIGGERS = {"threads_8": "handle", "asyncio_8": "handle_async"}
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
    """The charge a stored exchange is about: a charge read names it in its path, an audit's
    refund list in its query, a refund in its form body."""
    if ex.request.path.startswith("/v1/charges/"):
        return ex.request.path.rsplit("/", 1)[1]
    if ex.request.query:
        return parse_qs(ex.request.query)["charge"][0]
    return parse_qs(ex.request.body.decode())["charge"][0]


@pytest.mark.parametrize("scenario", ["threads_8", "asyncio_8"])
def test_eight_messages_are_eight_stored_runs_each_holding_only_its_own_charge(
    run_workflow, scenario
):
    """Each message is one run the SDK started and ended (#74): `attribution: "sdk"`, `ok`, its
    trigger the function the queue called and its args the message, which names the run's own
    charge. Each holds its read, its refund and irimi's L3 read of that charge and no other,
    whichever thread or task made them. The process run holds none of them."""
    shadow = run_workflow(W, scenario, "shadow")
    reader = shadow.stored()
    records = reader.list_runs()
    assert sorted(r.attribution for r in records) == ["process"] + ["sdk"] * 8
    runs = [r for r in records if r.attribution == "sdk"]
    assert {r.run_id for r in runs} == set(shadow.by_run())
    trigger = TRIGGERS[scenario]
    for record in runs:
        assert (record.outcome, record.error, record.exit_code) == ("ok", None, None)
        assert (record.sdk_version, record.agent_version) == (irimi.__version__, None)
        assert record.trigger is not None
        assert (record.trigger.name, record.trigger.entrypoint) == (trigger, f"{AGENT}:{trigger}")
        assert record.trigger.replayable
        message = record.trigger.args["message"]
        events = reader.load_run(record.run_id).events
        assert all(isinstance(e, Exchange) for e in events)
        assert {charge_named(e) for e in events if isinstance(e, Exchange)} == {message["charge"]}
        assert len(events) == 3, events
    (process,) = [r for r in records if r.attribution == "process"]
    assert reader.load_run(process.run_id).events == []


def test_every_client_went_through_the_proxy(run_workflow):
    shadow = run_workflow(W, "threads_8", "shadow")
    clients = {c.get("client", "urllib") for c in shadow.calls()}
    assert clients == set(CLIENTS)
    # Every one of them was answered by irimi on the write: none bypassed it.
    posts = {
        (c.get("client", "urllib"), c["answered_by"])
        for c in shadow.calls()
        if c["method"] == "POST"
    }
    assert posts == {(client, "fake-L1") for client in clients}


# A free-threaded 3.14 build starts a thread in a copy of its creator's context
# (`sys.flags.thread_inherit_context`), so there the bare thread keeps the run and this scenario's
# premise does not hold; `tests/test_sdk.py` pins that build's behaviour (#74).
@pytest.mark.skipif(
    bool(getattr(sys.flags, "thread_inherit_context", 0)),
    reason="threads inherit their creator's context on this build",
)
def test_a_thread_started_without_propagate_loses_its_run(run_workflow):
    shadow = run_workflow(W, "unpropagated_thread", "shadow")
    runs = [e["run"] for e in shadow.events("run.start")]
    assert len(runs) == 2
    # Each read belongs to its message's run; each refund, made in a thread started without
    # `sdk.propagate`, belongs to none. It was still faked, and carried no `Irimi-Run`, so it falls
    # back to the engine's own run (#75): the process run under `irimi shadow -- <cmd>`, and
    # `unattributed` only in serve mode (#77). That fallback is what this scenario is for.
    assert sorted((c["label"], c["run"], c["answered_by"]) for c in shadow.calls()) == sorted(
        [("read:ch_Q0", runs[0], None), ("read:ch_Q1", runs[1], None)]
        + [(f"refund:ch_Q{i}", None, "fake-L1") for i in (0, 1)]
    )
    # Stored the same way (#74): each message's run holds its read and nothing else, and both
    # refunds, which carried no `Irimi-Run`, land in the process run with their L3 reads.
    reader = shadow.stored()
    stored = {r.run_id: r.attribution for r in reader.list_runs()}
    assert sorted(stored.values()) == ["process", "sdk", "sdk"]
    for run_id in runs:
        [read] = reader.load_run(run_id).events
        assert isinstance(read, Exchange) and read.request.method == "GET"
    process = reader.load_run(shadow.process_run_id()).events
    refunds = [e for e in process if isinstance(e, Exchange) and e.request.method == "POST"]
    assert sorted(charge_named(e) for e in refunds) == ["ch_Q0", "ch_Q1"]


def test_a_nested_trigger_joins_the_run_it_is_called_in(run_workflow):
    shadow = run_workflow(W, "nested_trigger", "shadow")
    runs = shadow.by_run()
    assert len(runs) == 2 and None not in runs
    for labels in runs.values():
        assert own_charge_only(labels)
        assert sorted(label.split(":")[0] for label in labels) == ["audit", "read", "refund"]
    # The nested `audit` trigger started no run of its own, and irimi stored two runs, not four
    # (#74).
    assert [e["name"] for e in shadow.events("run.start")] == ["handle", "handle"]
    stored = [r for r in shadow.stored().list_runs() if r.attribution == "sdk"]
    assert {r.run_id for r in stored} == set(runs)
    assert {r.trigger.name for r in stored if r.trigger is not None} == {"handle"}
    # Its refund list is overlaid with the run's own faked refund.
    audits = [c for c in shadow.calls() if c["label"].startswith("audit:")]
    assert {c["answered_by"] for c in audits} == {"overlay"}


def test_one_failing_message_fails_only_its_own_run(run_workflow):
    shadow = run_workflow(W, "one_message_fails", "shadow")
    outcomes = ends(shadow)
    assert sorted(outcomes.values()) == ["error", "ok", "ok", "ok"]
    failed = next(run for run, outcome in outcomes.items() if outcome == "error")
    assert shadow.by_run()[failed] == ["read:ch_Q2"]
    # Stored: exactly one run ended in error, with the poison message's ValueError (#74).
    stored = [r for r in shadow.stored().list_runs() if r.attribution == "sdk"]
    assert sorted(r.outcome or "" for r in stored) == ["error", "ok", "ok", "ok"]
    [error] = [r for r in stored if r.outcome == "error"]
    assert error.run_id == failed
    assert error.error == ErrorInfo("builtins.ValueError", "cannot refund ch_Q2: poison message")
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


@pytest.mark.parametrize("scenario", ["threads_8", "asyncio_8"])
def test_runs_list_prints_one_line_per_message_and_one_for_the_process(run_workflow, scenario):
    """`irimi runs list` over the store: each message's `sdk` run, ended `ok` and named by its
    trigger (#74), holding its read, irimi's L3 read and its faked refund; and the process run,
    which holds none of them (#72). Each message's run started after the process run did, so the
    process run is listed last, the oldest."""
    shadow = run_workflow(W, scenario, "shadow")
    code, out, err = shadow.runs("list")
    assert (code, err) == (0, [])
    fields = [line.split("  ") for line in out]
    assert len(fields) == 9
    runs = sorted(f[0] for f in fields if f[4] == "sdk")
    assert runs == sorted(shadow.by_run())
    assert fields[-1][4] == "process"
    for f in fields:
        assert re.fullmatch(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d", f[1]), f
        if f[4] == "sdk":
            assert re.fullmatch(r"\d+\.\ds", f[2]), f
            assert [f[3], *f[5:]] == ["ok", TRIGGERS[scenario], "3 exchanges", "1 writes"]
        else:
            assert (f[0], f[3], f[4], f[6:]) == (
                shadow.process_run_id(),
                "ok",
                "process",
                ["0 exchanges", "0 writes"],
            )


# -- each way the SDK marks a run (#74) -----------------------------------------------------------


def stored_by_charge(shadow, args_of):
    """`{charge: stored run}` over the `sdk` runs, keyed by the charge `args_of(trigger args)`
    names, after checking that each run's exchanges are about that charge and no other."""
    reader = shadow.stored()
    found = {}
    for record in shadow.sdk_runs():
        assert record.trigger is not None
        charge = args_of(record.trigger.args)["charge"]
        events = reader.load_run(record.run_id).events
        assert {charge_named(e) for e in events if isinstance(e, Exchange)} == {charge}, events
        found[charge] = record
    return found


def test_each_async_with_sdk_run_block_is_one_run_with_its_message_as_the_trigger(run_workflow):
    """Each message is a block, `async with sdk.run(trigger=message, name="message")`, four at
    once under `asyncio.gather` (#74). Each is one `sdk` run named `message`, with no entrypoint,
    because no function names it, and the message itself as its args, replayable. The block that
    raised ends in error with its ValueError and holds only its read; the queue carries on. Bare,
    no run is started and the other three refunds land."""
    shadow = run_workflow(W, "async_run", "shadow")
    bare = run_workflow(W, "async_run", "bare")
    runs = shadow.by_run()
    assert None not in runs and len(runs) == 4
    found = stored_by_charge(shadow, lambda args: args)
    assert sorted(found) == [f"ch_Q{i}" for i in range(4)]
    assert {r.run_id for r in found.values()} == set(runs)
    for charge, record in found.items():
        assert record.trigger is not None
        assert (record.trigger.name, record.trigger.entrypoint) == ("message", None)
        assert record.trigger.replayable
        assert record.outcome == ("error" if charge == "ch_Q2" else "ok")
    assert found["ch_Q0"].trigger.args == {"charge": "ch_Q0", "amount": 100, "index": 0}
    poison = found["ch_Q2"]
    assert poison.error == ErrorInfo("builtins.ValueError", "cannot refund ch_Q2: poison message")
    assert runs[poison.run_id] == ["read:ch_Q2"]
    assert shadow.exchange_lines().count(REFUND_LINE) == 3
    for result in (shadow, bare):
        assert (result.result()["ok"], result.result()["failed"]) == (3, 1)
    assert len(bare.internet.writes()) == 3


def test_a_run_cancelled_at_shutdown_ends_in_error_with_its_cancellation(run_workflow):
    """The worker shuts down with one message in flight and cancels its task. Cancellation is a
    BaseException, not an Exception, and the SDK still ends the run (#74): `error`, under
    `asyncio.exceptions.CancelledError`, holding the read it made before it stuck, and no refund.
    The other two runs end `ok`, and the worker exits 0 in both modes."""
    shadow = run_workflow(W, "cancelled_on_shutdown", "shadow")
    bare = run_workflow(W, "cancelled_on_shutdown", "bare")
    assert shadow.exit_code == bare.exit_code == 0
    for result in (shadow, bare):
        failed = [(e["charge"], e["error"]) for e in result.events("message.failed")]
        assert failed == [("ch_Q1", "CancelledError")]
    logged = sorted((e["outcome"], e.get("error")) for e in shadow.events("run.end"))
    assert logged == [("error", "CancelledError"), ("ok", None), ("ok", None)]
    found = stored_by_charge(shadow, lambda args: args["message"])
    assert {c: r.outcome for c, r in found.items()} == {
        "ch_Q0": "ok",
        "ch_Q1": "error",
        "ch_Q2": "ok",
    }
    cancelled = found["ch_Q1"]
    assert cancelled.error == ErrorInfo("asyncio.exceptions.CancelledError", "")
    assert shadow.by_run()[cancelled.run_id] == ["read:ch_Q1"]
    assert shadow.exchange_lines().count(REFUND_LINE) == 2


def test_a_trigger_on_a_method_names_its_class_and_leaves_self_out_of_the_args(run_workflow):
    """`Worker.handle` and `Worker.handle_async` are triggers on methods (#74). The entrypoint
    names the class, `module:Worker.handle`, which replay (#84) can import; `self` is left out of
    the args, so the run stays replayable. An unnamed trigger is named by its `__qualname__`, a
    named one by its name. Every run records the version the deployment named, as the process run
    does (#70)."""
    shadow = run_workflow(W, "worker_methods", "shadow")
    found = stored_by_charge(shadow, lambda args: args["message"])
    sync = ("Worker.handle", f"{AGENT}:Worker.handle")
    async_ = ("worker.handle_async", f"{AGENT}:Worker.handle_async")
    assert {c: (r.trigger.name, r.trigger.entrypoint) for c, r in found.items() if r.trigger} == {
        "ch_Q0": sync,
        "ch_Q1": async_,
        "ch_Q2": sync,
        "ch_Q3": async_,
    }
    for record in found.values():
        assert record.trigger is not None and record.trigger.replayable
        assert set(record.trigger.args) == {"message"}
        assert (record.outcome, record.agent_version) == ("ok", AGENT_VERSION)
    [process] = [r for r in shadow.stored().list_runs() if r.attribution == "process"]
    assert process.agent_version == AGENT_VERSION
    assert shadow.exchange_lines().count(REFUND_LINE) == 4


def test_a_sync_trigger_that_returns_a_coroutine_hands_the_run_to_it(run_workflow):
    """`enqueue` is sync and returns a coroutine its caller's loop awaits, four at once under
    `asyncio.gather`. Its body runs only when the coroutine does, so the SDK hands the run over
    (#74): every call the coroutine makes carries its run, the `audit` trigger it calls joins that
    run rather than starting one, and the run ends with the coroutine's outcome. So the poison
    message's run, which raised inside the coroutine after the sync call had returned, is stored
    `error`, holding its read alone; each other run is `ok`, holding its read, its audit, its
    refund and irimi's L3 read of its own charge."""
    shadow = run_workflow(W, "coroutine_handoff", "shadow")
    runs = shadow.by_run()
    assert None not in runs, "a call with no run: the run ended before the coroutine ran"
    assert len(runs) == 4
    assert [e["name"] for e in shadow.events("run.start")] == ["enqueue"] * 4
    found = stored_by_charge(shadow, lambda args: args["message"])
    assert sorted(found) == [f"ch_Q{i}" for i in range(4)]
    for charge, record in found.items():
        labels = runs[record.run_id]
        assert own_charge_only(labels), labels
        assert record.trigger is not None
        assert (record.trigger.name, record.trigger.entrypoint) == ("enqueue", f"{AGENT}:enqueue")
        events = shadow.stored().load_run(record.run_id).events
        if charge == "ch_Q2":
            assert [label.split(":")[0] for label in labels] == ["read"]
            assert (record.outcome, record.error) == (
                "error",
                ErrorInfo("builtins.ValueError", "cannot refund ch_Q2: poison message"),
            )
            assert len(events) == 1
        else:
            assert sorted(label.split(":")[0] for label in labels) == ["audit", "read", "refund"]
            assert (record.outcome, record.error) == ("ok", None)
            assert len(events) == 4
    assert shadow.exchange_lines().count(REFUND_LINE) == 3


# -- runs that share a connection, and a task that outlives its run (#75) -------------------------


def test_runs_that_share_one_pooled_connection_never_mix(run_workflow):
    """Two concurrent requests don't mix (Phase 3's exit), on one connection. Eight runs on four
    threads send every call through one of two shared pools, a `requests.Session` and an
    `httpx.Client`, each holding a single connection to irimi. A faked refund leaves its
    connection open and is its run's last call, so the next call on that pool, another run's,
    goes out on the same connection: fewer connections were opened than calls made. irimi still
    stores each run's read, refund and L3 read in that run and no other, because it attributes by
    the `Irimi-Run` the SDK put on each request (#75), never by connection (D8)."""
    shadow = run_workflow(W, "one_pool_8", "shadow")
    assert {c["client"] for c in shadow.calls()} == set(POOLED)
    per_client = Counter(c["client"] for c in shadow.calls())
    assert per_client == dict.fromkeys(POOLED, 8)
    [opened] = [e["opened"] for e in shadow.events("pool")]
    assert set(opened) == set(POOLED)
    for client in POOLED:
        assert 1 <= opened[client] < per_client[client], opened
    found = stored_by_charge(shadow, lambda args: args["message"])
    assert sorted(found) == [f"ch_Q{i}" for i in range(8)]
    reader = shadow.stored()
    for record in found.values():
        assert record.outcome == "ok"
        assert len(reader.load_run(record.run_id).events) == 3
    assert reader.load_run(shadow.process_run_id()).events == []
    assert shadow.exchange_lines().count(REFUND_LINE) == 8
    assert len(run_workflow(W, "one_pool_8", "bare").internet.writes()) == 8


def test_a_task_a_sync_trigger_returns_files_its_calls_in_the_run_that_already_ended(run_workflow):
    """`schedule` is sync and returns an `asyncio.Task`, not a coroutine, so the SDK does not hand
    the run over (#74): the run ends `ok` as `schedule` returns, before the task has run a step.
    The task was created inside the run, so its context still holds the run's id, and the SDK
    labels its read and refund with it (#75, decision 10).

    LOOKS WRONG: every one of the task's exchanges is stored in a run that had already ended,
    after its `ended_at`, and the run reads back `ok` though its work had not started. #75 accepted
    it (its plan's decision 10, with a follow-up issue to file): either `hand_off` covers
    awaitables, or the task's calls belong to no run. This pins today's answer."""
    shadow = run_workflow(W, "task_outlives_trigger", "shadow")
    ends = {e["run"]: e["t"] for e in shadow.events("run.end")}
    assert len(ends) == 2 and set(ends) == set(shadow.by_run())
    for call in shadow.calls():
        assert call["t"] > ends[call["run"]], call
    found = stored_by_charge(shadow, lambda args: args["message"])
    assert sorted(found) == ["ch_Q0", "ch_Q1"]
    reader = shadow.stored()
    for record in found.values():
        assert record.outcome == "ok" and record.ended_at is not None
        events = reader.load_run(record.run_id).events
        assert sorted((e.request.method, e.request.path) for e in events) == [
            ("GET", f"/v1/charges/{record.trigger.args['message']['charge']}"),
            ("GET", f"/v1/charges/{record.trigger.args['message']['charge']}"),
            ("POST", "/v1/refunds"),
        ]
        assert min(e.started_at for e in events) > record.ended_at
    assert reader.load_run(shadow.process_run_id()).events == []
    assert shadow.exchange_lines().count(REFUND_LINE) == 2
