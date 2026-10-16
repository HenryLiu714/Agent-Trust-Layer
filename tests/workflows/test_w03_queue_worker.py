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
from examples.workflows.harness.run import (
    check_invariants,
    counterpart,
    replay_targets,
    rows_not_shown,
)
from examples.workflows.w03_queue_worker.agent import CLIENTS, LEDGER, POOLED
from examples.workflows.w03_queue_worker.scenarios import AGENT_VERSION
from irimi.exchange import Exchange
from irimi.trace import UNATTRIBUTED, ErrorInfo, ToolCall

W = "w03_queue_worker"
# Each test runs under `irimi shadow -- <cmd>` and under serve mode, its agent beside irimi (#77).
MODES = ("shadow", "serve")
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
def test_eight_concurrent_runs_print_eight_summaries_under_serve(run_workflow, scenario):
    """Under `irimi shadow --serve` (#77) the same eight messages are eight runs, and each run's
    summary is printed when the SDK ends it: eight `--- run <id> ---` blocks, one per run, each
    holding its own refund and no other run's. The universal checks hold each block to its stored
    summary (`test_invariants.py`). No process run exists to hold anything."""
    serve = run_workflow(W, scenario, "serve")
    runs = serve.by_run()
    assert None not in runs and len(runs) == 8
    blocks = serve.run_blocks()
    assert set(blocks) == set(runs)
    for run_id, block in blocks.items():
        [refund] = [line for line in block if "○ refund" in line]
        [read] = [c["label"] for c in serve.calls() if c["run"] == run_id and c["method"] == "GET"]
        assert refund.startswith("  ○ refund $") and read.split(":")[1] in refund, (block, read)
    assert [r.attribution for r in serve.stored().list_runs()] == ["sdk"] * 8
    assert serve.exchange_lines().count(REFUND_LINE) == 8
    assert serve.result()["ok"] == 8


@pytest.mark.parametrize("scenario", ["threads_8", "asyncio_8"])
@pytest.mark.parametrize("mode", MODES)
def test_eight_concurrent_runs_never_mix(run_workflow, mode, scenario):
    shadow = run_workflow(W, scenario, mode)
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


# The scenarios whose runs are truly concurrent: eight messages at once on four threads, under
# `asyncio.gather`, or on two shared one-connection pools, with and without the ledger's tools.
CONCURRENT = ("threads_8", "asyncio_8", "one_pool_8", "ledger_threads_8", "ledger_asyncio_8")


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("scenario", CONCURRENT)
def test_concurrent_runs_each_store_exactly_their_own_events(run_workflow, scenario, mode):
    """Phase 3's exit, "two concurrent requests don't mix", as equality, not containment: each of
    the eight runs stores exactly the requests the agent made in it, by method, host and path,
    exactly the tool calls it made in it, and irimi's own L3 reads of its own charge alone; and
    the engine's own run, the process run or under serve mode `unattributed` (#77), holds nothing.
    Under serve, each run's block, printed while the others still ran, names its own charge and
    no other run's."""
    shadow = run_workflow(W, scenario, mode)
    reader = shadow.stored()
    runs = {e["run"]: e for e in shadow.events("run.start")}
    assert len(runs) == 8 and set(shadow.by_run()) == set(runs)
    found = stored_by_charge(shadow, lambda args: args["message"])
    assert sorted(found) == [f"ch_Q{i}" for i in range(8)]
    for charge, record in found.items():
        events = reader.load_run(record.run_id).events
        made = Counter(
            (c["method"], *str(c["url"]).split("/", 1))
            for c in shadow.calls()
            if c["run"] == record.run_id
        )
        held = Counter(
            (e.request.method, e.request.host, e.request.path.lstrip("/"))
            for e in events
            if isinstance(e, Exchange) and e.issued_by == "agent"
        )
        assert held == made, (charge, held, made)
        engine = [e for e in events if isinstance(e, Exchange) and e.issued_by == "engine"]
        assert [e.request.path for e in engine] == [f"/v1/charges/{charge}"]
        tools = Counter(
            (t["name"], t["ran"]) for t in shadow.events("tool") if t["run"] == record.run_id
        )
        assert Counter((c.name, c.ran) for c in events if isinstance(c, ToolCall)) == tools
        assert all(e.run_id == record.run_id for e in events)
    assert reader.load_run(shadow.engine_run_id()).events == []
    if mode == "serve":
        blocks = shadow.run_blocks()
        assert set(blocks) == set(runs)
        for charge, record in found.items():
            named = {c for line in blocks[record.run_id] for c in re.findall(r"ch_Q\d", line)}
            assert named == {charge}, blocks[record.run_id]


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("scenario", CONCURRENT)
def test_phase_3_exit_holds_for_eight_concurrent_runs(run_workflow, scenario, mode):
    """Phase 3's exit (#78), "a server-style agent produces attributed runs with their triggers,
    and two concurrent requests don't mix", as `tests/test_phase3_exit.py` asserts it for two
    requests, held here for eight over W3's six clients, over two shared one-connection pools,
    and with tools. Exactly eight runs, all `sdk` and `ok`, plus the process run under
    `irimi shadow -- <cmd>` and nothing more under serve (#77); each run's trigger args name its
    own charge, and its exchanges and tool calls name that charge alone;
    `unattributed/` holds nothing; `irimi runs list` prints one line per run; each write tool was
    stood in, and the ledger it would have written holds no row; and the routing is proved from
    the store (#78's "Since #67"): the fake Stripe answered exactly the charge reads irimi stored
    as live, so none went past it. Under serve, each run's block holds its own refund (and its own
    stood-in tool write) alone. Invariants 1 to 5 hold."""
    result = run_workflow(W, scenario, mode)
    bare = run_workflow(W, scenario, "bare")
    reader = result.stored()
    records = reader.list_runs()
    process = ["process"] if mode == "shadow" else []
    assert sorted(r.attribution for r in records) == [*process, *["sdk"] * 8]
    runs = [r for r in records if r.attribution == "sdk"]
    assert {(r.outcome, r.error) for r in runs} == {("ok", None)}
    # What no run names is filed here: without the SDK's labels (#75), every request is.
    assert reader.load_run(UNATTRIBUTED).events == []
    ledger = scenario.startswith("ledger_")
    charges = {}
    for record in runs:
        assert record.trigger is not None
        charge = record.trigger.args["message"]["charge"]
        charges[charge] = record.run_id
        events = reader.load_run(record.run_id).events
        assert {charge_named(e) for e in events if isinstance(e, Exchange)} == {charge}, events
        calls = [e for e in events if isinstance(e, ToolCall)]
        assert {c.args["charge"] for c in calls} == ({charge} if ledger else set()), calls
        writes = [(c.kind, c.ran) for c in calls if c.kind == "write"]
        assert writes == ([("write", "shadow")] if ledger else []), calls
    assert sorted(charges) == [f"ch_Q{i}" for i in range(8)]
    code, out, err = result.runs("list")
    assert (code, len(out), err) == (0, 8 + len(process), [])
    assert sorted(line.split("  ", 1)[0] for line in out if "  sdk  " in line) == sorted(
        charges.values()
    )
    # Invariant 3, as #78 states it for its SQLite file: what the write tool would have written
    # does not exist. Bare, the same eight rows really land; a scenario with no tools has no ledger.
    if ledger:
        assert result.query(LEDGER, "SELECT * FROM refunds") == []
        assert len(bare.query(LEDGER, "SELECT * FROM refunds")) == 8
    else:
        assert not (result.state / LEDGER).exists()
    charge_reads = Counter(
        r.path for r in result.internet.requests("api.stripe.com") if r.method == "GET"
    )
    stored_reads = Counter(
        e.request.path
        for run_id in charges.values()
        for e in reader.load_run(run_id).events
        if isinstance(e, Exchange) and e.answered_by == "live" and e.request.method == "GET"
    )
    assert charge_reads == stored_reads == {f"/v1/charges/{c}": 2 for c in charges}
    if mode == "serve":
        blocks = result.run_blocks()
        assert set(blocks) == set(charges.values())
        for charge, run_id in charges.items():
            [refund] = [line for line in blocks[run_id] if "○ refund" in line]
            assert refund.endswith(f" on {charge}  unvalidated (L3 preconditions passed)"), refund
            tools = [line for line in blocks[run_id] if "○ tool" in line]
            assert len(tools) == (1 if ledger else 0), blocks[run_id]
    assert check_invariants(result, bare) == []


def charge_named(ex: Exchange) -> str:
    """The charge a stored exchange is about: a charge read names it in its path, an audit's
    refund list in its query, a refund in its form body."""
    if ex.request.path.startswith("/v1/charges/"):
        return ex.request.path.rsplit("/", 1)[1]
    if ex.request.query:
        return parse_qs(ex.request.query)["charge"][0]
    return parse_qs(ex.request.body.decode())["charge"][0]


@pytest.mark.parametrize("scenario", ["threads_8", "asyncio_8"])
@pytest.mark.parametrize("mode", MODES)
def test_eight_messages_are_eight_stored_runs_each_holding_only_its_own_charge(
    run_workflow, mode, scenario
):
    """Each message is one run the SDK started and ended (#74): `attribution: "sdk"`, `ok`, its
    trigger the function the queue called and its args the message, which names the run's own
    charge. Each holds its read, its refund and irimi's L3 read of that charge and no other,
    whichever thread or task made them. The process run holds none of them, and under serve
    mode (#77), which has none, nothing is left `unattributed`."""
    shadow = run_workflow(W, scenario, mode)
    reader = shadow.stored()
    records = reader.list_runs()
    process = ["process"] if mode == "shadow" else []
    assert sorted(r.attribution for r in records) == [*process] + ["sdk"] * 8
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
    assert reader.load_run(shadow.engine_run_id()).events == []


@pytest.mark.parametrize("mode", MODES)
def test_every_client_went_through_the_proxy(run_workflow, mode):
    shadow = run_workflow(W, "threads_8", mode)
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


@pytest.mark.skipif(
    bool(getattr(sys.flags, "thread_inherit_context", 0)),
    reason="threads inherit their creator's context on this build",
)
def test_a_thread_that_lost_its_run_is_unattributed_under_serve(run_workflow):
    """Serve mode has no process run to fall back on: a request no run labels is `unattributed`
    (#77). The two refunds made in threads started without `sdk.propagate` land there, with the
    L3 reads irimi made for them, and build no run; each message's run holds its read alone."""
    serve = run_workflow(W, "unpropagated_thread", "serve")
    runs = [e["run"] for e in serve.events("run.start")]
    reader = serve.stored()
    assert sorted(r.run_id for r in reader.list_runs()) == sorted(runs)
    for run_id in runs:
        [read] = reader.load_run(run_id).events
        assert isinstance(read, Exchange) and read.request.method == "GET"
    unattributed = [e for e in reader.load_run(UNATTRIBUTED).events if isinstance(e, Exchange)]
    refunds = [e for e in unattributed if e.request.method == "POST"]
    assert sorted(charge_named(e) for e in refunds) == ["ch_Q0", "ch_Q1"]
    assert all(e.run_id == UNATTRIBUTED and e.answered_by == "fake-L1" for e in refunds)
    assert len(unattributed) == 4  # each refund and irimi's L3 read of its charge
    # Neither refund is in any run's summary: no run holds it.
    assert not [line for block in serve.run_blocks().values() for line in block if "○" in line]


@pytest.mark.parametrize("mode", MODES)
def test_a_nested_trigger_joins_the_run_it_is_called_in(run_workflow, mode):
    shadow = run_workflow(W, "nested_trigger", mode)
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


@pytest.mark.parametrize("mode", MODES)
def test_one_failing_message_fails_only_its_own_run(run_workflow, mode):
    shadow = run_workflow(W, "one_message_fails", mode)
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


@pytest.mark.parametrize("mode", ["shadow", "serve"])
def test_a_second_run_sees_the_first_runs_faked_refund(run_workflow, mode):
    """The write log is the engine's, not the run's, by decision (#77): run 2's read of the charge
    is overlaid with run 1's faked refund, and L3 then refuses run 2's refund against it. That is
    what production would have done, since production has one Stripe, and the bare run agrees. The
    shadow week's report counts exactly this: a refund an earlier run already made would have
    been rejected, or was a duplicate (design §5.9). "Two concurrent requests don't mix", Phase 3's
    exit (#78), means attribution: each exchange and tool call is stored in the run that named it.
    What one run faked, the other still sees, under `irimi shadow -- <cmd>` and under serve mode
    alike."""
    shadow = run_workflow(W, "shared_charge", mode)
    bare = run_workflow(W, "shared_charge", "bare")
    saw = [e["amount_refunded"] for e in shadow.events("saw")]
    assert saw == [0, 3000] == [e["amount_refunded"] for e in bare.events("saw")]
    reads = [c["answered_by"] for c in shadow.calls() if c["method"] == "GET"]
    assert reads == [None, "overlay"]
    assert [c["status"] for c in shadow.calls() if c["method"] == "POST"] == [200, 400]
    assert len({c["run"] for c in shadow.calls()}) == 2
    refused = "  ✗ refund $30.00 on ch_QSHARED  would fail: amount_too_large"
    if mode == "shadow":
        assert refused in shadow.summary()
        return
    # Serve mode prints each run's own summary: the refund faked in the first, refused in the
    # second, each under its own run.
    first, second = (e["run"] for e in shadow.events("run.start"))
    blocks = shadow.run_blocks()
    assert "  ○ refund $30.00 on ch_QSHARED  unvalidated (L3 preconditions passed)" in blocks[first]
    assert refused in blocks[second] and refused not in blocks[first]


def test_a_second_runs_summary_says_the_first_runs_write_is_its_own(run_workflow):
    """LOOKS WRONG: the second run's summary says its read was `showing this run's writes`, but
    the write the overlay showed it is the first run's: the write log is the engine's (#77), and
    the host line's wording predates runs (#129). `irimi runs show` prints the same from the store
    under `irimi shadow -- <cmd>`; serve mode prints it for every run, unasked."""
    serve = run_workflow(W, "shared_charge", "serve")
    second = serve.events("run.start")[1]["run"]
    assert (
        "  api.stripe.com  1 read (1 showing this run's writes)  1 engine read  1 write "
        "intercepted" in serve.run_blocks()[second]
    )


@pytest.mark.parametrize("scenario", ["threads_8", "asyncio_8"])
@pytest.mark.parametrize("mode", MODES)
def test_runs_list_prints_one_line_per_message_and_one_for_the_process(
    run_workflow, mode, scenario
):
    """`irimi runs list` over the store: each message's `sdk` run, ended `ok` and named by its
    trigger (#74), holding its read, irimi's L3 read and its faked refund; and the process run,
    which holds none of them (#72). Each message's run started after the process run did, so the
    process run is listed last, the oldest. Under serve mode (#77) there is no process run: the
    eight runs are all it lists."""
    shadow = run_workflow(W, scenario, mode)
    code, out, err = shadow.runs("list")
    assert (code, err) == (0, [])
    fields = [line.split("  ") for line in out]
    assert len(fields) == (9 if mode == "shadow" else 8)
    runs = sorted(f[0] for f in fields if f[4] == "sdk")
    assert runs == sorted(shadow.by_run())
    assert (fields[-1][4] == "process") is (mode == "shadow")
    for f in fields:
        assert re.fullmatch(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d", f[1]), f
        if f[4] == "sdk":
            assert re.fullmatch(r"\d+\.\ds", f[2]), f
            assert [f[3], *f[5:]] == ["ok", TRIGGERS[scenario], "3 exchanges", "1 writes"]
        else:
            assert (f[0], f[3], f[4], f[6:]) == (
                shadow.engine_run_id(),
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


@pytest.mark.parametrize("mode", MODES)
def test_each_async_with_sdk_run_block_is_one_run_with_its_message_as_the_trigger(
    run_workflow, mode
):
    """Each message is a block, `async with sdk.run(trigger=message, name="message")`, four at
    once under `asyncio.gather` (#74). Each is one `sdk` run named `message`, with no entrypoint,
    because no function names it, and the message itself as its args, replayable. The block that
    raised ends in error with its ValueError and holds only its read; the queue carries on. Bare,
    no run is started and the other three refunds land."""
    shadow = run_workflow(W, "async_run", mode)
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


@pytest.mark.parametrize("mode", MODES)
def test_a_run_cancelled_at_shutdown_ends_in_error_with_its_cancellation(run_workflow, mode):
    """The worker shuts down with one message in flight and cancels its task. Cancellation is a
    BaseException, not an Exception, and the SDK still ends the run (#74): `error`, under
    `asyncio.exceptions.CancelledError`, holding the read it made before it stuck, and no refund.
    The other two runs end `ok`, and the worker exits 0 in both modes."""
    shadow = run_workflow(W, "cancelled_on_shutdown", mode)
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


@pytest.mark.parametrize("mode", MODES)
def test_a_trigger_on_a_method_names_its_class_and_leaves_self_out_of_the_args(run_workflow, mode):
    """`Worker.handle` and `Worker.handle_async` are triggers on methods (#74). The entrypoint
    names the class, `module:Worker.handle`, which replay (#84) can import; `self` is left out of
    the args, so the run stays replayable. An unnamed trigger is named by its `__qualname__`, a
    named one by its name. Every run records the version the deployment named, as the process run
    does (#70)."""
    shadow = run_workflow(W, "worker_methods", mode)
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
    if mode == "shadow":
        [process] = [r for r in shadow.stored().list_runs() if r.attribution == "process"]
        assert process.agent_version == AGENT_VERSION
    assert shadow.exchange_lines().count(REFUND_LINE) == 4


@pytest.mark.parametrize("mode", MODES)
def test_a_sync_trigger_that_returns_a_coroutine_hands_the_run_to_it(run_workflow, mode):
    """`enqueue` is sync and returns a coroutine its caller's loop awaits, four at once under
    `asyncio.gather`. Its body runs only when the coroutine does, so the SDK hands the run over
    (#74): every call the coroutine makes carries its run, the `audit` trigger it calls joins that
    run rather than starting one, and the run ends with the coroutine's outcome. So the poison
    message's run, which raised inside the coroutine after the sync call had returned, is stored
    `error`, holding its read alone; each other run is `ok`, holding its read, its audit, its
    refund and irimi's L3 read of its own charge."""
    shadow = run_workflow(W, "coroutine_handoff", mode)
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


@pytest.mark.parametrize("mode", MODES)
def test_runs_that_share_one_pooled_connection_never_mix(run_workflow, mode):
    """Two concurrent requests don't mix (Phase 3's exit), on one connection. Eight runs on four
    threads send every call through one of two shared pools, a `requests.Session` and an
    `httpx.Client`, each holding a single connection to irimi. A faked refund leaves its
    connection open and is its run's last call, so the next call on that pool, another run's,
    goes out on the same connection: fewer connections were opened than calls made. irimi still
    stores each run's read, refund and L3 read in that run and no other, because it attributes by
    the `Irimi-Run` the SDK put on each request (#75), never by connection (D8)."""
    shadow = run_workflow(W, "one_pool_8", mode)
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
    assert reader.load_run(shadow.engine_run_id()).events == []
    assert shadow.exchange_lines().count(REFUND_LINE) == 8
    assert len(run_workflow(W, "one_pool_8", "bare").internet.writes()) == 8


@pytest.mark.parametrize("mode", MODES)
def test_a_task_a_sync_trigger_returns_files_its_calls_in_the_run_that_already_ended(
    run_workflow, mode
):
    """`schedule` is sync and returns an `asyncio.Task`, not a coroutine, so the SDK does not hand
    the run over (#74): the run ends `ok` as `schedule` returns, before the task has run a step.
    The task was created inside the run, so its context still holds the run's id, and the SDK
    labels its read and refund with it (#75, decision 10).

    LOOKS WRONG: every one of the task's exchanges is stored in a run that had already ended,
    after its `ended_at`, and the run reads back `ok` though its work had not started. #75 accepted
    it (its plan's decision 10), and #111 is the follow-up: either `hand_off` covers awaitables, or
    the task's calls belong to no run. This pins today's answer."""
    shadow = run_workflow(W, "task_outlives_trigger", mode)
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
    assert reader.load_run(shadow.engine_run_id()).events == []
    assert shadow.exchange_lines().count(REFUND_LINE) == 2
    if mode == "serve":
        # Serve mode prints each run's block once its end is on disk (#77), which is before the
        # task has run a step. LOOKS WRONG: so each block says the run made no call at all, and
        # its refund is in no block, though the stored run holds it (`test_invariants.py`'s
        # `LATE_EVENTS`, #125).
        for record in found.values():
            assert shadow.run_blocks()[record.run_id][-1] == (
                "  0 exchanges · 0 live · 0 delegated · 0 virtualized"
            )
        assert not [line for line in shadow.summaries() if "○" in line]


# -- the worker's ledger tools: tool calls under concurrency (#76) --------------------------------


def stored_tool_calls(shadow, run_id):
    return [e for e in shadow.stored().load_run(run_id).events if isinstance(e, ToolCall)]


def amount_of(charge: str) -> int:
    return 100 + int(charge.removeprefix("ch_Q"))


@pytest.mark.parametrize("mode", MODES)
def test_tool_calls_from_four_threads_each_land_in_their_own_run(run_workflow, mode):
    """`threads_8` with the ledger: each message's read tool runs on its pool thread, and its write
    tool in the refund thread `sdk.propagate` handed the run to. Eight runs on four threads, and
    each holds its own two calls, for its own charge, and no other run's: none lost, none mixed,
    none left to the process run (#76). Bare, the eight refunds really reach the ledger."""
    shadow = run_workflow(W, "ledger_threads_8", mode)
    bare = run_workflow(W, "ledger_threads_8", "bare")
    found = stored_by_charge(shadow, lambda args: args["message"])
    assert sorted(found) == [f"ch_Q{i}" for i in range(8)]
    for charge, record in found.items():
        calls = stored_tool_calls(shadow, record.run_id)
        assert [(c.name, c.ran, c.args, c.result) for c in calls] == [
            (f"{AGENT}.ledger_balance", "real", {"charge": charge}, 0),
            (
                f"{AGENT}.record_refund",
                "shadow",
                {"charge": charge, "amount": amount_of(charge)},
                {"recorded": False, "charge": charge},
            ),
        ]
    assert stored_tool_calls(shadow, shadow.engine_run_id()) == []
    assert Counter(shadow.tool_call_lines()) == {
        f"real      read      tool {AGENT}.ledger_balance -> ok": 8,
        f"shadow    write     tool {AGENT}.record_refund -> ok": 8,
    }
    tool_lines = [line for line in shadow.summaries() if "○ tool" in line]
    assert tool_lines == [f"  ○ tool {AGENT}.record_refund  shadow stand-in ran"] * 8
    assert shadow.query(LEDGER, "SELECT * FROM refunds") == []
    assert sorted(bare.query(LEDGER, "SELECT * FROM refunds")) == [
        (f"ch_Q{i}", 100 + i) for i in range(8)
    ]


@pytest.mark.parametrize("mode", MODES)
def test_async_tool_calls_from_eight_tasks_each_land_in_their_task_s_run(run_workflow, mode):
    """`asyncio_8` with the ledger: an async read, an async write and a sync read that returns a
    coroutine, awaited in each message's own task, eight at once on one event loop, every one
    yielding to the others mid-call. Each run holds its three calls in the order its task made
    them (#76). The coroutine read's call is recorded with the list the coroutine returned: the
    ledger as the stood-in write left it, empty, where bare it holds the refund (no tool
    overlay, as W6's `read_after_write` pins)."""
    shadow = run_workflow(W, "ledger_asyncio_8", mode)
    bare = run_workflow(W, "ledger_asyncio_8", "bare")
    found = stored_by_charge(shadow, lambda args: args["message"])
    assert sorted(found) == [f"ch_Q{i}" for i in range(8)]
    for charge, record in found.items():
        assert record.outcome == "ok"
        calls = stored_tool_calls(shadow, record.run_id)
        assert [(c.name, c.ran, c.args, c.result) for c in calls] == [
            (f"{AGENT}.ledger_balance_async", "real", {"charge": charge}, 0),
            (
                f"{AGENT}.record_refund_async",
                "shadow",
                {"charge": charge, "amount": amount_of(charge)},
                {"recorded": False, "charge": charge},
            ),
            (f"{AGENT}.ledger_history", "real", {"charge": charge}, []),
        ]
    assert stored_tool_calls(shadow, shadow.engine_run_id()) == []
    assert len(shadow.tool_call_lines()) == 24
    for result, history in ((shadow, lambda charge: []), (bare, lambda c: [amount_of(c)])):
        assert {e["charge"]: e["history"] for e in result.events("ledger")} == {
            f"ch_Q{i}": history(f"ch_Q{i}") for i in range(8)
        }


@pytest.mark.skipif(
    bool(getattr(sys.flags, "thread_inherit_context", 0)),
    reason="threads inherit their creator's context on this build",
)
@pytest.mark.parametrize("mode", MODES)
def test_a_tool_called_in_a_thread_that_lost_its_run_lands_in_the_engines_own_run(
    run_workflow, mode
):
    """`unpropagated_thread` with the ledger: the write tool is called in the refund thread,
    started without `sdk.propagate`, so it is in no run, as that thread's request is (#75). Its
    call is recorded in the process run `IRIMI_RUN` names, still stood in, and the read tool,
    called on the trigger's own thread, in the message's run (#76). Under serve mode (#77) the
    write is filed in `unattributed`, with the refunds, and so is in no run's block."""
    shadow = run_workflow(W, "ledger_unpropagated", mode)
    runs = {e["run"]: e for e in shadow.events("run.start")}
    assert len(runs) == 2
    found = stored_by_charge(shadow, lambda args: args["message"])
    for charge, record in found.items():
        calls = stored_tool_calls(shadow, record.run_id)
        assert [(c.name, c.args) for c in calls] == [
            (f"{AGENT}.ledger_balance", {"charge": charge})
        ]
    process = stored_tool_calls(shadow, shadow.engine_run_id())
    assert sorted((c.name, c.ran, c.args["charge"]) for c in process) == [
        (f"{AGENT}.record_refund", "shadow", "ch_Q0"),
        (f"{AGENT}.record_refund", "shadow", "ch_Q1"),
    ]
    assert [t["run"] for t in shadow.events("tool") if t["kind"] == "write"] == [None, None]
    if mode == "shadow":
        assert shadow.stored().load_run(UNATTRIBUTED).events == []
    else:
        assert not [line for line in shadow.summaries() if "○" in line]


@pytest.mark.parametrize("mode", MODES)
def test_tools_on_a_worker_s_methods_keep_their_receiver_and_leave_it_out_of_the_args(
    run_workflow,
    mode,
):
    """The ledger as the `Worker`'s own methods (#76): `fee`, a static method, `journal`, a class
    method, each with `@sdk.tool` written above the descriptor, and `ack`, a plain method. Each is
    named by its `module.qualname`, `Worker.` included; the receiver is left out of the stored
    args, as a trigger's is (#74); and each stand-in is called as its method is, the class
    method's with the class and the plain method's with the worker, which their results show."""
    shadow = run_workflow(W, "ledger_worker", mode)
    bare = run_workflow(W, "ledger_worker", "bare")
    found = stored_by_charge(shadow, lambda args: args["message"])
    assert sorted(found) == [f"ch_Q{i}" for i in range(4)]
    for charge, record in found.items():
        amount = amount_of(charge)
        fee = amount // 10
        message = record.trigger.args["message"]
        calls = stored_tool_calls(shadow, record.run_id)
        assert [(c.name, c.kind, c.ran, c.args, c.result) for c in calls] == [
            (f"{AGENT}.Worker.fee", "read", "real", {"amount": amount}, fee),
            (
                f"{AGENT}.Worker.journal",
                "write",
                "shadow",
                {"charge": charge, "net": amount - fee},
                {"journaled": False, "by": "Worker"},
            ),
            (
                f"{AGENT}.Worker.ack",
                "write",
                "shadow",
                {"message": message},
                {"acked": False, "queue": "refunds"},
            ),
        ]
    assert [line for line in shadow.summaries() if "○ tool" in line] == [
        f"  ○ tool {AGENT}.Worker.journal  shadow stand-in ran",
        f"  ○ tool {AGENT}.Worker.ack  shadow stand-in ran",
    ] * 4
    assert shadow.query(LEDGER, "SELECT * FROM acks") == []
    assert [(e["journal"], e["ack"]) for e in bare.events("ledger")] == [
        ({"journaled": True, "by": "Worker"}, {"acked": True, "queue": "refunds"})
    ] * 4
    assert len(bare.query(LEDGER, "SELECT * FROM acks")) == 4


# ---------------------------------------------------------------------- irimi compare (#79)

NOT_A_VERDICT = "a comparison, not a verdict: nothing was replayed"
ARGS_DIFFER = "  (the triggers' args differ)"
# The trigger each scenario's runs below are started by, and the ledger write tool each refund's
# run stands in, if any.
TRIGGER_OF = {
    "threads_8": "handle",
    "asyncio_8": "handle_async",
    "one_pool_8": "handle",
    "ledger_threads_8": "handle",
    "ledger_asyncio_8": "handle_async",
}
LEDGER_WRITE = {"ledger_threads_8": "record_refund", "ledger_asyncio_8": "record_refund_async"}


def refund_text(i: int) -> str:
    """Message `i`'s refund of `100 + i` cents, as the summary prints it without its indent."""
    return f"○ refund $1.{i:02d} on ch_Q{i}  unvalidated (L3 preconditions passed)"


def run_of(result, index: int) -> str:
    """The stored run whose trigger was message `index`."""
    (run_id,) = [
        r.run_id
        for r in result.sdk_runs()
        if r.trigger is not None and r.trigger.args["message"]["index"] == index
    ]
    return run_id


def label(trigger: str, version: str = "-", entrypoint: str | None = None) -> str:
    return f"{trigger}  {AGENT}:{entrypoint or trigger}  agent {version}"


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("scenario", CONCURRENT)
def test_irimi_compare_of_two_concurrent_runs_shows_each_run_s_own_writes_alone(
    run_workflow, scenario, mode
):
    """Phase 3's exit through `irimi compare` (#79): the eight runs one process made at once,
    recorded into its one store, each compared with message 0's. Each pair is one refund changed,
    its charge and amount the run's own, and the ledger's write tool the same: a run that held
    another's refund or tool call would show a row more, and one that lost its own a row less."""
    shadow = run_workflow(W, scenario, mode)
    a = run_of(shadow, 0)
    trigger = TRIGGER_OF[scenario]
    tool = LEDGER_WRITE.get(scenario)
    same = [f"  = ○ tool {AGENT}.{tool}  shadow stand-in ran"] if tool else []
    for i in range(1, 8):
        b = run_of(shadow, i)
        code, out, err = shadow.compare_runs(a, b)
        assert (code, err) == (1, [])
        assert out == [
            f"irimi compare · {a} vs {b} · {NOT_A_VERDICT}",
            f"  A  {label(trigger)}",
            f"  B  {label(trigger)}",
            ARGS_DIFFER,
            "",
            f"  ~ A {refund_text(0)}",
            f"    B {refund_text(i)}",
            *same,
            "",
            f"  {len(same)} same · 1 changed · 0 only in A · 0 only in B",
        ]
        assert rows_not_shown(shadow, a, b, out) == []


# Pairs of runs one process made, by message index, A over B: the trigger each was started by, its
# agent version, and the rows compare prints between them.
PAIRS = {
    # Message 2 raises before its refund, and its run ends in error: the refund is message 0's.
    "one_message_fails": (
        (0, 2),
        (label("handle"), label("handle")),
        [f"  - {refund_text(0)}"],
    ),
    # Message 1 is still in flight at shutdown, and its run is cancelled before its refund.
    "cancelled_on_shutdown": (
        (0, 1),
        (label("handle_async"), label("handle_async")),
        [f"  - {refund_text(0)}"],
    ),
    # Two runs refund one charge in turn. The second is refused because the engine's one write
    # log shows it the first's faked refund (#77's decision; its summary calls that refund its
    # own, pinned LOOKS WRONG under #129). Each stored run holds its own refund alone, so the
    # compare is right where that summary is not.
    "shared_charge": (
        (0, len(CLIENTS)),
        (label("handle"), label("handle")),
        [
            "  ~ A ○ refund $30.00 on ch_QSHARED  unvalidated (L3 preconditions passed)",
            "    B ✗ refund $30.00 on ch_QSHARED  would fail: amount_too_large",
        ],
    ),
    # A sync method and an async one, under the worker's version: the triggers differ, and so
    # does nothing but the refund.
    "worker_methods": (
        (0, 1),
        (
            label("Worker.handle", AGENT_VERSION),
            label("worker.handle_async", AGENT_VERSION, "Worker.handle_async"),
        ),
        [f"  ~ A {refund_text(0)}", f"    B {refund_text(1)}"],
    ),
    # The task each trigger returns makes its calls after its run has ended, and they are stored
    # in that run (#75, pinned LOOKS WRONG under #111): compare reads the store, so it shows the
    # refunds that serve's block, printed at the run's end, left out (#77, #125).
    "task_outlives_trigger": (
        (0, 1),
        (label("schedule"), label("schedule")),
        [f"  ~ A {refund_text(0)}", f"    B {refund_text(1)}"],
    ),
}


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("scenario", PAIRS)
def test_irimi_compare_of_two_runs_one_process_made(run_workflow, scenario, mode):
    """#79 over runs that failed, were cancelled, refused or outlived: each stored run is compared
    as it was stored, ended or not, with every compared line one its own summary printed."""
    (i, j), (label_a, label_b), rows = PAIRS[scenario]
    shadow = run_workflow(W, scenario, mode)
    a, b = run_of(shadow, i), run_of(shadow, j)
    code, out, err = shadow.compare_runs(a, b)
    changed = sum(row.startswith("  ~ ") for row in rows)
    only_a = sum(row.startswith("  - ") for row in rows)
    assert (code, err) == (1, [])
    assert out == [
        f"irimi compare · {a} vs {b} · {NOT_A_VERDICT}",
        f"  A  {label_a}",
        f"  B  {label_b}",
        ARGS_DIFFER,
        "",
        *rows,
        "",
        f"  0 same · {changed} changed · {only_a} only in A · 0 only in B",
    ]
    assert rows_not_shown(shadow, a, b, out) == []


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("scenario", ["unpropagated_thread", "ledger_unpropagated"])
def test_irimi_compare_finds_a_lost_run_s_writes_in_the_engine_s_run(run_workflow, scenario, mode):
    """A refund made in a thread started without `sdk.propagate` belongs to no run (#75), and its
    ledger write too (#76): each lands in the engine's own run, the process run under
    `irimi shadow -- <cmd>` and `unattributed` under serve (#77). Compared with that run, message
    0's run made no write, and the engine's run made both messages' in the order they ended."""
    shadow = run_workflow(W, scenario, mode)
    a, engine = run_of(shadow, 0), shadow.engine_run_id()
    code, out, err = shadow.compare_runs(a, engine)
    tool = [f"  + ○ tool {AGENT}.record_refund  shadow stand-in ran"]
    added = [
        line
        for i in range(2)
        for line in [f"  + {refund_text(i)}", *(tool if scenario.startswith("ledger") else [])]
    ]
    engine_label = "-  -  agent -" if mode == "serve" else f"{sys.executable}  -  agent -"
    assert (code, err) == (1, [])
    assert out == [
        f"irimi compare · {a} vs {engine} · {NOT_A_VERDICT}",
        f"  A  {label('handle')}",
        f"  B  {engine_label}",
        ARGS_DIFFER,
        "",
        *added,
        "",
        f"  0 same · 0 changed · 0 only in A · {len(added)} only in B",
    ]
    assert rows_not_shown(shadow, a, engine, out) == []


# ------------------------------------------------------------------------------- replay (#82)
#
# One process, several runs: a replay answers from one of them (the issue's "Replay which run?"),
# so every other run's requests are missing from its recording. `test_replays.py` replays each run
# of every scenario here; these pin what that means for the runs it does not answer.


@pytest.mark.parametrize("index", range(8))
def test_replaying_one_of_eight_concurrent_runs_fakes_the_others_writes_unchecked(
    run_workflow, replayed, index
):
    """Each of `threads_8`'s eight runs replayed in turn. The replayed run's refund is its
    recording's, or, when another run's refund missed first and the replay diverged, checked again
    against its recording's own read of its charge: either way L3 passed, as recorded. Every other
    run's refund misses, is faked and never forwarded, and is checked against nothing: its charge
    read is not in the recording, and a replay never checks a precondition against live state, so
    it is `not_evaluable`, L2. Only the other runs' own charge reads reach the fake Stripe."""
    recorded = run_workflow(W, "threads_8", "shadow")
    run_id = sorted(r.run_id for r in recorded.sdk_runs())[index]
    _, replay = replayed(W, "threads_8", run_id=run_id)
    [mine] = [c for c in recorded.sdk_runs() if c.run_id == run_id]
    charge = mine.trigger.args["message"]["charge"]
    refunds = [ex for ex in replay.reported if ex.kind == "write"]
    assert len(refunds) == 8
    own = [ex for ex in refunds if f"charge={charge}&" in ex.request.body.decode() + "&"]
    others = [ex for ex in refunds if ex not in own]
    assert [ex.precondition for ex in own] == ["passed"]
    assert {(ex.precondition, ex.flags) for ex in others} == {
        ("not_evaluable", ("fidelity:L1", "replay-miss"))
    }
    checks = [ex for ex in replay.reported if ex.issued_by == "engine"]
    assert {(ex.answered_by, ex.response is None, ex.flags[-1]) for ex in checks} <= {
        ("recording", True, "replay-miss"),
        ("recording", False, "from-recording"),
    }
    assert sum(ex.response is None for ex in checks) == 7
    seen = [(r.method, r.path) for r in replay.internet.requests()]
    assert {m for m, _ in seen} == {"GET"}
    assert {p for _, p in seen} >= {f"/v1/charges/ch_Q{i}" for i in range(8)} - {
        f"/v1/charges/{charge}"
    }
    summary = replay.summaries()
    assert sum(" unvalidated (L2)" in line for line in summary) == 7
    assert (
        sum(f"on {charge}  unvalidated (L3 preconditions passed)" in line for line in summary) == 1
    )


def test_replaying_the_second_shared_charge_run_hands_its_refusal_to_the_first(
    run_workflow, replayed
):
    """LOOKS WRONG: a replay answers every run of the process from the one run's recording, so the
    first run's refund, identical to the second's, takes the second's recorded refusal, and its
    read the recorded read the overlay edited to show a refund no run has made yet. The second
    run, the one replayed, then misses everything: its read goes live, and its refund, checked
    against its recording's own read of the charge, passes. `irimi compare` of the second run and
    its replay shows the refusal gone."""
    recorded = run_workflow(W, "shared_charge", "shadow")
    second = max(recorded.sdk_runs(), key=lambda r: r.trigger.args["message"]["index"])
    _, replay = replayed(W, "shared_charge", run_id=second.run_id)
    assert replay.answered() == [
        ("read:ch_QSHARED", 200, "overlay"),
        ("refund:ch_QSHARED", 400, "fake-L1"),
        ("read:ch_QSHARED", 200, None),
        ("refund:ch_QSHARED", 200, "fake-L1"),
    ]
    first, replayed_second = (c["run"] for c in replay.calls() if c["label"].startswith("read"))
    assert first != replayed_second
    [mine] = [
        r.run_id
        for r in replay.stored().list_runs()
        if r.run_id == replayed_second and r.trigger.args["message"]["index"] == 6
    ]
    code, out, err = replay.compare_runs(second.run_id, mine)
    assert (code, out[4:], err) == (
        1,
        [
            "  ~ A ✗ refund $30.00 on ch_QSHARED  would fail: amount_too_large",
            "    B ○ refund $30.00 on ch_QSHARED  unvalidated (L3 preconditions passed)",
            "",
            "  0 same · 1 changed · 0 only in A · 0 only in B",
        ],
        [],
    )


def test_replaying_each_worker_run_answers_its_static_read_tool_from_the_recording_first_only(
    run_workflow, replayed
):
    """`ledger_worker`'s ledger as the worker's methods (#76): `Worker.fee`, a static-method read
    tool, `journal`, a class-method write, and `ack`, a plain method's. The worker takes its
    messages one at a time. Replayed, the first run's `fee` is answered from its recording, and
    neither write's real body runs (#83). LOOKS WRONG (#155): replaying a later run answers every
    run of the process from that run's recording, so the first run's refund, an HTTP write the
    recording lacks, diverges the replay before the replayed run reads, and its own `fee` runs
    for real."""
    recorded = run_workflow(W, "ledger_worker", "shadow")
    targets = replay_targets(recorded)
    assert len(targets) == 4
    for place, run_id in enumerate(targets):
        _, replay = replayed(W, "ledger_worker", run_id=run_id)
        mine = counterpart(recorded, replay, run_id)
        assert mine is not None
        stored = replay.stored().load_run(mine).events
        calls = [(c.name.rsplit(".", 1)[1], c.ran) for c in stored if isinstance(c, ToolCall)]
        fee = "recording" if place == 0 else "real"
        assert calls == [("fee", fee), ("journal", "shadow"), ("ack", "shadow")], place
