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
    replay_targets,
    rows_not_shown,
)
from examples.workflows.w03_queue_worker.agent import CLIENTS, LEDGER, POOLED
from examples.workflows.w03_queue_worker.scenarios import AGENT_VERSION
from irimi import report
from irimi.exchange import Exchange
from irimi.sdk.replay_child import CANNOT_CALL
from irimi.trace import UNATTRIBUTED, ErrorInfo, ToolCall

W = "w03_queue_worker"
# Each test runs under `irimi shadow -- <cmd>` and under serve mode, its agent beside irimi.
MODES = ("shadow", "serve")
AGENT = "examples.workflows.w03_queue_worker.agent"
# The trigger each scenario's runs are started by: `handle` on a thread pool, `handle_async` under
# `asyncio.gather`.
TRIGGERS = {"threads_8": "handle", "asyncio_8": "handle_async"}
REFUND_LINE = "faked         write     POST api.stripe.com/v1/refunds -> 200"
CHECKED = "faked; checked against live state: would succeed"


def own_charge_only(labels):
    return len({label.split(":", 1)[1] for label in labels}) == 1


def ends(result):
    return {e["run"]: e["outcome"] for e in result.events("run.end")}


@pytest.mark.parametrize("scenario", ["threads_8", "asyncio_8"])
def test_eight_concurrent_runs_print_eight_summaries_under_serve(run_workflow, scenario):
    """Under `irimi shadow --serve` the same eight messages are eight runs, and each run's
    summary is printed when the SDK ends it: eight `--- run <id> ---` blocks, one per run, each
    holding its own refund and no other run's. The universal checks hold each block to its stored
    summary (`test_invariants.py`). No process run exists to hold anything."""
    serve = run_workflow(W, scenario, "serve")
    runs = serve.by_run()
    assert None not in runs and len(runs) == 8
    blocks = serve.run_blocks()
    assert set(blocks) == set(runs)
    for run_id, block in blocks.items():
        [refund] = [line for line in block if "○ stripe  refund" in line]
        [read] = [c["label"] for c in serve.calls() if c["run"] == run_id and c["method"] == "GET"]
        assert refund.startswith("  ○ stripe  refund $") and read.split(":")[1] in refund, (
            block,
            read,
        )
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
    """Two concurrent requests don't mix, as equality, not containment: each of
    the eight runs stores exactly the requests the agent made in it, by method, host and path,
    exactly the tool calls it made in it, and irimi's own L3 reads of its own charge alone; and
    the engine's own run, the process run or under serve mode `unattributed`, holds nothing.
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
    """A server-style agent produces attributed runs with their triggers, and two concurrent
    requests don't mix, as `tests/test_phase3_exit.py` asserts it for two
    requests, held here for eight over W3's six clients, over two shared one-connection pools,
    and with tools. Exactly eight runs, all `sdk` and `ok`, plus the process run under
    `irimi shadow -- <cmd>` and nothing more under serve; each run's trigger args name its
    own charge, and its exchanges and tool calls name that charge alone;
    `unattributed/` holds nothing; `irimi runs list` prints one line per run; each write tool was
    stood in, and the ledger it would have written holds no row; and the routing is proved from
    the store: the fake Stripe answered exactly the charge reads irimi stored
    as live, so none went past it. Under serve, each run's block holds its own refund (and its own
    stood-in tool write) alone. Invariants 1 to 5 hold."""
    result = run_workflow(W, scenario, mode)
    bare = run_workflow(W, scenario, "bare")
    reader = result.stored()
    records = reader.list_runs()
    process = ["process"] if mode == "shadow" else []
    assert sorted(r.attribution for r in records) == [*process, *["sdk"] * 8]
    runs = [r for r in records if r.attribution == "sdk"]
    assert {(r.end.outcome, r.end.error) for r in runs} == {("ok", None)}
    # What no run names is filed here: without the SDK's labels, every request is.
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
    # The header row, then a row per run.
    assert (code, len(out), err) == (0, 1 + 8 + len(process), [])
    assert sorted(line.split()[0] for line in out if "  sdk  " in line) == sorted(charges.values())
    # Invariant 3 for a SQLite file: what the write tool would have written
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
            [refund] = [line for line in blocks[run_id] if "○ stripe  refund" in line]
            assert refund.endswith(f" on {charge}  {CHECKED}"), refund
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
    """Each message is one run the SDK started and ended: `attribution: "sdk"`, `ok`, its
    trigger the function the queue called and its args the message, which names the run's own
    charge. Each holds its read, its refund and irimi's L3 read of that charge and no other,
    whichever thread or task made them. The process run holds none of them, and under serve
    mode, which has none, nothing is left `unattributed`."""
    shadow = run_workflow(W, scenario, mode)
    reader = shadow.stored()
    records = reader.list_runs()
    process = ["process"] if mode == "shadow" else []
    assert sorted(r.attribution for r in records) == [*process] + ["sdk"] * 8
    runs = [r for r in records if r.attribution == "sdk"]
    assert {r.run_id for r in runs} == set(shadow.by_run())
    trigger = TRIGGERS[scenario]
    for record in runs:
        assert (record.end.outcome, record.end.error, record.end.exit_code) == ("ok", None, None)
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
# premise does not hold; `tests/test_sdk.py` pins that build's behaviour.
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
    # back to the engine's own run: the process run under `irimi shadow -- <cmd>`, and
    # `unattributed` only in serve mode. That fallback is what this scenario is for.
    assert sorted((c["label"], c["run"], c["answered_by"]) for c in shadow.calls()) == sorted(
        [("read:ch_Q0", runs[0], None), ("read:ch_Q1", runs[1], None)]
        + [(f"refund:ch_Q{i}", None, "fake-L1") for i in (0, 1)]
    )
    # Stored the same way: each message's run holds its read and nothing else, and both
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
    """Serve mode has no process run to fall back on: a request no run labels is `unattributed`.
    The two refunds made in threads started without `sdk.propagate` land there, with the
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
    # The nested `audit` trigger started no run of its own, and irimi stored two runs, not four.
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
    # Stored: exactly one run ended in error, with the poison message's ValueError.
    stored = [r for r in shadow.stored().list_runs() if r.attribution == "sdk"]
    assert sorted(r.end.outcome or "" for r in stored) == ["error", "ok", "ok", "ok"]
    [error] = [r for r in stored if r.end.outcome == "error"]
    assert error.run_id == failed
    assert error.end.error == ErrorInfo(
        "builtins.ValueError", "cannot refund ch_Q2: poison message"
    )
    assert shadow.exit_code == 0
    assert (shadow.result()["ok"], shadow.result()["failed"]) == (3, 1)
    assert shadow.exchange_lines().count(REFUND_LINE) == 3


PASSED_REFUND = (
    "  ○ stripe  refund $30.00 on ch_QSHARED  faked; checked against live state: would succeed"
)
REFUSED_REFUND = "  ✗ stripe  refund $30.00 on ch_QSHARED  faked; would fail: amount_too_large"


@pytest.mark.parametrize("mode", ["shadow", "serve"])
def test_a_second_run_never_sees_the_first_runs_faked_refund(run_workflow, mode):
    """The write log is the run's: run 2's read of the charge is live, not overlaid with run 1's
    faked refund, and L3 passes run 2's refund against live state and its own writes alone. The
    bare run, against one real Stripe, refuses the second refund: a run is judged by what it did,
    never by what another run only pretended to do. Under `irimi shadow -- <cmd>` and serve mode
    alike."""
    shadow = run_workflow(W, "shared_charge", mode)
    bare = run_workflow(W, "shared_charge", "bare")
    assert [e["amount_refunded"] for e in shadow.events("saw")] == [0, 0]
    assert [e["amount_refunded"] for e in bare.events("saw")] == [0, 3000]
    reads = [c["answered_by"] for c in shadow.calls() if c["method"] == "GET"]
    assert reads == [None, None]
    assert [c["status"] for c in shadow.calls() if c["method"] == "POST"] == [200, 200]
    assert [c["status"] for c in bare.calls() if c["method"] == "POST"] == [200, 400]
    assert len({c["run"] for c in shadow.calls()}) == 2
    if mode == "shadow":
        assert shadow.summary().count(PASSED_REFUND) == 2
        assert REFUSED_REFUND not in shadow.summary()
        return
    # Serve mode prints each run's own summary: each run's refund faked and passed.
    blocks = shadow.run_blocks()
    for start in shadow.events("run.start"):
        assert PASSED_REFUND in blocks[start["run"]]
        assert REFUSED_REFUND not in blocks[start["run"]]


def test_a_second_runs_summary_shows_none_of_the_first_runs_writes(run_workflow):
    """The second run's host line counts a plain read: the overlay showed it no write, because
    the only write on the charge is the first run's. Serve mode prints it for every run, and
    `irimi runs show` prints the same from the store."""
    serve = run_workflow(W, "shared_charge", "serve")
    second = serve.events("run.start")[1]["run"]
    assert (
        "  api.stripe.com  1 read  1 irimi check read  1 faked write" in serve.run_blocks()[second]
    )


@pytest.mark.parametrize("mode", MODES)
def test_two_concurrent_runs_on_one_charge_each_see_their_own_refund_alone(run_workflow, mode):
    """Two runs at once refund 30.00 each of one 49.00 charge, the second sent once the first is
    answered, and both read the charge again once both are faked. Neither run's precondition
    check sees the other's refund, so both pass, and each later read shows its own refund alone:
    30.00 refunded, never 60.00. The bare run, against one real Stripe, refuses the second."""
    shadow = run_workflow(W, "concurrent_shared_charge", mode)
    runs = [e["run"] for e in shadow.events("run.start")]
    assert len(runs) == 2 and set(shadow.by_run()) == set(runs)
    for run_id in runs:
        mine = [c for c in shadow.calls() if c["run"] == run_id]
        assert [(c["method"], c["status"], c["answered_by"]) for c in mine] == [
            ("GET", 200, None),
            ("POST", 200, "fake-L1"),
            ("GET", 200, "overlay"),
        ]
    assert sorted(e["amount_refunded"] for e in shadow.events("saw")) == [0, 0, 3000, 3000]
    assert shadow.internet.writes() == []
    bare = run_workflow(W, "concurrent_shared_charge", "bare")
    assert [c["status"] for c in bare.calls() if c["method"] == "POST"] == [200, 400]
    if mode == "shadow":
        assert shadow.summary().count(PASSED_REFUND) == 2
        assert REFUSED_REFUND not in shadow.summary()
        return
    blocks = shadow.run_blocks()
    for run_id in runs:
        assert PASSED_REFUND in blocks[run_id]
        assert REFUSED_REFUND not in blocks[run_id]
        assert (
            "  api.stripe.com  2 reads (1 showing this run's writes)  1 irimi check read  "
            "1 faked write" in blocks[run_id]
        )


@pytest.mark.parametrize("scenario", ["threads_8", "asyncio_8"])
@pytest.mark.parametrize("mode", MODES)
def test_runs_list_prints_one_line_per_message_and_one_for_the_process(
    run_workflow, mode, scenario
):
    """`irimi runs list` over the store: each message's `sdk` run, ended `ok` and named by its
    trigger, holding its read, irimi's L3 read and its faked refund; and the process run,
    which holds none of them. Each message's run started after the process run did, so the
    process run is listed last, the oldest. Under serve mode there is no process run: the
    eight runs are all it lists."""
    shadow = run_workflow(W, scenario, mode)
    code, out, err = shadow.runs("list")
    assert (code, err) == (0, [])
    assert out[0].split() == list(report.RUN_LIST_HEADER)
    fields = [re.split(r"  +", line) for line in out[1:]]
    assert len(fields) == (9 if mode == "shadow" else 8)
    runs = sorted(f[0] for f in fields if f[4] == "sdk")
    assert runs == sorted(shadow.by_run())
    assert (fields[-1][4] == "process") is (mode == "shadow")
    for f in fields:
        assert re.fullmatch(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d", f[1]), f
        if f[4] == "sdk":
            assert re.fullmatch(r"\d+\.\ds", f[2]), f
            assert [f[3], *f[5:]] == ["ok", TRIGGERS[scenario], "3 exchanges", "1 write"]
        else:
            assert (f[0], f[3], f[4], f[6:]) == (
                shadow.engine_run_id(),
                "ok",
                "process",
                ["0 exchanges", "0 writes"],
            )


# -- each way the SDK marks a run -----------------------------------------------------------------


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
    """Each message is a block, `async with sdk.run("message", inputs=message)`, four at
    once under `asyncio.gather`. Each is one `sdk` run named `message`, with no entrypoint,
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
        assert record.end.outcome == ("error" if charge == "ch_Q2" else "ok")
    assert found["ch_Q0"].trigger.args == {"charge": "ch_Q0", "amount": 100, "index": 0}
    poison = found["ch_Q2"]
    assert poison.end.error == ErrorInfo(
        "builtins.ValueError", "cannot refund ch_Q2: poison message"
    )
    assert runs[poison.run_id] == ["read:ch_Q2"]
    assert shadow.exchange_lines().count(REFUND_LINE) == 3
    for result in (shadow, bare):
        assert (result.result()["ok"], result.result()["failed"]) == (3, 1)
    assert len(bare.internet.writes()) == 3


@pytest.mark.parametrize("mode", MODES)
def test_a_run_cancelled_at_shutdown_ends_in_error_with_its_cancellation(run_workflow, mode):
    """The worker shuts down with one message in flight and cancels its task. Cancellation is a
    BaseException, not an Exception, and the SDK still ends the run: `error`, under
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
    assert {c: r.end.outcome for c, r in found.items()} == {
        "ch_Q0": "ok",
        "ch_Q1": "error",
        "ch_Q2": "ok",
    }
    cancelled = found["ch_Q1"]
    assert cancelled.end.error == ErrorInfo("asyncio.exceptions.CancelledError", "")
    assert shadow.by_run()[cancelled.run_id] == ["read:ch_Q1"]
    assert shadow.exchange_lines().count(REFUND_LINE) == 2


@pytest.mark.parametrize("mode", MODES)
def test_a_trigger_on_a_method_names_its_class_and_leaves_self_out_of_the_args(run_workflow, mode):
    """`Worker.handle` and `Worker.handle_async` are triggers on methods. The entrypoint
    names the class, `module:Worker.handle`, which replay can import; `self` is left out of
    the args, so the run stays replayable. An unnamed trigger is named by its `module:qualname`,
    a named one by its name. Every run records the version the deployment named, as the process run
    does."""
    shadow = run_workflow(W, "worker_methods", mode)
    found = stored_by_charge(shadow, lambda args: args["message"])
    sync = (f"{AGENT}:Worker.handle", f"{AGENT}:Worker.handle")
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
        assert (record.end.outcome, record.agent_version) == ("ok", AGENT_VERSION)
    if mode == "shadow":
        [process] = [r for r in shadow.stored().list_runs() if r.attribution == "process"]
        assert process.agent_version == AGENT_VERSION
    assert shadow.exchange_lines().count(REFUND_LINE) == 4


@pytest.mark.parametrize("mode", MODES)
def test_a_sync_trigger_that_returns_a_coroutine_hands_the_run_to_it(run_workflow, mode):
    """`enqueue` is sync and returns a coroutine its caller's loop awaits, four at once under
    `asyncio.gather`. Its body runs only when the coroutine does, so the SDK hands the run over:
    every call the coroutine makes carries its run, the `audit` trigger it calls joins that
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
            assert (record.end.outcome, record.end.error) == (
                "error",
                ErrorInfo("builtins.ValueError", "cannot refund ch_Q2: poison message"),
            )
            assert len(events) == 1
        else:
            assert sorted(label.split(":")[0] for label in labels) == ["audit", "read", "refund"]
            assert (record.end.outcome, record.end.error) == ("ok", None)
            assert len(events) == 4
    assert shadow.exchange_lines().count(REFUND_LINE) == 3


# -- runs that share a connection, and a task that outlives its run -------------------------------


@pytest.mark.parametrize("mode", MODES)
def test_runs_that_share_one_pooled_connection_never_mix(run_workflow, mode):
    """Two concurrent requests don't mix, on one connection. Eight runs on four
    threads send every call through one of two shared pools, a `requests.Session` and an
    `httpx.Client`, each holding a single connection to irimi. A faked refund leaves its
    connection open and is its run's last call, so the next call on that pool, another run's,
    goes out on the same connection: fewer connections were opened than calls made. irimi still
    stores each run's read, refund and L3 read in that run and no other, because it attributes by
    the `Irimi-Run` the SDK put on each request, never by connection."""
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
        assert record.end.outcome == "ok"
        assert len(reader.load_run(record.run_id).events) == 3
    assert reader.load_run(shadow.engine_run_id()).events == []
    assert shadow.exchange_lines().count(REFUND_LINE) == 8
    assert len(run_workflow(W, "one_pool_8", "bare").internet.writes()) == 8


@pytest.mark.parametrize("mode", MODES)
def test_a_task_a_sync_trigger_returns_files_its_calls_in_the_run_that_already_ended(
    run_workflow, mode
):
    """`schedule` is sync and returns an `asyncio.Task`, not a coroutine, so the SDK does not hand
    the run over: the run ends `ok` as `schedule` returns, before the task has run a step.
    The task was created inside the run, so its context still holds the run's id, and the SDK
    labels its read and refund with it.

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
        assert record.end.outcome == "ok" and record.end.ended_at is not None
        events = reader.load_run(record.run_id).events
        assert sorted((e.request.method, e.request.path) for e in events) == [
            ("GET", f"/v1/charges/{record.trigger.args['message']['charge']}"),
            ("GET", f"/v1/charges/{record.trigger.args['message']['charge']}"),
            ("POST", "/v1/refunds"),
        ]
        assert min(e.started_at for e in events) > record.end.ended_at
    assert reader.load_run(shadow.engine_run_id()).events == []
    assert shadow.exchange_lines().count(REFUND_LINE) == 2
    if mode == "serve":
        # Serve mode prints each run's block once its end is on disk, which is before the
        # task has run a step. LOOKS WRONG: so each block says the run made no call at all, and
        # its refund is in no block, though the stored run holds it (`test_invariants.py`'s
        # `LATE_EVENTS`, #125).
        for record in found.values():
            assert shadow.run_blocks()[record.run_id][-1] == (
                "  0 exchanges · 0 real · 0 answered by irimi (0 faked writes) · 0 sent to targets"
            )
        assert not [line for line in shadow.summaries() if "○" in line]


# -- the worker's ledger tools: tool calls under concurrency --------------------------------------


def stored_tool_calls(shadow, run_id):
    return [e for e in shadow.stored().load_run(run_id).events if isinstance(e, ToolCall)]


def amount_of(charge: str) -> int:
    return 100 + int(charge.removeprefix("ch_Q"))


@pytest.mark.parametrize("mode", MODES)
def test_tool_calls_from_four_threads_each_land_in_their_own_run(run_workflow, mode):
    """`threads_8` with the ledger: each message's read tool runs on its pool thread, and its write
    tool in the refund thread `sdk.propagate` handed the run to. Eight runs on four threads, and
    each holds its own two calls, for its own charge, and no other run's: none lost, none mixed,
    none left to the process run. Bare, the eight refunds really reach the ledger."""
    shadow = run_workflow(W, "ledger_threads_8", mode)
    bare = run_workflow(W, "ledger_threads_8", "bare")
    found = stored_by_charge(shadow, lambda args: args["message"])
    assert sorted(found) == [f"ch_Q{i}" for i in range(8)]
    for charge, record in found.items():
        calls = stored_tool_calls(shadow, record.run_id)
        assert [(c.name, c.ran, c.args, c.result) for c in calls] == [
            (f"{AGENT}:ledger_balance", "real", {"charge": charge}, 0),
            (
                f"{AGENT}:record_refund",
                "shadow",
                {"charge": charge, "amount": amount_of(charge)},
                {"recorded": False, "charge": charge},
            ),
        ]
    assert stored_tool_calls(shadow, shadow.engine_run_id()) == []
    assert Counter(shadow.tool_call_lines()) == {
        f"tool-real     read      {AGENT}:ledger_balance -> ok": 8,
        f"tool-fake     write     {AGENT}:record_refund -> ok": 8,
    }
    tool_lines = [line for line in shadow.summaries() if "○ tool" in line]
    assert tool_lines == [f"  ○ tool  {AGENT}:record_refund  fake ran instead"] * 8
    assert shadow.query(LEDGER, "SELECT * FROM refunds") == []
    assert sorted(bare.query(LEDGER, "SELECT * FROM refunds")) == [
        (f"ch_Q{i}", 100 + i) for i in range(8)
    ]


def test_two_write_tools_ending_at_once_print_in_the_order_they_ended(run_workflow):
    """`ledger_threads_8` with every odd message's ledger row written by a second write tool,
    `mark_refunded`: eight runs on four threads, tool calls of two names ending at once. Their
    posts can reach irimi in another order than they ended, and the live summary printed them as
    they came while a stored run prints them by `seq`, so the two summaries could differ.
    The summary prints the stood-in writes in the order they ended, the start and then the id
    breaking a tie, which is the order the stored calls say. Which order these posts come in is
    left to the threads: this holds the printed order to the stored one on every run, and
    `test_stored_runs.py` holds the summary of the runs together to the live one;
    `ledger_post_waits` reverses two posts every time. Bare, the eight rows really reach the
    ledger."""
    shadow = run_workflow(W, "ledger_two_tools_8", "shadow")
    bare = run_workflow(W, "ledger_two_tools_8", "bare")
    calls = [e for e in shadow.stored_events() if isinstance(e, ToolCall) and e.kind == "write"]
    assert Counter(c.name for c in calls) == {
        f"{AGENT}:record_refund": 4,
        f"{AGENT}:mark_refunded": 4,
    }
    ended = sorted(calls, key=lambda c: (c.ended_at, c.started_at, c.tool_call_id))
    assert [line for line in shadow.summary() if "○ tool" in line] == [
        f"  ○ tool  {c.name}  fake ran instead" for c in ended
    ]
    assert shadow.query(LEDGER, "SELECT * FROM refunds") == []
    assert sorted(bare.query(LEDGER, "SELECT * FROM refunds")) == [
        (f"ch_Q{i}", 100 + i) for i in range(8)
    ]


@pytest.mark.parametrize("mode", MODES)
def test_tool_writes_whose_posts_came_in_the_other_order_print_in_the_order_they_ended(
    run_workflow, mode
):
    """`ledger_post_waits` makes what `ledger_two_tools_8` leaves to chance happen every time. The
    first run's async ledger write ends while the loop's one executor thread is busy with the
    worker's own job, so its post waits off the loop; the second run's sync write starts after it
    ended, and posts at once. irimi gets the second first, as its stream lines show, and printed
    the live summary in that order while the runs together, read back, printed the first first.
    Under shadow the summary prints them in the order they ended; under serve each run's
    block holds its own. `test_stored_runs.py` holds the runs together to the live summary in both
    modes. Bare, both rows reach the ledger."""
    shadow = run_workflow(W, "ledger_post_waits", mode)
    bare = run_workflow(W, "ledger_post_waits", "bare")
    first, second = f"{AGENT}:record_refund_async", f"{AGENT}:mark_refunded"
    assert shadow.tool_call_lines() == [
        f"tool-fake     write     {second} -> ok",
        f"tool-fake     write     {first} -> ok",
    ]
    calls = {c.name: c for c in shadow.stored_events() if isinstance(c, ToolCall)}
    assert calls.keys() == {first, second}
    assert calls[first].ended_at <= calls[second].started_at
    assert calls[first].run_id != calls[second].run_id
    if mode == "shadow":
        assert [line for line in shadow.summary() if "○ tool" in line] == [
            f"  ○ tool  {first}  fake ran instead",
            f"  ○ tool  {second}  fake ran instead",
        ]
    else:
        blocks = shadow.run_blocks()
        assert {
            run_id: [line for line in blocks[run_id] if "○ tool" in line]
            for run_id in (calls[first].run_id, calls[second].run_id)
        } == {
            calls[first].run_id: [f"  ○ tool  {first}  fake ran instead"],
            calls[second].run_id: [f"  ○ tool  {second}  fake ran instead"],
        }
    assert shadow.query(LEDGER, "SELECT * FROM refunds") == []
    assert sorted(bare.query(LEDGER, "SELECT * FROM refunds")) == [("ch_Q0", 100), ("ch_Q1", 101)]


@pytest.mark.parametrize("mode", MODES)
def test_async_tool_calls_from_eight_tasks_each_land_in_their_task_s_run(run_workflow, mode):
    """`asyncio_8` with the ledger: an async read, an async write and a sync read that returns a
    coroutine, awaited in each message's own task, eight at once on one event loop, every one
    yielding to the others mid-call. Each run holds its three calls in the order its task made
    them. The coroutine read's call is recorded with the list the coroutine returned: the
    ledger as the stood-in write left it, empty, where bare it holds the refund (no tool
    overlay, as W6's `read_after_write` pins)."""
    shadow = run_workflow(W, "ledger_asyncio_8", mode)
    bare = run_workflow(W, "ledger_asyncio_8", "bare")
    found = stored_by_charge(shadow, lambda args: args["message"])
    assert sorted(found) == [f"ch_Q{i}" for i in range(8)]
    for charge, record in found.items():
        assert record.end.outcome == "ok"
        calls = stored_tool_calls(shadow, record.run_id)
        assert [(c.name, c.ran, c.args, c.result) for c in calls] == [
            (f"{AGENT}:ledger_balance_async", "real", {"charge": charge}, 0),
            (
                f"{AGENT}:record_refund_async",
                "shadow",
                {"charge": charge, "amount": amount_of(charge)},
                {"recorded": False, "charge": charge},
            ),
            (f"{AGENT}:ledger_history", "real", {"charge": charge}, []),
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
    started without `sdk.propagate`, so it is in no run, as that thread's request is. Its
    call is recorded in the process run `IRIMI_RUN` names, still stood in, and the read tool,
    called on the trigger's own thread, in the message's run. Under serve mode the
    write is filed in `unattributed`, with the refunds, and so is in no run's block."""
    shadow = run_workflow(W, "ledger_unpropagated", mode)
    runs = {e["run"]: e for e in shadow.events("run.start")}
    assert len(runs) == 2
    found = stored_by_charge(shadow, lambda args: args["message"])
    for charge, record in found.items():
        calls = stored_tool_calls(shadow, record.run_id)
        assert [(c.name, c.args) for c in calls] == [
            (f"{AGENT}:ledger_balance", {"charge": charge})
        ]
    process = stored_tool_calls(shadow, shadow.engine_run_id())
    assert sorted((c.name, c.ran, c.args["charge"]) for c in process) == [
        (f"{AGENT}:record_refund", "shadow", "ch_Q0"),
        (f"{AGENT}:record_refund", "shadow", "ch_Q1"),
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
    """The ledger as the `Worker`'s own methods: `fee`, a static method, `journal`, a class
    method, each with a tool decorator written above the descriptor, and `ack`, a plain method.
    Each is named by its `module:qualname`, `Worker.` included; the receiver is left out of the
    stored args, as a trigger's is; and each stand-in is called as its method is, the class
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
            (f"{AGENT}:Worker.fee", "read", "real", {"amount": amount}, fee),
            (
                f"{AGENT}:Worker.journal",
                "write",
                "shadow",
                {"charge": charge, "net": amount - fee},
                {"journaled": False, "by": "Worker"},
            ),
            (
                f"{AGENT}:Worker.ack",
                "write",
                "shadow",
                {"message": message},
                {"acked": False, "queue": "refunds"},
            ),
        ]
    assert [line for line in shadow.summaries() if "○ tool" in line] == [
        f"  ○ tool  {AGENT}:Worker.journal  fake ran instead",
        f"  ○ tool  {AGENT}:Worker.ack  fake ran instead",
    ] * 4
    assert shadow.query(LEDGER, "SELECT * FROM acks") == []
    assert [(e["journal"], e["ack"]) for e in bare.events("ledger")] == [
        ({"journaled": True, "by": "Worker"}, {"acked": True, "queue": "refunds"})
    ] * 4
    assert len(bare.query(LEDGER, "SELECT * FROM acks")) == 4


# ---------------------------------------------------------------------- irimi compare

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
    return (
        f"○ stripe  refund $1.{i:02d} on ch_Q{i}  faked; checked against live state: would succeed"
    )


def run_of(result, index: int) -> str:
    """The stored run whose trigger was message `index`."""
    (run_id,) = [
        r.run_id
        for r in result.sdk_runs()
        if r.trigger is not None and r.trigger.args["message"]["index"] == index
    ]
    return run_id


def label(trigger: str, version: str | None = None, entrypoint: str | None = None) -> str:
    """A run as compare names it, leaving out an agent version it did not record, and its
    entrypoint when it is the trigger's name, as an unnamed trigger's is."""
    agent = f"  agent {version}" if version else ""
    if trigger == f"{AGENT}:{entrypoint or trigger.partition(':')[2]}":
        return f"{trigger}{agent}"
    return f"{trigger}  {AGENT}:{entrypoint or trigger}{agent}"


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("scenario", CONCURRENT)
def test_irimi_compare_of_two_concurrent_runs_shows_each_run_s_own_writes_alone(
    run_workflow, scenario, mode
):
    """Concurrent runs stay apart through `irimi compare`: the eight runs one process made at once,
    recorded into its one store, each compared with message 0's. Each pair is one refund changed,
    its charge and amount the run's own, and the ledger's write tool changed with it, its line the
    same and its args the run's own: a run that held another's refund or tool call would show a
    row more, and one that lost its own a row less."""
    shadow = run_workflow(W, scenario, mode)
    a = run_of(shadow, 0)
    trigger = TRIGGER_OF[scenario]
    tool = LEDGER_WRITE.get(scenario)
    for i in range(1, 8):
        line = f"○ tool  {AGENT}:{tool}  fake ran instead"
        ledger = [
            f"  ~ A {line}",
            f"    B {line}",
            f"      args.amount: 100 -> {100 + i}",
            f'      args.charge: "ch_Q0" -> "ch_Q{i}"',
        ]
        b = run_of(shadow, i)
        code, out, err = shadow.compare_runs(a, b)
        assert (code, err) == (1, [])
        assert out == [
            f"irimi compare · {a} vs {b}",
            f"  A  {label(trigger)}",
            f"  B  {label(trigger)}",
            ARGS_DIFFER,
            "",
            f"  ~ A {refund_text(0)}",
            f"    B {refund_text(i)}",
            *(ledger if tool else []),
            "",
            f"  0 same · {2 if tool else 1} changed · 0 only in A · 0 only in B",
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
    # A sync method and an async one, under the worker's version: the triggers differ, and so
    # does nothing but the refund.
    "worker_methods": (
        (0, 1),
        (
            label(f"{AGENT}:Worker.handle", AGENT_VERSION),
            label("worker.handle_async", AGENT_VERSION, "Worker.handle_async"),
        ),
        [f"  ~ A {refund_text(0)}", f"    B {refund_text(1)}"],
    ),
    # The task each trigger returns makes its calls after its run has ended, and they are stored
    # in that run, pinned LOOKS WRONG above: compare reads the store, so it shows the refunds that
    # serve's block, printed at the run's end, left out. See #111.
    "task_outlives_trigger": (
        (0, 1),
        (label("schedule"), label("schedule")),
        [f"  ~ A {refund_text(0)}", f"    B {refund_text(1)}"],
    ),
}


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("scenario", PAIRS)
def test_irimi_compare_of_two_runs_one_process_made(run_workflow, scenario, mode):
    """Compare over runs that failed, were cancelled, refused or outlived: each stored run is
    compared as it was stored, ended or not, with every compared line one its own summary
    printed."""
    (i, j), (label_a, label_b), rows = PAIRS[scenario]
    shadow = run_workflow(W, scenario, mode)
    a, b = run_of(shadow, i), run_of(shadow, j)
    code, out, err = shadow.compare_runs(a, b)
    changed = sum(row.startswith("  ~ ") for row in rows)
    only_a = sum(row.startswith("  - ") for row in rows)
    assert (code, err) == (1, [])
    assert out == [
        f"irimi compare · {a} vs {b}",
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
@pytest.mark.parametrize("scenario", ["shared_charge", "concurrent_shared_charge"])
def test_irimi_compare_of_two_runs_on_one_charge_finds_the_same_refund(
    run_workflow, scenario, mode
):
    """Two runs refund one charge, in turn or at once: each run's refund is checked against its
    own writes alone, so the two runs made the same write, and compare says so."""
    shadow = run_workflow(W, scenario, mode)
    a, b = run_of(shadow, 0), run_of(shadow, len(CLIENTS))
    code, out, err = shadow.compare_runs(a, b)
    assert (code, err) == (0, [])
    assert out == [
        f"irimi compare · {a} vs {b}",
        f"  A  {label('handle')}",
        f"  B  {label('handle')}",
        ARGS_DIFFER,
        "",
        f"  = {PASSED_REFUND.strip()}",
        "",
        "  1 same · 0 changed · 0 only in A · 0 only in B",
    ]
    assert rows_not_shown(shadow, a, b, out) == []


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("scenario", ["unpropagated_thread", "ledger_unpropagated"])
def test_irimi_compare_finds_a_lost_run_s_writes_in_the_engine_s_run(run_workflow, scenario, mode):
    """A refund made in a thread started without `sdk.propagate` belongs to no run, and its
    ledger write too: each lands in the engine's own run, the process run under
    `irimi shadow -- <cmd>` and `unattributed` under serve. Compared with that run, message
    0's run made no write, and the engine's run made both messages' in the order they ended."""
    shadow = run_workflow(W, scenario, mode)
    a, engine = run_of(shadow, 0), shadow.engine_run_id()
    code, out, err = shadow.compare_runs(a, engine)
    tool = [f"  + ○ tool  {AGENT}:record_refund  fake ran instead"]
    added = [
        line
        for i in range(2)
        for line in [f"  + {refund_text(i)}", *(tool if scenario.startswith("ledger") else [])]
    ]
    # The unattributed pool records nothing of what started it; a process run its command.
    engine_label = "  B" if mode == "serve" else f"  B  {sys.executable}"
    assert (code, err) == (1, [])
    assert out == [
        f"irimi compare · {a} vs {engine}",
        f"  A  {label('handle')}",
        engine_label,
        ARGS_DIFFER,
        "",
        *added,
        "",
        f"  0 same · 0 changed · 0 only in A · {len(added)} only in B",
    ]
    assert rows_not_shown(shadow, a, engine, out) == []


# ------------------------------------------------------------------------------- replay
#
# One process, several runs: `irimi replay` replays one of them, its trigger called alone, so
# the replay is that run and no other. `test_replays.py` replays each run of every scenario here;
# these pin what that means: no other run of the process is in the replay to take its answers, as
# every one did when a replay re-ran the whole process.


@pytest.mark.parametrize("index", range(8))
def test_replaying_one_of_eight_concurrent_runs_replays_that_run_alone(
    run_workflow, replayed, index
):
    """Each of `threads_8`'s eight runs replayed in turn: its trigger, `handle(message)`, called
    alone. It reads its charge and refunds it, both answered from its own recording, the refund
    at L3 as recorded, and the other seven runs are not in the replay at all: nothing reaches the
    fake Stripe, and the replay's one run makes the one write its recording did."""
    recorded = run_workflow(W, "threads_8", "shadow")
    run_id = sorted(r.run_id for r in recorded.sdk_runs())[index]
    _, replay = replayed(W, "threads_8", run_id=run_id)
    [mine] = [c for c in recorded.sdk_runs() if c.run_id == run_id]
    charge = mine.trigger.args["message"]["charge"]
    [refund] = [ex for ex in replay.reported if ex.kind == "write"]
    assert f"charge={charge}&" in refund.request.body.decode() + "&"
    assert (refund.precondition.outcome, refund.flags) == ("passed", ("from-recording",))
    assert [ex for ex in replay.reported if ex.issued_by == "engine"] == []
    assert replay.internet.requests() == []
    assert {ex.run_id for ex in replay.reported} == {replay.process_run_id()}
    assert replay.exit_code == 0
    assert replay.replay_result()[2:4] == [
        "  no divergence: every call was answered from the recording",
        "  2 from the recording · 0 missed",
    ]


def test_replaying_the_second_shared_charge_run_replays_it_alone(run_workflow, replayed):
    """`shared_charge`'s second run refunds the charge the first already refunded in shadow, and
    its refund passed, because its read and its check saw its own writes alone. Replayed alone, it
    is answered as it was recorded, read and refund alike, and its comparison is the same write."""
    recorded = run_workflow(W, "shared_charge", "shadow")
    second = max(recorded.sdk_runs(), key=lambda r: r.trigger.args["message"]["index"])
    _, replay = replayed(W, "shared_charge", run_id=second.run_id)
    assert replay.answered() == [
        ("read:ch_QSHARED", 200, "recording"),
        ("refund:ch_QSHARED", 200, "fake-L1"),
    ]
    assert replay.internet.requests() == []
    result = replay.replay_result()
    assert (replay.exit_code, result[result.index("") + 1 :]) == (
        0,
        [
            f"  = {PASSED_REFUND.strip()}",
            "",
            "  1 same · 0 changed · 0 only in A · 0 only in B",
        ],
    )


def test_a_worker_method_s_run_cannot_be_called_alone_and_replays_by_its_command(
    run_workflow, replayed
):
    """`ledger_worker`'s runs are the worker's methods, whose instance a run does not record.
    Replayed alone, `replay_child` will not call a method without it: the replay is one that
    could not run, exit 2, its run holding nothing but the reason it ended. Replayed
    by its command, the worker's whole process runs, every run of it adopted into the replay's one
    run: the first run's `fee`, a static-method read tool, is answered from the recording,
    and the later runs, which the recording lacks, run theirs for real, while neither write's real
    body runs."""
    recorded = run_workflow(W, "ledger_worker", "shadow")
    targets = replay_targets(recorded)
    assert len(targets) == 4
    first = targets[0]
    _, alone = replayed(W, "ledger_worker", run_id=first)
    assert (alone.exit_code, alone.agent_exit_code()) == (2, CANNOT_CALL)
    why = "it is a method, and a run does not record the instance it was called on"
    # Said once, and nothing compared: the agent never ran.
    [said] = alone.replay_result()
    assert said.startswith("error: cannot call ") and said.endswith(f": {why}")
    stored = alone.stored().load_run(alone.process_run_id())
    assert (stored.events, stored.record.end.error) == ([], ErrorInfo("cannot-call", why))
    _, whole = replayed(W, "ledger_worker", run_id=first, command=True)
    stored = whole.stored().load_run(whole.process_run_id()).events
    calls = [(c.name.rsplit(".", 1)[1], c.ran) for c in stored if isinstance(c, ToolCall)]
    assert (
        calls
        == [("fee", "recording"), ("journal", "shadow"), ("ack", "shadow")]
        + [
            ("fee", "real"),
            ("journal", "shadow"),
            ("ack", "shadow"),
        ]
        * 3
    )
