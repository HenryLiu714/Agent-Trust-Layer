"""W3 `queue_worker`: one run per message, over threads, asyncio and six HTTP clients.

Order between runs is never asserted: the pool and the event loop interleave freely.
"""

import re
import sys
from collections import Counter
from typing import cast
from urllib.parse import parse_qs

import pytest

import irimi
from irimi.core.exchange import Exchange
from irimi.protocol.trace import UNATTRIBUTED, ErrorInfo, ToolCall
from irimi.replay.child import CANNOT_CALL
from irimi.report.runs import RUN_LIST_HEADER
from tests.e2e.workflows.agents.w03_queue_worker.agent import CLIENTS, LEDGER, POOLED
from tests.e2e.workflows.agents.w03_queue_worker.scenarios import AGENT_VERSION
from tests.e2e.workflows.harness.compare import rows_not_shown
from tests.e2e.workflows.harness.execute import replay_targets
from tests.e2e.workflows.harness.invariants import check_invariants

W = "w03_queue_worker"
# Each test runs under `irimi shadow -- <cmd>` and under serve mode, its agent beside irimi.
MODES = ("shadow", "serve")
AGENT = "tests.e2e.workflows.agents.w03_queue_worker.agent"
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
    """Under serve eight messages print eight run blocks, each holding its own refund alone.

    No process run exists to hold anything."""
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
    """Each of eight concurrent runs stores exactly its own requests, tool calls and check reads.

    Equality, not containment: the engine's own run holds nothing, and under serve each run's
    block names its own charge alone."""
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
    """Eight concurrent runs over six clients and shared pools are attributed and never mix.

    Exactly eight `sdk` runs ended `ok`, plus the process run under shadow. Each run's args,
    exchanges and tool calls name its own charge; `unattributed/` holds nothing; each write
    tool's fake ran and the ledger holds no row; and the fake Stripe answered exactly the reads
    irimi stored as live."""
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
        charged = {cast(dict, c.args)["charge"] for c in calls}
        assert charged == ({charge} if ledger else set()), calls
        writes = [(c.kind, c.ran) for c in calls if c.kind == "write"]
        assert writes == ([("write", "shadow")] if ledger else []), calls
    assert sorted(charges) == [f"ch_Q{i}" for i in range(8)]
    code, out, err = result.runs("list")
    # The header row, then a row per run.
    assert (code, len(out), err) == (0, 1 + 8 + len(process), [])
    assert sorted(line.split()[0] for line in out if "  sdk  " in line) == sorted(charges.values())
    # Invariant 3 for a SQLite file: what the write tool would have written does not exist. Bare,
    # the same eight rows land; a scenario with no tools has no ledger.
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
    """Return the charge a stored exchange is about, from its path, query or form body."""
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
    """Each message is an `sdk` run ended `ok` holding only its own charge's exchanges.

    Its trigger is the queue's function and its args the message. The process run holds none of
    them, and under serve nothing is `unattributed`."""
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


# A free-threaded build starts a thread in a copy of its creator's context, so the thread keeps
# the run there; `tests/sdk/test_runs.py` pins that build's behaviour.
@pytest.mark.skipif(
    bool(getattr(sys.flags, "thread_inherit_context", 0)),
    reason="threads inherit their creator's context on this build",
)
def test_a_thread_started_without_propagate_loses_its_run(run_workflow):
    shadow = run_workflow(W, "unpropagated_thread", "shadow")
    runs = [e["run"] for e in shadow.events("run.start")]
    assert len(runs) == 2
    # Each refund, made in a thread started without `sdk.propagate`, belongs to no run: it is
    # still faked, and falls back to the engine's own run (the process run, or `unattributed`).
    assert sorted((c["label"], c["run"], c["answered_by"]) for c in shadow.calls()) == sorted(
        [("read:ch_Q0", runs[0], None), ("read:ch_Q1", runs[1], None)]
        + [(f"refund:ch_Q{i}", None, "fake-L1") for i in (0, 1)]
    )
    # Stored the same way: each message's run holds its read alone, and both refunds land in the
    # process run with their check reads.
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
    """Under serve a request no run labels is `unattributed`, with its check reads.

    The refunds made in threads started without `sdk.propagate` land there; each message's run
    holds its read alone."""
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
    assert len(unattributed) == 4  # each refund and irimi's check read of its charge
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
    """The write log is per run: a second run's read and check never see the first run's refund.

    Bare, against one real Stripe, the second refund is refused."""
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
    """The second run's host line counts a plain read: the overlay showed it no other run's write.

    `irimi runs show` prints the same from the store."""
    serve = run_workflow(W, "shared_charge", "serve")
    second = serve.events("run.start")[1]["run"]
    assert (
        "  api.stripe.com  1 read  1 irimi check read  1 faked write" in serve.run_blocks()[second]
    )


@pytest.mark.parametrize("mode", MODES)
def test_two_concurrent_runs_on_one_charge_each_see_their_own_refund_alone(run_workflow, mode):
    """Two concurrent runs refunding one charge each check and read back their own refund alone.

    Each refunds 30.00 of 49.00, so both pass and each later read shows 30.00 refunded, never
    60.00. Bare, the second is refused."""
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
    """`runs list` prints each message's `sdk` run, then the process run, the oldest, last.

    Each message's run holds its read, its check read and its faked refund. Under serve there is
    no process run."""
    shadow = run_workflow(W, scenario, mode)
    code, out, err = shadow.runs("list")
    assert (code, err) == (0, [])
    assert out[0].split() == list(RUN_LIST_HEADER)
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
    """Return `{charge: stored run}` over the `sdk` runs, checking each holds only that charge."""
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
    """Each `async with sdk.run(...)` block is one replayable `sdk` run with the message as args.

    It has no entrypoint, since no function names it. The block that raised ends in error with
    its ValueError and holds only its read. Bare, no run starts and the other three refunds land."""
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
    """A run cancelled at shutdown ends `error` under `asyncio.exceptions.CancelledError`.

    It holds the read it made before it stuck, and no refund; the other two runs end `ok`."""
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
    """A trigger on a method is named `module:Worker.handle` and leaves `self` out of its args.

    So the run stays replayable. A named trigger takes its name, and every run records the
    deployment's agent version, as the process run does."""
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
    """A sync trigger that returns a coroutine hands its run to the coroutine.

    Every call the coroutine makes carries the run, the `audit` trigger it calls joins it, and
    the run ends with the coroutine's outcome: the poison message's run is `error`, holding its
    read alone."""
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
    """Runs sharing one pooled connection never mix: irimi attributes by `Irimi-Run`.

    Eight runs on four threads share two one-connection pools, so fewer connections open than
    calls are made, and each run still stores only its own read, refund and check read."""
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
    """A task a sync trigger returns files its calls in the run, which has already ended `ok`.

    The run is not handed to a task, but the task's context holds the run id, so the SDK labels
    its read and refund with it.

    KNOWN BUG (#111): every one of the task's exchanges is stored after its run's `ended_at`, in
    a run that reads back `ok` though its work had not started."""
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
        # task has run a step.
        # KNOWN BUG (#125): each block says the run made no call and its refund is in no block,
        # though the stored run holds it (`test_invariants.py`'s `LATE_EVENTS`).
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
    """Tool calls from four threads each land in their own run, none lost or mixed.

    The write tool runs in the refund thread `sdk.propagate` handed the run to. Bare, the eight
    refunds reach the ledger."""
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
    """The summary prints write-tool fakes in the order they ended, as the stored calls say.

    Posts may reach irimi out of order; the start, then the id, breaks a tie. `ledger_post_waits`
    reverses two posts every time. Bare, the eight rows reach the ledger."""
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
    """Tool writes whose posts arrive reversed still print in the order they ended.

    `ledger_post_waits` delays the first run's post, so irimi's stream shows the second first.
    Under serve each run's block holds its own. Bare, both rows reach the ledger."""
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
    """Async tool calls from eight concurrent tasks each land in their task's run, in order.

    The coroutine read records the list it returned: the ledger as the write's fake left it,
    empty, where bare it holds the refund. There is no tool overlay (W6 `read_after_write`)."""
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
    """A tool called in a thread with no run lands in the engine's run, its fake still run.

    That is the process run under shadow and `unattributed` under serve. The read tool, on the
    trigger's thread, stays in the message's run."""
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
    """Tools on a worker's methods are named `module:Worker.x` and leave the receiver out of args.

    `fee` is a static method, `journal` a class method and `ack` a plain one; each fake is called
    as its method is, which their results show."""
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
# The trigger each scenario's runs below are started by, and the ledger write tool whose fake each
# refund's run runs, if any.
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
    """Return a run's label as compare prints it, without unset or redundant parts."""
    agent = f"  agent {version}" if version else ""
    if trigger == f"{AGENT}:{entrypoint or trigger.partition(':')[2]}":
        return f"{trigger}{agent}"
    return f"{trigger}  {AGENT}:{entrypoint or trigger}{agent}"


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("scenario", CONCURRENT)
def test_irimi_compare_of_two_concurrent_runs_shows_each_run_s_own_writes_alone(
    run_workflow, scenario, mode
):
    """Compare of each concurrent run with message 0's shows only that run's own writes changed.

    Each pair is one refund and its ledger write changed; a run that held another's write would
    show a row more."""
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
    # The returned task's calls are stored in its run after the run ends (the known bug above),
    # so compare, reading the store, shows refunds serve's block left out. See #111.
    "task_outlives_trigger": (
        (0, 1),
        (label("schedule"), label("schedule")),
        [f"  ~ A {refund_text(0)}", f"    B {refund_text(1)}"],
    ),
}


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("scenario", PAIRS)
def test_irimi_compare_of_two_runs_one_process_made(run_workflow, scenario, mode):
    """Compare of failed, cancelled, refused or outliving runs compares each as stored."""
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
    """Two runs refunding one charge, in turn or at once, compare as the same write."""
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
    """Compare finds a lost thread's writes in the engine's own run, in the order they ended.

    The engine's run is the process run under shadow and `unattributed` under serve."""
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
# `irimi replay` calls one run's trigger alone, so no other run of the process takes its answers.


@pytest.mark.parametrize("index", range(8))
def test_replaying_one_of_eight_concurrent_runs_replays_that_run_alone(
    run_workflow, replayed, index
):
    """Replaying one of eight concurrent runs replays that run alone, from its own recording.

    Nothing reaches the fake Stripe, and the replay makes the one write its recording did."""
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
    """`shared_charge`'s second run replays alone as recorded, and compares the same write."""
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
    """A method trigger's run cannot be replayed alone, exit 2, but replays by its command.

    A run does not record the method's instance. By command, the first run's `fee` is answered
    from the recording and later runs' run for real; neither write's real body runs."""
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
