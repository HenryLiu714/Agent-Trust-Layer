"""W1 `ticket_triage`: a multi-turn LLM tool loop, pinned per prompt variant.

The labels are the calls in `examples/workflows/w01_ticket_triage/agent.py`; `#N` is the turn.
"""

import re
import shutil

import pytest

from examples.workflows.harness.run import check_invariants, compare
from examples.workflows.w01_ticket_triage.agent import DB_NAME
from irimi.trace import ErrorInfo, ToolCall, Trigger

W = "w01_ticket_triage"
# Each test runs under `irimi shadow -- <cmd>` and under serve mode, its agent beside irimi (#77).
MODES = ("shadow", "serve")
AGENT = "examples.workflows.w01_ticket_triage.agent"
REFUND = "issue_refund#2"


def replies(result):
    return result.query(DB_NAME, "SELECT ticket_id, body FROM replies")


@pytest.mark.parametrize("mode", MODES)
def test_a_full_refund_walks_the_same_loop_as_bare_and_neither_write_happens(run_workflow, mode):
    bare = run_workflow(W, "full_refund", "bare")
    shadow = run_workflow(W, "full_refund", mode)
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
    summary = shadow.summaries()
    refund = "  ○ refund $49.00 on ch_TICKET1  unvalidated (L3 preconditions passed)"
    # The tool write's line follows the HTTP write's (#76).
    reply = f"  ○ tool {AGENT}.send_reply  shadow stand-in ran"
    assert summary[summary.index(refund) : summary.index(refund) + 2] == [refund, reply]
    # irimi recorded both calls in the run, under the default name: the function's own
    # `module.qualname`, the real module's because the harness launches through `launch.py`.
    [run] = shadow.sdk_runs()
    stored = shadow.stored().load_run(run.run_id).events
    assert [(e.name, e.kind, e.ran) for e in stored if isinstance(e, ToolCall)] == [
        (f"{AGENT}.lookup_customer", "read", "real"),
        (f"{AGENT}.send_reply", "write", "shadow"),
    ]
    # One run, attributed by the SDK stand-in, ended ok.
    assert [e["outcome"] for e in shadow.events("run.end")] == ["ok"]


@pytest.mark.parametrize("mode", MODES)
def test_the_half_prompt_changes_only_the_refund(run_workflow, mode):
    bare = run_workflow(W, "half_refund", "bare")
    shadow = run_workflow(W, "half_refund", mode)
    assert bare.world.stripe.refunds[0]["amount"] == 2450
    assert "  ○ refund $24.50 on ch_TICKET1  unvalidated (L3 preconditions passed)" in (
        shadow.summaries()
    )
    # The loop has the same shape as the full refund's: the prompt moved one argument.
    full = run_workflow(W, "full_refund", mode)
    assert [c.get("label") for c in shadow.calls()] == [c.get("label") for c in full.calls()]
    assert len(shadow.world.llm.calls) == len(full.world.llm.calls) == 4


@pytest.mark.parametrize("mode", MODES)
def test_a_refund_larger_than_the_charge_is_refused_the_way_stripe_refuses_it(run_workflow, mode):
    bare = run_workflow(W, "too_large", "bare")
    shadow = run_workflow(W, "too_large", mode)
    assert bare.result()["refund_statuses"] == shadow.result()["refund_statuses"] == [400]
    assert shadow.by_label()[REFUND] == (400, "fake-L1")
    summary = shadow.summaries()
    assert "  ✗ refund $98.00 on ch_TICKET1  would fail: amount_too_large" in summary
    # A write L3 rejected fires nothing.
    assert "  These writes did not happen." in summary


@pytest.mark.parametrize("mode", MODES)
def test_a_second_refund_is_refused_because_the_overlay_shows_the_first(run_workflow, mode):
    bare = run_workflow(W, "double_refund", "bare")
    shadow = run_workflow(W, "double_refund", mode)
    assert bare.result()["refund_statuses"] == shadow.result()["refund_statuses"] == [200, 400]
    assert shadow.by_label()["issue_refund#3"] == (400, "fake-L1")
    summary = shadow.summaries()
    assert "  ○ refund $49.00 on ch_TICKET1  unvalidated (L3 preconditions passed)" in summary
    assert "  ✗ refund $49.00 on ch_TICKET1  would fail: charge_already_refunded" in summary
    # Each refund's L3 read went to Stripe; the second saw the first only through the overlay.
    reads = shadow.internet.requests("api.stripe.com")
    assert [(r.method, r.path) for r in reads] == [("GET", "/v1/charges/ch_TICKET1")] * 3


@pytest.mark.parametrize("mode", MODES)
def test_a_refund_the_model_repeats_with_the_same_key_is_one_write(run_workflow, mode):
    bare = run_workflow(W, "retried_refund", "bare")
    shadow = run_workflow(W, "retried_refund", mode)
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
    summary = shadow.summaries()
    assert "  api.stripe.com     1 read  1 engine read  1 write intercepted" in summary
    assert [line for line in summary if "refund $" in line] == [
        "  ○ refund $49.00 on ch_TICKET1  unvalidated (L3 preconditions passed)"
    ]


@pytest.mark.parametrize("mode", MODES)
def test_a_model_that_never_stops_is_cut_off_at_max_turns_in_both_modes(run_workflow, mode):
    bare = run_workflow(W, "runaway_loop", "bare")
    shadow = run_workflow(W, "runaway_loop", mode)
    assert bare.exit_code == shadow.exit_code == 1
    assert len(bare.world.llm.calls) == len(shadow.world.llm.calls) == 6
    assert shadow.result()["error"] == "RuntimeError: ticket T-1001: no answer after 6 turns"
    assert [(e["outcome"], e["error"]) for e in shadow.events("run.end")] == [
        ("error", "RuntimeError")
    ]
    assert replies(shadow) == replies(bare) == []
    # Stored under the exception's full name (#74); the log above has its short one.
    [run] = shadow.sdk_runs()
    assert (run.outcome, run.error) == (
        "error",
        ErrorInfo("builtins.RuntimeError", "ticket T-1001: no answer after 6 turns"),
    )


@pytest.mark.parametrize("mode", MODES)
def test_the_ticket_is_stored_as_the_runs_trigger_by_its_dataclass(run_workflow, mode):
    """#74's capture of a dataclass argument: the `Ticket` is stored as its class, by the module
    name `launch` keeps for the agent, and its fields, so replay (#84) can rebuild it."""
    shadow = run_workflow(W, "full_refund", mode)
    [run] = shadow.sdk_runs()
    ticket = {
        "ticket_id": "T-1001",
        "charge": "ch_TICKET1",
        "customer": "cus_TICKET1",
        "message": "I was charged twice for the same order",
    }
    assert run.trigger == Trigger(
        "handle_ticket",
        f"{AGENT}:handle_ticket",
        {"ticket": {"__irimi_type__": f"{AGENT}:Ticket", "value": ticket}},
        True,
    )
    assert (run.outcome, run.run_id) == ("ok", shadow.one("run.start")["run"])


# ---------------------------------------------------------------------- irimi compare (#79)

TRIAGE = f"handle_ticket  {AGENT}:handle_ticket  agent -"
FULL = "○ refund $49.00 on ch_TICKET1  unvalidated (L3 preconditions passed)"
REPLY = f"○ tool {AGENT}.send_reply  shadow stand-in ran"
# Whose summary printed a compared line: run A's for `=`, `-` and `~ A`, run B's for `+` and `B`.
PRINTED_BY = {"  = ": 0, "  - ": 0, "  ~ A ": 0, "  + ": 1, "    B ": 1}
COMPARED = {
    # The prompt edit: the same loop, and the one argument it moved.
    "half_refund": (
        1,
        [
            f"  ~ A {FULL}",
            "    B ○ refund $24.50 on ch_TICKET1  unvalidated (L3 preconditions passed)",
            f"  = {REPLY}",
            "",
            "  1 same · 1 changed · 0 only in A · 0 only in B",
        ],
    ),
    "too_large": (
        1,
        [
            f"  ~ A {FULL}",
            "    B ✗ refund $98.00 on ch_TICKET1  would fail: amount_too_large",
            f"  = {REPLY}",
            "",
            "  1 same · 1 changed · 0 only in A · 0 only in B",
        ],
    ),
    # The second refund is the one B added: L3 refused it, and an attempted write is a write.
    "double_refund": (
        1,
        [
            f"  = {FULL}",
            "  + ✗ refund $49.00 on ch_TICKET1  would fail: charge_already_refunded",
            f"  = {REPLY}",
            "",
            "  2 same · 0 changed · 0 only in A · 1 only in B",
        ],
    ),
    # The repeated send is the same write, answered by the idempotency store (#46): two runs, and
    # nothing between them to show.
    "retried_refund": (
        0,
        [f"  = {FULL}", f"  = {REPLY}", "", "  2 same · 0 changed · 0 only in A · 0 only in B"],
    ),
    "runaway_loop": (
        1,
        [f"  - {FULL}", f"  - {REPLY}", "", "  0 same · 0 changed · 2 only in A · 0 only in B"],
    ),
}


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("scenario", COMPARED)
def test_irimi_compare_shows_each_prompt_s_writes_against_the_full_refund_s(
    shared_store_run, scenario, mode
):
    """Two runs the real irimi recorded into one store, the same agent and ticket under two
    prompts: A under `irimi shadow`, B under shadow or serve. Every line compared is the line
    each run's own summary printed."""
    full = shared_store_run(W, "full_refund")
    other = shared_store_run(W, scenario, mode)
    assert check_invariants(other) == []
    code, out, err = compare(full, other)
    a, b = (result.one("run.start")["run"] for result in (full, other))
    assert a != b
    assert {a, b} <= {record.run_id for record in other.stored().list_runs()}
    assert err == []
    assert (code, out) == (
        COMPARED[scenario][0],
        [
            f"irimi compare · {a} vs {b} · a comparison, not a verdict: nothing was replayed",
            f"  A  {TRIAGE}",
            f"  B  {TRIAGE}",
            "",
            *COMPARED[scenario][1],
        ],
    )
    for line in out[4:-2]:
        prefix = next(p for p in PRINTED_BY if line.startswith(p))
        printed = (full, other)[PRINTED_BY[prefix]].summaries()
        assert f"  {line.removeprefix(prefix)}" in printed, line


def test_irimi_compare_says_which_run_it_could_not_read(shared_store_run, tmp_path):
    """#79's exit 2, end to end over the store `full_refund` and `half_refund` recorded into: a
    run id the store does not hold, on either side, is `no run`, and nothing goes to stdout; a run
    it holds damaged, in a copy of the store, `could not be read`, worded as `runs show` words it,
    the first side that cannot be read named."""
    full = shared_store_run(W, "full_refund")
    half = shared_store_run(W, "half_refund")
    a, b = (result.one("run.start")["run"] for result in (full, half))
    root = half.store_root()
    for missing in ("0123456789abcdef", "../escape", "unknown"):
        for pair in ((a, missing), (missing, b)):
            assert half.command("compare", *pair, "--store", str(root)) == (
                2,
                [],
                [f"error: no run {missing} in {root}"],
            )
    copy = tmp_path / "store"
    shutil.copytree(root, copy)
    events = copy / "runs" / b / "events.jsonl"
    lines = events.read_text().splitlines(keepends=True)
    # A line that is not JSON, not the last: damage, not a crash.
    events.write_text("".join([lines[0], "{not json\n", *lines[1:]]))
    assert half.command("compare", a, b, "--store", str(copy)) == (
        2,
        [],
        [
            f"error: run {b} could not be read: {events}, line 2: Expecting property name "
            "enclosed in double quotes: line 1 column 2 (char 1)"
        ],
    )
    # A record that is not JSON.
    events.write_text("".join(lines))
    record = copy / "runs" / a / "run.json"
    good = record.read_text()
    record.write_text("{")
    assert half.command("compare", a, b, "--store", str(copy)) == (
        2,
        [],
        [
            f"error: run {a} could not be read: {record}: Expecting property name enclosed in "
            "double quotes: line 1 column 2 (char 1)"
        ],
    )
    # A body the store no longer holds: every blob, so A, read first, is the one named.
    record.write_text(good)
    for blob in (copy / "blobs").iterdir():
        blob.unlink()
    code, out, err = half.command("compare", a, b, "--store", str(copy))
    assert (code, out) == (2, [])
    [line] = err
    missing_blob = re.fullmatch(
        rf"error: run {a} could not be read: blob ([0-9a-f]{{64}}) is missing: \[Errno 2\] No "
        rf"such file or directory: '{re.escape(str(copy))}/blobs/([0-9a-f]{{64}})'",
        line,
    )
    assert missing_blob is not None and missing_blob[1] == missing_blob[2], line


def test_irimi_compare_reads_a_run_whose_last_line_a_crash_cut(shared_store_run, tmp_path):
    """A store keeps a run whose last line was cut, as a crash mid-write leaves it, and reads it
    without that line (#70). Here `half_refund`'s run ends mid-way through its `send_reply` tool
    call, the run's last write, in a copy of the store.

    LOOKS WRONG: compare exits 1 and calls the reply a write only A made, and nothing it prints
    says B's record was cut: the reader of a cut record cannot tell it from a run that never
    replied (#79)."""
    full = shared_store_run(W, "full_refund")
    half = shared_store_run(W, "half_refund")
    a, b = (result.one("run.start")["run"] for result in (full, half))
    copy = tmp_path / "store"
    shutil.copytree(half.store_root(), copy)
    events = copy / "runs" / b / "events.jsonl"
    lines = events.read_text().splitlines(keepends=True)
    [last_write] = [i for i, line in enumerate(lines) if '"type": "tool_call"' in line][-1:]
    assert f"{AGENT}.send_reply" in lines[last_write]
    events.write_text("".join(lines[:last_write]) + lines[last_write][:40])
    assert half.command("compare", a, b, "--store", str(copy)) == (
        1,
        [
            f"irimi compare · {a} vs {b} · a comparison, not a verdict: nothing was replayed",
            f"  A  {TRIAGE}",
            f"  B  {TRIAGE}",
            "",
            f"  ~ A {FULL}",
            "    B ○ refund $24.50 on ch_TICKET1  unvalidated (L3 preconditions passed)",
            f"  - {REPLY}",
            "",
            "  0 same · 1 changed · 1 only in A · 0 only in B",
        ],
        [],
    )
