"""W1 `ticket_triage`: a multi-turn LLM tool loop, pinned per prompt variant.

Labels are the agent's call names, with `#N` for the turn.
"""

import dataclasses
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time

import pytest

from irimi.config import paths
from irimi.protocol.trace import ErrorInfo, JSONValue, ToolCall, Trigger
from tests.e2e.workflows.agents.w01_ticket_triage.agent import (
    DB_LOST_ENV,
    DB_NAME,
    RESULT_TAG_ENV,
    Ticket,
    handle_ticket,
)
from tests.e2e.workflows.agents.w01_ticket_triage.scenarios import CUSTOMER
from tests.e2e.workflows.harness.compare import compare
from tests.e2e.workflows.harness.execute import replay, sdk_replay
from tests.e2e.workflows.harness.invariants import check_invariants
from tests.e2e.workflows.harness.scenario import REPO_ROOT, workflows

W = "w01_ticket_triage"
# Each test runs under `irimi shadow -- <cmd>` and under serve mode, its agent beside irimi.
MODES = ("shadow", "serve")
PASSED = "faked; checked against live state: would succeed"
# A refund a replay missed: its check read found no answer in the recording.
UNCHECKED = "faked; could not check: irimi's check read of stripe got no usable answer"
AGENT = "tests.e2e.workflows.agents.w01_ticket_triage.agent"
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
    # The tool write: its real body ran bare and only its fake under shadow.
    assert bare.tools() == [(f"{AGENT}:lookup_customer", "real"), (f"{AGENT}:send_reply", "real")]
    assert shadow.tools() == [
        (f"{AGENT}:lookup_customer", "real"),
        (f"{AGENT}:send_reply", "shadow"),
    ]
    assert replies(bare) == [("T-1001", "Refund of 4900 requested (statuses: 200).")]
    assert replies(shadow) == []
    summary = shadow.summaries()
    refund = f"  ○ stripe  refund $49.00 on ch_TICKET1  {PASSED}"
    # The tool write's line follows the HTTP write's.
    reply = f"  ○ tool  {AGENT}:send_reply  fake ran instead"
    assert summary[summary.index(refund) : summary.index(refund) + 2] == [refund, reply]
    # irimi recorded both calls in the run, under the default name: the function's own
    # `module:qualname`, the real module's because the harness starts it with `python -m`.
    [run] = shadow.sdk_runs()
    stored = shadow.stored().load_run(run.run_id).events
    assert [(e.name, e.kind, e.ran) for e in stored if isinstance(e, ToolCall)] == [
        (f"{AGENT}:lookup_customer", "read", "real"),
        (f"{AGENT}:send_reply", "write", "shadow"),
    ]
    # One run, attributed by the SDK, ended ok.
    assert [e["outcome"] for e in shadow.events("run.end")] == ["ok"]


@pytest.mark.parametrize("mode", MODES)
def test_the_half_prompt_changes_only_the_refund(run_workflow, mode):
    bare = run_workflow(W, "half_refund", "bare")
    shadow = run_workflow(W, "half_refund", mode)
    assert bare.world.stripe.refunds[0]["amount"] == 2450
    assert f"  ○ stripe  refund $24.50 on ch_TICKET1  {PASSED}" in (shadow.summaries())
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
    assert "  ✗ stripe  refund $98.00 on ch_TICKET1  faked; would fail: amount_too_large" in summary
    # A write the precondition check rejected fires nothing, and all of its amount would fail.
    assert "  None of these writes reached the real service:" in summary
    assert not any(line.startswith("  would have fired:") for line in summary)
    assert "  money: $98.00 in faked writes; $98.00 of it would fail" in summary


@pytest.mark.parametrize("mode", MODES)
def test_a_second_refund_is_refused_because_the_overlay_shows_the_first(run_workflow, mode):
    bare = run_workflow(W, "double_refund", "bare")
    shadow = run_workflow(W, "double_refund", mode)
    assert bare.result()["refund_statuses"] == shadow.result()["refund_statuses"] == [200, 400]
    assert shadow.by_label()["issue_refund#3"] == (400, "fake-L1")
    summary = shadow.summaries()
    assert f"  ○ stripe  refund $49.00 on ch_TICKET1  {PASSED}" in summary
    assert (
        "  ✗ stripe  refund $49.00 on ch_TICKET1  faked; would fail: charge_already_refunded"
        in summary
    )
    # Each refund's check read went to Stripe; the second saw the first only through the overlay.
    reads = shadow.internet.requests("api.stripe.com")
    assert [(r.method, r.path) for r in reads] == [("GET", "/v1/charges/ch_TICKET1")] * 3


# What two runs of one scenario print differently in a summary: run ids, whole and cut to the
# summary's 8 characters, and the elapsed seconds.
RUN_IDS = re.compile(r"\b[0-9a-f]{16}\b|(?<=run )[0-9a-f]{8}\b|(?<=irimi replay )[0-9a-f]{8}\b")
ELAPSED = re.compile(r" · \d+\.\ds$")


def _masked(lines: list[str]) -> list[str]:
    """`lines` with the run ids and elapsed seconds in them masked."""
    return [ELAPSED.sub(" · <elapsed>", RUN_IDS.sub("<run>", line)) for line in lines]


@pytest.mark.parametrize("mode", MODES)
def test_quiet_prints_the_banner_and_the_summary_and_no_line_per_exchange(run_workflow, mode):
    """`--quiet` drops the stream and nothing else: the run and summary are `double_refund`'s."""
    loud = run_workflow(W, "double_refund", mode)
    quiet = run_workflow(W, "double_refund_quiet", mode)
    assert loud.exchange_lines() and loud.tool_call_lines()
    assert quiet.exchange_lines() == quiet.tool_call_lines() == []
    assert any(" · listening on " in line for line in quiet.irimi)
    assert quiet.summaries() and _masked(quiet.summaries()) == _masked(loud.summaries())
    assert check_invariants(quiet) == []


def test_runs_show_takes_a_unique_run_id_prefix_and_last(run_workflow):
    """`runs show` finds a run by its short id, by `last` or with no id, as by its whole id."""
    shadow = run_workflow(W, "double_refund", "shadow")
    [run] = shadow.sdk_runs()
    store = ("--store", str(shadow.store_root()))
    full = shadow.runs("show", run.run_id, *store)
    assert full[0] == 0 and full[2] == []
    assert shadow.runs("show", run.run_id[:8], *store) == full
    newest = shadow.stored().list_runs()[0].run_id
    last = shadow.runs("show", "last", *store)
    assert last == shadow.runs("show", newest, *store)
    assert last[0] == 0
    assert shadow.runs("show", *store) == last
    code, listed, err = shadow.runs("list", *store)
    assert (code, err) == (0, [])
    assert listed[0].split() == [
        "run",
        "started",
        "took",
        "status",
        "attribution",
        "trigger",
        "exchanges",
        "writes",
    ]


def test_the_shadow_summary_names_the_sdk_run_inside_the_process_and_how_to_replay_it(
    run_workflow,
):
    """The live summary covers the whole process and names the SDK run `irimi replay` takes."""
    shadow = run_workflow(W, "double_refund", "shadow")
    [run] = shadow.sdk_runs()
    summary = shadow.summary()
    process = shadow.process_run_id()
    assert summary[0].startswith(f"irimi shadow · run {process} (whole process) · 10 exchanges · ")
    assert summary[-3:] == [
        "",
        f"  run {run.run_id[:8]}  {AGENT}:handle_ticket(...)  10 exchanges · 2 writes",
        f"  replay with: irimi replay {run.run_id[:8]}",
    ]


@pytest.mark.parametrize("mode", MODES)
def test_a_refund_the_model_repeats_with_the_same_key_is_one_write(run_workflow, mode):
    bare = run_workflow(W, "retried_refund", "bare")
    shadow = run_workflow(W, "retried_refund", mode)
    assert bare.result()["refund_statuses"] == shadow.result()["refund_statuses"] == [200, 200]
    # Both sends carry `T-1001-turn2`. Stripe answers the retry with the same refund, marked
    # `Idempotent-Replayed`; irimi's idempotency store does the same without a second check read.
    assert len(bare.world.stripe.refunds) == 1
    for result in (bare, shadow):
        first, again = result.events("refund")
        assert first["id"] == again["id"]
        assert (first["replayed"], again["replayed"]) == (None, "true")
    refund = "faked         write     POST api.stripe.com/v1/refunds -> 200"
    assert [line for line in shadow.exchange_lines() if "/v1/refunds" in line] == [
        refund,
        f"{refund}  [idempotent-replay]",
    ]
    summary = shadow.summaries()
    assert "  api.stripe.com     1 read  1 irimi check read  1 faked write" in summary
    assert [line for line in summary if "refund $" in line] == [
        f"  ○ stripe  refund $49.00 on ch_TICKET1  {PASSED}"
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
    # Stored under the exception's full name; the log above has its short one.
    [run] = shadow.sdk_runs()
    assert (run.end.outcome, run.end.error) == (
        "error",
        ErrorInfo("builtins.RuntimeError", "ticket T-1001: no answer after 6 turns"),
    )


@pytest.mark.parametrize("mode", MODES)
def test_the_ticket_is_stored_as_the_runs_trigger_by_its_dataclass(run_workflow, mode):
    """A dataclass trigger argument is stored by its class and fields, so replay can rebuild it."""
    shadow = run_workflow(W, "full_refund", mode)
    [run] = shadow.sdk_runs()
    ticket: dict[str, JSONValue] = {
        "ticket_id": "T-1001",
        "charge": "ch_TICKET1",
        "customer": "cus_TICKET1",
        "message": "I was charged twice for the same order",
    }
    assert run.trigger == Trigger(
        f"{AGENT}:handle_ticket",
        f"{AGENT}:handle_ticket",
        {"ticket": {"__irimi_type__": f"{AGENT}:Ticket", "value": ticket}},
        True,
    )
    assert (run.end.outcome, run.run_id) == ("ok", shadow.one("run.start")["run"])


# ---------------------------------------------------------------------- irimi compare

TRIAGE = f"{AGENT}:handle_ticket"
FULL = f"○ stripe  refund $49.00 on ch_TICKET1  {PASSED}"
REPLY = f"○ tool  {AGENT}:send_reply  fake ran instead"
# The reply the full refund's run sends the customer, which its summary line does not print.
SENT = "Refund of 4900 requested (statuses: 200)."


def reply_changed(now: str, was: str = SENT) -> list[str]:
    """Return the reply tool's rows when the two runs sent different text.

    One line printed twice, and the field that differs under it."""
    return [f"  ~ A {REPLY}", f"    B {REPLY}", f'      args.body: "{was}" -> "{now}"']


# Whose summary printed a compared line: run A's for `=`, `-` and `~ A`, run B's for `+` and `B`.
# A field that differs, under a pair, is compare's own.
PRINTED_BY = {"  = ": 0, "  - ": 0, "  ~ A ": 0, "  + ": 1, "    B ": 1}
FIELD_LINE = "      "
COMPARED = {
    # The prompt edit: the same loop, the one argument it moved, and the reply that says so.
    "half_refund": (
        1,
        [
            f"  ~ A {FULL}",
            f"    B ○ stripe  refund $24.50 on ch_TICKET1  {PASSED}",
            *reply_changed("Refund of 2450 requested (statuses: 200)."),
            "",
            "  0 same · 2 changed · 0 only in A · 0 only in B",
        ],
    ),
    "too_large": (
        1,
        [
            f"  ~ A {FULL}",
            "    B ✗ stripe  refund $98.00 on ch_TICKET1  faked; would fail: amount_too_large",
            *reply_changed("Refund of 9800 requested (statuses: 400)."),
            "",
            "  0 same · 2 changed · 0 only in A · 0 only in B",
        ],
    ),
    # The second refund is B's: the check refused it, and an attempted write is a write.
    "double_refund": (
        1,
        [
            f"  = {FULL}",
            "  + ✗ stripe  refund $49.00 on ch_TICKET1  faked; would fail: charge_already_refunded",
            *reply_changed("Refund of 4900 requested (statuses: 200, 400)."),
            "",
            "  1 same · 1 changed · 0 only in A · 1 only in B",
        ],
    ),
    # The repeated send is the same write, answered by the idempotency store, and not compared;
    # the reply that counts both sends is a different reply.
    "retried_refund": (
        1,
        [
            f"  = {FULL}",
            *reply_changed("Refund of 4900 requested (statuses: 200, 200)."),
            "",
            "  1 same · 1 changed · 0 only in A · 0 only in B",
        ],
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
    """Compare of one ticket under two prompts prints the lines each run's summary printed.

    A is recorded under `irimi shadow`, B under shadow or serve, into one store."""
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
            f"irimi compare · {a} vs {b}",
            f"  A  {TRIAGE}",
            f"  B  {TRIAGE}",
            "",
            *COMPARED[scenario][1],
        ],
    )
    for line in out[4:-2]:
        if line.startswith(FIELD_LINE):
            continue
        prefix = next(p for p in PRINTED_BY if line.startswith(p))
        printed = (full, other)[PRINTED_BY[prefix]].summaries()
        assert f"  {line.removeprefix(prefix)}" in printed, line


def test_irimi_compare_says_which_run_it_could_not_read(shared_store_run, tmp_path):
    """Compare exits 2 and names the first side it cannot read: `no run` or `could not be read`.

    A damaged run is worded as `runs show` words it, and nothing goes to stdout."""
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
        rf"error: run {a} could not be read: {re.escape(str(copy))}/runs/{a}/events\.jsonl, line "
        rf"\d+: blob ([0-9a-f]{{64}}) is missing: \[Errno 2\] No such file or directory: "
        rf"'{re.escape(str(copy))}/blobs/([0-9a-f]{{64}})'",
        line,
    )
    assert missing_blob is not None and missing_blob[1] == missing_blob[2], line


def test_irimi_compare_reads_a_run_whose_last_line_a_crash_cut(shared_store_run, tmp_path):
    """A run whose last line a crash cut is read without that line.

    Here `half_refund`'s run ends mid-way through its `send_reply` tool call, in a copy of the
    store.

    KNOWN BUG: compare calls the reply a write only A made, and nothing it prints says B's record
    was cut, so a cut record reads as a run that never replied."""
    full = shared_store_run(W, "full_refund")
    half = shared_store_run(W, "half_refund")
    a, b = (result.one("run.start")["run"] for result in (full, half))
    copy = tmp_path / "store"
    shutil.copytree(half.store_root(), copy)
    events = copy / "runs" / b / "events.jsonl"
    lines = events.read_text().splitlines(keepends=True)
    [last_write] = [i for i, line in enumerate(lines) if '"type": "tool_call"' in line][-1:]
    assert f"{AGENT}:send_reply" in lines[last_write]
    events.write_text("".join(lines[:last_write]) + lines[last_write][:40])
    assert half.command("compare", a, b, "--store", str(copy)) == (
        1,
        [
            f"irimi compare · {a} vs {b}",
            f"  A  {TRIAGE}",
            f"  B  {TRIAGE}",
            "",
            f"  ~ A {FULL}",
            f"    B ○ stripe  refund $24.50 on ch_TICKET1  {PASSED}",
            f"  - {REPLY}",
            "",
            "  0 same · 1 changed · 1 only in A · 0 only in B",
        ],
        [],
    )


def _irimi_process(
    result, *argv: str, stdout: int = subprocess.PIPE, stderr: int = subprocess.PIPE
) -> subprocess.CompletedProcess[str]:
    """Run `irimi <argv>` in its own process over `result`'s home and working directory.

    Unlike `Result.command`, its stderr holds what irimi's logger warns, and its stdout or stderr
    may be a pipe whose reader has gone (`_closed_pipe`)."""
    return subprocess.run(
        [sys.executable, "-m", "irimi", *argv],
        env={**os.environ, paths.IRIMI_HOME_ENV: str(result.home)},
        cwd=result.cwd,
        stdout=stdout,
        stderr=stderr,
        text=True,
        timeout=60,
        check=False,
    )


def _closed_pipe() -> int:
    """Return the write end of a pipe whose read end is closed, as `... | head -1` leaves it."""
    read, write = os.pipe()
    os.close(read)
    return write


def test_runs_show_and_list_name_the_file_and_line_of_a_damaged_event(run_workflow, tmp_path):
    """`runs show` and `runs list` name the file and line of an event that does not decode.

    A damaged run.json is named by its file; `runs list` skips that run with a warning, and a
    warning into a stderr whose reader has gone ends it with 141, its listing whole."""
    result = run_workflow(W, "full_refund", "shadow")
    run = result.one("run.start")["run"]
    copy = tmp_path / "store"
    shutil.copytree(result.store_root(), copy)
    events = copy / "runs" / run / "events.jsonl"
    lines = events.read_text().splitlines(keepends=True)
    [reply] = [i for i, line in enumerate(lines) if f'"name": "{AGENT}:send_reply"' in line]
    damaged = lines[reply].replace(f'"name": "{AGENT}:send_reply"', '"name": ["send_reply"]')
    events.write_text("".join([*lines[:reply], damaged, *lines[reply + 1 :]]))
    refused = f"could not be read: {events}, line {reply + 1}: 'name' is a list, not a string"
    assert result.runs("show", run, "--store", str(copy)) == (
        2,
        [],
        [f"error: run {run} {refused}"],
    )
    code, out, err = result.runs("list", "--store", str(copy))
    assert (code, err) == (0, [f"warning: run {run} {refused}"])
    [listed] = [line for line in out if line.startswith(f"{run}  ")]
    assert listed.split()[-4:] == ["?", "exchanges", "?", "writes"], listed
    events.write_text("".join(lines))
    record = copy / "runs" / run / "run.json"
    stored = json.loads(record.read_text())
    record.write_text(json.dumps({**stored, "engine_version": [stored["engine_version"]]}))
    refused = f"{record}: 'engine_version' is a list, not a string"
    assert result.runs("show", run, "--store", str(copy)) == (
        2,
        [],
        [f"error: run {run} could not be read: {refused}"],
    )
    listed = _irimi_process(result, "runs", "list", "--store", str(copy))
    assert (listed.returncode, listed.stderr) == (
        0,
        f"irimi: skipping stored run {run}: {refused}\n",
    )
    others = [r.run_id for r in result.stored().list_runs() if r.run_id != run]
    assert [line.split()[0] for line in listed.stdout.splitlines()[1:]] == others != []
    # That warning into a stderr whose reader has gone: the logger swallows the failed write, and
    # the interpreter's flush of it at exit must not turn the exit into 120.
    write = _closed_pipe()
    try:
        quiet = _irimi_process(result, "runs", "list", "--store", str(copy), stderr=write)
    finally:
        os.close(write)
    unpiped: str | None = quiet.stderr  # None, as stderr went to `write`, not a capture
    assert (quiet.returncode, quiet.stdout, unpiped) == (141, listed.stdout, None)


def test_runs_show_and_compare_end_quietly_when_their_reader_goes_away(shared_store_run):
    """`compare`, `runs show` and `--help` piped to `head -1` exit 141 with no traceback.

    The same holds under `2>&1`, where the first failing write is the error printed on stderr."""
    full = shared_store_run(W, "full_refund")
    half = shared_store_run(W, "half_refund")
    a, b = (result.one("run.start")["run"] for result in (full, half))
    root = str(half.store_root())
    commands = [("compare", a, b, "--store", root), ("runs", "show", b, "--store", root)]
    for argv in [*commands, ("--help",)]:
        write = _closed_pipe()
        try:
            ended = _irimi_process(half, *argv, stdout=write)
        finally:
            os.close(write)
        assert (ended.returncode, ended.stderr) == (141, ""), argv
    # `2>&1 | head -1`, its reader gone before irimi's first line, an error on stderr: 141 too.
    refused = [
        ("compare", a, "absent", "--store", root),
        ("runs", "show", "absent", "--store", root),
    ]
    for argv in refused:
        write = _closed_pipe()
        try:
            ended = _irimi_process(half, *argv, stdout=write, stderr=write)
        finally:
            os.close(write)
        assert ended.returncode == 141, argv


# A command `irimi shadow` runs that says it has started, then waits for the test to say go, and
# exits 5: the test closes irimi's stdout between the two. W1's agent cannot be it here: its fake
# internet resolves names only in the test's own process.
WAITING_COMMAND = """import pathlib, sys, time
started, go = map(pathlib.Path, sys.argv[1:])
started.write_text("")
while not go.exists():
    time.sleep(0.02)
raise SystemExit(5)
"""


def test_irimi_shadow_whose_reader_goes_away_ends_quietly_with_its_run_stored(
    shared_store_run, tmp_path
):
    """`irimi shadow` whose stdout reader goes away exits 141 quietly, storing a started run.

    Gone before the banner: the command never starts and no run is stored. Gone mid-run: the run
    ends with the command's exit code, the engine stops and the store closes; other runs are
    untouched."""
    half = shared_store_run(W, "half_refund")
    copy = tmp_path / "store"
    shutil.copytree(half.store_root(), copy)
    before = {r.run_id for r in half.stored().list_runs()}
    started, go = tmp_path / "started", tmp_path / "go"
    shadow = ["shadow", "--port", "0", "--store", str(copy), "--", sys.executable, "-c"]
    shadow += [WAITING_COMMAND, str(started), str(go)]
    write = _closed_pipe()
    try:
        ended = _irimi_process(half, *shadow, stdout=write)
    finally:
        os.close(write)
    assert (ended.returncode, ended.stderr, started.exists()) == (141, "", False)
    stored = _irimi_process(half, "runs", "list", "--store", str(copy), "--limit", "1000")
    assert {line.split()[0] for line in stored.stdout.splitlines()[1:]} == before
    proc = subprocess.Popen(
        [sys.executable, "-m", "irimi", *shadow],
        env={**os.environ, paths.IRIMI_HOME_ENV: str(half.home)},
        cwd=half.cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert proc.stdout is not None and proc.stdout.readline().startswith("irimi shadow · run ")
        while not started.exists() and proc.poll() is None:
            time.sleep(0.02)
        proc.stdout.close()
        go.write_text("")
        _, err = proc.communicate(timeout=60)
    finally:
        if proc.poll() is None:
            proc.kill()
    assert (proc.returncode, err) == (141, "")
    listed = _irimi_process(half, "runs", "list", "--store", str(copy), "--limit", "1000")
    [new] = [line for line in listed.stdout.splitlines()[1:] if line.split()[0] not in before]
    assert re.split(r" {2,}", new)[3:5] == ["error (exit 5)", "process"], new
    shown = _irimi_process(half, "runs", "show", new.split()[0], "--store", str(copy))
    assert "exit code: 5" in shown.stdout.splitlines(), shown.stdout


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a directory whatever its mode")
def test_a_run_irimi_may_not_read_is_named_unreadable_not_missing(shared_store_run, tmp_path):
    """A run irimi may not read is named `could not be read`, not missing.

    `runs show` and `compare` exit 2 and say why; `runs list` warns that it skips it, and warns
    of a store directory it may not read."""
    full = shared_store_run(W, "full_refund")
    half = shared_store_run(W, "half_refund")
    a, b = (result.one("run.start")["run"] for result in (full, half))
    copy = tmp_path / "store"
    shutil.copytree(half.store_root(), copy)
    shut = copy / "runs" / b
    why = f"[Errno 13] Permission denied: '{shut}/run.json'"
    others = [r.run_id for r in half.stored().list_runs() if r.run_id != b]
    shut.chmod(0)
    try:
        refused = [f"error: run {b} could not be read: {why}"]
        assert half.runs("show", b, "--store", str(copy)) == (2, [], refused)
        assert half.command("compare", a, b, "--store", str(copy)) == (2, [], refused)
        # Every run: the store is the session's, which other tests record into.
        listed = _irimi_process(half, "runs", "list", "--store", str(copy), "--limit", "1000")
    finally:
        shut.chmod(0o700)
    assert (listed.returncode, listed.stderr) == (0, f"irimi: skipping stored run {b}: {why}\n")
    assert [line.split()[0] for line in listed.stdout.splitlines()[1:]] == others
    assert a in others
    # The store itself: `runs list` says why it could not list it, and `runs show` that a run in
    # it could not be read.
    copy.chmod(0)
    try:
        assert half.runs("list", "--store", str(copy)) == (
            2,
            [],
            [f"error: --store {copy}: permission denied"],
        )
        why = f"[Errno 13] Permission denied: '{copy / 'runs' / a / 'run.json'}'"
        assert half.runs("show", a, "--store", str(copy)) == (
            2,
            [],
            [f"error: run {a} could not be read: {why}"],
        )
    finally:
        copy.chmod(0o700)


# ------------------------------------------------------------------------------- replay

LLM = ("llm#1", "llm#2", "llm#3", "llm#4")
# What `handle_ticket` returns for the full refund: four model calls, one refund, and the model's
# last word.
FULL_REFUND_RETURNED = {"turns": 4, "refund_statuses": [200], "final": "Ticket resolved."}
LOOKUP_TOOL = f"{AGENT}:lookup_customer"
REPLY_TOOL = f"{AGENT}:send_reply"


def test_a_full_refund_replayed_unchanged_is_answered_from_its_recording(replayed):
    """`irimi replay <run>` calls the trigger again and answers every call from the recording.

    `irimi.replay.child` imports `handle_ticket` and revives its `Ticket`; the trigger walks the
    same loop and returns the same result, and nothing reaches the fake internet. `main()` never
    runs."""
    recorded, replay = replayed(W, "full_refund")
    assert check_invariants(replay) == []
    assert replay.exit_code == recorded.exit_code == 0
    assert replay.agent_exit_code() == 0
    assert replay.returned() == recorded.returned() == FULL_REFUND_RETURNED
    # The ticket it was called with is a `Ticket` again, revived from its capture.
    assert replay.one("run.start")["called_with"] == {"ticket": "Ticket"}
    assert recorded.one("run.start")["called_with"] == {"ticket": "Ticket"}
    assert not (replay.state / DB_NAME).exists()
    assert replay.answered() == [
        ("llm#1", 200, "recording"),
        ("get_charge#1", 200, "recording"),
        ("llm#2", 200, "recording"),
        (REFUND, 200, "fake-L1"),
        ("llm#3", 200, "recording"),
        ("llm#4", 200, "recording"),
    ]
    [refund] = [ex for ex in replay.reported if ex.kind == "write"]
    assert refund.flags == ("from-recording",)
    assert refund.precondition.outcome == "passed"
    # The refund's minted id is the recorded one, so every later call naming it matched.
    [recorded_refund] = [ex for ex in recorded.reported if ex.kind == "write"]
    assert refund.response.body == recorded_refund.response.body
    # No precondition read either: the write was checked when it was recorded.
    assert [ex for ex in replay.reported if ex.issued_by == "engine"] == []
    assert replay.internet.requests() == []
    # The replay is one run, its process run, which the trigger adopted, and it says what
    # it is.
    source = recorded.one("run.start")["run"]
    assert replay.one("run.start")["run"] == replay.process_run_id()
    records = [r for r in replay.stored().list_runs() if r.replay_of == source]
    assert [(r.run_id, r.mode, r.attribution) for r in records] == [
        (replay.process_run_id(), "replay", "process")
    ]
    assert records[0].trigger == recorded.stored().load_run(source).record.trigger
    summary = replay.summaries()
    # `runs show` opens a replay's stored summary `irimi replay`, its recording's `irimi shadow`.
    assert summary[0].startswith(f"irimi replay · run {replay.process_run_id()} · 6 exchanges · ")
    _, shown, _ = replay.runs("show", source, "--store", str(replay.store_root()))
    assert [line for line in shown if line.startswith("irimi ")] == [
        next(line for line in shown if line.startswith(f"irimi shadow · run {source} · "))
    ]
    assert "  api.stripe.com     1 read (1 from the recording)  1 faked write" in summary
    assert "  api.anthropic.com  4 llm (4 from the recording)" in summary
    assert f"  ○ stripe  refund $49.00 on ch_TICKET1  {PASSED}" in summary
    # The tools the proxy cannot see: the customer comes from the recording, so the real SQLite
    # read never runs, and the reply runs its fake.
    assert recorded.tools() == [(LOOKUP_TOOL, "real"), (REPLY_TOOL, "shadow")]
    assert replay.tools() == [(LOOKUP_TOOL, "recording"), (REPLY_TOOL, "shadow")]
    assert replay.tool_call_lines() == [
        f"tool-recorded read      {LOOKUP_TOOL} -> ok",
        f"tool-fake     write     {REPLY_TOOL} -> ok",
    ]
    # What `irimi replay` says of it: every call answered, and the same two writes.
    assert replay.replay_result() == [
        f"irimi replay · {source} -> {replay.process_run_id()}",
        "  agent exited 0",
        "  no divergence: every call was answered from the recording",
        "  7 from the recording · 0 missed",
        "",
        f"  = ○ stripe  refund $49.00 on ch_TICKET1  {PASSED}",
        f"  = ○ tool  {REPLY_TOOL}  fake ran instead",
        "",
        "  2 same · 0 changed · 0 only in A · 0 only in B",
    ]


def test_a_full_refund_replayed_without_its_database_reads_the_customer_from_the_recording(
    replayed,
):
    """A replay by command with the database gone reads the customer from the recording.

    A real `lookup_customer` would raise `no such table`; the database file is never opened, and
    the reply runs its fake."""
    recorded, replay = replayed(W, "full_refund", {DB_LOST_ENV: "1"}, command=True)
    assert check_invariants(replay) == []
    assert (recorded.state / DB_NAME).exists()
    assert not (replay.state / DB_NAME).exists()
    assert replay.exit_code == recorded.exit_code == replay.agent_exit_code() == 0
    assert replay.returned() == recorded.returned()
    assert replay.tools() == [(LOOKUP_TOOL, "recording"), (REPLY_TOOL, "shadow")]
    [lookup, _] = [e for e in replay.handed_events if isinstance(e, ToolCall)]
    [recorded_lookup, _] = [e for e in recorded.handed_events if isinstance(e, ToolCall)]
    assert (
        lookup.result == recorded_lookup.result == {"id": CUSTOMER, "name": "Ada", "tier": "gold"}
    )
    assert replay.internet.requests() == []
    # And `irimi replay` says so: no divergence, the same two writes.
    source = recorded.one("run.start")["run"]
    assert replay.replay_result() == [
        f"irimi replay · {source} -> {replay.process_run_id()}",
        "  agent exited 0",
        "  no divergence: every call was answered from the recording",
        "  7 from the recording · 0 missed",
        "",
        f"  = ○ stripe  refund $49.00 on ch_TICKET1  {PASSED}",
        f"  = ○ tool  {REPLY_TOOL}  fake ran instead",
        "",
        "  2 same · 0 changed · 0 only in A · 0 only in B",
    ]


def test_a_full_refund_replayed_with_the_half_prompt_diverges_at_the_first_llm_call(replayed):
    """`PROMPT=half` changes the system prompt, so the replay diverges at the first llm call.

    Every later llm call misses too. The charge read is still the recording's; the refund of 2450
    is faked and checked against the recording's read of the charge, never the fake Stripe's."""
    recorded, replay = replayed(W, "full_refund", {"PROMPT": "half"})
    assert check_invariants(replay) == []
    assert replay.exit_code == 1
    assert replay.answered() == [
        ("llm#1", 200, None),
        ("get_charge#1", 200, "recording"),
        ("llm#2", 200, None),
        (REFUND, 200, "fake-L1"),
        ("llm#3", 200, None),
        ("llm#4", 200, None),
    ]
    assert replay.exchange_lines()[0] == (
        "real          llm       POST api.anthropic.com/v1/messages -> 200  [replay-miss]"
    )
    [refund] = [ex for ex in replay.reported if ex.kind == "write"]
    assert refund.flags == ("replay-miss",)
    assert refund.precondition.outcome == "passed"
    [check] = [ex for ex in replay.reported if ex.issued_by == "engine"]
    assert (check.answered_by, check.request.path) == ("recording", "/v1/charges/ch_TICKET1")
    assert check.flags == ("from-recording",)
    assert replay.internet.requests("api.stripe.com") == []
    assert len(replay.internet.requests("api.anthropic.com")) == 4
    assert f"  ○ stripe  refund $24.50 on ch_TICKET1  {PASSED}" in (replay.summaries())
    assert replay.returned()["refund_statuses"] == recorded.returned()["refund_statuses"] == [200]
    # The customer is looked up before any write misses, so the recording still answers it. The
    # reply's text changed with the prompt, and its fake runs all the same.
    assert replay.tools() == [(LOOKUP_TOOL, "recording"), (REPLY_TOOL, "shadow")]
    # The trigger alone never seeds the helpdesk database, so no reply could have been written;
    # the replay by its command below writes none to the one its `main()` seeds.
    assert not (replay.state / DB_NAME).exists()
    # What `irimi replay` says of it: where it diverged, the first llm call, the refund it
    # changed, and the reply, whose line names no args but whose text changed with the refund.
    source = recorded.one("run.start")["run"]
    assert replay.replay_result() == [
        f"irimi replay · {source} -> {replay.process_run_id()}",
        "  agent exited 0",
        "  diverged at the agent's 1st call: real          llm       "
        "POST api.anthropic.com/v1/messages -> 200  [replay-miss]",
        "  2 from the recording · 5 missed",
        "",
        f"  ~ A ○ stripe  refund $49.00 on ch_TICKET1  {PASSED}",
        f"    B ○ stripe  refund $24.50 on ch_TICKET1  {PASSED}",
        *reply_changed("Refund of 2450 requested (statuses: 200)."),
        "",
        "  0 same · 2 changed · 0 only in A · 0 only in B",
    ]
    # The result file says the same: the divergence point's line, and the comparison's exit.
    assert replay.replay is not None
    assert (replay.replay.exit_code, replay.replay.diverged_at) == (1, FIRST_CALL)


def test_the_half_prompt_replayed_by_its_command_writes_no_reply_to_the_seeded_database(replayed):
    """A changed-prompt replay by command fakes its writes and leaves the seeded database alone.

    `main()` seeds the database; the refund changes to `$24.50`, nothing reaches the fake Stripe,
    and the reply runs its fake, so `replies` stays empty."""
    recorded, replay = replayed(W, "full_refund", {"PROMPT": "half"}, command=True)
    assert check_invariants(replay) == []
    assert (replay.exit_code, replay.agent_exit_code()) == (1, 0)
    assert (replay.state / DB_NAME).exists()
    assert replies(replay) == []
    assert replay.tools() == [(LOOKUP_TOOL, "recording"), (REPLY_TOOL, "shadow")]
    assert replay.internet.requests("api.stripe.com") == []
    source = recorded.one("run.start")["run"]
    assert replay.replay_result() == [
        f"irimi replay · {source} -> {replay.process_run_id()}",
        "  agent exited 0",
        "  diverged at the agent's 1st call: real          llm       "
        "POST api.anthropic.com/v1/messages -> 200  [replay-miss]",
        "  2 from the recording · 5 missed",
        "",
        f"  ~ A ○ stripe  refund $49.00 on ch_TICKET1  {PASSED}",
        f"    B ○ stripe  refund $24.50 on ch_TICKET1  {PASSED}",
        *reply_changed("Refund of 2450 requested (statuses: 200)."),
        "",
        "  0 same · 2 changed · 0 only in A · 0 only in B",
    ]


# What each scenario W1 goes further with prints replayed unchanged:
# `irimi replay`'s whole result, but its first line, which names the two runs.
UNCHANGED = {
    # One refund sent twice under one key: both meet their recordings, the retry as the
    # idempotency store's answer, and compare shows one refund, as the recording's summary did.
    "retried_refund": (
        0,
        [
            "  agent exited 0",
            "  no divergence: every call was answered from the recording",
            "  8 from the recording · 0 missed",
            "",
            f"  = {FULL}",
            f"  = {REPLY}",
            "",
            "  2 same · 0 changed · 0 only in A · 0 only in B",
        ],
    ),
    # The refund the check rejected is answered with its recorded rejection, and compares `=`.
    "double_refund": (
        0,
        [
            "  agent exited 0",
            "  no divergence: every call was answered from the recording",
            "  9 from the recording · 0 missed",
            "",
            f"  = {FULL}",
            "  = ✗ stripe  refund $49.00 on ch_TICKET1  faked; would fail: charge_already_refunded",
            f"  = {REPLY}",
            "",
            "  3 same · 0 changed · 0 only in A · 0 only in B",
        ],
    ),
    # Six model calls answered from the recording, and the trigger raising as recorded: the child
    # exits 1, which never decides the replay's exit, and there is no write to compare.
    "runaway_loop": (
        0,
        [
            "  agent exited 1",
            "  no divergence: every call was answered from the recording",
            "  12 from the recording · 0 missed",
            "",
            "  0 same · 0 changed · 0 only in A · 0 only in B",
        ],
    ),
}


@pytest.mark.parametrize("scenario", UNCHANGED)
def test_each_further_scenario_replayed_unchanged_does_not_diverge(replayed, scenario):
    """`retried_refund`, `double_refund` and `runaway_loop` replay unchanged with no divergence.

    Nothing reaches the fake internet, each trigger ends as recorded, and every write is the
    recording's."""
    recorded, replay = replayed(W, scenario)
    assert check_invariants(replay) == []
    assert replay.internet.requests() == []
    source = recorded.one("run.start")["run"]
    code, lines = UNCHANGED[scenario]
    assert replay.replay_result()[0] == (f"irimi replay · {source} -> {replay.process_run_id()}")
    assert (replay.exit_code, replay.replay_result()[1:]) == (code, lines)
    assert [e["outcome"] for e in replay.events("run.end")] == [
        e["outcome"] for e in recorded.events("run.end")
    ]


# Where a replay of a W1 run with another prompt first misses its recording: the first model call,
# since the system prompt is in every call.
FIRST_CALL = (
    "the agent's 1st call: real          llm       POST api.anthropic.com/v1/messages -> 200  "
    "[replay-miss]"
)


# What each of those replays says of its calls: how many the recording answered, and how many it
# missed, every model call among them.
EDITED_COUNTS = {
    "half_refund": "2 from the recording · 5 missed",
    "too_large": "2 from the recording · 5 missed",
    "double_refund": "3 from the recording · 6 missed",
    "retried_refund": "3 from the recording · 5 missed",
    "runaway_loop": "1 from the recording · 11 missed",
}


def _ending(result):
    """How the trigger's run ended, as the agent logged it: its outcome and its error's type."""
    return [(e["outcome"], e.get("error")) for e in result.events("run.end")]


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("scenario", COMPARED)
def test_the_full_refund_replayed_with_each_prompt_shows_what_compare_shows_of_its_run(
    run_workflow, replayed, scenario, mode
):
    """`full_refund` replayed with each `COMPARED` prompt shows what compare shows of the runs.

    It holds for a recording made under serve as under shadow. Each replay diverges at the first
    model call; only missed reads reach the fake Stripe, the reply runs its fake, and the trigger
    ends as that prompt's recording did."""
    prompt = workflows()[W].scenarios[scenario].env["PROMPT"]
    recorded, replay = replayed(W, "full_refund", {"PROMPT": prompt}, mode=mode)
    own = run_workflow(W, scenario, mode)
    assert check_invariants(replay) == []
    source = recorded.one("run.start")["run"]
    code, rows = COMPARED[scenario]
    ended = _ending(own)
    assert replay.replay_result() == [
        f"irimi replay · {source} -> {replay.process_run_id()}",
        f"  agent exited {0 if ended == [('ok', None)] else 1}",
        f"  diverged at {FIRST_CALL}",
        f"  {EDITED_COUNTS[scenario]}",
        "",
        *rows,
    ]
    assert replay.exit_code == code
    assert _ending(replay) == ended
    assert [(r.method, r.path) for r in replay.internet.writes()] == []
    assert {r.method for r in replay.internet.requests("api.stripe.com")} <= {"GET"}
    # The customer is read from the recording, there being no database, and the reply's fake ran;
    # the runaway prompt's model only reads the charge, and calls neither tool.
    called = [(LOOKUP_TOOL, "recording"), (REPLY_TOOL, "shadow")]
    assert replay.tools() == ([] if scenario == "runaway_loop" else called)
    assert not (replay.state / DB_NAME).exists()


# Each scenario replayed with `full_refund`'s prompt, as (exit, calls answered and missed, rows):
# `COMPARED` the other way round, the scenario's run now A, but for what a replay can know of a
# write its recording never made.
RESTORED = {
    "half_refund": (
        1,
        "2 from the recording · 5 missed",
        [
            f"  ~ A ○ stripe  refund $24.50 on ch_TICKET1  {PASSED}",
            f"    B {FULL}",
            *reply_changed(SENT, was="Refund of 2450 requested (statuses: 200)."),
            "",
            "  0 same · 2 changed · 0 only in A · 0 only in B",
        ],
    ),
    "too_large": (
        1,
        "2 from the recording · 5 missed",
        [
            "  ~ A ✗ stripe  refund $98.00 on ch_TICKET1  faked; would fail: amount_too_large",
            f"    B {FULL}",
            *reply_changed(SENT, was="Refund of 9800 requested (statuses: 400)."),
            "",
            "  0 same · 2 changed · 0 only in A · 0 only in B",
        ],
    ),
    "double_refund": (
        1,
        "3 from the recording · 4 missed",
        [
            f"  = {FULL}",
            "  - ✗ stripe  refund $49.00 on ch_TICKET1  faked; would fail: charge_already_refunded",
            *reply_changed(SENT, was="Refund of 4900 requested (statuses: 200, 400)."),
            "",
            "  1 same · 1 changed · 1 only in A · 0 only in B",
        ],
    ),
    # The retry is still one write, and the reply does not count it.
    "retried_refund": (
        1,
        "3 from the recording · 4 missed",
        [
            f"  = {FULL}",
            *reply_changed(SENT, was="Refund of 4900 requested (statuses: 200, 200)."),
            "",
            "  1 same · 1 changed · 0 only in A · 0 only in B",
        ],
    ),
    # Both writes are new. The recording holds no check read of the charge and a replay reads no
    # live state for one, so the refund could not be checked; with no currency known, its amount
    # prints as minor units.
    "runaway_loop": (
        1,
        "1 from the recording · 6 missed",
        [
            f"  + ○ stripe  refund 4900 (minor units, currency unknown) on ch_TICKET1  {UNCHECKED}",
            f"  + {REPLY}",
            "",
            "  0 same · 0 changed · 0 only in A · 2 only in B",
        ],
    ),
}


@pytest.mark.parametrize("scenario", RESTORED)
def test_each_scenario_replayed_with_the_full_refund_s_prompt_shows_the_writes_it_changes(
    replayed, scenario
):
    """Each `COMPARED` run replayed with `full_refund`'s prompt shows the writes the edit changes.

    Replayed by command, so `main()` seeds the database `runaway_loop`'s new lookup reads for
    real. Each diverges at the first model call and ends `ok`; only missed reads reach the fake
    Stripe, and `replies` stays empty."""
    recorded, replay = replayed(W, scenario, {"PROMPT": "full"}, command=True)
    assert check_invariants(replay) == []
    source = recorded.one("run.start")["run"]
    code, counts, rows = RESTORED[scenario]
    assert replay.replay_result() == [
        f"irimi replay · {source} -> {replay.process_run_id()}",
        "  agent exited 0",
        f"  diverged at {FIRST_CALL}",
        f"  {counts}",
        "",
        *rows,
    ]
    assert (replay.exit_code, replay.agent_exit_code()) == (code, 0)
    assert _ending(replay) == [("ok", None)]
    assert replay.returned() == FULL_REFUND_RETURNED
    assert replay.internet.writes() == []
    assert replies(replay) == []
    looked_up = "real" if scenario == "runaway_loop" else "recording"
    assert replay.tools() == [(LOOKUP_TOOL, looked_up), (REPLY_TOOL, "shadow")]


def test_a_full_refund_recorded_under_serve_is_replayed_as_one_recorded_under_shadow(replayed):
    """A run recorded under `irimi serve` replays as the shadow recording does: `no divergence`.

    `runs list` lists serve's run as `sdk`, with no process run, and the replay as `replay of`
    it."""
    recorded, replay = replayed(W, "full_refund", mode="serve")
    _, by_shadow = replayed(W, "full_refund")
    assert check_invariants(replay) == []
    assert replay.internet.requests() == []
    source = recorded.one("run.start")["run"]
    assert replay.replay_result()[0] == (f"irimi replay · {source} -> {replay.process_run_id()}")
    assert replay.replay_result()[1:] == by_shadow.replay_result()[1:]
    assert replay.replay_result()[2] == (
        "  no divergence: every call was answered from the recording"
    )
    assert (replay.exit_code, replay.agent_exit_code()) == (0, 0)
    assert replay.returned() == recorded.returned() == FULL_REFUND_RETURNED
    assert replay.tools() == [(LOOKUP_TOOL, "recording"), (REPLY_TOOL, "shadow")]
    assert not (replay.state / DB_NAME).exists()
    assert replay.listed_attributions() == {
        source: "sdk",
        replay.process_run_id(): f"replay of {source}",
    }


def test_sdk_replay_from_a_test_returns_the_result_irimi_replay_writes(
    run_workflow, replayed, tmp_path
):
    """`testing.replay(run_id)`, with or without the call, returns what `--result-file` holds.

    But for the replay's own run id; each replay holds the invariants and sends nothing out."""
    recorded, by_cli = replayed(W, "full_refund")
    expected = by_cli.replay
    assert expected is not None and (expected.exit_code, expected.diverged_at) == (0, None)
    run_id = recorded.one("run.start")["run"]
    ticket = Ticket("T-1001", "ch_TICKET1", CUSTOMER, "I was charged twice for the same order")
    for i, call in enumerate([(), (handle_ticket, ticket)]):
        by_sdk = sdk_replay(recorded, run_id, tmp_path / f"sdk{i}", *call)
        # Its `irimi` ran in this process, where the fake internet resolves and the harness
        # watches the engine: it saw every exchange the command's replay did.
        assert len(by_sdk.reported) == len(by_cli.reported) == 6
        assert check_invariants(by_sdk) == []
        assert by_sdk.internet.requests() == []
        got = by_sdk.replay
        assert got is not None and got.replay_run_id not in (None, expected.replay_run_id)
        lines = tuple(line.replace(got.replay_run_id, expected.replay_run_id) for line in got.lines)
        assert dataclasses.replace(got, replay_run_id=expected.replay_run_id, lines=lines) == (
            expected
        )


# The ticket `full_refund` recorded, captured as `irimi replay --args` takes it, with
# another message: what a developer passes to replay the run with other parameters.
OTHER_TICKET: dict[str, JSONValue] = {
    "ticket": {
        "__irimi_type__": f"{AGENT}:Ticket",
        "value": {
            "ticket_id": "T-1001",
            "charge": "ch_TICKET1",
            "customer": CUSTOMER,
            "message": "Please refund",
        },
    }
}


def test_a_full_refund_replayed_with_other_args_says_its_args_differ(replayed):
    """`--args` replaces the recorded args: the replay diverges, but compares the same, exit 0.

    The ticket's message is in the first model call, which misses. The refund is the recorded
    one, so the writes compare the same while the result says the args differ."""
    recorded, replay = replayed(W, "full_refund", options=("--args", json.dumps(OTHER_TICKET)))
    source = recorded.one("run.start")["run"]
    assert replay.replay_result() == [
        f"irimi replay · {source} -> {replay.process_run_id()}",
        "  agent exited 0",
        "  (the triggers' args differ)",
        "  diverged at the agent's 1st call: real          llm       "
        "POST api.anthropic.com/v1/messages -> 200  [replay-miss]",
        "  3 from the recording · 4 missed",
        "",
        f"  = ○ stripe  refund $49.00 on ch_TICKET1  {PASSED}",
        f"  = ○ tool  {REPLY_TOOL}  fake ran instead",
        "",
        "  2 same · 0 changed · 0 only in A · 0 only in B",
    ]
    assert (replay.exit_code, replay.agent_exit_code()) == (0, 0)
    record = replay.stored().load_run(replay.process_run_id()).record
    assert record.trigger == Trigger(
        f"{AGENT}:handle_ticket", f"{AGENT}:handle_ticket", OTHER_TICKET, True
    )
    assert replay.returned() == FULL_REFUND_RETURNED
    assert replay.internet.requests("api.stripe.com") == []
    assert len(replay.internet.requests("api.anthropic.com")) == 4
    assert check_invariants(replay) == []


def test_a_call_of_another_function_is_replayed_in_place_of_the_trigger(replayed):
    """`--entrypoint` and `--args` call another function in the trigger's place.

    Here `lookup_customer`: the recording answers it, the two writes are only in A, and the
    replay exits 1. The replay's run is named `module:lookup_customer`, as `@sdk.trigger` names a
    run."""
    options = (
        "--entrypoint",
        f"{AGENT}:lookup_customer",
        "--args",
        json.dumps({"customer": CUSTOMER}),
    )
    recorded, replay = replayed(W, "full_refund", options=options)
    source = recorded.one("run.start")["run"]
    assert replay.replay_result() == [
        f"irimi replay · {source} -> {replay.process_run_id()}",
        "  agent exited 0",
        "  (the triggers' args differ)",
        "  no divergence, but 6 of the recording's calls were never made",
        "  1 from the recording · 0 missed",
        "",
        f"  - ○ stripe  refund $49.00 on ch_TICKET1  {PASSED}",
        f"  - ○ tool  {REPLY_TOOL}  fake ran instead",
        "",
        "  0 same · 0 changed · 2 only in A · 0 only in B",
    ]
    assert (replay.exit_code, replay.agent_exit_code()) == (1, 0)
    assert replay.tools() == [(LOOKUP_TOOL, "recording")]
    assert replay.events("run.start") == []
    record = replay.stored().load_run(replay.process_run_id()).record
    assert record.trigger == Trigger(
        f"{AGENT}:lookup_customer", f"{AGENT}:lookup_customer", {"customer": CUSTOMER}, True
    )
    assert replay.internet.requests() == []
    assert check_invariants(replay) == []


def test_a_full_refund_replayed_under_the_agent_s_python_is_answered_from_its_recording(
    replayed, tmp_path, capfd
):
    """`--python` replays under the agent's Python when irimi's cannot import the trigger.

    Under irimi's Python the module does not import: the call is never made, the replay exits 2,
    and the hint names the Python used and the flag. With `--python agent-python` the replay is
    the unchanged one, exit 0."""
    python = tmp_path / "agent-python"
    python.write_text(
        f"#!/bin/sh\nexport PYTHONPATH={shlex.quote(str(REPO_ROOT))}\n"
        f'exec {shlex.quote(sys.executable)} "$@"\n'
    )
    python.chmod(0o755)
    no_path = {"PYTHONPATH": ""}
    recorded, _ = replayed(W, "full_refund")
    source = recorded.one("run.start")["run"]
    capfd.readouterr()
    alone = replay(recorded, source, tmp_path / "alone", no_path)
    hint = (
        f"hint: the trigger was called under irimi's own Python, {sys.executable}, which may not "
        "have the agent's packages; name the agent's Python with --python PATH"
    )
    assert capfd.readouterr().err.splitlines()[-1:] == [hint]
    # The result is the error and the hint, once each, and no comparison: the agent never ran.
    assert alone.replay_result() == [
        f"error: cannot call {AGENT}:handle_ticket: importing {AGENT} raised "
        "ModuleNotFoundError: No module named 'tests'",
        hint,
    ]
    assert (alone.exit_code, alone.agent_exit_code()) == (2, 126)
    assert alone.events("run.start") == []
    assert alone.internet.requests() == []
    agent = replay(recorded, source, tmp_path / "agent", no_path, ("--python", str(python)))
    assert "hint: " not in capfd.readouterr().err
    assert agent.replay_result() == [
        f"irimi replay · {source} -> {agent.process_run_id()}",
        "  agent exited 0",
        "  no divergence: every call was answered from the recording",
        "  7 from the recording · 0 missed",
        "",
        f"  = ○ stripe  refund $49.00 on ch_TICKET1  {PASSED}",
        f"  = ○ tool  {REPLY_TOOL}  fake ran instead",
        "",
        "  2 same · 0 changed · 0 only in A · 0 only in B",
    ]
    assert (agent.exit_code, agent.agent_exit_code()) == (0, 0)
    assert agent.returned() == FULL_REFUND_RETURNED
    assert agent.tools() == [(LOOKUP_TOOL, "recording"), (REPLY_TOOL, "shadow")]
    assert agent.internet.requests() == []
    assert check_invariants(agent) == []


@pytest.mark.parametrize(
    ("prompt", "again"),
    [
        ("full", {}),
        ("half", {"PROMPT": "half"}),
    ],
)
def test_a_replay_is_replayed_by_the_trigger_it_called(replayed, tmp_path, prompt, again):
    """A replay's run keeps the trigger it replayed, so `irimi replay` can call it again.

    Each second replay is answered whole from the first's run, whose live model calls it
    recorded. `runs list` lists the second as a replay of the first."""
    env = {} if prompt == "full" else {"PROMPT": prompt}
    _, first = replayed(W, "full_refund", env)
    before = [r.run_id for r in first.stored().list_runs()]
    second = replay(first, first.process_run_id(), tmp_path / "again", again)
    # The second replay records into a copy of the first's store, never the first's own, which
    # the session caches for every test that reads it (`test_replays.py`'s rule 9).
    assert [r.run_id for r in first.stored().list_runs()] == before
    assert check_invariants(first) == []
    [record] = [r for r in second.stored().list_runs() if r.replay_of == first.process_run_id()]
    assert (record.run_id, record.trigger) == (
        second.process_run_id(),
        first.stored().load_run(first.process_run_id()).record.trigger,
    )
    amount = "$49.00" if prompt == "full" else "$24.50"
    assert second.replay_result() == [
        f"irimi replay · {first.process_run_id()} -> {second.process_run_id()}",
        "  agent exited 0",
        "  no divergence: every call was answered from the recording",
        "  7 from the recording · 0 missed",
        "",
        f"  = ○ stripe  refund {amount} on ch_TICKET1  {PASSED}",
        f"  = ○ tool  {REPLY_TOOL}  fake ran instead",
        "",
        "  2 same · 0 changed · 0 only in A · 0 only in B",
    ]
    assert second.exit_code == 0
    assert second.internet.requests() == []
    assert second.returned() == first.returned()
    assert check_invariants(second) == []
    # `irimi runs list` names each replay after the run it replayed, a replay of a replay after
    # the first replay, and lists the recording's runs as they were.
    source = first.replay.source_run_id
    processes = [r for r in second.stored().list_runs() if r.attribution == "process"]
    recording = [r.run_id for r in processes if r.replay_of is None]
    assert second.listed_attributions() == {
        **dict.fromkeys(recording, "process"),
        source: "sdk",
        first.process_run_id(): f"replay of {source}",
        second.process_run_id(): f"replay of {first.process_run_id()}",
    }
    assert len(recording) == 1


# The full refund's replay's run.json damaged in the ways one can be, and how it is refused: a
# `replay_of` that no run id could be, by its length and never quoted, and a `mode` and
# `replay_of` that disagree.
DAMAGED_REPLAYS = {
    "five-megabytes": ({"replay_of": "x" * 5_000_000}, "is not a run id (5000000 characters)"),
    "a-path": ({"replay_of": "../x"}, "is not a run id (4 characters)"),
    "empty": ({"replay_of": ""}, "is not a run id (0 characters)"),
    "two-spaces": ({"replay_of": "a  b"}, "is not a run id (4 characters)"),
    "names-no-run": ({"replay_of": None}, "'mode' is 'replay', but 'replay_of' is null"),
    "a-shadow-run": ({"mode": "shadow"}, "'replay_of' names a run, but 'mode' is 'shadow'"),
}


@pytest.mark.parametrize("damage", DAMAGED_REPLAYS)
def test_a_replay_whose_run_json_names_no_run_it_replayed_is_refused(replayed, tmp_path, damage):
    """A replay with a damaged run.json is skipped by `runs list` and unreadable to show or replay.

    `runs list` warns naming the file and why, and lists the other runs as before."""
    _, replay = replayed(W, "full_refund")
    run = replay.process_run_id()
    copy = tmp_path / "store"
    shutil.copytree(replay.store_root(), copy)
    record = copy / "runs" / run / "run.json"
    changes, why = DAMAGED_REPLAYS[damage]
    record.write_text(json.dumps(json.loads(record.read_text()) | changes))
    refused = f"{record}: {why}" if why.startswith("'") else f"{record}: 'replay_of' {why}"
    damaged = dataclasses.replace(replay, store=copy)
    # In this process the store's warning goes to pytest's log capture, not to stderr, so
    # `listed_attributions` sees none; `runs list` in a process of its own, last, prints it.
    listed = replay.listed_attributions()
    assert listed[run].startswith("replay of ")
    assert damaged.listed_attributions() == {r: a for r, a in listed.items() if r != run}
    shown = damaged.runs("show", run, "--store", str(copy))
    assert shown == (2, [], [f"error: run {run} could not be read: {refused}"])
    assert damaged.command("replay", run, "--store", str(copy)) == (
        2,
        [],
        [f"error: run {run} could not be read: {refused}"],
    )
    warned = _irimi_process(replay, "runs", "list", "--store", str(copy))
    assert (warned.returncode, warned.stderr) == (
        0,
        f"irimi: skipping stored run {run}: {refused}\n",
    )


def test_a_run_recorded_into_a_shared_store_is_replayed_into_a_copy_of_it(
    shared_store_run, tmp_path
):
    """A run recorded with `--store` replays into a copy of that store, leaving the shared one."""
    recorded = shared_store_run(W, "full_refund")
    assert recorded.store is not None
    before = sorted(r.run_id for r in recorded.stored().list_runs())
    source = recorded.one("run.start")["run"]
    again = replay(recorded, source, tmp_path / "replay")
    assert sorted(r.run_id for r in recorded.stored().list_runs()) == before
    assert again.store_root() != recorded.store_root()
    assert sorted(r.run_id for r in again.stored().list_runs()) == sorted(
        [*before, again.process_run_id()]
    )
    assert again.replay_result()[2] == "  no divergence: every call was answered from the recording"
    assert check_invariants(again) == []


def test_the_half_prompt_s_replay_replayed_with_the_full_prompt_diverges_back(replayed, tmp_path):
    """The half prompt's replay, replayed with the full prompt, refunds `$49.00` again."""
    _, first = replayed(W, "full_refund", {"PROMPT": "half"})
    before = [r.run_id for r in first.stored().list_runs()]
    second = replay(first, first.process_run_id(), tmp_path / "back")
    assert [r.run_id for r in first.stored().list_runs()] == before
    assert second.replay_result() == [
        f"irimi replay · {first.process_run_id()} -> {second.process_run_id()}",
        "  agent exited 0",
        "  diverged at the agent's 1st call: real          llm       "
        "POST api.anthropic.com/v1/messages -> 200  [replay-miss]",
        "  2 from the recording · 5 missed",
        "",
        f"  ~ A ○ stripe  refund $24.50 on ch_TICKET1  {PASSED}",
        f"    B ○ stripe  refund $49.00 on ch_TICKET1  {PASSED}",
        *reply_changed(SENT, was="Refund of 2450 requested (statuses: 200)."),
        "",
        "  0 same · 2 changed · 0 only in A · 0 only in B",
    ]
    assert second.exit_code == 1
    assert second.internet.requests("api.stripe.com") == []
    assert second.returned() == FULL_REFUND_RETURNED
    assert check_invariants(second) == []


def test_a_full_refund_replayed_with_its_tool_results_changed_diverges_mid_conversation(replayed):
    """A code edit to tool results diverges at the second model call and changes no write.

    `TOOL_RESULT_TAG` adds a field to each tool result; the first call carries none and is
    answered from the recording. Given the same facts the model walks the same loop, so the
    refund meets its recording."""
    recorded, replay = replayed(W, "full_refund", {RESULT_TAG_ENV: "v2"})
    assert check_invariants(replay) == []
    assert replay.answered() == [
        ("llm#1", 200, "recording"),
        ("get_charge#1", 200, "recording"),
        ("llm#2", 200, None),
        (REFUND, 200, "fake-L1"),
        ("llm#3", 200, None),
        ("llm#4", 200, None),
    ]
    assert len(replay.internet.requests("api.anthropic.com")) == 3
    assert replay.internet.requests("api.stripe.com") == []
    [refund] = [ex for ex in replay.reported if ex.kind == "write"]
    assert refund.flags == ("from-recording",)
    assert replay.tools() == [(LOOKUP_TOOL, "recording"), (REPLY_TOOL, "shadow")]
    assert replay.returned() == recorded.returned()
    # What `irimi replay` says of it: where it diverged, the second model call, and exit 0, the
    # writes being the same.
    source = recorded.one("run.start")["run"]
    assert replay.exit_code == 0
    assert replay.replay_result() == [
        f"irimi replay · {source} -> {replay.process_run_id()}",
        "  agent exited 0",
        "  diverged at the agent's 4th call: real          llm       "
        "POST api.anthropic.com/v1/messages -> 200  [replay-miss]",
        "  4 from the recording · 3 missed",
        "",
        f"  = ○ stripe  refund $49.00 on ch_TICKET1  {PASSED}",
        f"  = ○ tool  {REPLY_TOOL}  fake ran instead",
        "",
        "  2 same · 0 changed · 0 only in A · 0 only in B",
    ]


def test_a_retried_refund_replays_as_the_two_refunds_it_recorded(replayed):
    """A refund sent twice with one key replays as its two recorded answers.

    The key is a header and never in the match, so a new key still meets the recording."""
    recorded, replay = replayed(W, "retried_refund")
    assert check_invariants(replay) == []
    refunds = [ex for ex in replay.reported if ex.kind == "write"]
    assert [ex.flags for ex in refunds] == [
        ("from-recording",),
        ("idempotent-replay", "from-recording"),
    ]
    assert replay.internet.requests() == []
    assert replay.returned() == recorded.returned()
    assert replay.returned()["refund_statuses"] == [200, 200]


@pytest.mark.parametrize("mode", MODES)
def test_a_verified_refund_reads_the_charge_again_and_the_overlay_shows_the_refund(
    run_workflow, mode
):
    """The overlay shows a faked refund in the live re-read of the charge, as bare shows it."""
    bare = run_workflow(W, "verified_refund", "bare")
    shadow = run_workflow(W, "verified_refund", mode)
    assert shadow.result(with_time=False) == bare.result(with_time=False)
    assert shadow.answered() == [
        ("llm#1", 200, None),
        ("get_charge#1", 200, None),
        ("llm#2", 200, None),
        (REFUND, 200, "fake-L1"),
        ("llm#3", 200, None),
        ("get_charge#3", 200, "overlay"),
        ("llm#4", 200, None),
        ("llm#5", 200, None),
    ]
    [again] = [ex for ex in shadow.reported if ex.answered_by == "overlay"]
    assert json.loads(again.response.body)["amount_refunded"] == 4900


def _metadata(result) -> set[str]:
    return {
        json.loads(ex.request.body)["metadata"]["user_id"]
        for ex in result.reported
        if ex.kind == "llm"
    }


def test_a_verified_refund_replayed_unchanged_serves_the_overlaid_read_as_recorded(replayed):
    """An unchanged replay serves the overlaid read as recorded, refund and all.

    Each model call's `metadata` is new in each process but says who asked, not what, so it does
    not stop the match."""
    recorded, replay = replayed(W, "verified_refund")
    assert check_invariants(replay) == []
    assert len(_metadata(recorded)) == len(_metadata(replay)) == 1
    assert _metadata(recorded) != _metadata(replay)
    assert replay.answered() == [
        ("llm#1", 200, "recording"),
        ("get_charge#1", 200, "recording"),
        ("llm#2", 200, "recording"),
        (REFUND, 200, "fake-L1"),
        ("llm#3", 200, "recording"),
        ("get_charge#3", 200, "overlay"),
        ("llm#4", 200, "recording"),
        ("llm#5", 200, "recording"),
    ]
    [again] = [ex for ex in replay.reported if ex.answered_by == "overlay"]
    [recorded_again] = [ex for ex in recorded.reported if ex.answered_by == "overlay"]
    assert again.flags == ("from-recording",)
    assert again.response.body == recorded_again.response.body
    assert replay.internet.requests() == []


def test_a_replay_that_makes_no_refund_reads_live_where_its_recording_shows_one(replayed):
    """A replay that makes no refund misses the recorded read that showed one, and reads live.

    `verify_only` reads the charge twice. The first read is the recording's; the second showed a
    refund this replay never made, so it misses and diverges, and the agent sees nothing
    refunded."""
    recorded, replay = replayed(W, "verified_refund", {"PROMPT": "verify_only"})
    assert check_invariants(replay) == []
    assert replay.answered() == [
        ("llm#1", 200, None),
        ("get_charge#1", 200, "recording"),
        ("llm#2", 200, None),
        ("get_charge#2", 200, None),
        ("llm#3", 200, None),
        ("llm#4", 200, None),
    ]
    first, second = [ex for ex in replay.reported if ex.request.path == "/v1/charges/ch_TICKET1"]
    assert first.flags == ("from-recording",)
    assert (second.answered_by, second.flags) == ("live", ("replay-miss",))
    assert json.loads(second.response.body)["amount_refunded"] == 0
    assert [(r.method, r.path) for r in replay.internet.requests("api.stripe.com")] == [
        ("GET", "/v1/charges/ch_TICKET1")
    ]
    assert replay.internet.writes() == []
    run_id = replay.one("run.start")["run"]
    [reply] = [
        e
        for e in replay.stored().load_run(run_id).events
        if isinstance(e, ToolCall) and e.name.endswith(":send_reply")
    ]
    assert isinstance(reply.args, dict)
    assert reply.args["body"] == "No refund issued; 0 already refunded."
    code, out, err = replay.compare_runs(recorded.one("run.start")["run"], run_id)
    # The reply says so, and compare shows the text that changed under the one line it prints.
    assert (code, out[4:], err) == (
        1,
        [
            f"  - {FULL}",
            *reply_changed("No refund issued; 0 already refunded."),
            "",
            "  0 same · 1 changed · 1 only in A · 0 only in B",
        ],
        [],
    )


@pytest.mark.parametrize("mode", MODES)
def test_a_checked_charge_reads_the_charge_twice_and_writes_nothing(run_workflow, mode):
    """`verify_only` refunds nothing: two live reads of the charge, and a reply saying so."""
    bare = run_workflow(W, "checked_charge", "bare")
    shadow = run_workflow(W, "checked_charge", mode)
    assert shadow.result(with_time=False) == bare.result(with_time=False)
    assert shadow.answered() == [
        ("llm#1", 200, None),
        ("get_charge#1", 200, None),
        ("llm#2", 200, None),
        ("get_charge#2", 200, None),
        ("llm#3", 200, None),
        ("llm#4", 200, None),
    ]
    assert bare.internet.writes() == shadow.internet.writes() == []


def test_a_replay_that_adds_a_refund_reads_live_after_it_and_sees_it(replayed):
    """A replay that adds a refund fakes it unchecked, then sees it through the overlay.

    `checked_charge` recorded no write and no charge read to check against, and a replay never
    reads live state for a check, so the refund could not be checked. Its later read goes live."""
    _recorded, replay = replayed(W, "checked_charge", {"PROMPT": "verify"})
    assert check_invariants(replay) == []
    assert replay.answered() == [
        ("llm#1", 200, None),
        ("get_charge#1", 200, "recording"),
        ("llm#2", 200, None),
        (REFUND, 200, "fake-L1"),
        ("llm#3", 200, None),
        ("get_charge#3", 200, "overlay"),
        ("llm#4", 200, None),
        ("llm#5", 200, None),
    ]
    [refund] = [ex for ex in replay.reported if ex.kind == "write"]
    assert (refund.precondition.outcome, refund.flags) == ("not_evaluable", ("replay-miss",))
    [check] = [ex for ex in replay.reported if ex.issued_by == "engine"]
    assert (check.answered_by, check.response, check.flags) == (
        "recording",
        None,
        ("replay-miss",),
    )
    # No read named a currency, so the amount is printed as minor units of an unknown currency.
    refund_line = (
        f"  ○ stripe  refund 4900 (minor units, currency unknown) on ch_TICKET1  {UNCHECKED}"
    )
    assert refund_line in replay.summaries()
    again = [ex for ex in replay.reported if ex.request.path == "/v1/charges/ch_TICKET1"][-1]
    assert again.flags == ("replay-miss",)
    assert json.loads(again.response.body)["amount_refunded"] == 4900
    assert [(r.method, r.path) for r in replay.internet.requests("api.stripe.com")] == [
        ("GET", "/v1/charges/ch_TICKET1")
    ]
    assert replay.internet.writes() == []


def _place(result, event: str, name: str) -> int:
    """Return where in the agent's log the first `event` named `name` is."""
    return next(
        i
        for i, o in enumerate(result.obs)
        if o["event"] == event and name in (o.get("name"), o.get("label"))
    )


def test_a_lookup_recorded_after_a_refund_the_replay_has_not_made_runs_for_real_alone(replayed):
    """A lookup recorded after a refund not yet replayed runs for real, without diverging.

    `late_lookup` replayed with `PROMPT=verify` looks up before it refunds, so the recorded
    lookup may show that refund. Only the model calls miss. By command, the lookup reads the
    seeded database; by the trigger alone, it raises `no such table`."""
    recorded, replay = replayed(W, "late_lookup", {"PROMPT": "verify"}, command=True)
    assert check_invariants(replay) == []
    assert _place(recorded, "http", REFUND) < _place(recorded, "tool", LOOKUP_TOOL)
    assert _place(replay, "tool", LOOKUP_TOOL) < _place(replay, "http", REFUND)
    assert recorded.tools() == replay.tools() == [(LOOKUP_TOOL, "real"), (REPLY_TOOL, "shadow")]
    [lookup] = [e for e in replay.handed_events if isinstance(e, ToolCall) and e.kind == "read"]
    assert (lookup.ran, lookup.result) == ("real", {"id": CUSTOMER, "name": "Ada", "tier": "gold"})
    assert replay.answered() == [
        ("llm#1", 200, None),
        ("get_charge#1", 200, "recording"),
        ("llm#2", 200, None),
        (REFUND, 200, "fake-L1"),
        ("llm#3", 200, None),
        ("get_charge#3", 200, "overlay"),
        ("llm#4", 200, None),
        ("llm#5", 200, None),
    ]
    stripe = [(ex.answered_by, ex.flags) for ex in replay.reported if ex.kind != "llm"]
    assert stripe == [
        ("recording", ("from-recording",)),
        ("fake-L1", ("from-recording",)),
        ("overlay", ("from-recording",)),
    ]
    assert replay.internet.requests("api.stripe.com") == []
    assert replay.returned() == recorded.returned()


# -- deleting runs while one is in progress -------------------------------------------------------

# What `retention_mid_run`'s agent asks irimi to delete while its run is in progress, by label.
RETENTION_COMMANDS = ("rm_run", "rm_last", "prune", "prune_dry_run")


@pytest.mark.parametrize("mode", MODES)
def test_runs_are_not_deleted_while_an_irimi_process_writes_the_store(run_workflow, mode, tmp_path):
    """Deleting runs mid-run is refused, exit 2, while an irimi process writes the store.

    `runs rm` and `runs prune` (dry run too) are held off by the writer lock under shadow and by
    `serve.lock` under serve; nothing is deleted until irimi stops. Bare has no store."""
    shadow = run_workflow(W, "retention_mid_run", mode)
    root = shadow.store_root()
    writer = "another irimi serve" if mode == "serve" else "another irimi process"
    refused = {
        "code": 2,
        "out": [],
        "err": [f"error: {writer} is writing the trace store {root}; stop it to delete runs"],
    }
    asked = {e.pop("label"): e for e in shadow.events("retention", with_time=False)}
    assert {label: {k: v for k, v in e.items() if k != "event"} for label, e in asked.items()} == (
        dict.fromkeys(RETENTION_COMMANDS, refused)
    )
    [run] = shadow.sdk_runs()
    full = run_workflow(W, "full_refund", mode)
    [full_run] = full.sdk_runs()
    assert len(shadow.stored().load_run(run.run_id).events) == len(
        full.stored().load_run(full_run.run_id).events
    )
    assert run.end is not None and run.end.outcome == "ok"
    # Stopped, the same run is deleted: in a copy of the home, so the session's run keeps its own.
    home = tmp_path / "home"
    shutil.copytree(shadow.home, home)
    stopped = dataclasses.replace(shadow, home=home)
    code, out, err = stopped.runs("rm", run.run_id)
    assert (code, out[0], err) == (0, f"deleted run {run.run_id}", [])
    assert re.fullmatch(r"deleted 1 run and \d+ stored bod(y|ies) no other run uses", out[-1])
    assert run.run_id not in [r.run_id for r in stopped.stored().list_runs()]
    bare = run_workflow(W, "retention_mid_run", "bare")
    nothing = f"in {bare.store_root()}"
    assert {e["label"]: (e["code"], e["out"], e["err"]) for e in bare.events("retention")} == {
        "rm_last": (2, [], [f"error: no runs {nothing}"]),
        "prune": (0, [f"no runs older than 1h {nothing}"], []),
        "prune_dry_run": (0, [f"no runs older than 1h {nothing}"], []),
    }
    assert bare.exit_code == shadow.exit_code == 0


def test_the_summary_starts_on_a_line_of_its_own_after_a_child_that_left_one_open(
    shared_store_run, tmp_path
):
    """A child whose output ends without a newline still gets irimi's summary on a fresh line.

    The real `irimi shadow` runs in its own process over W1's home, as `curl` prints a body."""
    full = shared_store_run(W, "full_refund")
    shadow = ["shadow", "--port", "0", "--store", str(tmp_path / "store"), "--"]
    child = [sys.executable, "-c", "print('no newline here', end='')"]
    ended = _irimi_process(full, *shadow, *child)
    assert ended.returncode == 0, ended.stderr
    lines = ended.stdout.splitlines()
    assert "no newline here" in lines
    after = lines[lines.index("no newline here") + 1 :]
    assert next(line for line in after if line).startswith("irimi shadow · run ")
