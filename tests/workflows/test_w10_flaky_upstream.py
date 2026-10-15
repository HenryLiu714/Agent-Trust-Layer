"""W10 `flaky_upstream`: which failures a shadow run shows, and which it cannot.

A failed READ is forwarded, so it fails under shadow too and the agent's retry runs, though not
always in the same shape: a reset arrives as the proxy's 502. A failed WRITE never happens under
shadow, because the write never leaves irimi: the agent's retry path for it is exercised only bare.
A failed L3 read is irimi's alone, and only its summary shows it. The agent is
`examples/workflows/w10_flaky_upstream/agent.py`.
"""

import hashlib

import pytest

from examples.workflows.harness.internet import bomb
from examples.workflows.harness.run import check_invariants, compare, rows_not_shown
from examples.workflows.w10_flaky_upstream.scenarios import BOMBED, CONTROL_DOWN
from irimi.engine.mitm import MAX_DECODED_BODY
from irimi.exchange import BODY_TRUNCATED_FLAG, UPSTREAM_ERROR_FLAG, Exchange
from irimi.trace import ErrorInfo

W = "w10_flaky_upstream"
# Each test runs under `irimi shadow -- <cmd>` and under serve mode, its agent beside irimi (#77).
MODES = ("shadow", "serve")
# What a shadow run holds besides a body it decodes: its exchanges, its store's queue, the fake
# internet's answers. Measured at 2 to 45 MiB on top of twice the cap (#94).
MEMORY_MARGIN = 64 * 1024 * 1024
REFUND_LINE = "  ○ refund $5.00 on ch_PAYOUT1  unvalidated (L3 preconditions passed)"
# What the agent saw when nothing failed: the list and the model live, the refund and the Slack
# post faked. A run whose SDK cannot reach irimi must see exactly this (#74).
ANSWERED = [
    ("list_charges", 200, None),
    ("api.anthropic.com/v1/messages", 200, None),
    ("refund", 200, "fake-L1"),
    ("slack.com/api/chat.postMessage", 200, "fake-L1"),
]


def statuses(result, label):
    return [c.get("status", c.get("error")) for c in result.calls(label)]


def retries(result):
    return [(r["label"], r["reason"]) for r in result.events("retry")]


def listed_status(result, run_id: str | None = None) -> str:
    """A run's status column in `irimi runs list` (#72): the process run's, or `run_id`'s."""
    code, out, err = result.runs("list")
    assert (code, err) == (0, [])
    run_id = run_id or result.process_run_id()
    [line] = [line for line in out if line.startswith(run_id + "  ")]
    return line.split("  ")[3]


def shown_outcome(result) -> list[str]:
    """The process run's `outcome:` line in `irimi runs show`, and the header lines after it."""
    code, out, err = result.runs("show", result.process_run_id())
    assert (code, err) == (0, [])
    start = out.index(next(line for line in out if line.startswith("outcome: ")))
    return out[start : out.index("", start)]


def test_a_rate_limited_read_fails_under_shadow_too_and_is_retried(run_workflow):
    for mode in ("bare", *MODES):
        result = run_workflow(W, "rate_limited", mode)
        assert statuses(result, "list_charges") == [429, 200]
        assert retries(result) == [("list_charges", 429)]
    for mode in MODES:
        lines = run_workflow(W, "rate_limited", mode).exchange_lines()
        assert lines[:2] == [
            "live      read      GET api.stripe.com/v1/charges -> 429",
            "live      read      GET api.stripe.com/v1/charges -> 200",
        ]


def test_a_server_error_on_a_read_is_forwarded_and_retried(run_workflow):
    for mode in ("bare", *MODES):
        result = run_workflow(W, "server_error_read", mode)
        assert statuses(result, "list_charges") == [500, 200]
    for mode in MODES:
        shadow = run_workflow(W, "server_error_read", mode)
        lines = shadow.exchange_lines()
        assert lines[0] == "live      read      GET api.stripe.com/v1/charges -> 500"
        assert REFUND_LINE in shadow.summaries()


@pytest.mark.parametrize("mode", MODES)
def test_a_read_the_agent_abandoned_is_recorded_as_an_upstream_error(run_workflow, mode):
    shadow = run_workflow(W, "read_timeout", mode)
    assert statuses(shadow, "list_charges") == ["TimeoutError", 200]
    assert statuses(run_workflow(W, "read_timeout", "bare"), "list_charges") == [
        "TimeoutError",
        200,
    ]
    # LOOKS WRONG: the upstream never failed: it answered late, after the AGENT gave up and hung up.
    # irimi records the abandoned exchange with no response and flags it `upstream-error`, which
    # reads as the service's fault. Which of the two reads prints first is a race between irimi
    # noticing the hang-up and the retry's answer, so their order is not pinned.
    assert sorted(shadow.exchange_lines()[:2]) == [
        "live      read      GET api.stripe.com/v1/charges -> -  [upstream-error]",
        "live      read      GET api.stripe.com/v1/charges -> 200",
    ]
    # Two agent reads and the refund's L3 read: the stall was not retried by irimi itself.
    assert [r.path for r in shadow.internet.requests("api.stripe.com")] == [
        "/v1/charges",
        "/v1/charges",
        "/v1/charges/ch_PAYOUT1",
    ]
    # LOOKS WRONG: stored as printed (#70), the upstream's fault.
    assert unanswered(shadow) == [("/v1/charges", (UPSTREAM_ERROR_FLAG,))]


@pytest.mark.parametrize("mode", MODES)
def test_a_reset_on_a_read_reaches_the_agent_as_an_unstamped_502(run_workflow, mode):
    bare = run_workflow(W, "reset_on_read", "bare")
    shadow = run_workflow(W, "reset_on_read", mode)
    assert statuses(bare, "list_charges") == ["ConnectionResetError", 200]
    assert retries(bare) == [("list_charges", "NetworkError")]
    # LOOKS WRONG: under shadow the proxy answers the reset itself, as an HTTP 502 with no
    # Irimi-Answered-By: the agent sees a status where bare it saw no answer at all. This agent
    # retries both, so it ends the same way; an agent that retries only one of them would not.
    assert [(c.get("status"), c.get("answered_by")) for c in shadow.calls("list_charges")] == [
        (502, None),
        (200, None),
    ]
    assert retries(shadow) == [("list_charges", 502)]
    assert sorted(shadow.exchange_lines()[:2]) == [
        "live      read      GET api.stripe.com/v1/charges -> -  [upstream-error]",
        "live      read      GET api.stripe.com/v1/charges -> 200",
    ]
    assert REFUND_LINE in shadow.summaries()
    # Stored as printed (#70): an exchange with no response reads back as one.
    assert unanswered(shadow) == [("/v1/charges", (UPSTREAM_ERROR_FLAG,))]


def unanswered(result) -> list[tuple[str, tuple[str, ...]]]:
    """`(path, flags)` of each stored exchange with no response."""
    return [
        (e.request.path, e.flags)
        for e in result.stored_events()
        if isinstance(e, Exchange) and e.response is None
    ]


@pytest.mark.parametrize("mode", MODES)
def test_a_failed_precondition_read_degrades_the_check_and_the_agent_never_knows(
    run_workflow, mode
):
    bare = run_workflow(W, "precondition_read_fails", "bare")
    shadow = run_workflow(W, "precondition_read_fails", mode)
    # The agent never reads the charge itself; only irimi's L3 check does.
    assert [r.path for r in bare.internet.requests("api.stripe.com")] == [
        "/v1/charges",
        "/v1/refunds",
    ]
    assert statuses(shadow, "refund") == statuses(bare, "refund") == [200]
    assert retries(shadow) == []
    assert "live      read      GET api.stripe.com/v1/charges/ch_PAYOUT1 -> 500" in (
        shadow.exchange_lines()
    )
    # The check could not be made, so the refund is faked at L2. LOOKS WRONG: with no currency
    # the L3 read would have supplied, its amount prints in minor units.
    refunds = [line for line in shadow.summaries() if "○ refund" in line]
    assert refunds == ["  ○ refund 500 on ch_PAYOUT1  unvalidated (L2)"]


@pytest.mark.parametrize("mode", MODES)
def test_a_reset_on_the_write_is_retried_bare_and_never_happens_under_shadow(run_workflow, mode):
    bare = run_workflow(W, "reset_on_write", "bare")
    shadow = run_workflow(W, "reset_on_write", mode)
    assert statuses(bare, "refund") == ["ConnectionResetError", 200]
    assert retries(bare) == [("refund", "NetworkError")]
    # Under shadow the refund is answered by irimi and never reaches the service that resets it,
    # so the agent's retry path is not exercised. A shadow run cannot show it.
    assert statuses(shadow, "refund") == [200]
    assert retries(shadow) == []
    # Bare, the key made the retry one refund.
    assert len(bare.world.stripe.refunds) == 1
    assert shadow.world.stripe.refunds == []


@pytest.mark.parametrize("mode", MODES)
def test_a_keyless_write_retried_after_a_500_is_two_posts_bare_and_one_fake_under_shadow(
    run_workflow,
    mode,
):
    bare = run_workflow(W, "retry_without_key", "bare")
    shadow = run_workflow(W, "retry_without_key", mode)
    refund_posts = [r for r in bare.internet.writes() if r.path == "/v1/refunds"]
    assert len(refund_posts) == 2
    assert all("idempotency-key" not in r.headers for r in refund_posts)
    assert statuses(bare, "refund") == [500, 200]
    assert len(bare.world.stripe.refunds) == 1  # the 500 was answered before Stripe acted
    # Shadow: the 500 cannot happen, the write is faked once, and the summary shows one refund.
    assert statuses(shadow, "refund") == [200]
    assert [line for line in shadow.summaries() if "○ refund" in line] == [REFUND_LINE]


@pytest.mark.parametrize("mode", MODES)
def test_a_model_that_answers_prose_leads_to_no_write_but_the_status_post(run_workflow, mode):
    shadow = run_workflow(W, "malformed_llm", mode)
    assert [e["text"][:5] for e in shadow.events("llm_fallback")] == ["Sure!"]
    assert shadow.calls("refund") == []
    assert (
        "live      llm       POST api.anthropic.com/v1/messages -> 200" in shadow.exchange_lines()
    )
    assert [line for line in shadow.summaries() if "○" in line] == [
        '  ○ post to #C0PAYOUT: "payout sync: no action"  unvalidated (L3 preconditions passed)'
    ]
    assert len(shadow.world.llm.calls) == 1


@pytest.mark.parametrize("mode", MODES)
def test_an_agent_that_raises_after_its_write_still_shows_the_write(run_workflow, mode):
    shadow = run_workflow(W, "raise_after_write", mode)
    assert shadow.exit_code == run_workflow(W, "raise_after_write", "bare").exit_code == 1
    [end] = shadow.events("run.end")
    assert (end["outcome"], end["error"]) == ("error", "RuntimeError")
    # Stored under the exception's full name (#74), the refund id it names included.
    [run] = shadow.sdk_runs()
    assert run.outcome == "error" and run.error is not None
    assert run.error.type == "builtins.RuntimeError"
    assert run.error.message.startswith("ledger export failed after refund re_")
    summary = shadow.summaries()
    assert REFUND_LINE in summary
    # LOOKS WRONG: the summary does not say the agent failed. Nothing in it tells this run from
    # one that finished.
    assert not any("error" in line.lower() or "exit" in line for line in summary)
    if mode == "serve":
        # Serve mode's block for the run is its stored summary (#77), which says no more: the
        # run's own status is in `irimi runs list`, as the SDK ended it.
        assert listed_status(shadow, run.run_id) == "error"
        return
    # The stored run does say so (#70); the summary above still does not.
    assert process_run(shadow) == ("error", 1, ErrorInfo("exit", "exited 1"))
    # `irimi runs list` and `runs show` print it where the summary cannot (#72). The summary pin
    # above stays: a stored run's summary is the live one, and the live one says nothing.
    assert listed_status(shadow) == "error"
    assert shown_outcome(shadow) == ["outcome: error", "error: exit: exited 1", "exit code: 1"]


@pytest.mark.parametrize("mode", MODES)
def test_an_agent_killed_after_its_write_leaves_a_run_with_no_end(run_workflow, mode):
    bare = run_workflow(W, "sigterm_mid_run", "bare")
    shadow = run_workflow(W, "sigterm_mid_run", mode)
    # 143 is 128 + SIGTERM, the shell's spelling, in both modes.
    assert (bare.exit_code, shadow.exit_code) == (143, 143)
    # The run started and never ended: an incomplete run (`outcome: None`), as stored below.
    assert len(shadow.events("run.start")) == 1
    assert shadow.events("run.end") == []
    assert shadow.calls("refund")[0]["answered_by"] == "fake-L1"
    if mode == "serve":
        _killed_under_serve(shadow)
        return
    assert REFUND_LINE in shadow.summaries()
    # LOOKS WRONG: as after a raise, the summary ends as a finished run's does, with no word that
    # the agent was killed.
    summary = shadow.summaries()
    assert summary[-1] == (
        "  These writes did not happen. Would have fired: refund.created, charge.refunded."
    )
    # The header names the process run, 16 random hex characters, which can spell `143` by chance.
    said = [line.replace(shadow.engine_run_id(), "<run>").lower() for line in summary]
    assert not any(word in line for line in said for word in ("143", "kill", "signal"))
    # The stored process run ends with the child's code, which is how the kill is visible (#70).
    assert process_run(shadow) == ("error", 143, ErrorInfo("exit", "exited 143"))
    assert listed_status(shadow) == "error"
    assert shown_outcome(shadow) == ["outcome: error", "error: exit: exited 143", "exit code: 143"]
    # The agent's own run is the one the SDK started (#74), and it reads back whole, the faked
    # refund included, with no end: the default SIGTERM handler raises nothing, so the SDK never
    # posted one, and the run is incomplete, as a killed run is (#70).
    reader = shadow.stored()
    [run] = shadow.sdk_runs()
    assert (run.run_id, run.outcome, run.ended_at) == (shadow.one("run.start")["run"], None, None)
    stored = [e for e in reader.load_run(run.run_id).events if isinstance(e, Exchange)]
    assert [(e.request.method, e.request.path, e.answered_by) for e in stored][-1] == (
        "POST",
        "/v1/refunds",
        "fake-L1",
    )


def _killed_under_serve(serve) -> None:
    """Serve mode prints a run's summary when the SDK ends it (#77), and a killed agent's SDK never
    does: no block is printed, and the run is stored `incomplete`, its faked refund in it.

    LOOKS WRONG: so nothing serve prints summarizes the refund. Its exchange line went by as it
    happened, and `irimi runs show` has it, but a shadow week's output never counts a write made
    in a run that did not end, where `irimi shadow -- <cmd>` counted it in the process's block.
    Telling such a run from one still going is #107's."""
    [run] = serve.sdk_runs()
    assert (run.run_id, run.outcome, run.ended_at) == (serve.one("run.start")["run"], None, None)
    assert serve.run_blocks() == {} and REFUND_LINE not in serve.summaries()
    assert listed_status(serve, run.run_id) == "incomplete"
    code, out, err = serve.runs("show", run.run_id)
    assert (code, err) == (0, []) and REFUND_LINE in out


def process_run(result) -> tuple[object, object, object]:
    """`(outcome, exit_code, error)` of the process run `irimi shadow` stored."""
    (run,) = [r for r in result.stored().list_runs() if r.attribution == "process"]
    return run.outcome, run.exit_code, run.error


@pytest.mark.parametrize("mode", MODES)
def test_an_agent_interrupted_after_its_write_ends_its_run_in_keyboard_interrupt(
    run_workflow, mode
):
    """Ctrl-C mid-run: Python raises KeyboardInterrupt, a BaseException, not an Exception. The SDK
    posts the run's end as it propagates (#74), so unlike after SIGTERM the run is complete:
    `error`, under `builtins.KeyboardInterrupt`, with the faked refund in it. The interrupt still
    ends the agent, 130 (128 + SIGINT) in both modes."""
    bare = run_workflow(W, "sigint_mid_run", "bare")
    shadow = run_workflow(W, "sigint_mid_run", mode)
    assert (bare.exit_code, shadow.exit_code) == (130, 130)
    [end] = shadow.events("run.end")
    assert (end["outcome"], end["error"]) == ("error", "KeyboardInterrupt")
    [run] = shadow.sdk_runs()
    assert (run.run_id, run.outcome) == (end["run"], "error")
    assert run.error == ErrorInfo("builtins.KeyboardInterrupt", "")
    stored = [e for e in shadow.stored().load_run(run.run_id).events if isinstance(e, Exchange)]
    assert [(e.request.method, e.request.path, e.answered_by) for e in stored][-1] == (
        "POST",
        "/v1/refunds",
        "fake-L1",
    )
    assert bare.events("run.end") == []
    if mode == "serve":
        # The SDK ended the run, so serve printed its block, the faked refund in it (#77).
        assert REFUND_LINE in shadow.run_blocks()[run.run_id]
        return
    assert process_run(shadow) == ("error", 130, ErrorInfo("exit", "exited 130"))


# The kind of failure each `CONTROL_DOWN` scenario's one warning names, as the SDK logs it.
CONTROL_KIND = {
    "control_unset": "(unset: IRIMI_CONTROL is not set)",
    "control_unreachable": "(unreachable: ",
    "control_refused": "(rejected: HTTP 404: ",
}


@pytest.mark.parametrize("scenario", CONTROL_DOWN)
@pytest.mark.parametrize("mode", MODES)
def test_an_sdk_that_cannot_reach_irimi_warns_once_and_changes_nothing(
    run_workflow, mode, scenario
):
    """The SDK never raises into the agent (#74). With its control endpoint unset, gone, or
    refusing every post, the run goes on: its id is current, logged and on every request, and the
    agent makes the calls it makes with the SDK working and exits 0, as it does bare. The SDK
    logs one WARNING on `irimi.sdk`, though both the start and the end failed: once per kind of
    failure. irimi never hears of the run, so the store holds only the `header` run its labelled
    requests made, with no trigger and no end (#70)."""
    bare = run_workflow(W, scenario, "bare")
    shadow = run_workflow(W, scenario, mode)
    assert shadow.exit_code == bare.exit_code == 0
    [start] = shadow.events("run.start")
    [end] = shadow.events("run.end")
    assert (end["run"], end["outcome"]) == (start["run"], "ok")
    assert {c["run"] for c in shadow.calls()} == {start["run"]}
    assert shadow.answered() == ANSWERED
    # Under serve mode (#77) the run's end never reached irimi, so no block is printed for it, and
    # LOOKS WRONG (#107): nothing serve prints summarizes its refund, as `_killed_under_serve`.
    assert (REFUND_LINE in shadow.summaries()) is (mode == "shadow")
    [warning] = shadow.events("log")
    assert (warning["logger"], warning["level"]) == ("irimi.sdk", "WARNING")
    assert start["run"] in warning["message"]
    # For the reason the scenario took the endpoint away, the kind `client.ControlClient` names.
    assert CONTROL_KIND[scenario] in warning["message"]
    assert bare.events("log") == bare.events("run.start") == []
    records = shadow.stored().list_runs()
    process = ["process"] if mode == "shadow" else []
    assert sorted(r.attribution for r in records) == ["header", *process]
    [header] = [r for r in records if r.attribution == "header"]
    assert (header.run_id, header.trigger, header.outcome) == (start["run"], None, None)


@pytest.mark.parametrize("scenario", sorted(BOMBED))
@pytest.mark.parametrize("mode", MODES)
def test_a_bomb_on_the_read_reaches_the_agent_whole_and_is_recorded_empty(
    run_workflow, mode, scenario
):
    """The read is answered with a body that decodes to 1 GiB, in gzip (2 MiB), br or zstd (a few
    KB to 100 KB), which the agent never asked for and cannot read. irimi decodes it only up to its
    cap (#94): the agent gets every byte it was sent, the bomb's own, so it does what it does bare
    - finds no charges and posts that there was nothing to do. The read is recorded and stored
    empty, flagged `body-truncated` once: the start of a document is not one redaction can read,
    and this one's credential, on a line of its own, never reaches disk (universal invariant 4)."""
    encoding = BOMBED[scenario]
    sent = bomb(encoding)
    bare = run_workflow(W, scenario, "bare")
    shadow = run_workflow(W, scenario, mode)
    for result in (bare, shadow):
        assert result.exit_code == 0
        assert statuses(result, "list_charges") == [200]
        assert result.result(with_time=False) == {"event": "result", "outcome": "no action"}
        assert result.events("listed", with_time=False) == [
            {
                "event": "listed",
                "bytes": len(sent),
                "sha256": hashlib.sha256(sent).hexdigest(),
                "encoding": encoding,
            }
        ]
    assert [c["answered_by"] for c in shadow.calls("list_charges")] == [None]
    assert shadow.exchange_lines()[0] == (
        "live      read      GET api.stripe.com/v1/charges -> 200  [body-truncated]"
    )
    [read] = [e for e in shadow.reported if e.request.path == "/v1/charges"]
    assert read.response is not None and (read.response.body, read.flags) == (
        b"",
        (BODY_TRUNCATED_FLAG,),
    )
    [stored] = [
        e
        for e in shadow.stored_events()
        if isinstance(e, Exchange) and e.request.path == "/v1/charges"
    ]
    assert stored.response is not None and (stored.response.body, stored.flags) == (
        b"",
        (BODY_TRUNCATED_FLAG,),
    )
    assert stored.response.header("content-encoding") == encoding
    # The run and, under `irimi shadow`, the process run; serve mode has none (#77).
    runs = 2 if mode == "shadow" else 1
    assert [r.dropped_events for r in shadow.stored().list_runs()] == [0] * runs


@pytest.mark.parametrize("scenario", sorted(BOMBED))
def test_a_bomb_costs_irimi_its_cap_in_memory_not_what_it_decodes_to(traced_run, scenario):
    """#94's bound, through the real `irimi shadow`: a body that decodes to a GiB costs the process
    irimi runs in no more than twice the cap it decodes to - a codec assembles its output once and
    copies it once - and a margin for everything else a run holds, never the GiB."""
    result, peak = traced_run(W, scenario)
    assert result.exit_code == 0
    assert "[body-truncated]" in result.exchange_lines()[0]
    assert peak < 2 * MAX_DECODED_BODY + MEMORY_MARGIN


# ---------------------------------------------------------------------- irimi compare (#79)

NOT_A_VERDICT = "a comparison, not a verdict: nothing was replayed"
SYNC = "sync_payouts  examples.workflows.w10_flaky_upstream.agent:sync_payouts  agent -"
REFUND = REFUND_LINE.removeprefix("  ")
POSTED = '○ post to #C0PAYOUT: "refunded 500 on ch_PAYOUT1"  unvalidated (L3 preconditions passed)'
NO_ACTION = '○ post to #C0PAYOUT: "payout sync: no action"  unvalidated (L3 preconditions passed)'
WROTE_BOTH = [f"  = {REFUND}", f"  = {POSTED}"]
# Each scenario's run against `rate_limited`'s, both recorded into one store: the exit code,
# whether the triggers' args differ, and the rows. `rate_limited` retried its read and then
# refunded and posted, as every run here would have with nothing failing.
AGAINST_A_RETRIED_READ = {
    # A read that failed and was retried, however it failed, is no write: nothing to show.
    "server_error_read": (0, False, WROTE_BOTH),
    "reset_on_read": (0, False, WROTE_BOTH),
    # A write that failed bare never fails under shadow, and one without a key is still one
    # write: nothing to show either (`retry_without_key`'s trigger was told to send no key).
    "reset_on_write": (0, False, WROTE_BOTH),
    "retry_without_key": (0, True, WROTE_BOTH),
    # irimi's own L3 read failed, so it could not check the refund, nor price it: the refund
    # changed by its fidelity, not by anything the agent did differently (#79's README: a
    # difference can come from the world between two runs).
    "precondition_read_fails": (
        1,
        False,
        [f"  ~ A {REFUND}", "    B ○ refund 500 on ch_PAYOUT1  unvalidated (L2)", f"  = {POSTED}"],
    ),
    # The model answered prose, and the agent posted that it did nothing: the refund is gone, and
    # the post is the same post, changed.
    "malformed_llm": (1, False, [f"  - {REFUND}", f"  ~ A {POSTED}", f"    B {NO_ACTION}"]),
    # The read was a bomb the agent could not parse (#94): no charge to refund.
    "gzip_bomb_read": (1, False, [f"  - {REFUND}", f"  ~ A {POSTED}", f"    B {NO_ACTION}"]),
    # The agent raised, was interrupted or was killed after its refund: the post it never made.
    "raise_after_write": (1, True, [f"  = {REFUND}", f"  - {POSTED}"]),
    "sigint_mid_run": (1, True, [f"  = {REFUND}", f"  - {POSTED}"]),
    # Killed, its run never ended: an incomplete run compares as what it stored.
    "sigterm_mid_run": (1, True, [f"  = {REFUND}", f"  - {POSTED}"]),
    # The SDK could not reach irimi, so the run is the `header` run its requests made, with no
    # trigger and so no args: the same writes.
    "control_unset": (0, True, WROTE_BOTH),
}


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("scenario", AGAINST_A_RETRIED_READ)
def test_irimi_compare_shows_what_each_failure_changed_about_the_runs_writes(
    shared_store_run, scenario, mode
):
    """W10's failures against a run whose only trouble was a retried read (#79), recorded into
    one store, `rate_limited` under shadow and each other under shadow or serve: a failed read or
    a failed write is nothing compare can show, and a crash, a kill, a model that answered prose
    and irimi's own failed check are each the rows below. Every row is a line its own run's
    summary printed."""
    code, args_differ, rows = AGAINST_A_RETRIED_READ[scenario]
    retried = shared_store_run(W, "rate_limited")
    other = shared_store_run(W, scenario, mode)
    assert check_invariants(other) == []
    out = compare(retried, other)
    a, b = (result.one("run.start")["run"] for result in (retried, other))
    header_b = "-  -  agent -" if scenario in CONTROL_DOWN else SYNC
    totals = [sum(row.startswith(p) for row in rows) for p in ("  = ", "  ~ ", "  - ", "  + ")]
    assert out == (
        code,
        [
            f"irimi compare · {a} vs {b} · {NOT_A_VERDICT}",
            f"  A  {SYNC}",
            f"  B  {header_b}",
            *(["  (the triggers' args differ)"] if args_differ else []),
            "",
            *rows,
            "",
            "  {} same · {} changed · {} only in A · {} only in B".format(*totals),
        ],
        [],
    )
    assert rows_not_shown(other, a, b, out[1]) == []
