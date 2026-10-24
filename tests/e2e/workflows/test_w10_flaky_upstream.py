"""W10 `flaky_upstream`: which failures a shadow run shows, and which it cannot.

A failed read fails under shadow too; a failed write never happens, since the write never leaves."""

import hashlib
import re

import pytest

from irimi.core.exchange import Exchange, Flag
from irimi.core.limits import MAX_DECODED_BODY
from irimi.protocol.trace import ErrorInfo
from tests.e2e.workflows.agents.w10_flaky_upstream.scenarios import BOMBED, CONTROL_DOWN
from tests.e2e.workflows.harness.compare import compare, rows_not_shown
from tests.e2e.workflows.harness.internet import bomb
from tests.e2e.workflows.harness.invariants import check_invariants

W = "w10_flaky_upstream"
# Each test runs under `irimi shadow -- <cmd>` and under serve mode, its agent beside irimi.
MODES = ("shadow", "serve")
# What a shadow run holds besides a body it decodes: its exchanges, its store's queue, the fake
# internet's answers. Measured at 2 to 45 MiB on top of twice the cap.
MEMORY_MARGIN = 64 * 1024 * 1024
REFUND_LINE = (
    "  ○ stripe  refund $5.00 on ch_PAYOUT1  faked; checked against live state: would succeed"
)
# What the agent saw when nothing failed: the list and the model live, the refund and the Slack
# post faked. A run whose SDK cannot reach irimi must see exactly this.
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
    """Return a run's status column in `irimi runs list`: the process run's, or `run_id`'s."""
    code, out, err = result.runs("list")
    assert (code, err) == (0, [])
    run_id = run_id or result.process_run_id()
    [line] = [line for line in out if line.startswith(run_id + "  ")]
    return re.split(r"  +", line)[3]


def shown_outcome(result) -> list[str]:
    """Return the process run's `outcome:` line in `irimi runs show`, and the lines after it."""
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
            "real          read      GET api.stripe.com/v1/charges -> 429",
            "real          read      GET api.stripe.com/v1/charges -> 200",
        ]


def test_a_server_error_on_a_read_is_forwarded_and_retried(run_workflow):
    for mode in ("bare", *MODES):
        result = run_workflow(W, "server_error_read", mode)
        assert statuses(result, "list_charges") == [500, 200]
    for mode in MODES:
        shadow = run_workflow(W, "server_error_read", mode)
        lines = shadow.exchange_lines()
        assert lines[0] == "real          read      GET api.stripe.com/v1/charges -> 500"
        assert REFUND_LINE in shadow.summaries()


@pytest.mark.parametrize("mode", MODES)
def test_a_read_the_agent_abandoned_is_recorded_as_an_upstream_error(run_workflow, mode):
    shadow = run_workflow(W, "read_timeout", mode)
    assert statuses(shadow, "list_charges") == ["TimeoutError", 200]
    assert statuses(run_workflow(W, "read_timeout", "bare"), "list_charges") == [
        "TimeoutError",
        200,
    ]
    # KNOWN BUG (#93): an exchange the agent hung up on is recorded with no response and
    # `upstream-error`, which reads as the service's fault, though the upstream only answered late.
    # The two reads race to print first, so their order is not pinned.
    assert sorted(shadow.exchange_lines()[:2]) == [
        "real          read      GET api.stripe.com/v1/charges -> -  [upstream-error]",
        "real          read      GET api.stripe.com/v1/charges -> 200",
    ]
    # Two agent reads and irimi's check read: irimi itself does not retry the stall.
    assert [r.path for r in shadow.internet.requests("api.stripe.com")] == [
        "/v1/charges",
        "/v1/charges",
        "/v1/charges/ch_PAYOUT1",
    ]
    # The known bug above, as stored: the hang-up is the upstream's fault on disk too.
    assert unanswered(shadow) == [("/v1/charges", (Flag.UPSTREAM_ERROR,))]


@pytest.mark.parametrize("mode", MODES)
def test_a_reset_on_a_read_reaches_the_agent_as_an_unstamped_502(run_workflow, mode):
    bare = run_workflow(W, "reset_on_read", "bare")
    shadow = run_workflow(W, "reset_on_read", mode)
    assert statuses(bare, "list_charges") == ["ConnectionResetError", 200]
    assert retries(bare) == [("list_charges", "NetworkError")]
    # KNOWN BUG: under shadow the proxy answers the reset with a 502 and no Irimi-Answered-By, so
    # the agent sees a status where bare it saw no answer. This agent retries both, so it ends the
    # same way; an agent that retries only one of them would not.
    assert [(c.get("status"), c.get("answered_by")) for c in shadow.calls("list_charges")] == [
        (502, None),
        (200, None),
    ]
    assert retries(shadow) == [("list_charges", 502)]
    assert sorted(shadow.exchange_lines()[:2]) == [
        "real          read      GET api.stripe.com/v1/charges -> -  [upstream-error]",
        "real          read      GET api.stripe.com/v1/charges -> 200",
    ]
    assert REFUND_LINE in shadow.summaries()
    # Stored as printed: an exchange with no response reads back as one.
    assert unanswered(shadow) == [("/v1/charges", (Flag.UPSTREAM_ERROR,))]


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
    # The agent never reads the charge itself; only irimi's check read does.
    assert [r.path for r in bare.internet.requests("api.stripe.com")] == [
        "/v1/charges",
        "/v1/refunds",
    ]
    assert statuses(shadow, "refund") == statuses(bare, "refund") == [200]
    assert retries(shadow) == []
    assert "irimi-check   read      GET api.stripe.com/v1/charges/ch_PAYOUT1 -> 500" in (
        shadow.exchange_lines()
    )
    # The check could not be made, and the summary says why; with no currency from the check
    # read, the amount prints in minor units, currency unknown.
    refunds = [line for line in shadow.summaries() if "○ stripe  refund" in line]
    assert refunds == [f"  {UNCHECKED_REFUND}"]


# The refund whose precondition check read got a 500, as its summary line prints it.
UNCHECKED_REFUND = (
    "○ stripe  refund 500 (minor units, currency unknown) on ch_PAYOUT1  faked; could not check: "
    "stripe answered irimi's check read with 500"
)


@pytest.mark.parametrize("mode", MODES)
def test_a_reset_on_the_write_is_retried_bare_and_never_happens_under_shadow(run_workflow, mode):
    bare = run_workflow(W, "reset_on_write", "bare")
    shadow = run_workflow(W, "reset_on_write", mode)
    assert statuses(bare, "refund") == ["ConnectionResetError", 200]
    assert retries(bare) == [("refund", "NetworkError")]
    # Under shadow the refund never reaches the service that resets it, so a shadow run cannot
    # exercise the agent's retry path.
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
    assert [line for line in shadow.summaries() if "○ stripe  refund" in line] == [REFUND_LINE]


@pytest.mark.parametrize("mode", MODES)
def test_a_model_that_answers_prose_leads_to_no_write_but_the_status_post(run_workflow, mode):
    shadow = run_workflow(W, "malformed_llm", mode)
    assert [e["text"][:5] for e in shadow.events("llm_fallback")] == ["Sure!"]
    assert shadow.calls("refund") == []
    assert (
        "real          llm       POST api.anthropic.com/v1/messages -> 200"
        in shadow.exchange_lines()
    )
    assert [line for line in shadow.summaries() if "○" in line] == [f"  {NO_ACTION}"]
    assert len(shadow.world.llm.calls) == 1


@pytest.mark.parametrize("mode", MODES)
def test_an_agent_that_raises_after_its_write_still_shows_the_write(run_workflow, mode):
    shadow = run_workflow(W, "raise_after_write", mode)
    assert shadow.exit_code == run_workflow(W, "raise_after_write", "bare").exit_code == 1
    [end] = shadow.events("run.end")
    assert (end["outcome"], end["error"]) == ("error", "RuntimeError")
    # Stored under the exception's full name, the refund id it names included.
    [run] = shadow.sdk_runs()
    assert run.end.outcome == "error" and run.end.error is not None
    assert run.end.error.type == "builtins.RuntimeError"
    assert run.end.error.message.startswith("ledger export failed after refund re_")
    summary = shadow.summaries()
    assert REFUND_LINE in summary
    # KNOWN BUG: nothing in the summary says the agent failed, so it reads as a finished run's.
    assert not any("error" in line.lower() or "exit" in line for line in summary)
    if mode == "serve":
        # Serve's block is the stored summary, which says no more; `irimi runs list` has the status.
        assert listed_status(shadow, run.run_id) == "error"
        return
    # The stored run does say so; the summary above still does not.
    assert process_run(shadow) == ("error", 1, ErrorInfo("exit", "exited 1"))
    # `irimi runs list` and `runs show` print it; a stored run's summary is the live one.
    assert listed_status(shadow) == "error (exit 1)"
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
    # KNOWN BUG: as after a raise, nothing in the summary says the agent was killed.
    summary = shadow.summaries()
    sdk_run = shadow.one("run.start")["run"]
    assert "  would have fired: refund.created, charge.refunded" in summary
    assert summary[-1] == f"  replay with: irimi replay {sdk_run[:8]}"
    # Run ids are random hex, which can spell `143` by chance, so they are masked.
    said = [
        line.replace(shadow.engine_run_id(), "<run>").replace(sdk_run[:8], "<run>").lower()
        for line in summary
    ]
    assert not any(word in line for line in said for word in ("143", "kill", "signal"))
    # The stored process run ends with the child's code, which is how the kill is visible.
    assert process_run(shadow) == ("error", 143, ErrorInfo("exit", "exited 143"))
    assert listed_status(shadow) == "error (exit 143)"
    assert shown_outcome(shadow) == ["outcome: error", "error: exit: exited 143", "exit code: 143"]
    # The SDK's run reads back whole with no end: the default SIGTERM handler raises nothing, so
    # the SDK never posts one.
    reader = shadow.stored()
    [run] = shadow.sdk_runs()
    assert (run.run_id, run.end) == (shadow.one("run.start")["run"], None)
    stored = [e for e in reader.load_run(run.run_id).events if isinstance(e, Exchange)]
    assert [(e.request.method, e.request.path, e.answered_by) for e in stored][-1] == (
        "POST",
        "/v1/refunds",
        "fake-L1",
    )


def _killed_under_serve(serve) -> None:
    """Assert a killed agent's run prints no serve block and is stored `incomplete`, refund and all.

    KNOWN BUG (#107): nothing serve prints summarizes the refund, where `irimi shadow -- <cmd>`
    counts it in the process's block. Its exchange line went by as it happened, and `irimi runs
    show` has it, but serve's output never counts a write made in a run that did not end."""
    [run] = serve.sdk_runs()
    assert (run.run_id, run.end) == (serve.one("run.start")["run"], None)
    assert serve.run_blocks() == {} and REFUND_LINE not in serve.summaries()
    assert listed_status(serve, run.run_id) == "incomplete"
    code, out, err = serve.runs("show", run.run_id)
    assert (code, err) == (0, []) and REFUND_LINE in out


def process_run(result) -> tuple[object, object, object]:
    """Return `(outcome, exit_code, error)` of the process run `irimi shadow` stored."""
    (run,) = [r for r in result.stored().list_runs() if r.attribution == "process"]
    return run.end.outcome, run.end.exit_code, run.end.error


@pytest.mark.parametrize("mode", MODES)
def test_an_agent_interrupted_after_its_write_ends_its_run_in_keyboard_interrupt(
    run_workflow, mode
):
    """Ctrl-C mid-run ends the run in error as `builtins.KeyboardInterrupt`, the refund in it.

    The SDK posts the end as the interrupt propagates, unlike after SIGTERM, and the agent exits
    130 in both modes."""
    bare = run_workflow(W, "sigint_mid_run", "bare")
    shadow = run_workflow(W, "sigint_mid_run", mode)
    assert (bare.exit_code, shadow.exit_code) == (130, 130)
    [end] = shadow.events("run.end")
    assert (end["outcome"], end["error"]) == ("error", "KeyboardInterrupt")
    [run] = shadow.sdk_runs()
    assert (run.run_id, run.end.outcome) == (end["run"], "error")
    assert run.end.error == ErrorInfo("builtins.KeyboardInterrupt", "")
    stored = [e for e in shadow.stored().load_run(run.run_id).events if isinstance(e, Exchange)]
    assert [(e.request.method, e.request.path, e.answered_by) for e in stored][-1] == (
        "POST",
        "/v1/refunds",
        "fake-L1",
    )
    assert bare.events("run.end") == []
    if mode == "serve":
        # The SDK ended the run, so serve printed its block, the faked refund in it.
        assert REFUND_LINE in shadow.run_blocks()[run.run_id]
        return
    assert process_run(shadow) == ("error", 130, ErrorInfo("exit", "exited 130"))


# The kind of failure each `CONTROL_DOWN` scenario's one warning names, as the SDK logs it.
CONTROL_KIND = {
    "control_unset": "(unset: IRIMI_CONTROL is not set)",
    "control_unreachable": "(unreachable: ",
    "control_refused": "(rejected: HTTP 404: ",
    "control_stalled": "(timeout: ",
}


@pytest.mark.parametrize("scenario", CONTROL_DOWN)
@pytest.mark.parametrize("mode", MODES)
def test_an_sdk_that_cannot_reach_irimi_warns_once_and_changes_nothing(
    run_workflow, mode, scenario
):
    """An SDK that cannot reach irimi warns once and changes nothing (the SDK never raises).

    Its control endpoint is unset, gone, refusing or never answering; the run's id still labels
    every request, and the agent exits 0 as it does bare. The store holds only the `header` run
    the labels made."""
    bare = run_workflow(W, scenario, "bare")
    shadow = run_workflow(W, scenario, mode)
    assert shadow.exit_code == bare.exit_code == 0
    [start] = shadow.events("run.start")
    [end] = shadow.events("run.end")
    assert (end["run"], end["outcome"]) == (start["run"], "ok")
    assert {c["run"] for c in shadow.calls()} == {start["run"]}
    assert shadow.answered() == ANSWERED
    # Under serve the run's end never reached irimi, so no block is printed for it.
    # KNOWN BUG (#107): nothing serve prints summarizes its refund, as in `_killed_under_serve`.
    assert (REFUND_LINE in shadow.summaries()) is (mode == "shadow")
    [warning] = shadow.events("log")
    assert (warning["logger"], warning["level"]) == ("irimi.sdk", "WARNING")
    assert start["run"] in warning["message"]
    # The warning names the kind of failure the scenario caused.
    assert CONTROL_KIND[scenario] in warning["message"]
    assert bare.events("log") == bare.events("run.start") == []
    records = shadow.stored().list_runs()
    process = ["process"] if mode == "shadow" else []
    assert sorted(r.attribution for r in records) == ["header", *process]
    [header] = [r for r in records if r.attribution == "header"]
    assert (header.run_id, header.trigger, header.end) == (start["run"], None, None)


@pytest.mark.parametrize("scenario", sorted(BOMBED))
@pytest.mark.parametrize("mode", MODES)
def test_a_bomb_on_the_read_reaches_the_agent_whole_and_is_recorded_empty(
    run_workflow, mode, scenario
):
    """A read bombed to 1 GiB reaches the agent byte for byte and is recorded empty, flagged once.

    irimi decodes only to its cap, and redaction cannot read the start of a document, so the body
    is not stored and its credential never reaches disk. The agent acts as it does bare."""
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
        "real          read      GET api.stripe.com/v1/charges -> 200  [body-truncated]"
    )
    [read] = [e for e in shadow.reported if e.request.path == "/v1/charges"]
    assert read.response is not None and (read.response.body, read.flags) == (
        b"",
        (Flag.BODY_TRUNCATED,),
    )
    [stored] = [
        e
        for e in shadow.stored_events()
        if isinstance(e, Exchange) and e.request.path == "/v1/charges"
    ]
    assert stored.response is not None and (stored.response.body, stored.flags) == (
        b"",
        (Flag.BODY_TRUNCATED,),
    )
    assert stored.response.header("content-encoding") == encoding
    # The run and, under `irimi shadow`, the process run; serve mode has none.
    runs = 2 if mode == "shadow" else 1
    assert [r.dropped_events for r in shadow.stored().list_runs()] == [0] * runs


@pytest.mark.parametrize("scenario", sorted(BOMBED))
def test_a_bomb_costs_irimi_its_cap_in_memory_not_what_it_decodes_to(traced_run, scenario):
    """A body that decodes to a GiB costs irimi at most twice its cap and a margin in memory.

    A codec assembles its output once and copies it once."""
    result, peak = traced_run(W, scenario)
    assert result.exit_code == 0
    assert "[body-truncated]" in result.exchange_lines()[0]
    assert peak < 2 * MAX_DECODED_BODY + MEMORY_MARGIN


# ---------------------------------------------------------------------- irimi compare

SYNC = "tests.e2e.workflows.agents.w10_flaky_upstream.agent:sync_payouts"
REFUND = REFUND_LINE.removeprefix("  ")
CHECKED = "faked; checked against live state: would succeed"
POSTED = f'○ slack  post to #C0PAYOUT: "refunded 500 on ch_PAYOUT1"  {CHECKED}'
NO_ACTION = f'○ slack  post to #C0PAYOUT: "payout sync: no action"  {CHECKED}'
WROTE_BOTH = [f"  = {REFUND}", f"  = {POSTED}"]
# Each scenario compared against `rate_limited`, which retried a read and then refunded and
# posted: (exit code, whether the triggers' args differ, rows).
AGAINST_A_RETRIED_READ = {
    # A read that failed and was retried, however it failed, is no write: nothing to show.
    "server_error_read": (0, False, WROTE_BOTH),
    "reset_on_read": (0, False, WROTE_BOTH),
    # A write that failed bare never fails under shadow, and one without a key is still one
    # write: nothing to show either (`retry_without_key`'s trigger was told to send no key).
    "reset_on_write": (0, False, WROTE_BOTH),
    "retry_without_key": (0, True, WROTE_BOTH),
    # irimi's check read failed, so it could neither check nor price the refund: a difference the
    # live service, not the agent, made.
    "precondition_read_fails": (
        1,
        False,
        [
            f"  ~ A {REFUND}",
            f"    B {UNCHECKED_REFUND}",
            f"  = {POSTED}",
        ],
    ),
    # The model answered prose, and the agent posted that it did nothing: the refund is gone, and
    # the post is the same post, changed.
    "malformed_llm": (1, False, [f"  - {REFUND}", f"  ~ A {POSTED}", f"    B {NO_ACTION}"]),
    # The read was a bomb the agent could not parse: no charge to refund.
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
    """`irimi compare` shows no failed read or write, but does show crashes and failed checks.

    Each scenario runs against `rate_limited` in one store. Every row is a line its own run's
    summary printed."""
    code, args_differ, rows = AGAINST_A_RETRIED_READ[scenario]
    retried = shared_store_run(W, "rate_limited")
    other = shared_store_run(W, scenario, mode)
    assert check_invariants(other) == []
    out = compare(retried, other)
    a, b = (result.one("run.start")["run"] for result in (retried, other))
    header_b = "" if scenario in CONTROL_DOWN else SYNC
    totals = [sum(row.startswith(p) for row in rows) for p in ("  = ", "  ~ ", "  - ", "  + ")]
    assert out == (
        code,
        [
            f"irimi compare · {a} vs {b}",
            f"  A  {SYNC}",
            f"  B  {header_b}".rstrip(),
            *(["  (the triggers' args differ)"] if args_differ else []),
            "",
            *rows,
            "",
            "  {} same · {} changed · {} only in A · {} only in B".format(*totals),
        ],
        [],
    )
    assert rows_not_shown(other, a, b, out[1]) == []


def test_a_refund_whose_precondition_read_failed_replays_as_unvalidated(replayed):
    """A refund recorded unchecked replays with its recorded `not_evaluable` and no check read.

    Nothing reaches the fake Stripe, and the summary says the recorded run could not check it."""
    _recorded, replay = replayed(W, "precondition_read_fails")
    [refund] = [ex for ex in replay.reported if ex.request.path == "/v1/refunds"]
    assert (refund.precondition.outcome, refund.flags) == (
        "not_evaluable",
        ("from-recording",),
    )
    assert [ex for ex in replay.reported if ex.issued_by == "engine"] == []
    assert replay.internet.requests() == []
    assert (
        "  ○ stripe  refund 500 (minor units, currency unknown) on ch_PAYOUT1  faked; could not "
        "check: the recorded run could not check it"
    ) in replay.summaries()


@pytest.mark.parametrize(
    ("scenario", "status"), [("rate_limited", 429), ("server_error_read", 500)]
)
def test_a_read_the_service_refused_replays_its_refusal_and_the_retry(replayed, scenario, status):
    """A replay hands back a recorded 429 or 500, and the agent's retry the recorded 200.

    Nothing reaches the fake Stripe, whose fault is armed again for the replay."""
    recorded, replay = replayed(W, scenario)
    assert [(c["status"], c["answered_by"]) for c in replay.calls("list_charges")] == [
        (status, "recording"),
        (200, "recording"),
    ]
    assert replay.events("retry", with_time=False) == recorded.events("retry", with_time=False)
    assert replay.returned() == recorded.returned()
    assert replay.internet.requests() == []


@pytest.mark.parametrize("scenario", ["reset_on_read", "read_timeout"])
def test_a_read_whose_recording_lost_its_upstream_fails_again_live(replayed, scenario):
    """A read recorded with no answer misses on replay, fails again live, and the retry replays.

    The miss keeps `upstream-error` beside `replay-miss`."""
    recorded, replay = replayed(W, scenario)
    [lost] = [ex for ex in recorded.reported if Flag.UPSTREAM_ERROR in ex.flags]
    first, retry = [ex for ex in replay.reported if ex.request.path == "/v1/charges"]
    assert (first.answered_by, first.response, first.flags) == (
        "live",
        None,
        ("replay-miss", Flag.UPSTREAM_ERROR),
    )
    assert lost.flags == (Flag.UPSTREAM_ERROR,)
    assert (retry.answered_by, retry.flags) == (
        "recording",
        ("from-recording",),
    )
    assert [(r.method, r.path) for r in replay.internet.requests()] == [("GET", "/v1/charges")]
    assert replay.returned() == recorded.returned()


@pytest.mark.parametrize("scenario", ["raise_after_write", "sigterm_mid_run", "sigint_mid_run"])
def test_a_run_that_died_after_its_write_replays_to_the_same_end(replayed, scenario):
    """A run that died after its write replays to the same end and exit code.

    The refund replays with its minted id, so the trigger raises, is killed or is interrupted as
    before. The replay's run ends with the recorded error type, or for a kill with its exit."""
    recorded, replay = replayed(W, scenario)
    assert replay.agent_exit_code() == recorded.exit_code != 0
    [refund] = [ex for ex in replay.reported if ex.request.path == "/v1/refunds"]
    [recorded_refund] = [ex for ex in recorded.reported if ex.request.path == "/v1/refunds"]
    assert refund.flags == ("from-recording",)
    assert refund.response.body == recorded_refund.response.body
    (source,) = recorded.sdk_runs()
    (mine,) = [r for r in replay.stored().list_runs() if r.replay_of == source.run_id]
    assert mine.end is not None
    assert (mine.attribution, mine.end.outcome, mine.end.exit_code) == (
        "process",
        "error",
        recorded.exit_code,
    )
    killed = ErrorInfo("exit", f"exited {recorded.exit_code}")
    assert mine.end.error == (killed if source.end is None else source.end.error)
    assert (
        mine.end.error.type
        == {
            "raise_after_write": "builtins.RuntimeError",
            "sigterm_mid_run": "exit",
            "sigint_mid_run": "builtins.KeyboardInterrupt",
        }[scenario]
    )
    ended = [(e["outcome"], e.get("error")) for e in recorded.events("run.end")]
    assert [(e["outcome"], e.get("error")) for e in replay.events("run.end")] == ended
    assert replay.internet.requests() == []


@pytest.mark.parametrize("mode", MODES)
def test_an_error_on_a_read_of_a_faked_refund_is_never_turned_into_the_refund(run_workflow, mode):
    """A 503 on reading back a faked refund is left as sent, unstamped, and flagged `partial`.

    Production would have got it too. The retry's 404 is answered with the faked refund, as bare
    retries the 503 into the real one."""
    shadow = run_workflow(W, "refund_read_unavailable", mode)
    bare = run_workflow(W, "refund_read_unavailable", "bare")
    assert [(c["status"], c["answered_by"]) for c in shadow.calls("confirm_refund")] == [
        (503, None),
        (200, "overlay"),
    ]
    assert statuses(bare, "confirm_refund") == [503, 200]
    assert retries(shadow) == retries(bare) == [("confirm_refund", 503)]
    [refunded] = shadow.events("refunded")
    [confirmed] = shadow.events("confirmed")
    assert (confirmed["status"], confirmed["id"]) == (200, refunded["id"])
    reads = [
        (e.response.status if e.response else None, e.answered_by, e.overlay)
        for e in shadow.stored_events()
        if isinstance(e, Exchange) and e.request.path.startswith("/v1/refunds/")
    ]
    assert reads == [(503, "live", "partial"), (200, "overlay", "full")]
    lines = [line for line in shadow.summaries() if "/v1/refunds/" in line]
    path = f"/v1/refunds/{refunded['id']}"
    assert lines == [
        f"    ↳ GET {path} did not show it  real (partial)",
        f"    ↳ GET {path} saw it  overlay",
    ]
    assert shadow.internet.writes() == []


@pytest.mark.parametrize("mode", MODES)
def test_an_error_that_is_one_long_key_is_stored_as_its_placeholder(run_workflow, mode):
    """An error that is one long key is stored as its placeholder: no part of it, never empty.

    The key runs thousands of characters past the message's cut, and is redacted before it."""
    shadow = run_workflow(W, "raise_with_key", mode)
    assert shadow.exit_code == run_workflow(W, "raise_with_key", "bare").exit_code == 1
    [run] = shadow.sdk_runs()
    assert run.end is not None and run.end.error is not None
    assert run.end.error.type == "builtins.RuntimeError"
    assert re.fullmatch(r"<redacted:[0-9a-f]{16}>", run.end.error.message)
    code, out, err = shadow.runs("show", run.run_id)
    assert (code, err) == (0, [])
    assert [line for line in out if line.startswith("error: ")] == [
        f"error: builtins.RuntimeError: {run.end.error.message}"
    ]
    assert check_invariants(shadow) == []


@pytest.mark.parametrize("mode", MODES)
def test_two_writes_checked_at_once_each_say_why_their_own_check_failed(run_workflow, mode):
    """Two refunds checked at once each say why their own check read failed (429, then 401).

    Only the 429 counts as would have fired. The ordering where both reads are stored before
    either refund is `tests/test_report.py`'s."""
    shadow = run_workflow(W, "two_checks_refused", mode)
    assert [
        (c["label"], c["status"], c["answered_by"])
        for c in shadow.calls()
        if c["label"] and c["label"].startswith("refund_")
    ] == [("refund_ch_PAYOUT1", 200, "fake-L1"), ("refund_ch_PAYOUT2", 200, "fake-L1")]
    lines = [line for line in shadow.summaries() if "○ stripe  refund" in line]
    assert lines == [
        f"  ○ stripe  refund 500 (minor units, currency unknown) on {charge}  faked; could not "
        f"check: stripe answered irimi's check read with {status}"
        for charge, status in (("ch_PAYOUT1", 429), ("ch_PAYOUT2", 401))
    ]
    assert "  would have fired: refund.created, charge.refunded" in shadow.summaries()
    checks = [
        (e.request.path, e.response.status if e.response else None)
        for e in shadow.stored_events()
        if isinstance(e, Exchange) and e.issued_by == "engine"
    ]
    assert checks == [("/v1/charges/ch_PAYOUT1", 429), ("/v1/charges/ch_PAYOUT2", 401)]
    assert shadow.internet.writes() == []


@pytest.mark.parametrize("mode", MODES)
def test_a_control_endpoint_that_never_answers_is_waited_for_once(run_workflow, mode):
    """A control endpoint that never answers costs the agent one SDK timeout, not one per post.

    The SDK pauses every post after the start times out, so the run's end is never sent."""
    shadow = run_workflow(W, "control_stalled", mode)
    assert [e["count"] for e in shadow.events("control_connections")] == [1]
