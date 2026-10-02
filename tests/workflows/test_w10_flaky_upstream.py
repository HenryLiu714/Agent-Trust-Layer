"""W10 `flaky_upstream`: which failures a shadow run shows, and which it cannot.

A failed READ is forwarded, so it fails under shadow too and the agent's retry runs, though not
always in the same shape: a reset arrives as the proxy's 502. A failed WRITE never happens under
shadow, because the write never leaves irimi: the agent's retry path for it is exercised only bare.
A failed L3 read is irimi's alone, and only its summary shows it. The agent is
`examples/workflows/w10_flaky_upstream/agent.py`.
"""

from irimi.exchange import UPSTREAM_ERROR_FLAG, Exchange
from irimi.trace import ErrorInfo

W = "w10_flaky_upstream"
REFUND_LINE = "  ○ refund $5.00 on ch_PAYOUT1  unvalidated (L3 preconditions passed)"


def statuses(result, label):
    return [c.get("status", c.get("error")) for c in result.calls(label)]


def retries(result):
    return [(r["label"], r["reason"]) for r in result.events("retry")]


def listed_status(result) -> str:
    """The process run's status column in `irimi runs list` (#72)."""
    code, out, err = result.runs("list")
    assert (code, err) == (0, [])
    [line] = [line for line in out if line.startswith(result.process_run_id() + "  ")]
    return line.split("  ")[3]


def shown_outcome(result) -> list[str]:
    """The process run's `outcome:` line in `irimi runs show`, and the header lines after it."""
    code, out, err = result.runs("show", result.process_run_id())
    assert (code, err) == (0, [])
    start = out.index(next(line for line in out if line.startswith("outcome: ")))
    return out[start : out.index("", start)]


def test_a_rate_limited_read_fails_under_shadow_too_and_is_retried(run_workflow):
    for mode in ("bare", "shadow"):
        result = run_workflow(W, "rate_limited", mode)
        assert statuses(result, "list_charges") == [429, 200]
        assert retries(result) == [("list_charges", 429)]
    lines = run_workflow(W, "rate_limited", "shadow").exchange_lines()
    assert lines[:2] == [
        "live      read      GET api.stripe.com/v1/charges -> 429",
        "live      read      GET api.stripe.com/v1/charges -> 200",
    ]


def test_a_server_error_on_a_read_is_forwarded_and_retried(run_workflow):
    for mode in ("bare", "shadow"):
        result = run_workflow(W, "server_error_read", mode)
        assert statuses(result, "list_charges") == [500, 200]
    shadow = run_workflow(W, "server_error_read", "shadow")
    assert shadow.exchange_lines()[0] == "live      read      GET api.stripe.com/v1/charges -> 500"
    assert REFUND_LINE in shadow.summary()


def test_a_read_the_agent_abandoned_is_recorded_as_an_upstream_error(run_workflow):
    shadow = run_workflow(W, "read_timeout", "shadow")
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


def test_a_reset_on_a_read_reaches_the_agent_as_an_unstamped_502(run_workflow):
    bare = run_workflow(W, "reset_on_read", "bare")
    shadow = run_workflow(W, "reset_on_read", "shadow")
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
    assert REFUND_LINE in shadow.summary()
    # Stored as printed (#70): an exchange with no response reads back as one.
    assert unanswered(shadow) == [("/v1/charges", (UPSTREAM_ERROR_FLAG,))]


def unanswered(result) -> list[tuple[str, tuple[str, ...]]]:
    """`(path, flags)` of each stored exchange with no response."""
    return [
        (e.request.path, e.flags)
        for e in result.stored_events()
        if isinstance(e, Exchange) and e.response is None
    ]


def test_a_failed_precondition_read_degrades_the_check_and_the_agent_never_knows(run_workflow):
    bare = run_workflow(W, "precondition_read_fails", "bare")
    shadow = run_workflow(W, "precondition_read_fails", "shadow")
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
    refunds = [line for line in shadow.summary() if "○ refund" in line]
    assert refunds == ["  ○ refund 500 on ch_PAYOUT1  unvalidated (L2)"]


def test_a_reset_on_the_write_is_retried_bare_and_never_happens_under_shadow(run_workflow):
    bare = run_workflow(W, "reset_on_write", "bare")
    shadow = run_workflow(W, "reset_on_write", "shadow")
    assert statuses(bare, "refund") == ["ConnectionResetError", 200]
    assert retries(bare) == [("refund", "NetworkError")]
    # Under shadow the refund is answered by irimi and never reaches the service that resets it,
    # so the agent's retry path is not exercised. A shadow run cannot show it.
    assert statuses(shadow, "refund") == [200]
    assert retries(shadow) == []
    # Bare, the key made the retry one refund.
    assert len(bare.world.stripe.refunds) == 1
    assert shadow.world.stripe.refunds == []


def test_a_keyless_write_retried_after_a_500_is_two_posts_bare_and_one_fake_under_shadow(
    run_workflow,
):
    bare = run_workflow(W, "retry_without_key", "bare")
    shadow = run_workflow(W, "retry_without_key", "shadow")
    refund_posts = [r for r in bare.internet.writes() if r.path == "/v1/refunds"]
    assert len(refund_posts) == 2
    assert all("idempotency-key" not in r.headers for r in refund_posts)
    assert statuses(bare, "refund") == [500, 200]
    assert len(bare.world.stripe.refunds) == 1  # the 500 was answered before Stripe acted
    # Shadow: the 500 cannot happen, the write is faked once, and the summary shows one refund.
    assert statuses(shadow, "refund") == [200]
    assert [line for line in shadow.summary() if "○ refund" in line] == [REFUND_LINE]


def test_a_model_that_answers_prose_leads_to_no_write_but_the_status_post(run_workflow):
    shadow = run_workflow(W, "malformed_llm", "shadow")
    assert [e["text"][:5] for e in shadow.events("llm_fallback")] == ["Sure!"]
    assert shadow.calls("refund") == []
    assert (
        "live      llm       POST api.anthropic.com/v1/messages -> 200" in shadow.exchange_lines()
    )
    assert [line for line in shadow.summary() if "○" in line] == [
        '  ○ post to #C0PAYOUT: "payout sync: no action"  unvalidated (L3 preconditions passed)'
    ]
    assert len(shadow.world.llm.calls) == 1


def test_an_agent_that_raises_after_its_write_still_shows_the_write(run_workflow):
    shadow = run_workflow(W, "raise_after_write", "shadow")
    assert shadow.exit_code == run_workflow(W, "raise_after_write", "bare").exit_code == 1
    [end] = shadow.events("run.end")
    assert (end["outcome"], end["error"]) == ("error", "RuntimeError")
    # Stored under the exception's full name (#74), the refund id it names included.
    [run] = shadow.sdk_runs()
    assert run.outcome == "error" and run.error is not None
    assert run.error.type == "builtins.RuntimeError"
    assert run.error.message.startswith("ledger export failed after refund re_")
    summary = shadow.summary()
    assert REFUND_LINE in summary
    # LOOKS WRONG: the summary does not say the agent failed. Nothing in it tells this run from
    # one that finished.
    assert not any("error" in line.lower() or "exit" in line for line in summary)
    # The stored run does say so (#70); the summary above still does not.
    assert process_run(shadow) == ("error", 1, ErrorInfo("exit", "exited 1"))
    # `irimi runs list` and `runs show` print it where the summary cannot (#72). The summary pin
    # above stays: a stored run's summary is the live one, and the live one says nothing.
    assert listed_status(shadow) == "error"
    assert shown_outcome(shadow) == ["outcome: error", "error: exit: exited 1", "exit code: 1"]


def test_an_agent_killed_after_its_write_leaves_a_run_with_no_end(run_workflow):
    bare = run_workflow(W, "sigterm_mid_run", "bare")
    shadow = run_workflow(W, "sigterm_mid_run", "shadow")
    # 143 is 128 + SIGTERM, the shell's spelling, in both modes.
    assert (bare.exit_code, shadow.exit_code) == (143, 143)
    # The run started and never ended: an incomplete run (`outcome: None`), as stored below.
    assert len(shadow.events("run.start")) == 1
    assert shadow.events("run.end") == []
    assert shadow.calls("refund")[0]["answered_by"] == "fake-L1"
    assert REFUND_LINE in shadow.summary()
    # LOOKS WRONG: as after a raise, the summary ends as a finished run's does, with no word that
    # the agent was killed.
    summary = shadow.summary()
    assert summary[-1] == (
        "  These writes did not happen. Would have fired: refund.created, charge.refunded."
    )
    assert not any(word in line.lower() for line in summary for word in ("143", "kill", "signal"))
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


def process_run(result) -> tuple[object, object, object]:
    """`(outcome, exit_code, error)` of the process run `irimi shadow` stored."""
    (run,) = [r for r in result.stored().list_runs() if r.attribution == "process"]
    return run.outcome, run.exit_code, run.error
