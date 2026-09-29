"""W2 `nightly_reconcile`: a scheduled batch at volume, pinned per scenario.

The agent is `examples/workflows/w02_nightly_reconcile/agent.py`. Its ledger is a SQLite file in
the run's state directory, seeded by the agent before the run; only `mark_reconciled` (a write
tool) changes it.
"""

from examples.workflows.w02_nightly_reconcile.agent import LEDGER

W = "w02_nightly_reconcile"
AGENT = "examples.workflows.w02_nightly_reconcile.agent"
CUSTOMER_UPDATE = "fake-L1   write     POST api.stripe.com/v1/customers/"


def reconciled_rows(result):
    [(count,)] = result.query(LEDGER, "SELECT count(*) FROM ledger WHERE reconciled_on IS NOT NULL")
    return count


def test_a_clean_night_pages_everything_and_posts_one_summary(run_workflow):
    shadow = run_workflow(W, "clean", "shadow")
    assert len(shadow.calls("list_charges")) == 2  # twelve charges, pages of ten
    assert shadow.result(with_time=False) == {"event": "result", "total": 0, "slack_ok": True}
    # LOOKS WRONG (#61): the human template writes `#{channel}` over a channel id, so an id
    # prints as `#C0RECON`, a channel name no one has.
    assert (
        '  ○ post to #C0RECON: "reconciled 0 for 2026-09-27"  unvalidated (L3 preconditions passed)'
        in shadow.summary()
    )
    # The fallback SDK's run wrapped the whole job, and the read tool ran for real inside it.
    run_id = shadow.events("run.start")[0]["run"]
    assert [t["run"] for t in shadow.events("tool")] == [run_id]
    # #76's default tool name is the real module's, because the harness launches every agent
    # through `examples.workflows.launch` rather than as `__main__`.
    assert shadow.tools("read") == [(f"{AGENT}.load_ledger", "real")]


def test_forty_mismatches_are_forty_faked_writes_and_the_ledger_is_untouched(run_workflow):
    shadow = run_workflow(W, "forty_mismatches", "shadow")
    bare = run_workflow(W, "forty_mismatches", "bare")
    assert len(shadow.calls("list_charges")) == 5
    assert [c["answered_by"] for c in shadow.calls("tag_customer")] == ["fake-L1"] * 40
    assert sum(line.startswith(CUSTOMER_UPDATE) for line in shadow.exchange_lines()) == 40
    summary = shadow.summary()
    assert "  api.stripe.com  5 reads  40 writes intercepted" in summary
    assert sum("○ update customer cus_N0" in line for line in summary) == 40
    assert "  47 exchanges · 6 live · 0 delegated · 41 virtualized" in summary
    # Forty writes fire one event name, listed once.
    assert summary[-1] == "  These writes did not happen. Would have fired: customer.updated."
    # The write tool ran its stand-in forty times under shadow, and for real forty times bare.
    assert shadow.tools("write") == [(f"{AGENT}.mark_reconciled", "shadow")] * 40
    assert bare.tools("write") == [(f"{AGENT}.mark_reconciled", "real")] * 40
    assert reconciled_rows(shadow) == 0
    assert reconciled_rows(bare) == 40
    assert len(bare.internet.writes()) == 41  # forty customers and the Slack post


def test_a_datetime_trigger_is_only_logged_today(run_workflow):
    shadow = run_workflow(W, "datetime_trigger", "shadow")
    [trigger] = shadow.events("trigger")
    # Under #74 this value captures as `{"__irimi_repr__": ...}` and the run is not replayable
    # (#84 then asks for `--args`). Today the fallback SDK records no trigger at all.
    assert trigger["type"] == "datetime"
    assert trigger["value"].startswith("datetime.datetime(2026, 9, 27, 2, 0")
    assert [c["url"] for c in shadow.calls("tag_customer")] == [
        "api.stripe.com/v1/customers/cus_N001",
        "api.stripe.com/v1/customers/cus_N000",
    ]


def test_the_same_date_twice_is_six_writes_in_both_modes(run_workflow):
    shadow = run_workflow(W, "same_date_twice", "shadow")
    bare = run_workflow(W, "same_date_twice", "bare")
    # Nothing in the run stops the repeat, so the report shows each customer update twice: the
    # duplicate a Phase 4 report must flag. Bare, the same six reach Stripe.
    lines = [line for line in shadow.summary() if "○ update customer" in line]
    assert lines == [f"  ○ update customer cus_N00{i}  unvalidated (L1)" for i in (2, 1, 0)] * 2
    assert [r.path for r in bare.internet.writes() if r.host == "api.stripe.com"] == [
        f"/v1/customers/cus_N00{i}" for i in (2, 1, 0)
    ] * 2
    assert shadow.result()["total"] == bare.result()["total"] == 6


def test_paging_from_a_minted_refund_is_translated_and_finds_the_real_one(run_workflow):
    shadow = run_workflow(W, "refund_then_page", "shadow")
    bare = run_workflow(W, "refund_then_page", "bare")
    [seen] = shadow.events("refunds")
    assert seen["minted"].startswith("re_") and seen["minted"] != "re_PRIOR000"
    # The first page shows the refund irimi faked; the page after it shows the real, older one,
    # exactly as it does bare.
    assert seen["first"] == [seen["minted"]]
    assert seen["after"] == bare.events("refunds")[0]["after"] == ["re_PRIOR000"]
    assert [
        (c["label"], c["answered_by"]) for c in shadow.calls() if c["label"] != "list_charges"
    ] == [
        ("refund", "fake-L1"),
        ("list_refunds", "overlay"),
        ("page_refunds", None),
        (None, "fake-L1"),
    ]
    # irimi dropped the cursor naming its own refund before forwarding (#53): Stripe never heard
    # the minted id.
    refund_reads = [r.query for r in shadow.internet.requests() if r.path == "/v1/refunds"]
    assert refund_reads == [{"charge": "ch_N000", "limit": "1"}] * 2
    summary = shadow.summary()
    i = summary.index("  ○ refund $1.00 on ch_N000  unvalidated (L3 preconditions passed)")
    assert summary[i + 1] == "    ↳ GET /v1/refunds saw it  overlay"
    # The translated page is `live` with no line of its own.
    assert "live      read      GET api.stripe.com/v1/refunds -> 200" in shadow.exchange_lines()
