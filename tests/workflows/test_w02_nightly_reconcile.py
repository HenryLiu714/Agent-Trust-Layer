"""W2 `nightly_reconcile`: a scheduled batch at volume, pinned per scenario.

The agent is `examples/workflows/w02_nightly_reconcile/agent.py`. Its ledger is a SQLite file in
the run's state directory, seeded by the agent before the run; only `mark_reconciled` (a write
tool) changes it.
"""

import pytest

from examples.workflows.harness.run import check_invariants, compare, replay, replayed_run_id
from examples.workflows.w02_nightly_reconcile.agent import LEDGER, SOURCE_ENV, customer_id
from irimi.exchange import Exchange

W = "w02_nightly_reconcile"
# Each test runs under `irimi shadow -- <cmd>` and under serve mode, its agent beside irimi.
MODES = ("shadow", "serve")
AGENT = "examples.workflows.w02_nightly_reconcile.agent"
CUSTOMER_UPDATE = "faked         write     POST api.stripe.com/v1/customers/"
PASSED = "faked; checked against live state: would succeed"
TOTALS = "  47 exchanges · 6 real · 41 answered by irimi (41 faked writes) · 0 sent to targets"


def reconciled_rows(result):
    [(count,)] = result.query(LEDGER, "SELECT count(*) FROM ledger WHERE reconciled_on IS NOT NULL")
    return count


@pytest.mark.parametrize("mode", MODES)
def test_a_clean_night_pages_everything_and_posts_one_summary(run_workflow, mode):
    shadow = run_workflow(W, "clean", mode)
    assert len(shadow.calls("list_charges")) == 2  # twelve charges, pages of ten
    assert shadow.result(with_time=False) == {"event": "result", "total": 0, "slack_ok": True}
    # LOOKS WRONG (#61): the human template writes `#{channel}` over a channel id, so an id
    # prints as `#C0RECON`, a channel name no one has.
    assert (
        f'  ○ slack  post to #C0RECON: "reconciled 0 for 2026-09-27"  {PASSED}'
        in shadow.summaries()
    )
    # The SDK's run wrapped the whole job, and the read tool ran for real inside it.
    run_id = shadow.events("run.start")[0]["run"]
    assert [t["run"] for t in shadow.events("tool")] == [run_id]
    # The default tool name is the real module's, because the harness launches every agent
    # through `examples.workflows.launch` rather than as `__main__`.
    assert shadow.tools("read") == [(f"{AGENT}.load_ledger", "real")]


@pytest.mark.parametrize("mode", MODES)
def test_forty_mismatches_are_forty_faked_writes_and_the_ledger_is_untouched(run_workflow, mode):
    shadow = run_workflow(W, "forty_mismatches", mode)
    bare = run_workflow(W, "forty_mismatches", "bare")
    assert len(shadow.calls("list_charges")) == 5
    assert [c["answered_by"] for c in shadow.calls("tag_customer")] == ["fake-L1"] * 40
    assert sum(line.startswith(CUSTOMER_UPDATE) for line in shadow.exchange_lines()) == 40
    summary = shadow.summaries()
    assert "  api.stripe.com  5 reads  40 faked writes" in summary
    assert sum("○ stripe  update customer cus_N0" in line for line in summary) == 40
    assert TOTALS in summary
    # Forty writes fire one event name, listed once.
    assert "  would have fired: customer.updated" in summary
    # And forty ledger marks, one line each after the forty updates, as the HTTP writes are each
    # a line however many there are.
    tool_lines = [line for line in summary if line.startswith("  ○ tool ")]
    assert tool_lines == [f"  ○ tool  {AGENT}.mark_reconciled  fake ran instead"] * 40
    assert summary.index(tool_lines[0]) > max(
        i for i, line in enumerate(summary) if "○ stripe  update customer" in line
    )
    assert (
        shadow.tool_call_lines().count(f"tool-fake     write     {AGENT}.mark_reconciled -> ok")
        == 40
    )
    # The write tool ran its stand-in forty times under shadow, and for real forty times bare.
    assert shadow.tools("write") == [(f"{AGENT}.mark_reconciled", "shadow")] * 40
    assert bare.tools("write") == [(f"{AGENT}.mark_reconciled", "real")] * 40
    assert reconciled_rows(shadow) == 0
    assert reconciled_rows(bare) == 40
    assert len(bare.internet.writes()) == 41  # forty customers and the Slack post


@pytest.mark.parametrize("mode", MODES)
def test_forty_mismatches_store_all_47_exchanges_and_drop_none(run_workflow, mode):
    """The busiest run in the corpus, stored whole: every exchange irimi reported is on
    disk, and no run records a dropped event. Under serve mode the run is the only one,
    with no process run beside it, and its block is printed from the store at its end."""
    shadow = run_workflow(W, "forty_mismatches", mode)
    stored = [e for e in shadow.stored_events() if isinstance(e, Exchange)]
    assert len(stored) == len(shadow.exchange_lines()) == 47
    process = ["0 exchanges 0 writes"] if mode == "shadow" else []
    assert [r.dropped_events for r in shadow.stored().list_runs()] == [0] * (len(process) + 1)
    # `irimi runs list` counts them in the run that holds them, and its writes are the summary's
    # `41 faked writes`: forty customer updates and the Slack post. Its last two columns, after
    # the header row.
    code, out, err = shadow.runs("list")
    assert (code, err) == (0, [])
    counts = sorted(" ".join(line.split()[-4:]) for line in out[1:])
    assert counts == [*process, "47 exchanges 41 writes"]
    assert TOTALS in shadow.summaries()


@pytest.mark.parametrize("mode", MODES)
def test_a_datetime_trigger_is_stored_by_its_repr_and_cannot_be_replayed(run_workflow, mode):
    shadow = run_workflow(W, "datetime_trigger", mode)
    [trigger] = shadow.events("trigger")
    assert trigger["type"] == "datetime"
    assert trigger["value"].startswith("datetime.datetime(2026, 9, 27, 2, 0")
    # A `datetime` is not JSON, so the SDK captures it as its repr and the run is not replayable.
    # `sdk.run` records no entrypoint, so `irimi replay` refuses it for that first, before it
    # would ask for `--args`: only `--call` or `-- CMD` replays it.
    [run] = shadow.sdk_runs()
    assert run.trigger is not None
    assert (run.trigger.name, run.trigger.entrypoint) == ("nightly-reconcile", None)
    when = "datetime.datetime(2026, 9, 27, 2, 0, tzinfo=datetime.timezone.utc)"
    assert run.trigger.args == {"date": {"__irimi_repr__": when}}
    assert not run.trigger.replayable
    code, out, err = shadow.command("replay", run.run_id, "--store", str(shadow.store_root()))
    assert (code, out) == (2, [])
    assert err == [f"error: run {run.run_id} has no entrypoint and no argv; pass --call or -- CMD"]
    assert [c["url"] for c in shadow.calls("tag_customer")] == [
        "api.stripe.com/v1/customers/cus_N001",
        "api.stripe.com/v1/customers/cus_N000",
    ]


@pytest.mark.parametrize("mode", MODES)
def test_the_same_date_twice_is_six_writes_in_both_modes(run_workflow, mode):
    shadow = run_workflow(W, "same_date_twice", mode)
    bare = run_workflow(W, "same_date_twice", "bare")
    # Nothing in the run stops the repeat, so the report shows each customer update twice: the
    # duplicate a future check for repeated writes would flag. Bare, the same six reach Stripe.
    lines = [line for line in shadow.summaries() if "○ stripe  update customer" in line]
    assert (
        lines
        == [f"  ○ stripe  update customer cus_N00{i}  faked; not checked" for i in (2, 1, 0)] * 2
    )
    assert [r.path for r in bare.internet.writes() if r.host == "api.stripe.com"] == [
        f"/v1/customers/cus_N00{i}" for i in (2, 1, 0)
    ] * 2
    assert shadow.result()["total"] == bare.result()["total"] == 6


@pytest.mark.parametrize("mode", MODES)
def test_paging_from_a_minted_refund_is_translated_and_finds_the_real_one(run_workflow, mode):
    shadow = run_workflow(W, "refund_then_page", mode)
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
    # irimi dropped the cursor naming its own refund before forwarding: Stripe never heard
    # the minted id.
    refund_reads = [r.query for r in shadow.internet.requests() if r.path == "/v1/refunds"]
    assert refund_reads == [{"charge": "ch_N000", "limit": "1"}] * 2
    summary = shadow.summaries()
    i = summary.index(f"  ○ stripe  refund $1.00 on ch_N000  {PASSED}")
    assert summary[i + 1] == "    ↳ GET /v1/refunds saw it  overlay"
    # The translated page is `live` with no line of its own.
    assert "real          read      GET api.stripe.com/v1/refunds -> 200" in shadow.exchange_lines()


def test_irimi_compare_shows_a_clean_night_and_forty_mismatches_eighty_writes_apart(
    shared_store_run,
):
    """Compare at volume: forty customer updates and forty write tools only B made, each in the
    order B made it, and the one Slack post both made, changed by its count."""
    clean = shared_store_run(W, "clean")
    forty = shared_store_run(W, "forty_mismatches")
    assert check_invariants(forty) == []
    code, out, err = compare(clean, forty)
    a, b = (result.one("run.start")["run"] for result in (clean, forty))
    reconciled = "○ tool  examples.workflows.w02_nightly_reconcile.agent.mark_reconciled"
    added = [
        line
        for i in reversed(range(40))
        for line in (
            f"  + ○ stripe  update customer {customer_id(i)}  faked; not checked",
            f"  + {reconciled}  fake ran instead",
        )
    ]
    post = 'slack  post to #C0RECON: "reconciled {} for 2026-09-27"  ' + PASSED
    assert (code, err) == (1, [])
    assert out == [
        f"irimi compare · {a} vs {b}",
        "  A  nightly-reconcile",
        "  B  nightly-reconcile",
        "",
        *added,
        f"  ~ A ○ {post.format(0)}",
        f"    B ○ {post.format(40)}",
        "",
        "  0 same · 1 changed · 0 only in A · 80 only in B",
    ]


def test_a_replay_that_tags_each_customer_otherwise_compares_its_updates_changed(replayed):
    """Two customer updates, `POST /v1/customers/cus_N00x`, replayed with `RECONCILE_SOURCE` set,
    so each sends one more field. The map's sentence, `update customer {customer}`, names none of
    what an update sets, so the two runs print the same lines for both; compared by what they
    sent, each is changed, the field that differs is printed under it, and the replay exits 1.
    Replayed by its command: the run is an `sdk.run` block, which records no entrypoint."""
    recorded, replay = replayed(W, "two_mismatches", {SOURCE_ENV: "backfill"}, command=True)
    assert check_invariants(replay) == []
    assert replay.internet.writes() == []
    source = recorded.one("run.start")["run"]
    reconciled = "○ tool  examples.workflows.w02_nightly_reconcile.agent.mark_reconciled"
    post = f'slack  post to #C0RECON: "reconciled 2 for 2026-09-27"  {PASSED}'
    rows = [
        line
        for i in (1, 0)
        for line in (
            f"  ~ A ○ stripe  update customer {customer_id(i)}  faked; not checked",
            f"    B ○ stripe  update customer {customer_id(i)}  faked; not checked",
            '      body.metadata.source: (absent) -> "backfill"',
            f"  = {reconciled}  fake ran instead",
        )
    ]
    compared = [
        *rows,
        f"  = ○ {post}",
        "",
        "  3 same · 2 changed · 0 only in A · 0 only in B",
    ]
    assert replay.exit_code == 1
    result = replay.replay_result()
    assert result[0] == f"irimi replay · {source} → {replay.process_run_id()}"
    assert result[result.index("") + 1 :] == compared
    # `irimi compare` over the same two runs prints the same rows and exits as the replay did.
    code, out, err = replay.compare_runs(source, replay.process_run_id())
    assert (code, err) == (1, [])
    assert out[out.index("") + 1 :] == compared


def test_a_refund_then_page_replay_answers_the_page_naming_its_minted_refund(replayed):
    """The refund is answered with its recorded answer, so its minted `re_` is the recorded one,
    and the page whose cursor names it is translated as it was recorded and answered from
    the recording. Nothing reaches the fake internet. Replayed by its command: the run is an
    `sdk.run` block, which records no entrypoint to call."""
    recorded, replay = replayed(W, "refund_then_page", command=True)
    assert replay.events("refunds", with_time=False) == recorded.events("refunds", with_time=False)
    # Each answer is what it was when recorded: the first page is still the overlay's, its
    # bytes edited to show the refund, and the page after it the service's, from disk.
    assert [(c["label"], c["answered_by"]) for c in replay.calls()] == [
        ("list_charges", "recording"),
        ("refund", "fake-L1"),
        ("list_refunds", "overlay"),
        ("page_refunds", "recording"),
        (None, "fake-L1"),
    ]
    assert replay.internet.requests() == []
    summary = replay.summaries()
    i = summary.index(f"  ○ stripe  refund $1.00 on ch_N000  {PASSED}")
    assert summary[i + 1] == "    ↳ GET /v1/refunds saw it  overlay"
    assert (
        "  api.stripe.com  3 reads (1 showing this run's writes, 3 from the recording)"
        "  1 faked write"
    ) in summary


def test_a_replay_of_a_replay_answers_the_page_from_the_first_replay(replayed, tmp_path):
    """A replay is a stored run like any other, so it can be replayed. The engine records a
    read as it would ask it, translated, whoever answers it, so the second replay finds the page
    the first one answered just as the first found the recorded one."""
    recorded, first = replayed(W, "refund_then_page", command=True)
    before = [r.run_id for r in first.stored().list_runs()]
    second = replay(first, replayed_run_id(first), tmp_path / "again", command=True)
    # Into a copy of the first replay's store, which the session caches for other tests.
    assert [r.run_id for r in first.stored().list_runs()] == before
    assert [(c["label"], c["answered_by"]) for c in second.calls()] == [
        (c["label"], c["answered_by"]) for c in first.calls()
    ]
    assert second.internet.requests() == []
    assert second.events("refunds", with_time=False) == recorded.events("refunds", with_time=False)
    # `runs list` lists each replay as the run it replayed, and `runs show` names it. Every
    # run the store holds, by its attribution column: the recording's two as they were.
    source = replayed_run_id(recorded)
    middle, last = first.process_run_id(), second.process_run_id()
    assert second.listed_attributions() == {
        recorded.process_run_id(): "process",
        source: "sdk",
        middle: f"replay of {source}",
        last: f"replay of {middle}",
    }
    for run_id, of in ((middle, source), (last, middle)):
        code, shown, _ = second.runs("show", run_id, "--store", str(second.store_root()))
        assert code == 0
        assert shown[:4] == [
            f"run: {run_id}",
            "attribution: process",
            "mode: replay",
            f"replay of: {of}",
        ]
    # A shadow run's block names neither.
    code, shown, _ = second.runs("show", source, "--store", str(second.store_root()))
    assert code == 0
    assert shown[:3] == [f"run: {source}", "attribution: sdk", "trigger: nightly-reconcile"]
