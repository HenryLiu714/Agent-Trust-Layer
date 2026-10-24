"""W6 `crm_db_agent`: labelled database and file writes run their fakes under shadow.

Which body ran is read from the agent's `tool` events and state, irimi's view from its store."""

from typing import cast

import pytest

from irimi.core.limits import MAX_ERROR_MESSAGE
from irimi.core.secret_shapes import CUT_MARK
from irimi.protocol.trace import REPR_KEY, UNATTRIBUTED, ErrorInfo, ToolCall
from irimi.record import redact
from irimi.sdk._capture import MAX_REPR
from tests.e2e.workflows.agents.agentkit import LOOKUP_REFUSED_ENV
from tests.e2e.workflows.agents.w06_crm_db_agent.agent import (
    CONNECT_RETRIES,
    DB_NAME,
    SEED,
    UNREACHABLE_HOST,
    DriverOptions,
)
from tests.e2e.workflows.agents.w06_crm_db_agent.scenarios import STALE_REPLAY_RUN
from tests.e2e.workflows.harness.compare import compare, rows_not_shown
from tests.e2e.workflows.harness.invariants import CANARIES, check_invariants

W = "w06_crm_db_agent"
# Each test runs under `irimi shadow -- <cmd>` and under serve mode, its agent beside irimi.
MODES = ("shadow", "serve")
WRITE_TOOLS = [
    "crm.upsert_account",
    "crm.upsert_account",
    "crm.bulk_update",
    "crm.delete_stale",
    "crm.export_csv",
    "crm.notify_owner",
]
# The totals line of a run whose only exchanges are the two enrichment reads.
TWO_REAL = "  2 exchanges · 2 real · 0 answered by irimi (0 faked writes) · 0 sent to targets"
# The seeded `smb` accounts, as `find_accounts` returns them.
SEED_SMB = [
    {
        "id": i,
        "name": n,
        "segment": s,
        "last_seen": seen,
        "score": score,
        "industry": industry,
    }
    for i, n, s, seen, score, industry in SEED
    if s == "smb"
]


# The CRM after `_touch_last_seen("acc_1")`, the write no label covers, which is real in every mode.
TOUCHED = [("acc_1", "Acme", "smb", "2026-09-28", 0, None), *SEED[1:]]


def rows(result):
    return result.query(DB_NAME, "SELECT * FROM accounts ORDER BY id")


def tool_calls(result) -> list[ToolCall]:
    """The tool calls irimi stored, in the order the run made them: one run holds them all."""
    return [e for e in result.stored_events() if isinstance(e, ToolCall)]


def files(result):
    return sorted(str(p.relative_to(result.state)) for p in result.state.rglob("*") if p.is_file())


@pytest.mark.parametrize("mode", MODES)
def test_under_shadow_every_write_tool_runs_its_stand_in_and_the_crm_is_as_seeded(
    run_workflow, mode
):
    shadow = run_workflow(W, "enrich", mode)
    assert shadow.tools("write") == [(name, "shadow") for name in WRITE_TOOLS]
    assert rows(shadow) == SEED
    assert files(shadow) == ["crm.sqlite3"]
    # The fakes' answers are what the agent reported, not the real bodies'.
    assert shadow.result()["deleted"] == 0
    assert shadow.result()["exported"] == "<not written: smb.csv>"
    assert shadow.result()["notified"] == {"stood_in": True}


def test_bare_the_same_calls_really_change_the_crm_and_write_files(run_workflow):
    bare = run_workflow(W, "enrich", "bare")
    assert bare.tools("write") == [(name, "real") for name in WRITE_TOOLS]
    assert rows(bare) == [
        ("acc_1", "Acme", "smb", "2026-01-01", 87, "manufacturing"),
        ("acc_3", "Initech", "enterprise", "2026-06-01", 0, None),
    ]
    assert files(bare) == ["crm.sqlite3", "exports/smb.csv", "outbox.jsonl"]
    assert bare.result()["deleted"] == 1


@pytest.mark.parametrize("mode", MODES)
def test_read_tools_run_for_real_in_both_modes_and_the_call_sequence_is_the_same(
    run_workflow, mode
):
    bare = run_workflow(W, "enrich", "bare")
    shadow = run_workflow(W, "enrich", mode)
    assert [n for n, _ in bare.tools()] == [n for n, _ in shadow.tools()]
    assert {ran for _, ran in shadow.tools("read")} == {"real"}
    # Decimal and datetime came back from the read tool as themselves: not JSON, not revivable
    # once recorded, so a replay must run `score_account` for real.
    assert {tuple(e["types"]) for e in shadow.events("scored")} == {("Decimal", "datetime")}


@pytest.mark.parametrize("mode", MODES)
def test_http_made_inside_a_read_tool_is_an_ordinary_exchange_of_the_run(run_workflow, mode):
    shadow = run_workflow(W, "enrich", mode)
    assert shadow.exchange_lines() == [
        "real          read      GET enrich.internal/v1/companies/acc_1 -> 200",
        "real          read      GET enrich.internal/v1/companies/acc_2 -> 200",
    ]
    # Each exchange is in the run the tool was called in, with no link to the call, and
    # neither count holds a tool call.
    [run] = shadow.sdk_runs()
    stored = shadow.stored().load_run(run.run_id).events
    assert [e.request.path for e in stored if not isinstance(e, ToolCall)] == [
        "/v1/companies/acc_1",
        "/v1/companies/acc_2",
    ]
    assert TWO_REAL in shadow.summaries()


@pytest.mark.parametrize("mode", MODES)
def test_irimi_prints_every_tool_call_and_a_summary_line_for_each_stood_in_write(
    run_workflow, mode
):
    """The stream prints every tool call, and the summary one line per write tool whose fake ran.

    A read tool gets no summary line. The run's only writes are tool writes, and the summary still
    says they did not happen."""
    shadow = run_workflow(W, "enrich", mode)
    assert shadow.tool_call_lines() == [
        "tool-real     read      crm.connect_with -> ok",
        "tool-real     read      crm.find_accounts -> ok",
        "tool-real     read      crm.fetch_enrichment -> ok",
        "tool-real     read      crm.score_account -> ok",
        "tool-fake     write     crm.upsert_account -> ok",
        "tool-real     read      crm.fetch_enrichment -> ok",
        "tool-real     read      crm.score_account -> ok",
        "tool-fake     write     crm.upsert_account -> ok",
        "tool-fake     write     crm.bulk_update -> ok",
        "tool-fake     write     crm.delete_stale -> ok",
        "tool-fake     write     crm.export_csv -> ok",
        "tool-fake     write     crm.notify_owner -> ok",
    ]
    summary = shadow.summaries()
    assert summary[1 : summary.index("  enrich.internal  2 reads")] == [
        "",
        "  None of these writes reached the real service:",
        *[f"  ○ tool  {name}  fake ran instead" for name in WRITE_TOOLS],
        "",
    ]
    assert TWO_REAL in summary


@pytest.mark.parametrize("mode", MODES)
def test_every_tool_call_is_stored_in_its_run_with_what_ran_and_what_it_returned(
    run_workflow, mode
):
    shadow = run_workflow(W, "enrich", mode)
    [run] = shadow.sdk_runs()
    calls = tool_calls(shadow)
    assert {call.run_id for call in calls} == {run.run_id}
    # What the run's own log says ran is what irimi stored, call for call.
    assert [(c.name, c.ran) for c in calls] == shadow.tools()
    assert {(c.kind, c.ran) for c in calls} == {("read", "real"), ("write", "shadow")}
    assert [c.name for c in calls if c.kind == "write"] == WRITE_TOOLS
    by_name = {c.name: c for c in calls}
    # Args are bound against the real function; the result is the fake's.
    assert by_name["crm.export_csv"].args == {"rows": SEED_SMB, "name": "smb"}
    assert by_name["crm.export_csv"].result == "<not written: smb.csv>"
    assert by_name["crm.notify_owner"].result == {"stood_in": True}
    assert by_name["crm.delete_stale"].result == 0
    # Values JSON cannot hold are stored as their repr, which a replay cannot revive.
    assert {k: set(v) for k, v in cast(dict, by_name["crm.score_account"].result).items()} == {
        "score": {REPR_KEY},
        "scored_at": {REPR_KEY},
    }


@pytest.mark.parametrize("mode", MODES)
def test_a_credential_in_a_tool_s_args_is_stored_as_its_placeholder(run_workflow, mode):
    """A read tool's DSN argument is stored with only its key redacted.

    The "nothing unredacted on disk" invariant holds the whole home to it too."""
    shadow = run_workflow(W, "enrich", mode)
    [connect] = [c for c in tool_calls(shadow) if c.name == "crm.connect_with"]
    dsn = cast(dict, connect.args)["dsn"]
    assert CANARIES["STRIPE_API_KEY"] not in dsn
    assert dsn.startswith("postgres://crm:<redacted:") and dsn.endswith(">@db.internal/crm")
    assert connect.result == {"connected": "db.internal/crm"}


@pytest.mark.parametrize("mode", MODES)
def test_a_read_tool_after_a_stood_in_write_sees_the_old_row(run_workflow, mode):
    # No tool overlay exists, so under shadow the read tool shows the old row: a gap pinned on
    # purpose. The unlabelled write is real in every mode, so the second read shows `last_seen`.
    bare = run_workflow(W, "read_after_write", "bare")
    assert bare.result()["saw_rename"] is True
    shadow = run_workflow(W, "read_after_write", mode)
    assert shadow.result()["saw_rename"] is False
    assert (
        shadow.result()["last_seen"] == bare.result()["last_seen"] == ["2026-01-01", "2026-09-28"]
    )
    assert rows(shadow) == TOUCHED


@pytest.mark.parametrize("mode", ["bare", *MODES])
def test_a_stale_irimi_replay_run_asks_no_recording_but_where_irimi_cannot_remove_it(
    run_workflow, mode
):
    """A stale exported `IRIMI_REPLAY_RUN` makes no SDK ask a recording, in any mode.

    `irimi shadow` removes it from its child. A bare agent's SDK is inert. A serve agent keeps it,
    but serve sets no `IRIMI_RUN`, and the SDK adopts a replay's run only when irimi names both."""
    result = run_workflow(W, "read_after_write", mode)
    expected = None if mode == "shadow" else STALE_REPLAY_RUN
    assert [start["replay"] for start in result.events("start")] == [expected]
    assert result.tools("read") == [("crm.find_accounts", "real")] * 2
    if mode != "bare":  # bare, the one warning is the real write's (`test_control_endpoint.py`)
        assert result.events("log") == []
        # The run is the SDK's own, started and stored, never the stale id adopted.
        [start] = result.events("run.start")
        assert start["run"] != STALE_REPLAY_RUN
        assert [(r.run_id, r.attribution) for r in result.sdk_runs()] == [(start["run"], "sdk")]


@pytest.mark.parametrize("mode", MODES)
def test_an_unlabelled_database_write_happens_for_real_under_shadow(run_workflow, mode):
    # irimi cannot see a write that is neither HTTP nor an @sdk.write_tool. A future check that
    # flags a production database URL in the agent's environment would flip this assertion.
    shadow = run_workflow(W, "unlabeled_write", mode)
    assert rows(shadow)[0] == ("acc_1", "Acme", "smb", "2026-09-28", 0, None)
    assert shadow.exchange_lines() == []
    assert rows(run_workflow(W, "unlabeled_write", "bare")) == rows(shadow)


@pytest.mark.parametrize("mode", MODES)
def test_a_raising_read_tool_ends_the_run_in_error_under_shadow(run_workflow, mode):
    shadow = run_workflow(W, "tool_raises", mode)
    bare = run_workflow(W, "tool_raises", "bare")
    assert shadow.exit_code == bare.exit_code == 1
    for result in (shadow, bare):
        assert [e["key"] for e in result.events("lookup_failed")] == ["'acc_1'"]
    ends = shadow.events("run.end")
    assert [(e["outcome"], e["error"]) for e in ends] == [("error", "KeyError")]
    # Stored under the exception's full name.
    [run] = shadow.sdk_runs()
    assert (run.end.outcome, run.end.error) == (
        "error",
        ErrorInfo("builtins.KeyError", "'acc_missing'"),
    )
    # Every tool that raised is recorded with its error and no result, caught or not.
    assert [(c.name, c.result, c.error) for c in tool_calls(shadow)] == [
        ("crm.connect_with", None, connect_error(shadow)),
        ("crm.flaky_lookup", None, ErrorInfo("builtins.KeyError", "'acc_1'")),
        ("crm.score_account", None, ErrorInfo("builtins.KeyError", "'acc_missing'")),
    ]
    assert shadow.tool_call_lines() == [
        "tool-real     read      crm.connect_with -> raised builtins.ConnectionError",
        "tool-real     read      crm.flaky_lookup -> raised builtins.KeyError",
        "tool-real     read      crm.score_account -> raised builtins.KeyError",
    ]
    # Bare, the SDK is inactive: no run is opened at all.
    assert bare.events("run.start") == []


def connect_error(shadow) -> ErrorInfo:
    """Return the stored error `connect_with` raised in `tool_raises`, its key a whole placeholder.

    The message quotes the key across the 1000-character cut; the store redacts before it cuts."""
    key = CANARIES["STRIPE_API_KEY"]
    said = f"{CONNECT_RETRIES}could not connect with postgres://crm:{key}@{UNREACHABLE_HOST}/crm"
    assert said.index(key) < MAX_ERROR_MESSAGE < said.index(key) + len(key)
    hidden = redact.placeholder(redact.load_key(shadow.home), key)
    return ErrorInfo("builtins.ConnectionError", said.replace(key, hidden)[:MAX_ERROR_MESSAGE])


@pytest.mark.parametrize("mode", MODES)
def test_an_sdk_error_quoting_a_key_across_the_cut_is_stored_as_its_placeholder(run_workflow, mode):
    """An error message quoting a key across the cut is stored with the key's own placeholder.

    The store redacts the whole message, then cuts it, so it keeps the placeholder the args hold and
    no part of the key. The agent caught the error and ran on, in every mode."""
    shadow = run_workflow(W, "tool_raises", mode)
    [connect] = [c for c in tool_calls(shadow) if c.name == "crm.connect_with"]
    hidden = redact.placeholder(redact.load_key(shadow.home), CANARIES["STRIPE_API_KEY"])
    assert connect.error == connect_error(shadow)
    assert f"could not connect with postgres://crm:{hidden}@" in connect.error.message
    assert cast(dict, connect.args)["dsn"] == f"postgres://crm:{hidden}@{UNREACHABLE_HOST}/crm"
    for result in (shadow, run_workflow(W, "tool_raises", "bare")):
        assert result.events("connect_failed", with_time=False) == [
            {"event": "connect_failed", "error": "ConnectionError"}
        ]


@pytest.mark.parametrize("mode", MODES)
def test_a_repr_quoting_a_key_across_the_sdks_cut_keeps_no_part_of_it(run_workflow, mode):
    """A captured repr quoting a key across the SDK's cut is cut where the key starts, and marked.

    The SDK cuts before irimi redacts, and a key's first characters no longer look like a key, so
    cutting at 1000 would put them on disk. The store's copy is the SDK's, the mark for the key."""
    shadow = run_workflow(W, "tool_raises", mode)
    [connect] = [c for c in tool_calls(shadow) if c.name == "crm.connect_with"]
    key = CANARIES["OPENAI_API_KEY"]
    said = repr(DriverOptions(key))
    assert said.index(key) < MAX_REPR < said.index(key) + len(key)
    kept = said[: said.index(key)] + CUT_MARK
    assert cast(dict, connect.args)["options"] == {REPR_KEY: kept}
    assert kept.endswith("', password='<redacted>") and len(kept) <= MAX_REPR
    on_disk = [p.read_bytes() for p in shadow.store_root().rglob("*") if p.is_file()]
    assert not any(key[:12].encode() in data for data in on_disk)
    assert check_invariants(shadow) == []


@pytest.mark.parametrize("mode", MODES)
def test_each_decoration_time_mistake_raises_type_error(run_workflow, mode):
    shadow = run_workflow(W, "decoration_errors", mode)
    assert shadow.result()["raised"] == [
        "write_without_fake",
        "read_with_fake",
        "bare_write_tool",
        "fake_not_callable",
        "sync_fake_for_async",
        "async_fake_for_sync",
        "generator",
        "generator_fake",
        "fake_is_the_function",
    ]
    assert (
        run_workflow(W, "decoration_errors", "bare").result()["raised"]
        == (shadow.result()["raised"])
    )


@pytest.mark.parametrize("mode", MODES)
def test_a_write_stand_in_that_raises_hands_the_agent_its_own_exception_and_a_retry_runs(
    run_workflow,
    mode,
):
    """A fake's exception reaches the agent as the very object raised, and its retry is recorded.

    `merge_accounts` first names a missing account; its fake raises as the real merge would. Both
    calls are recorded and printed, the summary says the fake raised, and the run ends `ok`."""
    shadow = run_workflow(W, "merge_retry", mode)
    bare = run_workflow(W, "merge_retry", "bare")
    for result in (shadow, bare):
        failed = [(e["error"], e["same_object"]) for e in result.events("merge_failed")]
        assert failed == [("no such account: acc_9", True)]
    assert shadow.tools() == [("crm.merge_accounts", "shadow")] * 2
    assert bare.tools() == [("crm.merge_accounts", "real")] * 2
    assert rows(shadow) == SEED
    assert [row[0] for row in rows(bare)] == ["acc_1", "acc_3"]
    assert shadow.result()["merged"] == {"kept": "acc_1", "merged": "acc_2", "stood_in": True}
    assert bare.result()["merged"] == {"kept": "acc_1", "merged": "acc_2"}
    [run] = shadow.sdk_runs()
    assert run.end.outcome == "ok"
    # Args are bound against the real `merge_accounts(keep, drop)`, not the fake's `*ids`.
    assert [(c.run_id, c.args, c.result, c.error) for c in tool_calls(shadow)] == [
        (
            run.run_id,
            {"keep": "acc_1", "drop": "acc_9"},
            None,
            ErrorInfo("builtins.LookupError", "no such account: acc_9"),
        ),
        (
            run.run_id,
            {"keep": "acc_1", "drop": "acc_2"},
            {"kept": "acc_1", "merged": "acc_2", "stood_in": True},
            None,
        ),
    ]
    assert shadow.tool_call_lines() == [
        "tool-fake     write     crm.merge_accounts -> raised builtins.LookupError",
        "tool-fake     write     crm.merge_accounts -> ok",
    ]
    summary = shadow.summaries()
    totals = "  0 exchanges · 0 real · 0 answered by irimi (0 faked writes) · 0 sent to targets"
    assert summary[1 : summary.index(totals) + 1] == [
        "",
        "  None of these writes reached the real service:",
        "  ○ tool  crm.merge_accounts  fake raised builtins.LookupError",
        "  ○ tool  crm.merge_accounts  fake ran instead",
        "",
        totals,
    ]


def test_under_serve_a_tool_called_outside_every_run_is_unattributed(run_workflow):
    """Under serve, a tool called in no run is stored in `unattributed/`.

    Serve has no process run. The trigger's own calls are its run's, and only its faked write is in
    its summary."""
    serve = run_workflow(W, "outside_run", "serve")
    [run] = serve.sdk_runs()
    reader = serve.stored()
    assert [(c.name, c.ran, c.args) for c in reader.load_run(UNATTRIBUTED).events] == [
        ("crm.delete_stale", "shadow", {"cutoff": "2025-06-01"}),
        ("crm.find_accounts", "real", {"segment": "smb"}),
    ]
    assert [(c.name, c.ran) for c in reader.load_run(run.run_id).events] == [
        ("crm.find_accounts", "real"),
        ("crm.upsert_account", "shadow"),
    ]
    assert [line for line in serve.run_blocks()[run.run_id] if "○ tool" in line] == [
        "  ○ tool  crm.upsert_account  fake ran instead"
    ]
    assert len(serve.tool_call_lines()) == 4
    assert rows(serve) == SEED


def test_a_tool_called_outside_every_run_lands_in_the_process_run(run_workflow):
    """Under `irimi shadow`, a tool called in no run is recorded in the process run.

    That is the run `IRIMI_RUN` names; the trigger's own calls are its run's, and nothing is left
    `unattributed`."""
    shadow = run_workflow(W, "outside_run", "shadow")
    bare = run_workflow(W, "outside_run", "bare")
    [run] = shadow.sdk_runs()
    process = shadow.process_run_id()
    assert [(t["name"], t["ran"], t["run"]) for t in shadow.events("tool")] == [
        ("crm.delete_stale", "shadow", None),
        ("crm.find_accounts", "real", None),
        ("crm.find_accounts", "real", run.run_id),
        ("crm.upsert_account", "shadow", run.run_id),
    ]
    reader = shadow.stored()

    def held(run_id):
        return [(c.name, c.ran, c.args) for c in reader.load_run(run_id).events]

    assert held(process) == [
        ("crm.delete_stale", "shadow", {"cutoff": "2025-06-01"}),
        ("crm.find_accounts", "real", {"segment": "smb"}),
    ]
    assert [(name, ran) for name, ran, _ in held(run.run_id)] == [
        ("crm.find_accounts", "real"),
        ("crm.upsert_account", "shadow"),
    ]
    assert reader.load_run(UNATTRIBUTED).events == []
    assert shadow.tool_call_lines() == [
        "tool-fake     write     crm.delete_stale -> ok",
        "tool-real     read      crm.find_accounts -> ok",
        "tool-real     read      crm.find_accounts -> ok",
        "tool-fake     write     crm.upsert_account -> ok",
    ]
    assert [line for line in shadow.summary() if "○ tool" in line] == [
        "  ○ tool  crm.delete_stale  fake ran instead",
        "  ○ tool  crm.upsert_account  fake ran instead",
    ]
    # The read after the faked delete still sees the stale account: no tool overlay, as in
    # `read_after_write`. Bare, it was really deleted.
    assert [(e["stale"], e["smb"]) for e in shadow.events("housekeeping")] == [(0, 2)]
    assert [(e["stale"], e["smb"]) for e in bare.events("housekeeping")] == [(1, 1)]
    assert rows(shadow) == SEED
    assert rows(bare) == [
        ("acc_1", "Acme", "smb", "2026-01-01", 50, None),
        ("acc_3", "Initech", "enterprise", "2026-06-01", 0, None),
    ]


@pytest.mark.parametrize("mode", MODES)
def test_http_made_inside_a_write_stand_in_is_an_ordinary_exchange_of_the_run(run_workflow, mode):
    """A request a fake makes is the run's like any other, stored before the tool call.

    `sync_industry`'s fake checks the account upstream; the call and the request are not linked.
    Bare, the real body writes the row and makes no request."""
    shadow = run_workflow(W, "stand_in_http", mode)
    bare = run_workflow(W, "stand_in_http", "bare")
    [run] = shadow.sdk_runs()
    assert [(c["label"], c["status"], c["answered_by"], c["run"]) for c in shadow.calls()] == [
        ("stand_in_check", 200, None, run.run_id)
    ]
    assert shadow.exchange_lines() == [
        "real          read      GET enrich.internal/v1/companies/acc_1 -> 200"
    ]
    assert shadow.tool_call_lines() == ["tool-fake     write     crm.sync_industry -> ok"]
    events = shadow.stored().load_run(run.run_id).events
    assert [type(e).__name__ for e in events] == ["Exchange", "ToolCall"]
    assert events[0].request.path == "/v1/companies/acc_1"
    assert events[1].result == {
        "stood_in": True,
        "industry": "aerospace",
        "upstream": "manufacturing",
    }
    assert "  ○ tool  crm.sync_industry  fake ran instead" in shadow.summaries()
    assert rows(shadow) == SEED
    assert bare.calls() == []
    assert rows(bare)[0] == ("acc_1", "Acme", "smb", "2026-01-01", 0, "aerospace")


@pytest.mark.parametrize("mode", MODES)
def test_a_sync_tool_that_returns_a_coroutine_is_recorded_when_the_coroutine_ends(
    run_workflow, mode
):
    """A tool call that returns a coroutine ends, and is stored, when the coroutine ends.

    So a read the coroutine awaits is stored before it, and the stored result is what the
    coroutine returned. `lookup_owner` is an async read tool; `Outbox.send_reminder` is an async
    static method with a static-method fake, which is accepted. `last_contact`'s pending coroutine
    spans `find_accounts`, so the find is stored first and within its span."""
    shadow = run_workflow(W, "async_tools", mode)
    bare = run_workflow(W, "async_tools", "bare")
    assert shadow.tools() == [
        ("crm.lookup_owner", "real"),
        ("crm.last_contact", "real"),
        ("crm.find_accounts", "real"),
        ("crm.schedule_followup", "shadow"),
        ("crm.lookup_owner", "real"),
        ("crm.send_reminder", "shadow"),
    ]
    [run] = shadow.sdk_runs()
    first, found, contact, inner, schedule, remind = shadow.stored().load_run(run.run_id).events
    calls = (first, found, contact, inner, schedule, remind)
    assert [(c.name, c.ran, c.awaited) for c in calls] == [
        ("crm.lookup_owner", "real", False),
        ("crm.find_accounts", "real", False),
        ("crm.last_contact", "real", True),
        ("crm.lookup_owner", "real", False),
        ("crm.schedule_followup", "shadow", True),
        ("crm.send_reminder", "shadow", False),
    ]
    # `last_contact` is stored with what the coroutine returned, and `find_accounts` within it.
    assert contact.result == {"id": "acc_1", "last_seen": "2026-01-01"}
    assert contact.started_at <= found.started_at <= found.ended_at <= contact.ended_at
    assert schedule.args == {"account_id": "acc_1", "days": 7}
    owner = "owner@acme.example"
    assert schedule.result == {"stood_in": True, "owner": owner, "in_days": 7}
    assert inner.result == first.result == {"id": "acc_1", "owner": owner}
    assert schedule.started_at <= inner.started_at <= inner.ended_at <= schedule.ended_at
    assert (remind.args, remind.result) == (
        {"account_id": "acc_1", "owner": owner},
        {"stood_in": True, "to": owner},
    )
    assert shadow.tool_call_lines() == [
        "tool-real     read      crm.lookup_owner -> ok",
        "tool-real     read      crm.find_accounts -> ok",
        "tool-real     read      crm.last_contact -> ok",
        "tool-real     read      crm.lookup_owner -> ok",
        "tool-fake     write     crm.schedule_followup -> ok",
        "tool-fake     write     crm.send_reminder -> ok",
    ]
    assert [line for line in shadow.summaries() if "○ tool" in line] == [
        "  ○ tool  crm.schedule_followup  fake ran instead",
        "  ○ tool  crm.send_reminder  fake ran instead",
    ]
    assert shadow.result()["scheduled"] == {"stood_in": True, "owner": owner, "in_days": 7}
    assert shadow.result()["reminded"] == {"stood_in": True, "to": owner}
    assert shadow.result()["last_seen"] == bare.result()["last_seen"] == "2026-01-01"
    assert shadow.result()["segment"] == bare.result()["segment"] == ["acc_1", "acc_2"]
    assert files(shadow) == ["crm.sqlite3"]
    assert bare.result()["scheduled"] == {"scheduled": "acc_1", "owner": owner, "in_days": 7}
    assert bare.result()["reminded"] == {"sent": "acc_1", "to": owner}
    assert files(bare) == ["crm.sqlite3", "followups.jsonl", "outbox.jsonl"]


# ---------------------------------------------------------------------- irimi compare

ENRICH = "enrich_accounts  tests.e2e.workflows.agents.w06_crm_db_agent.agent:enrich_accounts"


@pytest.mark.parametrize("mode", MODES)
def test_irimi_compare_shows_the_retry_s_failed_merge_as_the_one_it_added(shared_store_run, mode):
    """`irimi compare` pairs identical tool calls before it pairs by name.

    `merge_retry` adds a raising call of the same tool `merge_once` makes, so pairing by name alone
    would call the successful merge changed. B runs under shadow or serve."""
    once = shared_store_run(W, "merge_once")
    retry = shared_store_run(W, "merge_retry", mode)
    assert check_invariants(retry) == []
    code, out, err = compare(once, retry)
    a, b = (result.one("run.start")["run"] for result in (once, retry))
    assert (code, err) == (1, [])
    assert out == [
        f"irimi compare · {a} vs {b}",
        f"  A  {ENRICH}",
        f"  B  {ENRICH}",
        "  (the triggers' args differ)",
        "",
        "  + ○ tool  crm.merge_accounts  fake raised builtins.LookupError",
        "  = ○ tool  crm.merge_accounts  fake ran instead",
        "",
        "  1 same · 0 changed · 0 only in A · 1 only in B",
    ]
    assert rows_not_shown(retry, a, b, out) == []


@pytest.mark.parametrize("mode", MODES)
def test_two_writes_at_once_are_compared_in_the_order_they_ended(run_workflow, mode):
    """The summary and `irimi compare` order overlapping writes by when they ended (`seq`).

    In `overlapping_writes` the archive starts first and ends after the flag."""
    shadow = run_workflow(W, "overlapping_writes", mode)
    bare = run_workflow(W, "overlapping_writes", "bare")
    assert bare.result()["flagged"] == {"flagged": True}
    assert bare.result()["archived"] == {"archived": True}
    flag, archive = tool_calls(shadow)
    assert (flag.name, archive.name) == ("crm.flag_account", "crm.archive_account")
    assert archive.started_at < flag.started_at <= flag.ended_at <= archive.ended_at
    [run] = shadow.sdk_runs()
    rows = [
        "  = ○ tool  crm.flag_account  fake ran instead",
        "  = ○ tool  crm.archive_account  fake ran instead",
    ]
    assert shadow.compare_runs(run.run_id, run.run_id) == (
        0,
        [
            f"irimi compare · {run.run_id} vs {run.run_id}",
            f"  A  {ENRICH}",
            f"  B  {ENRICH}",
            "",
            *rows,
            "",
            "  2 same · 0 changed · 0 only in A · 0 only in B",
        ],
        [],
    )
    assert [line for line in shadow.summaries() if "○ tool" in line] == [f"  {r[4:]}" for r in rows]


# ------------------------------------------------------------------------------- replay


def replayed_calls(replay) -> list[ToolCall]:
    """Return the tool calls the replay's own engine stored, in `seq` order, not the recording's."""
    return [e for e in replay.handed_events if isinstance(e, ToolCall)]


def test_enrich_replayed_answers_its_read_tools_from_the_recording(replayed):
    """A replay answers each recorded read tool from the recording; its real body never runs.

    `score_account`'s result holds reprs that do not revive, so it runs for real: the one miss.
    Every write runs its fake, and nothing reaches the fake internet. The trigger is called alone,
    so `main()` never seeds the database, which no tool needs."""
    recorded, replay = replayed(W, "enrich")
    assert replay.exit_code == recorded.exit_code == replay.agent_exit_code() == 0
    assert [name for name, _ in replay.tools()] == [name for name, _ in recorded.tools()]
    per_account = [
        ("crm.fetch_enrichment", "recording"),
        ("crm.score_account", "real"),
        ("crm.upsert_account", "shadow"),
    ]
    assert replay.tools() == [
        ("crm.connect_with", "recording"),
        ("crm.find_accounts", "recording"),
        *per_account,
        *per_account,
        ("crm.bulk_update", "shadow"),
        ("crm.delete_stale", "shadow"),
        ("crm.export_csv", "shadow"),
        ("crm.notify_owner", "shadow"),
    ]
    assert [(c.name, c.ran) for c in replayed_calls(replay)] == replay.tools()
    assert replay.internet.requests() == []
    assert replay.exchange_lines() == []
    # The agent got what it got the first time, the revived values and the real ones alike.
    assert replay.events("scored", with_time=False) == recorded.events("scored", with_time=False)
    assert replay.returned() == recorded.returned()
    assert not (replay.state / DB_NAME).exists()


def test_enrich_replayed_where_no_lookup_reaches_irimi_runs_each_read_for_real(replayed):
    """A replay whose lookups cannot reach irimi runs each read tool for real and warns once.

    `agentkit.lookup_gateway` refuses the lookup route and forwards every post. The agent ends as
    recorded, and `fetch_enrichment`'s GET is answered from the recording. Replayed by its command,
    since the gateway and the logger are set up in the agent's `main()`."""
    recorded, replay = replayed(W, "enrich", {LOOKUP_REFUSED_ENV: "1"}, command=True)
    assert replay.exit_code == recorded.exit_code == replay.agent_exit_code() == 0
    assert replay.result(with_time=False) == recorded.result(with_time=False)
    assert replay.tools("read") == [(name, "real") for name, _ in recorded.tools("read")]
    assert len(replay.tools("read")) == 6
    assert replay.tools("write") == recorded.tools("write")
    assert [(c.name, c.ran) for c in replayed_calls(replay)] == replay.tools()
    [warned] = replay.events("log", with_time=False)
    assert (warned["logger"], warned["level"]) == ("irimi.sdk", "WARNING")
    assert warned["message"].startswith(
        "irimi: could not look up a recorded call of tool crm.connect_with in run "
    )
    assert "(rejected: HTTP 502: " in warned["message"]
    assert warned["message"].endswith(
        "; the tool runs for real; further failures of this kind are not logged"
    )
    assert replay.exchange_lines() == [
        f"recorded      read      GET enrich.internal/v1/companies/{account} -> 200  "
        "[from-recording]"
        for account in ("acc_1", "acc_2")
    ]
    assert replay.internet.requests() == []
    assert rows(replay) == SEED


def test_a_credential_in_a_replayed_tool_s_args_matches_its_placeholder(replayed):
    """A replayed tool's live args match the recording only once redacted with the store's key."""
    recorded, replay = replayed(W, "enrich")
    [stored] = [c for c in tool_calls(recorded) if c.name == "crm.connect_with"]
    assert CANARIES["STRIPE_API_KEY"] not in cast(dict, stored.args)["dsn"]
    [again] = [
        c for c in tool_calls(replay) if (c.name, c.ran) == ("crm.connect_with", "recording")
    ]
    assert (again.args, again.result) == (stored.args, stored.result)


def test_read_after_write_replayed_matches_both_reads_in_order(replayed):
    """Two replayed calls of one read tool take the two recorded results in recorded order.

    So the replay shows the old row, as shadow did. Replayed by its command, so the unlabelled write
    runs for real against the database the agent's `main()` creates."""
    recorded, replay = replayed(W, "read_after_write", command=True)
    assert replay.tools() == [
        ("crm.find_accounts", "recording"),
        ("crm.upsert_account", "shadow"),
        ("crm.find_accounts", "recording"),
    ]
    assert replay.result()["saw_rename"] is recorded.result()["saw_rename"] is False
    # The unlabelled write made the two recorded results differ.
    assert replay.result()["last_seen"] == recorded.result()["last_seen"]
    assert recorded.result()["last_seen"] == ["2026-01-01", "2026-09-28"]
    first, second = (c for c in replayed_calls(replay) if c.name == "crm.find_accounts")
    assert [
        cast(list, first.result)[0]["last_seen"],
        cast(list, second.result)[0]["last_seen"],
    ] == [
        "2026-01-01",
        "2026-09-28",
    ]
    # The stale `IRIMI_REPLAY_RUN` the scenario exported is irimi's own run in a replay.
    assert [start["replay"] for start in replay.events("start")] == [replay.process_run_id()]


def test_tool_raises_replayed_raises_what_was_recorded_and_the_agent_catches_it_again(replayed):
    """A recorded tool error is raised again as a `RecordedToolError` the agent's handler catches.

    It subclasses `KeyError` and is named `KeyError`. The uncaught one ends the run where it ended,
    and the replay child exits 1 as the agent did."""
    recorded, replay = replayed(W, "tool_raises")
    assert replay.agent_exit_code() == recorded.exit_code == 1
    assert replay.exit_code == 0
    assert replay.events("lookup_failed", with_time=False) == [
        {"event": "lookup_failed", "error": "KeyError", "key": "'acc_1'"}
    ]
    assert replay.tool_call_lines() == [
        "tool-recorded read      crm.connect_with -> raised builtins.ConnectionError",
        "tool-recorded read      crm.flaky_lookup -> raised builtins.KeyError",
        "tool-recorded read      crm.score_account -> raised builtins.KeyError",
    ]
    assert [(c.name, c.error) for c in replayed_calls(replay)] == [
        ("crm.connect_with", connect_error(recorded)),
        ("crm.flaky_lookup", ErrorInfo("builtins.KeyError", "'acc_1'")),
        ("crm.score_account", ErrorInfo("builtins.KeyError", "'acc_missing'")),
    ]
    # The run ends on the recorded type: `irimi replay` ends it with what the child said it raised.
    assert replay.one("run.end")["error"] == recorded.one("run.end")["error"] == "KeyError"
    [source] = recorded.sdk_runs()
    record = replay.stored().load_run(replay.process_run_id()).record
    assert (record.end.outcome, record.end.error) == (source.end.outcome, source.end.error)
    assert record.end.error == ErrorInfo("builtins.KeyError", "'acc_missing'")


def test_async_tools_replayed_answers_each_read_with_what_its_await_got(replayed):
    """A replay answers async and coroutine-returning read tools with what their await got.

    Neither real body runs, including the `lookup_owner` call inside `schedule_followup`'s fake.
    `find_accounts`, recorded within `last_contact`'s span but not called by it, is answered too."""
    recorded, replay = replayed(W, "async_tools")
    assert replay.exit_code == recorded.exit_code == replay.agent_exit_code() == 0
    assert replay.tools() == [
        ("crm.lookup_owner", "recording"),
        ("crm.last_contact", "recording"),
        ("crm.find_accounts", "recording"),
        ("crm.schedule_followup", "shadow"),
        ("crm.lookup_owner", "recording"),
        ("crm.send_reminder", "shadow"),
    ]
    calls = replayed_calls(replay)
    assert [(c.name, c.ran, c.awaited) for c in calls] == [
        ("crm.lookup_owner", "recording", False),
        ("crm.find_accounts", "recording", False),
        ("crm.last_contact", "recording", True),
        ("crm.lookup_owner", "recording", False),
        ("crm.schedule_followup", "shadow", True),
        ("crm.send_reminder", "shadow", False),
    ]
    recorded_calls = [e for e in recorded.handed_events if isinstance(e, ToolCall)]
    assert [(c.name, c.args, c.result) for c in calls] == [
        (c.name, c.args, c.result) for c in recorded_calls
    ]
    assert replay.returned() == recorded.returned()
