"""W6 `crm_db_agent`: labelled database and file writes run as their stand-ins under shadow.

What a tool call did is read from the agent's observation log (`tool` events say which body ran)
and from the state directory itself: the SQLite CRM and the files the write tools would create.
irimi does not see tool calls yet; the assertions on its output flip when #76 records them.
"""

from examples.workflows.w06_crm_db_agent.agent import DB_NAME, SEED
from irimi.trace import ErrorInfo

W = "w06_crm_db_agent"
WRITE_TOOLS = [
    "crm.upsert_account",
    "crm.upsert_account",
    "crm.bulk_update",
    "crm.delete_stale",
    "crm.export_csv",
    "crm.notify_owner",
]


def rows(result):
    return result.query(DB_NAME, "SELECT * FROM accounts ORDER BY id")


def files(result):
    return sorted(str(p.relative_to(result.state)) for p in result.state.rglob("*") if p.is_file())


def test_under_shadow_every_write_tool_runs_its_stand_in_and_the_crm_is_as_seeded(run_workflow):
    shadow = run_workflow(W, "enrich", "shadow")
    assert shadow.tools("write") == [(name, "shadow") for name in WRITE_TOOLS]
    assert rows(shadow) == SEED
    assert files(shadow) == ["crm.sqlite3"]
    # The stand-ins' answers are what the agent reported, not the real bodies'.
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


def test_read_tools_run_for_real_in_both_modes_and_the_call_sequence_is_the_same(run_workflow):
    bare = run_workflow(W, "enrich", "bare")
    shadow = run_workflow(W, "enrich", "shadow")
    assert [n for n, _ in bare.tools()] == [n for n, _ in shadow.tools()]
    assert {ran for _, ran in shadow.tools("read")} == {"real"}
    # Decimal and datetime came back from the read tool as themselves: not JSON, not revivable
    # once recorded, so a replay (#83) must run `score_account` for real.
    assert {tuple(e["types"]) for e in shadow.events("scored")} == {("Decimal", "datetime")}


def test_http_made_inside_a_read_tool_is_an_ordinary_exchange_of_the_run(run_workflow):
    shadow = run_workflow(W, "enrich", "shadow")
    assert shadow.exchange_lines() == [
        "live      read      GET enrich.internal/v1/companies/acc_1 -> 200",
        "live      read      GET enrich.internal/v1/companies/acc_2 -> 200",
    ]
    # Six write tools ran as stand-ins and irimi's summary says nothing about them. When #76
    # lands it prints one `○ tool <name>  shadow stand-in ran` line per write tool call.
    summary = "\n".join(shadow.summary())
    assert "crm." not in summary
    assert "2 exchanges · 2 live · 0 delegated · 0 virtualized" in summary


def test_a_read_tool_after_a_stood_in_write_sees_the_old_row(run_workflow):
    # No tool overlay exists: the stand-in wrote nothing, so the read tool shows the old row
    # under shadow while bare shows the rename. This is the gap, pinned on purpose.
    assert run_workflow(W, "read_after_write", "bare").result()["saw_rename"] is True
    shadow = run_workflow(W, "read_after_write", "shadow")
    assert shadow.result()["saw_rename"] is False
    assert rows(shadow) == SEED


def test_an_unlabelled_database_write_happens_for_real_under_shadow(run_workflow):
    # irimi cannot see a write that is neither HTTP nor an @sdk.tool. The case for Phase 4's
    # "production database URL" readiness check; this assertion flips when that check lands.
    shadow = run_workflow(W, "unlabeled_write", "shadow")
    assert rows(shadow)[0] == ("acc_1", "Acme", "smb", "2026-09-28", 0, None)
    assert shadow.exchange_lines() == []
    assert rows(run_workflow(W, "unlabeled_write", "bare")) == rows(shadow)


def test_a_raising_read_tool_ends_the_run_in_error_under_shadow(run_workflow):
    shadow = run_workflow(W, "tool_raises", "shadow")
    bare = run_workflow(W, "tool_raises", "bare")
    assert shadow.exit_code == bare.exit_code == 1
    for result in (shadow, bare):
        assert [e["key"] for e in result.events("lookup_failed")] == ["'acc_1'"]
    ends = shadow.events("run.end")
    assert [(e["outcome"], e["error"]) for e in ends] == [("error", "KeyError")]
    # Stored under the exception's full name (#74).
    [run] = shadow.sdk_runs()
    assert (run.outcome, run.error) == ("error", ErrorInfo("builtins.KeyError", "'acc_missing'"))
    # Bare, the SDK is inactive: no run is opened at all.
    assert bare.events("run.start") == []


def test_each_decoration_time_mistake_raises_type_error(run_workflow):
    shadow = run_workflow(W, "decoration_errors", "shadow")
    assert shadow.result()["raised"] == [
        "write_without_shadow",
        "read_with_shadow",
        "bad_kind",
        "stand_in_not_callable",
        "sync_stand_in_for_async",
        "async_stand_in_for_sync",
        "generator",
    ]
    assert (
        run_workflow(W, "decoration_errors", "bare").result()["raised"]
        == (shadow.result()["raised"])
    )
