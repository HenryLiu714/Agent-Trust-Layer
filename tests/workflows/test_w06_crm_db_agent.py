"""W6 `crm_db_agent`: labelled database and file writes run as their stand-ins under shadow.

What a tool call did is read from the agent's observation log (`tool` events say which body ran)
and from the state directory itself: the SQLite CRM and the files the write tools would create.
What irimi made of it is read from its stream, its summary and its store: `@sdk.tool` posts every
call to the run it was made in (#76).
"""

from examples.workflows.harness.run import CANARIES
from examples.workflows.w06_crm_db_agent.agent import DB_NAME, SEED
from irimi.sdk.capture import REPR_KEY
from irimi.trace import UNATTRIBUTED, ErrorInfo, ToolCall

W = "w06_crm_db_agent"
WRITE_TOOLS = [
    "crm.upsert_account",
    "crm.upsert_account",
    "crm.bulk_update",
    "crm.delete_stale",
    "crm.export_csv",
    "crm.notify_owner",
]
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


def rows(result):
    return result.query(DB_NAME, "SELECT * FROM accounts ORDER BY id")


def tool_calls(result) -> list[ToolCall]:
    """The tool calls irimi stored, in the order the run made them: one run holds them all."""
    return [e for e in result.stored_events() if isinstance(e, ToolCall)]


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
    # Each exchange is in the run the tool was called in, with no link to the call (#76), and
    # neither count holds a tool call.
    [run] = shadow.sdk_runs()
    stored = shadow.stored().load_run(run.run_id).events
    assert [e.request.path for e in stored if not isinstance(e, ToolCall)] == [
        "/v1/companies/acc_1",
        "/v1/companies/acc_2",
    ]
    assert "  2 exchanges · 2 live · 0 delegated · 0 virtualized" in shadow.summary()


def test_irimi_prints_every_tool_call_and_a_summary_line_for_each_stood_in_write(run_workflow):
    """In the stream, one line per call in `exchange_line`'s columns, among the exchanges; in the
    summary, one line per write tool whose stand-in ran, after the writes, and none for a read.
    The run's only writes are tool writes, and the summary still says they did not happen."""
    shadow = run_workflow(W, "enrich", "shadow")
    assert shadow.tool_call_lines() == [
        "real      read      tool crm.connect_with -> ok",
        "real      read      tool crm.find_accounts -> ok",
        "real      read      tool crm.fetch_enrichment -> ok",
        "real      read      tool crm.score_account -> ok",
        "shadow    write     tool crm.upsert_account -> ok",
        "real      read      tool crm.fetch_enrichment -> ok",
        "real      read      tool crm.score_account -> ok",
        "shadow    write     tool crm.upsert_account -> ok",
        "shadow    write     tool crm.bulk_update -> ok",
        "shadow    write     tool crm.delete_stale -> ok",
        "shadow    write     tool crm.export_csv -> ok",
        "shadow    write     tool crm.notify_owner -> ok",
    ]
    summary = shadow.summary()
    assert summary[summary.index("  enrich.internal  2 reads") + 1 :] == [
        "",
        *[f"  ○ tool {name}  shadow stand-in ran" for name in WRITE_TOOLS],
        "",
        "  2 exchanges · 2 live · 0 delegated · 0 virtualized",
        "  These writes did not happen.",
    ]


def test_every_tool_call_is_stored_in_its_run_with_what_ran_and_what_it_returned(run_workflow):
    shadow = run_workflow(W, "enrich", "shadow")
    [run] = shadow.sdk_runs()
    calls = tool_calls(shadow)
    assert {call.run_id for call in calls} == {run.run_id}
    # What the run's own log says ran is what irimi stored, call for call.
    assert [(c.name, c.ran) for c in calls] == shadow.tools()
    assert {(c.kind, c.ran) for c in calls} == {("read", "real"), ("write", "shadow")}
    assert [c.name for c in calls if c.kind == "write"] == WRITE_TOOLS
    by_name = {c.name: c for c in calls}
    # Args are bound against the real function; the result is the stand-in's.
    assert by_name["crm.export_csv"].args == {"rows": SEED_SMB, "name": "smb"}
    assert by_name["crm.export_csv"].result == "<not written: smb.csv>"
    assert by_name["crm.notify_owner"].result == {"stood_in": True}
    assert by_name["crm.delete_stale"].result == 0
    # Values JSON cannot hold are stored as their repr, which a replay cannot revive (#83).
    assert {k: set(v) for k, v in by_name["crm.score_account"].result.items()} == {
        "score": {REPR_KEY},
        "scored_at": {REPR_KEY},
    }


def test_a_credential_in_a_tool_s_args_is_stored_as_its_placeholder(run_workflow):
    """`connect_with` is a read tool given a DSN that carries a key: its stored args are redacted,
    and only the key is (#69, #70). Universal invariant 4 holds the whole home to it too."""
    shadow = run_workflow(W, "enrich", "shadow")
    [connect] = [c for c in tool_calls(shadow) if c.name == "crm.connect_with"]
    dsn = connect.args["dsn"]
    assert CANARIES["STRIPE_API_KEY"] not in dsn
    assert dsn.startswith("postgres://crm:<redacted:") and dsn.endswith(">@db.internal/crm")
    assert connect.result == {"connected": "db.internal/crm"}


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
    # Both tools that raised are recorded with what they raised and no result, the caught one and
    # the one that ended the run (#76).
    assert [(c.name, c.result, c.error) for c in tool_calls(shadow)] == [
        ("crm.flaky_lookup", None, ErrorInfo("builtins.KeyError", "'acc_1'")),
        ("crm.score_account", None, ErrorInfo("builtins.KeyError", "'acc_missing'")),
    ]
    assert shadow.tool_call_lines() == [
        "real      read      tool crm.flaky_lookup -> raised builtins.KeyError",
        "real      read      tool crm.score_account -> raised builtins.KeyError",
    ]
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
        "generator_stand_in",
        "stand_in_is_the_function",
    ]
    assert (
        run_workflow(W, "decoration_errors", "bare").result()["raised"]
        == (shadow.result()["raised"])
    )


def test_a_write_stand_in_that_raises_hands_the_agent_its_own_exception_and_a_retry_runs(
    run_workflow,
):
    """`merge_accounts` first names an account that does not exist. Its stand-in validates as the
    real merge does and raises; the SDK records the call with its error and re-raises the very
    object the stand-in raised, so the agent catches it and retries with the right account (#76).
    Both calls are recorded and printed, the first with what it raised; the summary says the
    stand-in raised, and the run, which caught it, ends `ok`."""
    shadow = run_workflow(W, "merge_retry", "shadow")
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
    assert run.outcome == "ok"
    # Args are bound against the real `merge_accounts(keep, drop)`, not the stand-in's `*ids`.
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
        "shadow    write     tool crm.merge_accounts -> raised builtins.LookupError",
        "shadow    write     tool crm.merge_accounts -> ok",
    ]
    assert shadow.summary()[1:] == [
        "",
        "  ○ tool crm.merge_accounts  shadow stand-in raised builtins.LookupError",
        "  ○ tool crm.merge_accounts  shadow stand-in ran",
        "",
        "  0 exchanges · 0 live · 0 delegated · 0 virtualized",
        "  These writes did not happen.",
    ]


def test_a_tool_called_outside_every_run_lands_in_the_process_run(run_workflow):
    """Housekeeping before the trigger: `delete_stale` and `find_accounts` are called in no run,
    so under `irimi shadow -- <cmd>` each is recorded in the process run that `IRIMI_RUN` names,
    and printed like any other (#76). The trigger's own calls are its run's. Nothing is left
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
        "shadow    write     tool crm.delete_stale -> ok",
        "real      read      tool crm.find_accounts -> ok",
        "real      read      tool crm.find_accounts -> ok",
        "shadow    write     tool crm.upsert_account -> ok",
    ]
    assert [line for line in shadow.summary() if "○ tool" in line] == [
        "  ○ tool crm.delete_stale  shadow stand-in ran",
        "  ○ tool crm.upsert_account  shadow stand-in ran",
    ]
    # The read after the stood-in delete still sees the stale account: no tool overlay, as in
    # `read_after_write`. Bare, it was really deleted.
    assert [(e["stale"], e["smb"]) for e in shadow.events("housekeeping")] == [(0, 2)]
    assert [(e["stale"], e["smb"]) for e in bare.events("housekeeping")] == [(1, 1)]
    assert rows(shadow) == SEED
    assert rows(bare) == [
        ("acc_1", "Acme", "smb", "2026-01-01", 50, None),
        ("acc_3", "Initech", "enterprise", "2026-06-01", 0, None),
    ]


def test_http_made_inside_a_write_stand_in_is_an_ordinary_exchange_of_the_run(run_workflow):
    """`sync_industry`'s stand-in checks the account upstream before saying what it would have
    written. Its request is the run's like any other: labelled by the SDK, forwarded, printed and
    stored in the run, before the tool call, which is posted once the stand-in returned, with no
    link between them (#76). Bare, the real body writes the row and makes no request at all."""
    shadow = run_workflow(W, "stand_in_http", "shadow")
    bare = run_workflow(W, "stand_in_http", "bare")
    [run] = shadow.sdk_runs()
    assert [(c["label"], c["status"], c["answered_by"], c["run"]) for c in shadow.calls()] == [
        ("stand_in_check", 200, None, run.run_id)
    ]
    assert shadow.exchange_lines() == [
        "live      read      GET enrich.internal/v1/companies/acc_1 -> 200"
    ]
    assert shadow.tool_call_lines() == ["shadow    write     tool crm.sync_industry -> ok"]
    events = shadow.stored().load_run(run.run_id).events
    assert [type(e).__name__ for e in events] == ["Exchange", "ToolCall"]
    assert events[0].request.path == "/v1/companies/acc_1"
    assert events[1].result == {
        "stood_in": True,
        "industry": "aerospace",
        "upstream": "manufacturing",
    }
    assert "  ○ tool crm.sync_industry  shadow stand-in ran" in shadow.summary()
    assert rows(shadow) == SEED
    assert bare.calls() == []
    assert rows(bare)[0] == ("acc_1", "Acme", "smb", "2026-01-01", 0, "aerospace")


def test_a_sync_tool_that_returns_a_coroutine_is_recorded_when_the_coroutine_ends(run_workflow):
    """`schedule_followup` is sync and returns its work as a coroutine; its stand-in does the same.
    The call ends when the coroutine does (#76), so the read tool the coroutine awaits is stored
    before it, though the agent called it after, and the stored result is what the coroutine
    returned, not the coroutine. `lookup_owner` is an async read tool, awaited in the run's event
    loop. Last, `Outbox.send_reminder`, an async static method whose stand-in is a static method
    in the same class body: judged as the async function inside it, so accepted, and run in the
    write's place (#76)."""
    shadow = run_workflow(W, "async_tools", "shadow")
    bare = run_workflow(W, "async_tools", "bare")
    assert shadow.tools() == [
        ("crm.lookup_owner", "real"),
        ("crm.schedule_followup", "shadow"),
        ("crm.lookup_owner", "real"),
        ("crm.send_reminder", "shadow"),
    ]
    [run] = shadow.sdk_runs()
    first, inner, schedule, remind = shadow.stored().load_run(run.run_id).events
    assert [(c.name, c.ran) for c in (first, inner, schedule, remind)] == [
        ("crm.lookup_owner", "real"),
        ("crm.lookup_owner", "real"),
        ("crm.schedule_followup", "shadow"),
        ("crm.send_reminder", "shadow"),
    ]
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
        "real      read      tool crm.lookup_owner -> ok",
        "real      read      tool crm.lookup_owner -> ok",
        "shadow    write     tool crm.schedule_followup -> ok",
        "shadow    write     tool crm.send_reminder -> ok",
    ]
    assert [line for line in shadow.summary() if "○ tool" in line] == [
        "  ○ tool crm.schedule_followup  shadow stand-in ran",
        "  ○ tool crm.send_reminder  shadow stand-in ran",
    ]
    assert shadow.result()["scheduled"] == {"stood_in": True, "owner": owner, "in_days": 7}
    assert shadow.result()["reminded"] == {"stood_in": True, "to": owner}
    assert files(shadow) == ["crm.sqlite3"]
    assert bare.result()["scheduled"] == {"scheduled": "acc_1", "owner": owner, "in_days": 7}
    assert bare.result()["reminded"] == {"sent": "acc_1", "to": owner}
    assert files(bare) == ["crm.sqlite3", "followups.jsonl", "outbox.jsonl"]
